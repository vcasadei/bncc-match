"""Testes da captura de transcrições (backoff, throttle, bronze, silver→gold)."""

from __future__ import annotations

import json
import random
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from youtube_transcript_api import IpBlocked, NoTranscriptFound

from bncc_match.catalog.store import (
    load_transcript_to_gold,
    load_videos_to_gold,
)
from bncc_match.catalog.transcript import (
    capture_transcript,
    fetch_transcript,
    reset_transcript_throttle,
)
from bncc_match.catalog.transform import (
    make_video_transform,
    normalize_transcript_record,
    validate_transcript_record,
    validate_video_item,
)
from bncc_match.data_lake import silver, write_bronze


class FakeTranscriptApi:
    """Dublê da ``YouTubeTranscriptApi`` (``fetch``)."""

    def __init__(self, fetched: Any = None, exc: Exception | None = None) -> None:
        self._fetched = fetched
        self._exc = exc
        self.calls: list[tuple[str, list[str]]] = []

    def fetch(self, video_id: str, languages: list[str] | None = None) -> Any:
        self.calls.append((video_id, list(languages or [])))
        if self._exc is not None:
            raise self._exc
        return self._fetched


class FlakyTranscriptApi:
    """Falha ``fail_times`` vezes (erro transitório) e depois responde."""

    def __init__(self, fail_times: int, fetched: Any) -> None:
        self._fail_times = fail_times
        self._fetched = fetched
        self.calls = 0

    def fetch(self, video_id: str, languages: list[str] | None = None) -> Any:
        self.calls += 1
        if self.calls <= self._fail_times:
            raise IpBlocked(video_id)
        return self._fetched


class _FakeFetched:
    """Objeto com a forma de ``FetchedTranscript`` (iterável + idioma)."""

    def __init__(self, texts: list[str], lang: str = "pt") -> None:
        self.snippets = [SimpleNamespace(text=text) for text in texts]
        self.language_code = lang

    def __iter__(self) -> Any:
        return iter(self.snippets)


def test_transcricao_disponivel_registra_idioma() -> None:
    """Cenário 'Transcrição disponível' (task 4.1/4.4)."""
    api = FakeTranscriptApi(fetched=_FakeFetched(["Olá", "mundo"]))
    result = fetch_transcript("abc123", api=api)

    assert result.status == "transcribed"
    assert result.text == "Olá mundo"
    assert result.language == "pt"
    assert api.calls == [("abc123", ["pt", "en"])]


def test_video_sem_legendas_nao_crasha() -> None:
    """Cenário 'Vídeo sem legendas' (task 4.5): ``unavailable``, sem retry."""
    api = FakeTranscriptApi(exc=NoTranscriptFound("abc123", ["pt", "en"], []))
    result = fetch_transcript("abc123", api=api)

    assert result.status == "unavailable"
    assert result.text == ""
    assert result.language is None
    assert len(api.calls) == 1  # permanente: não retenta


def test_falha_transitoria_esgota_tentativas() -> None:
    """Erros transitórios esgotados → ``failed`` (reprocessável)."""
    sleeps: list[float] = []
    api = FakeTranscriptApi(exc=IpBlocked("abc123"))
    result = fetch_transcript(
        "abc123",
        api=api,
        max_attempts=3,
        backoff_base=1.0,
        sleep=sleeps.append,
        rng=random.Random(42),
    )

    assert result.status == "failed"
    assert len(sleeps) == 2  # espera entre tentativas (não após a última)


def test_backoff_crescente_com_jitter() -> None:
    """Backoff exponencial + jitter: delays crescentes (determinístico)."""
    sleeps: list[float] = []
    api = FlakyTranscriptApi(fail_times=2, fetched=_FakeFetched(["ok"]))
    result = fetch_transcript(
        "abc123",
        api=api,
        max_attempts=3,
        backoff_base=1.0,
        sleep=sleeps.append,
        rng=random.Random(7),
    )

    assert result.status == "transcribed"
    assert api.calls == 3
    assert len(sleeps) == 2
    assert sleeps[1] > sleeps[0]
    # 1ª espera: 2^0 * base + jitter ∈ [1, 2); 2ª: 2^1 * base + jitter ∈ [2, 3)
    assert 1.0 <= sleeps[0] < 2.0
    assert 2.0 <= sleeps[1] < 3.0


def test_throttle_intervalo_entre_chamadas() -> None:
    """Task 4.7: intervalo mínimo entre duas capturas consecutivas."""
    reset_transcript_throttle()
    api = FakeTranscriptApi(fetched=_FakeFetched(["a"]))
    t0 = time.monotonic()
    fetch_transcript("abc123", api=api, throttle_seconds=0.3)
    fetch_transcript("def456", api=api, throttle_seconds=0.3)
    elapsed = time.monotonic() - t0
    reset_transcript_throttle()

    assert elapsed >= 0.3


def test_captura_grava_bronze(lake_root: Path) -> None:
    """Task 4.2: registro bruto em JSONL, com carimbo e manifesto."""
    api = FakeTranscriptApi(fetched=_FakeFetched(["texto bruto"]))
    result = capture_transcript("abc123", api=api, root=lake_root)

    assert result.status == "transcribed"
    files = list((lake_root / "bronze" / "youtube_transcript").glob("*/*.jsonl"))
    assert len(files) == 1
    line = json.loads(files[0].read_text(encoding="utf-8").strip())
    assert line["video_id"] == "abc123"
    assert line["raw_text"] == "texto bruto"
    assert line["language"] == "pt"
    assert line["status"] == "transcribed"
    assert line["captured_at"]


def _seed_video(lake_root: Path, youtube_id: str = "abc123") -> None:
    """Semeadura mínima de um vídeo no gold (bronze → silver → gold)."""
    write_bronze(
        "youtube_videos",
        [
            json.dumps(
                {
                    "id": youtube_id,
                    "snippet": {
                        "title": "vídeo",
                        "channelTitle": "Canal",
                    },
                }
            )
        ],
        root=lake_root,
    )
    silver.transform(
        "video",
        "youtube_videos",
        make_video_transform(),
        validate_video_item,
        root=lake_root,
    )
    load_videos_to_gold(root=lake_root)


def test_transcript_silver_gold(lake_root: Path, gold_db: Path) -> None:
    """Task 4.3: bronze → silver → ``transcript`` no gold + status do vídeo."""
    _seed_video(lake_root)
    capture_transcript(
        "abc123", api=FakeTranscriptApi(fetched=_FakeFetched(["texto"])), root=lake_root
    )
    silver.transform(
        "transcript",
        "youtube_transcript",
        normalize_transcript_record,
        validate_transcript_record,
        root=lake_root,
    )
    result = load_transcript_to_gold(root=lake_root)

    assert result["transcripts"] == 1
    assert result["skipped"] == 0

    con = sqlite3.connect(gold_db)
    try:
        row = con.execute(
            "SELECT t.status, t.language, t.text"
            " FROM transcript t JOIN video v ON v.id = t.video_id"
            " WHERE v.youtube_id = ?",
            ("abc123",),
        ).fetchone()
        assert row == ("transcribed", "pt", "texto")
        status = con.execute(
            "SELECT status FROM video WHERE youtube_id = ?", ("abc123",)
        ).fetchone()
        assert status[0] == "transcribed"
    finally:
        con.close()


def test_transcript_sem_video_no_catalogo_e_pulada(
    lake_root: Path, gold_db: Path
) -> None:
    """Vídeo ausente do catálogo é pulado (contado em ``skipped``)."""
    capture_transcript(
        "inexistente",
        api=FakeTranscriptApi(fetched=_FakeFetched(["x"])),
        root=lake_root,
    )
    silver.transform(
        "transcript",
        "youtube_transcript",
        normalize_transcript_record,
        validate_transcript_record,
        root=lake_root,
    )
    result = load_transcript_to_gold(root=lake_root)
    assert result["transcripts"] == 0
    assert result["skipped"] == 1


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        ({"video_id": "abc", "raw_text": "  oi  "}, "oi"),
        ({"video_id": "abc", "raw_text": None}, ""),
        (
            {
                "video_id": "abc",
                "raw_text": "x",
                "language": "pt",
                "status": "unavailable",
            },
            "x",
        ),
    ],
)
def test_normalizacao_transcript(record: dict[str, Any], expected: Any) -> None:
    """Task 4.3: strip de whitespace e normalização de campos."""
    if "status" in record:
        row = normalize_transcript_record(record)
        assert row is not None
        assert row["status"] == "unavailable"
    row = normalize_transcript_record(record)
    assert row is not None
    assert row["text"] == expected
    assert row["video_id"] == "abc"


def test_validacao_transcript_rejeita_sem_video_id() -> None:
    assert validate_transcript_record({"video_id": ""}) is False
    assert validate_transcript_record({}) is False
    assert validate_transcript_record({"video_id": "abc"}) is True
