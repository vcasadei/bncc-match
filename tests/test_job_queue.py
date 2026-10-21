"""Testes da fila persistente de jobs (quota, retry, retomada, dedup)."""

from __future__ import annotations

import json
import random
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from util import (
    count_video_topics,
    count_videos,
    job_field,
    node_ids_by_type,
    video_field,
    write_payload,
)

import bncc_match.queue.store as qs
from bncc_match.curriculum import pipeline
from bncc_match.data_lake import write_bronze
from bncc_match.queue import (
    DEFAULT_DAILY_BUDGET,
    backoff_delay,
    enqueue_failed_transcripts,
    enqueue_topics,
    leaf_node_ids,
    list_failed_transcripts,
    process_next,
    queue_status,
    quota_status,
    run_queue,
)
from bncc_match.youtube.client import TransientYouTubeError

T0 = datetime(2026, 10, 21, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def curriculum_nodes(
    lake_root: Path,
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
    tmp_path: Path,
) -> list[int]:
    """Ingesta o currículo mínimo e retorna os IDs dos nós folha (skills)."""
    payload = write_payload(tmp_path / "bncc.json", minimal_items)
    pipeline.run_ingest(raw_file=payload)
    return node_ids_by_type(gold_db, "skill")


def _fake_search(
    video_ids: list[str],
    fail_times: int = 0,
    error: Exception | None = None,
) -> Callable[..., list[dict[str, Any]]]:
    """Busca fake: falha ``fail_times`` vezes e depois retorna os vídeos."""
    err = error if error is not None else TransientYouTubeError("http 429")
    state = {"calls": 0}

    def _search(
        query: str,
        max_results: int = 10,
        api_key: str | None = None,
        service: Any = None,
    ) -> list[dict[str, Any]]:
        state["calls"] += 1
        if state["calls"] <= fail_times:
            raise err
        return [
            {
                "youtube_id": vid,
                "title": f"vídeo {vid}",
                "channel_name": "Canal Teste",
            }
            for vid in video_ids
        ]

    return _search


def _fake_enrich() -> Callable[..., list[dict[str, Any]]]:
    """Enrich fake: grava itens brutos no bronze (como o cliente real)."""

    def _enrich(
        video_ids: list[str],
        api_key: str | None = None,
        service: Any = None,
    ) -> list[dict[str, Any]]:
        items = [
            {
                "id": vid,
                "snippet": {
                    "title": f"vídeo {vid}",
                    "channelTitle": "Canal Teste",
                    "description": f"descrição {vid}",
                    "publishedAt": "2024-01-15T08:30:00Z",
                    "thumbnails": {"default": {"url": f"https://thumb/{vid}.jpg"}},
                },
                "contentDetails": {"duration": "PT10M"},
            }
            for vid in video_ids
        ]
        if items:
            write_bronze(
                "youtube_videos",
                [json.dumps(item, ensure_ascii=False) for item in items],
            )
        return []

    return _enrich


def test_topicos_viram_itens_de_fila(
    gold_db: Path, curriculum_nodes: list[int]
) -> None:
    """Task 5.4: um item pendente por tópico; duplicados não criam extras."""
    count = enqueue_topics(curriculum_nodes)
    assert count == 3
    assert queue_status() == {"pending": 3}

    count2 = enqueue_topics(curriculum_nodes)  # mesmos tópicos de novo
    assert count2 == 3
    assert queue_status() == {"pending": 3}  # reativados, não duplicados
    assert job_field(gold_db, "search_topic", str(curriculum_nodes[0]), "attempts") == 0


def test_nos_folha_por_etapa(gold_db: Path, curriculum_nodes: list[int]) -> None:
    """Task 5.1: nós folha = os 3 skills da fixture mínima."""
    assert leaf_node_ids() == sorted(curriculum_nodes)
    assert len(leaf_node_ids(stage="fundamental")) == 1
    assert len(leaf_node_ids(stage="medio")) == 1


def test_process_next_completa_fluxo(
    lake_root: Path,
    gold_db: Path,
    curriculum_nodes: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Task 5.2: busca → bronze → silver → gold, job ``completed``."""
    monkeypatch.setattr(qs, "search_videos", _fake_search(["abc123"]))
    monkeypatch.setattr(qs, "enrich_videos", _fake_enrich())
    enqueue_topics(curriculum_nodes)

    result = process_next(daily_budget=DEFAULT_DAILY_BUDGET)

    assert result["status"] == "completed"
    assert result["videos"] == 1
    assert result["linked"] == 1
    assert "BNCC" in result["query"]
    key = str(curriculum_nodes[0])
    assert job_field(gold_db, "search_topic", key, "status") == "completed"
    assert job_field(gold_db, "search_topic", key, "quota_consumed") == 100
    assert count_videos(gold_db) == 1
    assert video_field(gold_db, "abc123", "title") == "vídeo abc123"
    assert count_video_topics(gold_db) == 1
    assert job_field(gold_db, "transcribe_video", "abc123", "status") == "pending"
    assert quota_status(daily_budget=DEFAULT_DAILY_BUDGET)["units_used"] == 100


def test_cota_200_max_2_buscas(
    lake_root: Path,
    gold_db: Path,
    curriculum_nodes: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tasks 5.3/5.6: orçamento 200 → 2 buscas; 3ª fica pendente."""
    monkeypatch.setattr(qs, "search_videos", _fake_search(["abc123"]))
    monkeypatch.setattr(qs, "enrich_videos", _fake_enrich())
    enqueue_topics(curriculum_nodes)

    r1 = process_next(daily_budget=200)
    r2 = process_next(daily_budget=200)
    r3 = process_next(daily_budget=200)

    assert r1["status"] == "completed"
    assert r2["status"] == "completed"
    assert r3["status"] == "quota_exhausted"
    assert quota_status(daily_budget=200)["units_used"] == 200
    assert quota_status(daily_budget=200)["units_remaining"] == 0

    statuses = [
        job_field(gold_db, "search_topic", str(node), "status")
        for node in curriculum_nodes
    ]
    assert statuses == ["completed", "completed", "pending"]
    # reagendado para a janela de cota seguinte (início do dia UTC seguinte)
    next_retry = job_field(
        gold_db, "search_topic", str(curriculum_nodes[2]), "next_retry_at"
    )
    assert next_retry is not None
    parsed = datetime.fromisoformat(str(next_retry))
    expected_day = (datetime.now(UTC).date() + timedelta(days=1)).isoformat()
    assert str(parsed.date()) == expected_day


def test_consumo_persistido_entre_execucoes(
    lake_root: Path,
    gold_db: Path,
    curriculum_nodes: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Task 5.7: nova execução carrega o consumo salvo do banco."""
    monkeypatch.setattr(qs, "search_videos", _fake_search(["abc123"]))
    monkeypatch.setattr(qs, "enrich_videos", _fake_enrich())
    enqueue_topics(curriculum_nodes)

    results = run_queue(max_items=10, daily_budget=200)
    assert [r["status"] for r in results] == [
        "completed",
        "completed",
        "quota_exhausted",
    ]
    # Consumo persistido no banco (não em memória).
    assert quota_status(daily_budget=200)["units_used"] == 200

    # "Novo processo": o 3º job volta a ser elegível, mas o consumo
    # salvo é carregado → cota esgota de novo.
    eligible = datetime(2026, 10, 21, 0, 0, 0).strftime("%Y-%m-%d %H:%M:%S.%f")
    con = sqlite3.connect(gold_db)
    con.execute(
        "UPDATE job_queue SET next_retry_at = ? WHERE job_type = 'search_topic'",
        (eligible,),
    )
    con.commit()
    con.close()

    fresh = process_next(daily_budget=200)
    assert fresh["status"] == "quota_exhausted"


def test_enumeracao_parcial_por_cota(
    lake_root: Path,
    gold_db: Path,
    curriculum_nodes: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Task 5.5: ``max_items`` processa um subconjunto, demais pendentes."""
    monkeypatch.setattr(qs, "search_videos", _fake_search(["abc123"]))
    monkeypatch.setattr(qs, "enrich_videos", _fake_enrich())
    enqueue_topics(curriculum_nodes)

    results = run_queue(max_items=1, daily_budget=200)

    # 1 search concluído + 2 searches pendentes + 1 transcrição pendente.
    assert [r["status"] for r in results] == ["completed"]
    assert queue_status() == {"completed": 1, "pending": 3}


def test_erro_transitorio_retentado(
    lake_root: Path,
    gold_db: Path,
    curriculum_nodes: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Task 5.8: 429 duas vezes → retry com backoff; 3ª tentativa conclui."""
    monkeypatch.setattr(qs, "search_videos", _fake_search(["abc123"], fail_times=2))
    monkeypatch.setattr(qs, "enrich_videos", _fake_enrich())
    enqueue_topics(curriculum_nodes)
    key = str(curriculum_nodes[0])

    r1 = process_next(daily_budget=DEFAULT_DAILY_BUDGET, now=T0)
    assert r1["status"] == "retry_scheduled"
    assert r1["attempts"] == 1
    assert r1["next_retry_at"] > T0  # backoff + jitter no futuro
    assert job_field(gold_db, "search_topic", key, "status") == "retry"

    r2 = process_next(
        daily_budget=DEFAULT_DAILY_BUDGET,
        now=r1["next_retry_at"] + timedelta(seconds=1),
    )
    assert r2["status"] == "retry_scheduled"
    assert r2["attempts"] == 2
    # backoff cresce: 2ª espera tem base de 60s (início ≥ T0 + 31s)
    assert r2["next_retry_at"] > T0 + timedelta(seconds=90)

    r3 = process_next(
        daily_budget=DEFAULT_DAILY_BUDGET,
        now=r2["next_retry_at"] + timedelta(seconds=1),
    )
    assert r3["status"] == "completed"
    assert job_field(gold_db, "search_topic", key, "attempts") == 3
    assert count_videos(gold_db) == 1


def test_esgotamento_dead(
    lake_root: Path,
    gold_db: Path,
    curriculum_nodes: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Task 5.9: 429 sempre → estado terminal ``dead`` após max_attempts."""
    monkeypatch.setattr(qs, "search_videos", _fake_search(["abc123"], fail_times=10**6))
    monkeypatch.setattr(qs, "enrich_videos", _fake_enrich())
    enqueue_topics(curriculum_nodes[:1])
    key = str(curriculum_nodes[0])

    now = T0
    r = process_next(daily_budget=DEFAULT_DAILY_BUDGET, now=now)
    assert r["status"] == "retry_scheduled"
    now = r["next_retry_at"] + timedelta(seconds=1)
    r = process_next(daily_budget=DEFAULT_DAILY_BUDGET, now=now)
    assert r["status"] == "retry_scheduled"
    now = r["next_retry_at"] + timedelta(seconds=1)
    r = process_next(daily_budget=DEFAULT_DAILY_BUDGET, now=now)

    assert r["status"] == "dead"
    assert job_field(gold_db, "search_topic", key, "status") == "dead"
    assert job_field(gold_db, "search_topic", key, "attempts") == 3
    assert job_field(gold_db, "search_topic", key, "last_error")
    # ``dead`` não é reprocessado
    assert (
        process_next(daily_budget=DEFAULT_DAILY_BUDGET, now=now + timedelta(hours=1))[
            "status"
        ]
        == "none_available"
    )


def test_interrupcao_retomada(
    lake_root: Path,
    gold_db: Path,
    curriculum_nodes: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Task 5.10: concluídos não são reprocessados; ``in_progress`` morto retoma."""
    monkeypatch.setattr(qs, "search_videos", _fake_search(["abc123"]))
    monkeypatch.setattr(qs, "enrich_videos", _fake_enrich())
    enqueue_topics(curriculum_nodes[:2])

    r1 = process_next(daily_budget=DEFAULT_DAILY_BUDGET, now=T0)
    assert r1["status"] == "completed"
    completed_job_id = r1["job_id"]

    # Simula crash: job 2 fica ``in_progress`` com timestamp obsoleto.
    stale = (T0 - timedelta(seconds=700)).strftime("%Y-%m-%d %H:%M:%S.%f")
    con = sqlite3.connect(gold_db)
    con.execute(
        "UPDATE job_queue SET status = 'in_progress', updated_at = ?"
        " WHERE job_type = 'search_topic' AND reference_key = ?",
        (stale, str(curriculum_nodes[1])),
    )
    con.commit()
    con.close()

    r2 = process_next(daily_budget=DEFAULT_DAILY_BUDGET, now=T0 + timedelta(hours=1))
    assert r2["status"] == "completed"
    assert r2["job_id"] != completed_job_id
    assert count_videos(gold_db) == 1  # sem duplicata
    assert count_video_topics(gold_db) == 2


def test_nodo_inexistente_vira_dead(
    gold_db: Path, curriculum_nodes: list[int], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tópico sem nó no gold → erro permanente → ``dead`` (sem loop)."""
    monkeypatch.setattr(qs, "search_videos", _fake_search(["abc123"]))
    monkeypatch.setattr(qs, "enrich_videos", _fake_enrich())
    enqueue_topics([9999])

    result = process_next(daily_budget=DEFAULT_DAILY_BUDGET)

    assert result["status"] == "dead"
    assert "9999" in (result.get("last_error") or "")


def test_transcribe_falho_reprocessavel(
    lake_root: Path,
    gold_db: Path,
    curriculum_nodes: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Task 4.6: falha de transcrição é listável e reprocessável, sem duplicar."""
    monkeypatch.setattr(qs, "search_videos", _fake_search(["abc123"]))
    monkeypatch.setattr(qs, "enrich_videos", _fake_enrich())
    enqueue_topics(curriculum_nodes[:1])
    process_next(daily_budget=DEFAULT_DAILY_BUDGET, now=T0)
    assert job_field(gold_db, "transcribe_video", "abc123", "status") == "pending"

    # 1ª transcrição: falha transitória esgotada → ``failed``.
    from bncc_match.catalog.transcript import TranscriptionResult

    def _failed_capture(video_id: str, **kwargs: Any) -> TranscriptionResult:
        from bncc_match.data_lake import write_bronze as wb

        # Como o fluxo real, a captura grava o resultado (mesmo falha).
        line = {
            "video_id": video_id,
            "raw_text": "",
            "language": None,
            "status": "failed",
            "captured_at": datetime.now(UTC).isoformat(),
        }
        wb("youtube_transcript", [json.dumps(line)])
        return TranscriptionResult(
            video_id=video_id, status="failed", text="", language=None
        )

    def _ok_capture(video_id: str, **kwargs: Any) -> TranscriptionResult:
        from bncc_match.data_lake import write_bronze as wb

        line = {
            "video_id": video_id,
            "raw_text": "texto",
            "language": "pt",
            "status": "transcribed",
            "captured_at": datetime.now(UTC).isoformat(),
        }
        wb("youtube_transcript", [json.dumps(line)])
        return TranscriptionResult(
            video_id=video_id, status="transcribed", text="texto", language="pt"
        )

    monkeypatch.setattr(qs, "capture_transcript", _failed_capture)
    r = process_next(daily_budget=DEFAULT_DAILY_BUDGET, now=T0 + timedelta(seconds=1))
    assert r["status"] == "completed"
    assert r["transcript_status"] == "failed"
    assert video_field(gold_db, "abc123", "status") == "transcript_failed"

    # Listável + reenfileirável.
    assert list_failed_transcripts() == ["abc123"]
    assert enqueue_failed_transcripts() == 1
    assert job_field(gold_db, "transcribe_video", "abc123", "status") == "pending"

    # Reprocessada com sucesso: transcript única, vídeo transcrito.
    monkeypatch.setattr(qs, "capture_transcript", _ok_capture)
    r2 = process_next(daily_budget=DEFAULT_DAILY_BUDGET, now=T0 + timedelta(seconds=2))
    assert r2["status"] == "completed"
    assert r2["transcript_status"] == "transcribed"
    assert video_field(gold_db, "abc123", "status") == "transcribed"

    con = sqlite3.connect(gold_db)
    count = con.execute("SELECT COUNT(*) FROM transcript").fetchone()
    con.close()
    assert int(count[0]) == 1  # sem duplicata


def test_quota_reset_diaria(gold_db: Path) -> None:
    """Consumo de dias anteriores não conta no orçamento do dia atual."""
    con = sqlite3.connect(gold_db)
    con.execute(
        "INSERT INTO quota_state (day, units_used) VALUES (?, ?)",
        ("2026-10-20", 9000),
    )
    con.commit()
    con.close()

    status = quota_status(daily_budget=10000)
    assert status["units_used"] == 0
    assert status["units_remaining"] == 10000


def test_cota_nao_consome_tentativa(
    gold_db: Path,
    curriculum_nodes: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reagendamento por cota não conta contra ``max_attempts``."""
    monkeypatch.setattr(qs, "search_videos", _fake_search(["abc123"]))
    monkeypatch.setattr(qs, "enrich_videos", _fake_enrich())
    enqueue_topics(curriculum_nodes[:1])

    process_next(daily_budget=50)  # 50 < 100: cota esgota antes da busca
    key = str(curriculum_nodes[0])
    assert job_field(gold_db, "search_topic", key, "status") == "pending"
    assert job_field(gold_db, "search_topic", key, "attempts") == 0


def test_erro_inesperado_vira_dead(
    gold_db: Path,
    curriculum_nodes: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exceção não classificada → ``dead`` (sem loop, inspecionável)."""

    def _boom(
        query: str,
        max_results: int = 10,
        api_key: str | None = None,
        service: Any = None,
    ) -> list[dict[str, Any]]:
        raise ValueError("bug inesperado")

    monkeypatch.setattr(qs, "search_videos", _boom)
    monkeypatch.setattr(qs, "enrich_videos", _fake_enrich())
    enqueue_topics(curriculum_nodes[:1])

    result = process_next(daily_budget=DEFAULT_DAILY_BUDGET, now=T0)

    assert result["status"] == "dead"
    assert "ValueError" in (result.get("last_error") or "")


def test_transcribe_video_inexistente_vira_dead(lake_root: Path, gold_db: Path) -> None:
    """Job de transcrição para vídeo ausente do catálogo → ``dead``."""
    ts = "2026-10-21 00:00:00.000000"
    con = sqlite3.connect(gold_db)
    con.execute(
        "INSERT INTO job_queue (job_type, reference_key, status, attempts,"
        " max_attempts, quota_consumed, created_at, updated_at)"
        " VALUES ('transcribe_video', 'fantasma', 'pending', 0, 3, 0, ?, ?)",
        (ts, ts),
    )
    con.commit()
    con.close()

    result = process_next(daily_budget=DEFAULT_DAILY_BUDGET, now=T0)

    assert result["status"] == "dead"
    assert "fantasma" in (result.get("last_error") or "")


def test_video_ausente_do_gold_e_ignorado(
    lake_root: Path,
    gold_db: Path,
    curriculum_nodes: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ID buscado sem chegar ao gold é ignorado (sem link/job de transcrição)."""
    monkeypatch.setattr(qs, "search_videos", _fake_search(["abc123", "fantasma"]))
    # Enrich grava apenas o abc123 (o ``fantasma`` sumiu da API).
    monkeypatch.setattr(qs, "enrich_videos", _fake_enrich_only(["abc123"]))
    enqueue_topics(curriculum_nodes[:1])

    result = process_next(daily_budget=DEFAULT_DAILY_BUDGET, now=T0)

    assert result["status"] == "completed"
    assert result["videos"] == 2
    assert result["linked"] == 1
    assert count_videos(gold_db) == 1
    assert job_field(gold_db, "transcribe_video", "abc123", "status") == "pending"
    assert job_field(gold_db, "transcribe_video", "fantasma", "status") is None


def _fake_enrich_only(video_ids: list[str]) -> Callable[..., list[dict[str, Any]]]:
    """Enrich fake que grava no bronze apenas os IDs informados."""

    def _enrich(
        requested: list[str],
        api_key: str | None = None,
        service: Any = None,
    ) -> list[dict[str, Any]]:
        items = [
            {
                "id": vid,
                "snippet": {"title": f"vídeo {vid}", "channelTitle": "Canal"},
            }
            for vid in video_ids
        ]
        if items:
            write_bronze(
                "youtube_videos",
                [json.dumps(i, ensure_ascii=False) for i in items],
            )
        return []

    return _enrich


def test_transcribe_job_dead_e_reativado(
    lake_root: Path,
    gold_db: Path,
    curriculum_nodes: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Job de transcrição ``dead`` é reativado ao relinkar o vídeo."""
    monkeypatch.setattr(qs, "search_videos", _fake_search(["abc123"]))
    monkeypatch.setattr(qs, "enrich_videos", _fake_enrich())
    enqueue_topics(curriculum_nodes[:2])

    process_next(daily_budget=DEFAULT_DAILY_BUDGET, now=T0)
    con = sqlite3.connect(gold_db)
    con.execute(
        "UPDATE job_queue SET status = 'dead', attempts = 2"
        " WHERE job_type = 'transcribe_video' AND reference_key = 'abc123'"
    )
    con.commit()
    con.close()

    # 2º tópico encontra o mesmo vídeo → job dead volta a ficar pendente.
    r = process_next(daily_budget=DEFAULT_DAILY_BUDGET, now=T0 + timedelta(seconds=1))
    assert r["status"] == "completed"
    assert job_field(gold_db, "transcribe_video", "abc123", "status") == "pending"
    assert job_field(gold_db, "transcribe_video", "abc123", "attempts") == 0


def test_enqueue_failed_sem_job_cria_novo(
    lake_root: Path, gold_db: Path, curriculum_nodes: list[int]
) -> None:
    """Vídeo com falha e sem job → ``enqueue_failed_transcripts`` cria."""
    # Vídeo com status de falha, sem job de transcrição.
    con = sqlite3.connect(gold_db)
    con.execute(
        "INSERT INTO video (youtube_id, title, channel_name, description,"
        " url, status)"
        " VALUES ('falhou', 't', 'c', '', 'u', 'transcript_failed')"
    )
    con.commit()
    con.close()

    count = enqueue_failed_transcripts()

    assert count == 1
    assert job_field(gold_db, "transcribe_video", "falhou", "status") == "pending"
    assert list_failed_transcripts() == ["falhou"]


def test_build_search_query_por_tipo(
    lake_root: Path,
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """Skill: sem o próprio código; outros tipos: com o nome; pai órfão OK."""
    payload = write_payload(tmp_path / "bncc.json", minimal_items)
    pipeline.run_ingest(raw_file=payload)
    from bncc_match.db import default_session_factory
    from bncc_match.models import CurriculumNode
    from bncc_match.queue import build_search_query

    factory = default_session_factory()
    with factory() as session:
        skill_id = node_ids_by_type(gold_db, "skill")[0]
        skill = session.get(CurriculumNode, skill_id)
        assert skill is not None
        skill_query = build_search_query(skill, session)
        assert "BNCC" in skill_query
        assert skill.name not in skill_query  # código da habilidade excluído

        # Nó não-skill: o próprio nome entra na busca.
        con = sqlite3.connect(gold_db)
        con.execute(
            "INSERT INTO curriculum_node (id, parent_id, stage, type, name)"
            " VALUES (1000, 9999, 'fundamental', 'knowledge_object',"
            " 'Tema Órfão')"
        )
        con.commit()
        con.close()
        node = session.get(CurriculumNode, 1000)
        assert node is not None
        query = build_search_query(node, session)

    assert query == "Tema Órfão BNCC"  # pai inexistente não quebra


def test_backoff_delay_crescente_jitter() -> None:
    """Backoff exponencial + jitter: cresce com as tentativas."""
    delays = [
        backoff_delay(i, base_seconds=30.0, rng=random.Random(1)) for i in (1, 2, 3)
    ]
    assert 30.0 <= delays[0] < 60.0
    assert 60.0 <= delays[1] < 90.0
    assert 120.0 <= delays[2] < 150.0
    # teto respeitado
    assert (
        backoff_delay(30, base_seconds=30.0, max_seconds=3600.0, rng=random.Random(1))
        <= 3600.0
    )
