"""Testes do cliente YouTube Data API v3 (busca + metadados + bronze)."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from googleapiclient.errors import HttpError

from bncc_match.data_lake import manifest as lake_manifest
from bncc_match.youtube.client import (
    QuotaExceededError,
    TransientYouTubeError,
    classify_http_error,
    enrich_videos,
    iso8601_to_seconds,
    pick_thumbnail,
    search_videos,
)

SEARCH_PAYLOAD: dict[str, Any] = {
    "items": [
        {
            "id": {"kind": "youtube#video", "videoId": "abc123"},
            "snippet": {
                "title": "Matemática 5º ano",
                "channelTitle": "Canal Escola",
            },
        },
        {
            # Sem ``videoId``: resultado de canal, deve ser ignorado.
            "id": {"kind": "youtube#channel", "channelId": "UC999"},
            "snippet": {"title": "Canal irrelevante", "channelTitle": "X"},
        },
    ]
}

VIDEOS_PAYLOAD: dict[str, Any] = {
    "items": [
        {
            "id": "abc123",
            "snippet": {
                "title": "Matemática 5º ano",
                "channelTitle": "Canal Escola",
                "description": "Aula sobre números naturais.",
                "publishedAt": "2023-05-01T12:00:00Z",
                "thumbnails": {
                    "default": {"url": "https://i.ytimg.com/default.jpg"},
                    "medium": {"url": "https://i.ytimg.com/medium.jpg"},
                },
            },
            "contentDetails": {"duration": "PT1H2M3S"},
        }
    ]
}


class _Executable:
    """Simula o objeto de request (``.execute()``)."""

    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def execute(self) -> dict[str, Any]:
        if isinstance(self._payload, BaseException):
            raise self._payload
        assert isinstance(self._payload, dict)
        return self._payload


class _List:
    """Simula ``.search().list(...)``/``.videos().list(...)``."""

    def __init__(self, svc: FakeYouTubeService, which: str) -> None:
        self._svc = svc
        self._which = which

    def list(self, **kwargs: Any) -> _Executable:
        self._svc.calls.append((self._which, kwargs))
        return _Executable(self._svc.payloads[self._which])


class FakeYouTubeService:
    """Dublê do serviço ``youtube/v3`` (busca + vídeos).

    Attributes:
        payloads: Payload por método (dict de resposta ou exceção).
        calls: Chamadas registradas ``(método, kwargs)``.
    """

    def __init__(
        self,
        search_payload: Any | None = None,
        videos_payload: Any | None = None,
    ) -> None:
        self.payloads = {
            "search": search_payload if search_payload is not None else {"items": []},
            "videos": videos_payload if videos_payload is not None else {"items": []},
        }
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def search(self) -> _List:
        return _List(self, "search")

    def videos(self) -> _List:
        return _List(self, "videos")


def _http_error(status: int, reason: str | None = None) -> HttpError:
    """Constrói um ``HttpError`` com status e motivo no corpo JSON.

    Args:
        status: Código HTTP.
        reason: Motivo no corpo (ex.: ``quotaExceeded``); ``None`` = vazio.

    Returns:
        Exceção pronta para simular a API.
    """
    resp = SimpleNamespace(status=status, reason="erro")
    if reason is not None:
        content = json.dumps(
            {"error": {"errors": [{"reason": reason, "code": status}]}}
        ).encode()
    else:
        content = b"{}"
    return HttpError(resp, content)


def test_busca_retorna_itens_do_topico(lake_root: Path) -> None:
    """Cenário 'Busca retorna resultados para um tópico'."""
    svc = FakeYouTubeService(search_payload=SEARCH_PAYLOAD)
    items = search_videos("Matemática Números naturais", service=svc)

    assert items == [
        {
            "youtube_id": "abc123",
            "title": "Matemática 5º ano",
            "channel_name": "Canal Escola",
        }
    ]
    method, kwargs = svc.calls[0]
    assert method == "search"
    assert kwargs["q"] == "Matemática Números naturais"
    assert kwargs["maxResults"] == 10


def test_busca_grava_bronze_e_manifesto(lake_root: Path) -> None:
    """Task 2.3: itens brutos em JSONL, um por linha, no manifesto."""
    svc = FakeYouTubeService(search_payload=SEARCH_PAYLOAD)
    search_videos("q", service=svc)

    files = list((lake_root / "bronze" / "youtube_search").glob("*/*.jsonl"))
    assert len(files) == 1
    lines = files[0].read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2  # um item bruto por linha
    assert json.loads(lines[0])["id"]["videoId"] == "abc123"

    entries = [e for e in lake_manifest.list_files() if e["source"] == "youtube_search"]
    assert len(entries) == 1
    assert entries[0]["row_count"] == 2


def test_enrich_retorna_metadados_completos(lake_root: Path) -> None:
    """Task 2.2/2.4: descrição, duração, data, thumbnail e URL."""
    svc = FakeYouTubeService(videos_payload=VIDEOS_PAYLOAD)
    enriched = enrich_videos(["abc123"], service=svc)

    assert len(enriched) == 1
    meta = enriched[0]
    assert meta["youtube_id"] == "abc123"
    assert meta["title"] == "Matemática 5º ano"
    assert meta["channel_name"] == "Canal Escola"
    assert meta["description"] == "Aula sobre números naturais."
    assert meta["duration_seconds"] == 3723
    assert meta["published_at"] == "2023-05-01T12:00:00Z"
    assert meta["url"] == "https://www.youtube.com/watch?v=abc123"
    assert meta["thumbnail_url"] == "https://i.ytimg.com/medium.jpg"

    files = list((lake_root / "bronze" / "youtube_videos").glob("*/*.jsonl"))
    assert len(files) == 1
    assert json.loads(files[0].read_text().strip())["id"] == "abc123"


def test_enrich_chunking_50_por_chamada(lake_root: Path) -> None:
    """IDs acima de 50 geram múltiplas chamadas ``videos.list``."""
    svc = FakeYouTubeService(videos_payload=VIDEOS_PAYLOAD)
    enrich_videos([f"vid{i:02d}" for i in range(75)], service=svc)
    assert len(svc.calls) == 2
    first_ids = svc.calls[0][1]["id"].split(",")
    assert len(first_ids) == 50


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("PT1H2M3S", 3723),
        ("PT45S", 45),
        ("PT2M", 120),
        ("PT1H", 3600),
        ("PT0S", 0),
        ("inválido", 0),
    ],
)
def test_duracao_iso8601(raw: str, expected: int) -> None:
    """Duração ISO 8601 da API em segundos."""
    assert iso8601_to_seconds(raw) == expected


def test_sem_chave_api_erro(monkeypatch: pytest.MonkeyPatch) -> None:
    """``YOUTUBE_API_KEY`` ausente → RuntimeError (sem rede)."""
    monkeypatch.delenv("YOUTUBE_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="YOUTUBE_API_KEY"):
        search_videos("q")


def test_quota_exceeded_403(lake_root: Path) -> None:
    """Cenário 'Busca excede a cota': 403 quotaExceeded → erro de domínio."""
    svc = FakeYouTubeService(search_payload=_http_error(403, "quotaExceeded"))
    with pytest.raises(QuotaExceededError):
        search_videos("q", service=svc)
    assert not (lake_root / "bronze").exists()  # nada gravado


def test_429_vira_erro_transitorio(lake_root: Path) -> None:
    """429/rateLimit → erro transitório (candidato a retry)."""
    svc = FakeYouTubeService(search_payload=_http_error(429, "rateLimitExceeded"))
    with pytest.raises(TransientYouTubeError):
        search_videos("q", service=svc)


def test_classify_http_error() -> None:
    """Classificação: quota, transitório e inclassificável."""
    assert isinstance(
        classify_http_error(_http_error(403, "quotaExceeded")), QuotaExceededError
    )
    assert isinstance(
        classify_http_error(_http_error(403, "dailyLimitExceeded")),
        QuotaExceededError,
    )
    assert isinstance(classify_http_error(_http_error(429)), TransientYouTubeError)
    assert isinstance(classify_http_error(_http_error(500)), TransientYouTubeError)
    assert classify_http_error(_http_error(400)) is None


def test_pick_thumbnail() -> None:
    """Thumbnail: medium tem prioridade; default como fallback."""
    assert pick_thumbnail({"default": {"url": "d"}, "medium": {"url": "m"}}) == "m"
    assert pick_thumbnail({"default": {"url": "d"}}) == "d"
    assert pick_thumbnail({}) is None
    assert pick_thumbnail(None) is None
