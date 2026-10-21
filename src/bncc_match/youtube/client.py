"""Cliente da YouTube Data API v3 (busca + metadados) com escrita em bronze.

A chave da API vem estritamente da variável de ambiente
``YOUTUBE_API_KEY`` — nunca hardcoded, nunca em log. Cada item bruto das
respostas (``items``) é gravado em bronze JSONL (fontes
``youtube_search`` e ``youtube_videos``), registrado no manifesto da data
lake.

Erros de quota (403 ``quotaExceeded``/``dailyLimitExceeded``) viram
:class:`QuotaExceededError` e erros transitórios (429/5xx) viram
:class:`TransientYouTubeError`, para a fila decidir retry/reagendamento.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

from googleapiclient.errors import HttpError

from bncc_match.data_lake.bronze import write as write_bronze

#: Unidades de quota da YouTube Data API v3 por chamada.
SEARCH_COST = 100
VIDEOS_LIST_COST = 1

_QUOTA_REASONS = {"quotaExceeded", "dailyLimitExceeded"}

_DURATION_RE = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?")

_ENV_API_KEY = "YOUTUBE_API_KEY"


class YouTubeClientError(Exception):
    """Erro base do cliente YouTube."""


class QuotaExceededError(YouTubeClientError):
    """Cota diária esgotada (403 quotaExceeded/dailyLimitExceeded)."""


class TransientYouTubeError(YouTubeClientError):
    """Erro transitório (429/5xx) — candidato a retry com backoff."""


def classify_http_error(error: HttpError) -> YouTubeClientError | None:
    """Classifica um ``HttpError`` da API em erro de domínio.

    Args:
        error: Exceção da YouTube Data API.

    Returns:
        :class:`QuotaExceededError`, :class:`TransientYouTubeError` ou
        ``None`` (erro não transitório, que deve propagar).
    """
    status = error.resp.status
    reason = ""
    content = error.content
    if isinstance(content, (bytes, bytearray)):
        content = content.decode("utf-8", errors="replace")
    try:
        body: Any = json.loads(content)
        errors = (body.get("error") or {}).get("errors") or [{}]
        reason = str(errors[0].get("reason", ""))
    except (json.JSONDecodeError, AttributeError, IndexError):
        reason = ""
    if status == 403 and reason in _QUOTA_REASONS:
        return QuotaExceededError(f"quota esgotada ({reason})")
    if status == 429 or status >= 500 or reason == "rateLimitExceeded":
        return TransientYouTubeError(f"erro transitório (http {status}, {reason})")
    return None


def _build_service(api_key: str | None = None) -> Any:
    """Constrói o serviço ``youtube/v3`` a partir da chave do ambiente.

    Args:
        api_key: Chave explícita; ``None`` usa ``YOUTUBE_API_KEY``.

    Returns:
        Recurso da API (``googleapiclient.discovery.Resource``).

    Raises:
        RuntimeError: Se a chave não estiver definida.
    """
    from googleapiclient.discovery import build

    key = api_key if api_key is not None else os.environ.get(_ENV_API_KEY, "")
    if not key:
        raise RuntimeError(f"variável de ambiente {_ENV_API_KEY} não definida")
    return build("youtube", "v3", developerKey=key, cache_discovery=False)


def _write_bronze_items(source: str, items: Any) -> None:
    """Grava os itens brutos da resposta em bronze JSONL (um por linha).

    Cada item da resposta (``items`` da API) é uma linha JSON bruta —
    sem limpeza — para a silver normalizar depois. Respostas sem itens
    não geram arquivo (bronze é append-only, sem vazios).

    Args:
        source: Fonte bronze (``youtube_search``/``youtube_videos``).
        items: Lista ``items`` da resposta bruta.
    """
    if not isinstance(items, list) or not items:
        return
    write_bronze(
        source, [json.dumps(item, ensure_ascii=False, default=str) for item in items]
    )


def iso8601_to_seconds(duration: str) -> int:
    """Converte duração ISO 8601 (``PT1H2M3S``) em segundos.

    Args:
        duration: Duração no formato da API (``contentDetails.duration``).

    Returns:
        Duração total em segundos (0 se o formato não reconhecer).
    """
    match = _DURATION_RE.match(duration)
    if not match:
        return 0
    hours, minutes, seconds = (int(g or 0) for g in match.groups())
    return hours * 3600 + minutes * 60 + seconds


def pick_thumbnail(thumbnails: Any) -> str | None:
    """Escolhe a melhor miniatura disponível (medium → default).

    Args:
        thumbnails: Dicionário ``snippet.thumbnails`` da API.

    Returns:
        URL da miniatura ou ``None``.
    """
    if not isinstance(thumbnails, dict):
        return None
    for quality in ("medium", "default"):
        entry = thumbnails.get(quality)
        if isinstance(entry, dict) and entry.get("url"):
            return str(entry["url"])
    return None


def _execute_or_raise(request: Any) -> dict[str, Any]:
    """Executa a requisição e converte ``HttpError`` em erro de domínio.

    Args:
        request: Objeto de request da API (com ``.execute()``).

    Returns:
        Resposta bruta (dicionário).

    Raises:
        QuotaExceededError: Cota esgotada.
        TransientYouTubeError: Erro transitório (429/5xx).
    """
    try:
        resp = request.execute()
    except HttpError as error:
        classified = classify_http_error(error)
        if classified is not None:
            raise classified from error
        raise
    return dict(resp)


def search_videos(
    query: str,
    max_results: int = 10,
    api_key: str | None = None,
    service: Any = None,
) -> list[dict[str, Any]]:
    """Busca vídeos na YouTube Data API (``search.list``, 100 unidades).

    Args:
        query: Texto da busca (tópico curricular).
        max_results: Máximo de resultados (1-50).
        api_key: Chave explícita; ``None`` usa o ambiente.
        service: Serviço injetado (testes); ``None`` constrói o real.

    Returns:
        Lista de itens ``{youtube_id, title, channel_name}``.

    Raises:
        RuntimeError: Chave ausente.
        QuotaExceededError / TransientYouTubeError: ver :func:`_execute_or_raise`.
    """
    svc = service if service is not None else _build_service(api_key)
    request = svc.search().list(part="snippet", q=query, maxResults=max_results)
    resp = _execute_or_raise(request)
    _write_bronze_items("youtube_search", resp.get("items", []))
    items: list[dict[str, Any]] = []
    for entry in resp.get("items", []):
        video_id = (entry.get("id") or {}).get("videoId")
        if not video_id:
            continue
        snippet = entry.get("snippet") or {}
        items.append(
            {
                "youtube_id": str(video_id),
                "title": str(snippet.get("title", "")),
                "channel_name": str(snippet.get("channelTitle", "")),
            }
        )
    return items


def enrich_videos(
    video_ids: list[str],
    api_key: str | None = None,
    service: Any = None,
) -> list[dict[str, Any]]:
    """Enriquece vídeos via ``videos.list`` (1 unidade por chamada).

    Args:
        video_ids: IDs de vídeo (chunks de até 50 por chamada).
        api_key: Chave explícita; ``None`` usa o ambiente.
        service: Serviço injetado (testes); ``None`` constrói o real.

    Returns:
        Lista de metadados completos: ``youtube_id``, ``title``,
        ``channel_name``, ``description``, ``duration_seconds``,
        ``published_at`` (ISO), ``url``, ``thumbnail_url``.
    """
    svc = service if service is not None else _build_service(api_key)
    enriched: list[dict[str, Any]] = []
    for start in range(0, len(video_ids), 50):
        chunk = video_ids[start : start + 50]
        request = svc.videos().list(part="snippet,contentDetails", id=",".join(chunk))
        resp = _execute_or_raise(request)
        _write_bronze_items("youtube_videos", resp.get("items", []))
        for entry in resp.get("items", []):
            video_id = str(entry.get("id", ""))
            if not video_id:
                continue
            snippet = entry.get("snippet") or {}
            details = entry.get("contentDetails") or {}
            enriched.append(
                {
                    "youtube_id": video_id,
                    "title": str(snippet.get("title", "")),
                    "channel_name": str(snippet.get("channelTitle", "")),
                    "description": str(snippet.get("description", "")),
                    "duration_seconds": iso8601_to_seconds(
                        str(details.get("duration", ""))
                    ),
                    "published_at": snippet.get("publishedAt"),
                    "url": f"https://www.youtube.com/watch?v={video_id}",
                    "thumbnail_url": pick_thumbnail(snippet.get("thumbnails")),
                }
            )
    return enriched
