"""Transformações bronze → silver do catálogo de vídeos e transcrições.

Domínio ``video``: cada linha bronze é um item bruto da YouTube Data API
(um item de ``search.list`` ou de ``videos.list``). A normalização produz
a linha flat da silver e deduplica por ``youtube_id`` dentro de cada
execução de transform (a silver de um domínio é a união sem duplicatas).

Domínio ``transcript``: cada linha bronze é um registro bruto de captura
de legendas (``video_id``, ``raw_text``, ``language``, ``status``,
``captured_at``).
"""

from __future__ import annotations

from typing import Any

from bncc_match.data_lake.silver import TransformFn
from bncc_match.youtube.client import iso8601_to_seconds, pick_thumbnail


def _clean_str(value: Any) -> str:
    """Converte ``value`` em string sem espaços nas extremidades."""
    if not isinstance(value, str):
        return ""
    return value.strip()


def video_item_youtube_id(record: dict[str, Any]) -> str | None:
    """Extrai o ``youtube_id`` de um item bruto da YouTube API.

    Aceita os dois formatos: ``videos.list`` (``id`` é string) e
    ``search.list`` (``id`` é objeto com ``videoId``).

    Args:
        record: Item bruto (linha do bronze).

    Returns:
        ``youtube_id`` ou ``None`` se ausente.
    """
    raw = record.get("id")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    if isinstance(raw, dict):
        video_id = raw.get("videoId")
        if isinstance(video_id, str) and video_id.strip():
            return video_id.strip()
    return None


def validate_video_item(record: dict[str, Any]) -> bool:
    """Um item de vídeo é válido se tem ``youtube_id`` não vazio."""
    return video_item_youtube_id(record) is not None


def normalize_video_item(record: dict[str, Any]) -> dict[str, Any] | None:
    """Normaliza o item bruto para a linha flat da silver de vídeos.

    Args:
        record: Item bruto (``youtube_videos``/``youtube_search``).

    Returns:
        Linha silver (``youtube_id``, ``title``, ``channel_name``,
        ``description``, ``duration_seconds``, ``published_at``, ``url``,
        ``thumbnail_url``) ou ``None`` se sem ``youtube_id``.
    """
    youtube_id = video_item_youtube_id(record)
    if youtube_id is None:
        return None
    snippet = record.get("snippet")
    if not isinstance(snippet, dict):
        snippet = {}
    details = record.get("contentDetails")
    if not isinstance(details, dict):
        details = {}
    duration = details.get("duration")
    published = snippet.get("publishedAt")
    return {
        "youtube_id": youtube_id,
        "title": _clean_str(snippet.get("title")),
        "channel_name": _clean_str(snippet.get("channelTitle")),
        "description": _clean_str(snippet.get("description")),
        "duration_seconds": iso8601_to_seconds(str(duration)) if duration else None,
        "published_at": str(published) if published else None,
        "url": f"https://www.youtube.com/watch?v={youtube_id}",
        "thumbnail_url": pick_thumbnail(snippet.get("thumbnails")),
    }


def make_video_transform() -> TransformFn:
    """Cria a função de transform com deduplicação por ``youtube_id``.

    O ``seen`` é compartilhado entre os registros de uma mesma execução
    de ``silver.transform``: como o bronze é cumulativo, o mesmo vídeo
    aparece em respostas brutas sucessivas e só a primeira ocorrência
    é mantida na silver.

    Returns:
        TransformFn que deduplica por ``youtube_id``.
    """
    seen: set[str] = set()

    def transform(record: dict[str, Any]) -> dict[str, Any] | None:
        row = normalize_video_item(record)
        if row is None:
            return None
        if row["youtube_id"] in seen:
            return None
        seen.add(row["youtube_id"])
        return row

    return transform


def validate_transcript_record(record: dict[str, Any]) -> bool:
    """Um registro de transcrição é válido se tem ``video_id`` não vazio."""
    video_id = record.get("video_id")
    return isinstance(video_id, str) and bool(video_id.strip())


def normalize_transcript_record(record: dict[str, Any]) -> dict[str, Any] | None:
    """Normaliza o registro bruto de captura para a linha flat da silver.

    Args:
        record: Registro bruto (``video_id``, ``raw_text``, ``language``,
            ``status``, ``captured_at``).

    Returns:
        Linha silver (``video_id``, ``text``, ``language``, ``status``,
        ``captured_at``) ou ``None`` se sem ``video_id``.
    """
    video_id = record.get("video_id")
    if not isinstance(video_id, str) or not video_id.strip():
        return None
    raw_text = record.get("raw_text")
    language = record.get("language")
    captured_at = record.get("captured_at")
    status = record.get("status")
    return {
        "video_id": video_id.strip(),
        "text": raw_text.strip() if isinstance(raw_text, str) else "",
        "language": _clean_str(language) or None,
        "status": _clean_str(status) or "failed",
        "captured_at": str(captured_at) if captured_at else "",
    }
