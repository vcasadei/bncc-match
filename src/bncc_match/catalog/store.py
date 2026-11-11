"""Carga da silver para o gold e associações do catálogo de vídeos.

``load_videos_to_gold`` popula a tabela ``video`` via upsert por
``youtube_id`` (reexecução idempotente, sem duplicatas).
``load_transcript_to_gold`` popula ``transcript`` (a captura mais recente
de cada vídeo vence, por ``captured_at``) e sincroniza o ``status`` do
vídeo. ``link_video_to_topic`` grava a associação vídeo ↔ tópico na
``video_topic`` (idempotente, chave composta).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from bncc_match.data_lake.paths import resolve_root
from bncc_match.db import default_session_factory
from bncc_match.models import (
    TRANSCRIPT_FAILED,
    TRANSCRIPT_TRANSCRIBED,
    VIDEO_DISCOVERED,
    VIDEO_TRANSCRIBED,
    VIDEO_TRANSCRIPT_FAILED,
    Transcript,
    Video,
    VideoTopic,
)


def _parse_iso(value: Any) -> datetime | None:
    """Converte string ISO 8601 em ``datetime`` UTC sem fuso (ou ``None``).

    Args:
        value: Data/hora no formato da YouTube API (ex.: com sufixo ``Z``).

    Returns:
        Datetime UTC normalizado ou ``None``.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        offset = parsed.utcoffset() or timedelta(0)
        return (parsed - offset).replace(tzinfo=None)
    return parsed


def _video_fields(record: dict[str, Any]) -> dict[str, Any]:
    """Extrai os campos de metadados da linha silver para o model."""
    duration = record.get("duration_seconds")
    thumbnail = record.get("thumbnail_url")
    youtube_id = str(record.get("youtube_id") or "")
    url = str(record.get("url") or "")
    if not url:
        url = f"https://www.youtube.com/watch?v={youtube_id}"
    return {
        "title": str(record.get("title") or ""),
        "channel_name": str(record.get("channel_name") or ""),
        "description": str(record.get("description") or ""),
        "duration_seconds": int(duration)
        if isinstance(duration, (int, float))
        else None,
        "published_at": _parse_iso(record.get("published_at")),
        "url": url,
        "thumbnail_url": str(thumbnail) if thumbnail else None,
    }


def _read_silver_rows(domain: str, root: Path | None) -> list[dict[str, Any]]:
    """Lê a união de todos os Parquet do domínio (cumulativo).

    Diferentemente do domínio ``curriculum``, o catálogo de vídeos é
    acumulativo: cada transform reprocessa todo o bronze e a visão atual
    é a união de todos os arquivos do domínio.

    Args:
        domain: Domínio no silver (ex.: ``video``).
        root: Raiz da lake; ``None`` usa a padrão.

    Returns:
        Lista de linhas (dicionários) do domínio.
    """
    r = resolve_root(root)
    if not list((r / "silver" / domain).glob("*.parquet")):
        return []
    pattern = str(r / "silver" / domain / "*.parquet").replace("'", "''")
    con = duckdb.connect()
    try:
        cols = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{pattern}')"
        ).fetchall()
        rows = con.execute(f"SELECT * FROM read_parquet('{pattern}')").fetchall()
    finally:
        con.close()
    names = [str(c[0]) for c in cols]
    return [dict(zip(names, row)) for row in rows]


def _factory_or(
    session_factory: sessionmaker[Session] | None,
) -> sessionmaker[Session]:
    """Retorna a fábrica informada ou a padrão do gold."""
    return session_factory if session_factory is not None else default_session_factory()


def load_videos_to_gold(
    domain: str = "video",
    root: Path | None = None,
    session: Session | None = None,
    session_factory: sessionmaker[Session] | None = None,
) -> dict[str, Any]:
    """Popula ``video`` a partir da silver via upsert por ``youtube_id``.

    Args:
        domain: Domínio de origem no silver.
        root: Raiz da lake; ``None`` usa a padrão.
        session: Sessão já aberta (transação do chamador); ``None`` abre.
        session_factory: Fábrica usada quando ``session`` é ``None``.

    Returns:
        Dicionário com ``videos`` (quantidade processada).
    """
    records = _read_silver_rows(domain, root)

    def _load(s: Session) -> int:
        loaded = 0
        for record in records:
            youtube_id = str(record.get("youtube_id") or "")
            if not youtube_id:
                continue
            video = (
                s.execute(select(Video).where(Video.youtube_id == youtube_id))
                .scalars()
                .first()
            )
            fields = _video_fields(record)
            if video is None:
                s.add(Video(youtube_id=youtube_id, status=VIDEO_DISCOVERED, **fields))
            else:
                for column, value in fields.items():
                    setattr(video, column, value)
            loaded += 1
        return loaded

    if session is not None:
        loaded = _load(session)
    else:
        with _factory_or(session_factory)() as s, s.begin():
            loaded = _load(s)
    return {"videos": loaded}


def load_transcript_to_gold(
    domain: str = "transcript",
    root: Path | None = None,
    session: Session | None = None,
    session_factory: sessionmaker[Session] | None = None,
) -> dict[str, Any]:
    """Popula ``transcript`` a partir da silver (a mais recente por vídeo).

    Vídeos ausentes do catálogo são pulados (contados em ``skipped``).
    O ``status`` do vídeo é sincronizado: ``transcribed`` → vídeo
    transcrito; ``failed`` → vídeo com falha (reprocessável).

    Args:
        domain: Domínio de origem no silver.
        root: Raiz da lake; ``None`` usa a padrão.
        session: Sessão já aberta; ``None`` abre.
        session_factory: Fábrica usada quando ``session`` é ``None``.

    Returns:
        Dicionário com ``transcripts`` e ``skipped``.
    """
    records = _read_silver_rows(domain, root)

    # A captura mais recente de cada vídeo (ISO 8601 ordena em string).
    latest: dict[str, dict[str, Any]] = {}
    for record in records:
        video_key = str(record.get("video_id") or "")
        if not video_key:
            continue
        previous = latest.get(video_key)
        if previous is None or str(record.get("captured_at") or "") >= str(
            previous.get("captured_at") or ""
        ):
            latest[video_key] = record

    def _load(s: Session) -> tuple[int, int]:
        loaded = 0
        skipped = 0
        for video_key, record in latest.items():
            video = (
                s.execute(select(Video).where(Video.youtube_id == video_key))
                .scalars()
                .first()
            )
            if video is None:
                skipped += 1
                continue
            text = str(record.get("text") or "")
            language = record.get("language")
            status = str(record.get("status") or "failed")
            transcript = (
                s.execute(select(Transcript).where(Transcript.video_id == video.id))
                .scalars()
                .first()
            )
            if transcript is None:
                s.add(
                    Transcript(
                        video_id=video.id,
                        text=text,
                        language=language,
                        status=status,
                    )
                )
            else:
                transcript.text = text
                transcript.language = language
                transcript.status = status
            if status == TRANSCRIPT_TRANSCRIBED:
                video.status = VIDEO_TRANSCRIBED
            elif status == TRANSCRIPT_FAILED:
                video.status = VIDEO_TRANSCRIPT_FAILED
            loaded += 1
        return loaded, skipped

    if session is not None:
        loaded, skipped = _load(session)
    else:
        with _factory_or(session_factory)() as s, s.begin():
            loaded, skipped = _load(s)
    return {"transcripts": loaded, "skipped": skipped}


def link_video_to_topic(
    video_id: int,
    curriculum_node_id: int,
    session: Session | None = None,
    session_factory: sessionmaker[Session] | None = None,
) -> None:
    """Registra a associação vídeo ↔ tópico (idempotente, chave composta).

    O mesmo vídeo pode pertencer a múltiplos tópicos sem duplicar o
    registro em ``video``.

    Args:
        video_id: PK do vídeo.
        curriculum_node_id: PK do nó curricular (tópico).
        session: Sessão já aberta; ``None`` abre.
        session_factory: Fábrica usada quando ``session`` é ``None``.
    """

    def _link(s: Session) -> None:
        existing = s.execute(
            select(VideoTopic).where(
                VideoTopic.video_id == video_id,
                VideoTopic.curriculum_node_id == curriculum_node_id,
            )
        ).first()
        if existing is None:
            s.add(VideoTopic(video_id=video_id, curriculum_node_id=curriculum_node_id))

    if session is not None:
        _link(session)
    else:
        with _factory_or(session_factory)() as s, s.begin():
            _link(s)
