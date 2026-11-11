"""Models SQLAlchemy do bncc-match (gold SQLite).

Currículo BNCC: ``curriculum_node`` (árvore tipada), ``skill`` (``code``
único como chave natural) e ``ingestion_run`` (proveniência).

Catálogo de vídeos: ``video`` (``youtube_id`` único = chave natural),
``video_topic`` (junção vídeo ↔ tópico) e ``transcript`` (1:1 por vídeo).

Fila: ``job_queue`` (estado, tentativas, retry, quota) e ``quota_state``
(consumo diário de quota da YouTube Data API).
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from bncc_match.db import Base

# Valores canônicos do campo ``stage`` (etapa da BNCC).
STAGE_INFANTIL = "infantil"
STAGE_FUNDAMENTAL = "fundamental"
STAGE_MEDIO = "medio"

# Valores canônicos do campo ``type`` (nível hierárquico do nó).
NODE_STAGE = "stage"
NODE_AREA = "area"
NODE_COMPONENT = "component"
NODE_THEMATIC_UNIT = "thematic_unit"
NODE_KNOWLEDGE_OBJECT = "knowledge_object"
NODE_SKILL = "skill"


class CurriculumNode(Base):
    """Um nó da árvore curricular, tipado por etapa e nível hierárquico.

    A árvore é representada por ``parent_id`` (auto-referência). ``stage``
    identifica a etapa (infantil/fundamental/medio) e ``type`` o nível
    (stage, area, component, thematic_unit, knowledge_object, skill).
    ``extras_json`` guarda metadados específicos da etapa em JSON.
    """

    __tablename__ = "curriculum_node"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    parent_id: Mapped[int | None] = mapped_column(
        ForeignKey("curriculum_node.id"), nullable=True
    )
    stage: Mapped[str] = mapped_column(String, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    extras_json: Mapped[str | None] = mapped_column(Text, nullable=True)


class Skill(Base):
    """Dado rico de uma habilidade, com o ``code`` oficial como chave natural.

    ``node_id`` aponta para o nó ``skill`` na árvore (curriculum_node),
    permitindo recuperar a hierarquia de ancestrais. ``text`` preserva o
    enunciado completo para correspondência semântica; ``year`` é o
    ano/série (ex.: 5 no Fundamental, 13 no Médio; nulo no Infantil).
    """

    __tablename__ = "skill"

    code: Mapped[str] = mapped_column(String, primary_key=True)
    node_id: Mapped[int | None] = mapped_column(
        ForeignKey("curriculum_node.id"), nullable=True
    )
    text: Mapped[str] = mapped_column(Text, nullable=False, default="")
    year: Mapped[int | None] = mapped_column(nullable=True)
    status: Mapped[str] = mapped_column(String, nullable=False, default="active")


class IngestionRun(Base):
    """Registro de proveniência de uma execução de ingestão."""

    __tablename__ = "ingestion_run"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source_url: Mapped[str] = mapped_column(String, nullable=False)
    source_version: Mapped[str | None] = mapped_column(String, nullable=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)


# Status de vídeo na fila de ingestão.
VIDEO_DISCOVERED = "discovered"
VIDEO_TRANSCRIBED = "transcribed"
VIDEO_TRANSCRIPT_FAILED = "transcript_failed"

# Status de transcrição.
TRANSCRIPT_TRANSCRIBED = "transcribed"
TRANSCRIPT_UNAVAILABLE = "unavailable"
TRANSCRIPT_FAILED = "failed"

# Tipos de job na fila.
JOB_SEARCH_TOPIC = "search_topic"
JOB_TRANSCRIBE_VIDEO = "transcribe_video"

# Status de job.
JOB_PENDING = "pending"
JOB_IN_PROGRESS = "in_progress"
JOB_COMPLETED = "completed"
JOB_RETRY = "retry"
JOB_DEAD = "dead"


class Video(Base):
    """Metadados de um vídeo do YouTube, com ``youtube_id`` como chave natural."""

    __tablename__ = "video"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    youtube_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    title: Mapped[str] = mapped_column(String, nullable=False, default="")
    channel_name: Mapped[str] = mapped_column(String, nullable=False, default="")
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    duration_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    url: Mapped[str] = mapped_column(String, nullable=False, default="")
    thumbnail_url: Mapped[str | None] = mapped_column(String, nullable=True)
    status: Mapped[str] = mapped_column(
        String, nullable=False, default=VIDEO_DISCOVERED
    )


class VideoTopic(Base):
    """Associação N:N entre vídeo e tópico curricular (nó da árvore)."""

    __tablename__ = "video_topic"

    video_id: Mapped[int] = mapped_column(ForeignKey("video.id"), primary_key=True)
    curriculum_node_id: Mapped[int] = mapped_column(
        ForeignKey("curriculum_node.id"), primary_key=True
    )


class Transcript(Base):
    """Transcrição (legendas) de um vídeo — uma por vídeo (FK única)."""

    __tablename__ = "transcript"

    video_id: Mapped[int] = mapped_column(ForeignKey("video.id"), primary_key=True)
    text: Mapped[str] = mapped_column(Text, nullable=False, default="")
    language: Mapped[str | None] = mapped_column(String, nullable=True)
    status: Mapped[str] = mapped_column(String, nullable=False)


class JobQueue(Base):
    """Item de trabalho persistente (tópico a buscar / vídeo a transcrever)."""

    __tablename__ = "job_queue"
    __table_args__ = (
        UniqueConstraint("job_type", "reference_key", name="uq_job_type_ref"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    job_type: Mapped[str] = mapped_column(String, nullable=False)
    reference_key: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default=JOB_PENDING)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    last_error: Mapped[str | None] = mapped_column(String, nullable=True)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    quota_consumed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class QuotaState(Base):
    """Consumo de quota da YouTube Data API por dia (janela diária)."""

    __tablename__ = "quota_state"

    day: Mapped[str] = mapped_column(String, primary_key=True)
    units_used: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
