"""Models SQLAlchemy do currículo BNCC (gold).

Três tabelas:

- ``curriculum_node``: árvore de nós tipados (stage/area/component/
  thematic_unit/knowledge_object/skill) com referência de pai.
- ``skill``: dado rico da habilidade (``code`` único como chave natural,
  ``node_id`` FK para o nó correspondente, ``text``, ``year``).
- ``ingestion_run``: proveniência de cada ingestão (fonte, versão, data).
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, Text
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
