"""Carrega a silver (Parquet) no gold (SQLite).

A árvore de ``curriculum_node`` é reconstruída a cada execução (truncate +
insert, em transação) e as skills entram via upsert por ``code`` — o
código oficial é a chave de identidade. Habilidades que saíram da fonte
são marcadas com ``status = 'absent'`` (e o vínculo com o nó é rompido),
preservando histórico sem duplicar registros.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
from sqlalchemy import delete, update
from sqlalchemy.orm import Session, sessionmaker

from bncc_match.data_lake.paths import resolve_root
from bncc_match.db import default_session_factory
from bncc_match.models import NODE_SKILL, CurriculumNode, IngestionRun, Skill


def _read_silver(domain: str, root: Path | None = None) -> list[dict[str, Any]]:
    """Lê a visão atual do silver: registros da última ingestão.

    O bronze é imutável/acumulativo, então a silver reprocessa o histórico
    a cada execução. Cada registro carrega ``ingest_run`` (timestamp do
    fetch de origem); a visão atual do gold é o conjunto com a maior chave.

    Args:
        domain: Domínio no silver.
        root: Raiz da lake.

    Returns:
        Lista de registros (dicionários coluna → valor) da última ingestão.
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
    names = [c[0] for c in cols]
    records = [dict(zip(names, row)) for row in rows]
    latest = max((str(rec.get("ingest_run") or "") for rec in records), default="")
    return [rec for rec in records if str(rec.get("ingest_run") or "") == latest]


def load_to_gold(
    domain: str = "curriculum",
    source_url: str = "",
    source_version: str | None = None,
    root: Path | None = None,
    session_factory: sessionmaker[Session] | None = None,
) -> dict[str, Any]:
    """Carrega a silver no gold (``curriculum_node`` + ``skill``).

    Tudo em uma única transação: reconstrói a árvore de nós a partir dos
    ``path`` dos registros, faz upsert das skills por ``code``
    (``status = 'active'``), marca como ``'absent'`` as que saíram da
    fonte e registra a execução em ``ingestion_run``.

    Args:
        domain: Domínio no silver (ex.: ``curriculum``).
        source_url: URL da fonte (proveniência em ``ingestion_run``).
        source_version: Versão da fonte (proveniência em ``ingestion_run``).
        root: Raiz da lake; ``None`` usa o raiz padrão.
        session_factory: Fábrica de sessões do gold; ``None`` usa o padrão.

    Returns:
        Dicionário com ``nodes``, ``skills``, ``absent`` e ``run_id``.
    """
    factory = (
        session_factory if session_factory is not None else default_session_factory()
    )
    records = _read_silver(domain, root)

    node_count = 0
    skill_count = 0
    absent_count = 0
    run_id = 0

    with factory() as session, session.begin():
        session.execute(delete(CurriculumNode))
        node_map: dict[tuple[str, ...], int] = {}
        current_codes: list[str] = []

        for record in records:
            code = str(record.get("code") or "").strip()
            stage = str(record.get("stage") or "").strip()
            path_raw = record.get("path_json")
            path: Any = json.loads(path_raw) if path_raw else []
            if not isinstance(path, list):
                path = []
            extras_json = record.get("extras_json")
            text = str(record.get("text") or "")
            year = record.get("year")
            current_codes.append(code)

            parent_id: int | None = None
            prefix: tuple[str, ...] = ()
            for level in path:
                if not isinstance(level, (list, tuple)) or len(level) < 2:
                    continue
                node_type = str(level[0])
                node_name = str(level[1])
                prefix = prefix + (stage, node_type, node_name)
                if prefix in node_map:
                    parent_id = node_map[prefix]
                    continue
                node = CurriculumNode(
                    stage=stage,
                    type=node_type,
                    name=node_name,
                    parent_id=parent_id,
                )
                session.add(node)
                session.flush()
                node_map[prefix] = node.id
                parent_id = node.id
                node_count += 1

            skill_prefix = prefix + (stage, NODE_SKILL, code)
            if skill_prefix in node_map:
                skill_node_id = node_map[skill_prefix]
            else:
                skill_node = CurriculumNode(
                    stage=stage,
                    type=NODE_SKILL,
                    name=code,
                    parent_id=parent_id,
                    extras_json=extras_json,
                )
                session.add(skill_node)
                session.flush()
                node_map[skill_prefix] = skill_node.id
                skill_node_id = skill_node.id
                node_count += 1

            existing = session.get(Skill, code)
            if existing is None:
                session.add(
                    Skill(
                        code=code,
                        node_id=skill_node_id,
                        text=text,
                        year=year,
                    )
                )
            else:
                existing.node_id = skill_node_id
                existing.text = text
                existing.year = year
                existing.status = "active"
            skill_count += 1

        if current_codes:
            stmt = (
                update(Skill)
                .where(Skill.status == "active")
                .where(Skill.code.not_in(current_codes))
                .values(status="absent", node_id=None)
            )
        else:
            stmt = (
                update(Skill)
                .where(Skill.status == "active")
                .values(status="absent", node_id=None)
            )
        res: Any = session.execute(stmt)
        absent_count = int(res.rowcount or 0)

        run = IngestionRun(
            source_url=source_url,
            source_version=source_version,
            timestamp=datetime.now(UTC),
        )
        session.add(run)
        session.flush()
        run_id = run.id

    return {
        "nodes": node_count,
        "skills": skill_count,
        "absent": absent_count,
        "run_id": run_id,
    }
