"""Testes da migration Alembic do gold (up/down/up) e da fábrica de engines."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from alembic.config import Config
from sqlalchemy import text

from alembic import command
from bncc_match.db import Base, make_engine

REPO_ROOT = Path(__file__).resolve().parents[1]

EXPECTED_TABLES = {"curriculum_node", "ingestion_run", "skill"}


def _tables(db: Path) -> set[str]:
    """Lista as tabelas de um SQLite.

    Args:
        db: Caminho do SQLite.

    Returns:
        Nomes das tabelas.
    """
    con = sqlite3.connect(db)
    try:
        rows = con.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    finally:
        con.close()
    return {str(name) for (name,) in rows}


def _alembic_config() -> Config:
    """Configuração Alembic apontando para o alembic.ini do repo.

    Returns:
        Objeto Config do Alembic.
    """
    return Config(str(REPO_ROOT / "alembic.ini"))


def test_migration_up_down_up(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Tasks 1.1/1.2: upgrade cria, downgrade remove, upgrade reconstrói."""
    db = tmp_path / "migrate.db"
    monkeypatch.setenv("BNCC_DB_PATH", str(db))

    command.upgrade(_alembic_config(), "head")
    assert EXPECTED_TABLES <= _tables(db)

    command.downgrade(_alembic_config(), "-1")
    assert not (EXPECTED_TABLES & _tables(db))

    command.upgrade(_alembic_config(), "head")
    assert EXPECTED_TABLES <= _tables(db)


def test_make_engine_in_memory() -> None:
    """Engine em memória (StaticPool) aceita o schema dos models."""
    engine = make_engine(in_memory=True)
    Base.metadata.create_all(engine)
    with engine.connect() as con:
        count = con.execute(text("SELECT COUNT(*) FROM skill")).scalar_one()
    assert count == 0
    engine.dispose()


def test_make_engine_caminho_padrao(gold_db: Path) -> None:
    """Sem argumentos, usa BNCC_DB_PATH (gold padrão configurado)."""
    engine = make_engine()
    assert engine.url.database == str(gold_db)
    engine.dispose()
