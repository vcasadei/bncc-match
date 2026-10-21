"""Fixtures compartilhadas dos testes (data lake + currículo)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from bncc_match import models  # noqa: F401  # registra as tables no Base.metadata
from bncc_match.db import Base, make_engine

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture(autouse=True)
def lake_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isola cada teste em um diretório temporário para a data lake.

    Define ``BNCC_LAKE_ROOT`` para um ``tmp_path`` único, de modo que
    ``write``/``transform``/``manifest``/CLI usem um raiz limpo por teste.

    Args:
        tmp_path: Diretório temporário do pytest.
        monkeypatch: Ferramenta de patch do pytest.

    Returns:
        Path do raiz da lake temporário.
    """
    root = tmp_path / "data"
    monkeypatch.setenv("BNCC_LAKE_ROOT", str(root))
    return root


@pytest.fixture
def gold_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Cria um SQLite gold temporário com o schema dos models.

    Define ``BNCC_DB_PATH`` para um arquivo em ``tmp_path`` e aplica o
    schema via ``Base.metadata.create_all`` (a migration real é coberta
    em ``tests/test_curriculum_migration.py``).

    Args:
        tmp_path: Diretório temporário do pytest.
        monkeypatch: Ferramenta de patch do pytest.

    Returns:
        Path do arquivo SQLite do gold.
    """
    db = tmp_path / "gold" / "curriculum.db"
    monkeypatch.setenv("BNCC_DB_PATH", str(db))
    engine = make_engine(db)
    Base.metadata.create_all(engine)
    engine.dispose()
    return db


@pytest.fixture
def minimal_items() -> list[dict[str, Any]]:
    """Itens da fixture BNCC mínima (1 habilidade por etapa)."""
    raw = (FIXTURES_DIR / "bncc_minimal.json").read_text(encoding="utf-8")
    items: list[dict[str, Any]] = json.loads(raw)
    return items
