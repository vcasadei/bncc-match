"""Helpers compartilhados dos testes da capability curriculum."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def load_minimal_items() -> list[dict[str, Any]]:
    """Carrega a fixture BNCC mínima (1 item por etapa).

    Returns:
        Lista de itens brutos (dicts) da fixture.
    """
    raw = (FIXTURES_DIR / "bncc_minimal.json").read_text(encoding="utf-8")
    items: list[dict[str, Any]] = json.loads(raw)
    return items


def write_payload(path: Path, items: list[dict[str, Any]]) -> Path:
    """Grava itens como payload JSON (lista) no caminho dado.

    Args:
        path: Caminho de destino.
        items: Itens brutos a gravar.

    Returns:
        O caminho gravado.
    """
    path.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def count_skills(db: Path) -> int:
    """Conta registros na tabela ``skill`` do gold.

    Args:
        db: Caminho do SQLite.

    Returns:
        Número de registros.
    """
    con = sqlite3.connect(db)
    try:
        row = con.execute("SELECT COUNT(*) FROM skill").fetchone()
    finally:
        con.close()
    return int(row[0]) if row is not None else 0


def count_nodes(db: Path) -> int:
    """Conta registros na tabela ``curriculum_node`` do gold.

    Args:
        db: Caminho do SQLite.

    Returns:
        Número de registros.
    """
    con = sqlite3.connect(db)
    try:
        row = con.execute("SELECT COUNT(*) FROM curriculum_node").fetchone()
    finally:
        con.close()
    return int(row[0]) if row is not None else 0


def skill_statuses(db: Path) -> dict[str, str]:
    """Mapeia ``code`` → ``status`` das skills no gold.

    Args:
        db: Caminho do SQLite.

    Returns:
        Dicionário code → status.
    """
    con = sqlite3.connect(db)
    try:
        rows = con.execute("SELECT code, status FROM skill").fetchall()
    finally:
        con.close()
    return {str(code): str(status) for code, status in rows}


def skill_field(db: Path, code: str, column: str) -> Any:
    """Lê um campo de uma skill pelo código.

    Args:
        db: Caminho do SQLite.
        code: Código da habilidade.
        column: Coluna a ler (ex.: ``node_id``, ``text``).

    Returns:
        Valor da coluna (``None`` se a skill não existir).
    """
    con = sqlite3.connect(db)
    try:
        row = con.execute(
            f"SELECT {column} FROM skill WHERE code = ?", (code,)
        ).fetchone()
    finally:
        con.close()
    return row[0] if row is not None else None
