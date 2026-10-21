"""Testes da consulta da árvore curricular (curriculum.query)."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from util import write_payload

from bncc_match.curriculum import pipeline, query


def _ingest_minima(
    gold_db: Path, tmp_path: Path, minimal_items: list[dict[str, Any]]
) -> None:
    """Ingesta a fixture mínima no gold.

    Args:
        gold_db: Caminho do SQLite do gold.
        tmp_path: Diretório temporário.
        minimal_items: Itens da fixture.
    """
    payload = write_payload(tmp_path / "bncc.json", minimal_items)
    pipeline.run_ingest(raw_file=payload)


def _by_type(ancestors: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Indexa ancestrais por ``type``.

    Args:
        ancestors: Lista de ancestrais.

    Returns:
        Dicionário type → ancestral.
    """
    return {str(a["type"]): a for a in ancestors}


def test_fundamental_expo_hierarquia_completa(
    gold_db: Path, tmp_path: Path, minimal_items: list[dict[str, Any]]
) -> None:
    """Cenário 'Habilidade do Ensino Fundamental expõe a hierarquia completa'."""
    _ingest_minima(gold_db, tmp_path, minimal_items)
    skill = query.get_skill("EF05MA03")
    assert skill is not None
    levels = _by_type(skill["ancestors"])
    assert levels["stage"]["name"] == "Ensino Fundamental"
    assert levels["area"]["name"] == "Matemática"
    assert levels["component"]["name"] == "Matemática"
    assert levels["thematic_unit"]["name"] == "Números"
    assert levels["knowledge_object"]["name"] == "Números naturais"
    assert skill["year"] == 5  # ano/série
    assert skill["status"] == "active"


def test_infantil_expo_campo_e_faixa(
    gold_db: Path, tmp_path: Path, minimal_items: list[dict[str, Any]]
) -> None:
    """Cenário 'Objetivo da Educação Infantil expõe campo e faixa etária'."""
    _ingest_minima(gold_db, tmp_path, minimal_items)
    skill = query.get_skill("EI03A01")
    assert skill is not None
    levels = _by_type(skill["ancestors"])
    assert "thematic_unit" not in levels
    assert "knowledge_object" not in levels
    assert levels["component"]["name"] == "Escuta, fala, pensamento e imaginação"
    assert levels["area"]["name"] == "4 a 6 anos"
    assert levels["stage"]["name"] == "Educação Infantil"
    assert skill["year"] is None  # Infantil não tem ano/série


def test_medio_expo_area_e_competencia(
    gold_db: Path, tmp_path: Path, minimal_items: list[dict[str, Any]]
) -> None:
    """Habilidade do Médio expõe área, competência e etapa."""
    _ingest_minima(gold_db, tmp_path, minimal_items)
    skill = query.get_skill("EM13CHS101")
    assert skill is not None
    levels = _by_type(skill["ancestors"])
    assert levels["area"]["name"] == "Ciências Humanas e Sociais Aplicadas"
    assert "component" in levels  # competência específica
    assert skill["year"] == 13
    assert skill["stage"] == "medio"


def test_codigo_inexistente_retorna_vazio(
    gold_db: Path, tmp_path: Path, minimal_items: list[dict[str, Any]]
) -> None:
    """Cenário 'Consulta de código inexistente': vazio, sem exceção."""
    _ingest_minima(gold_db, tmp_path, minimal_items)
    assert query.get_skill("EF99ZZ99") is None


def test_skill_com_no_invalido_retorna_sem_ancestrais(
    gold_db: Path,
) -> None:
    """Skill com node_id pendurado (nó inexistente) não quebra a consulta."""
    con = sqlite3.connect(gold_db)
    con.execute(
        "INSERT INTO skill (code, node_id, text, year, status) "
        "VALUES ('EF01MA99', 999, 'texto', 1, 'active')"
    )
    con.commit()
    con.close()
    skill = query.get_skill("EF01MA99")
    assert skill is not None
    assert skill["ancestors"] == []
    assert skill["stage"] == ""
