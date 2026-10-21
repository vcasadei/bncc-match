"""Testes do pipeline de ingestão bronze → silver → gold (cenários da spec)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pytest
from util import count_nodes, count_skills, skill_statuses, write_payload

from bncc_match.curriculum import pipeline, query, source
from bncc_match.curriculum.load import load_to_gold


def test_ingestao_completa_bencc_minima(
    lake_root: Path,
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """Cenário 'Ingestão bem-sucedida da BNCC completa'.

    1 habilidade por etapa persiste no gold, com as 3 hierarquias e
    proveniência registrada.
    """
    payload = write_payload(tmp_path / "bncc.json", minimal_items)
    result = pipeline.run_ingest(raw_file=payload, source_version="2026-10")

    # bronze: JSONL em data/bronze/bncc/<data>/
    assert Path(result["bronze_path"]).is_file()
    bronze_files = list((lake_root / "bronze" / "bncc").glob("*/*.jsonl"))
    assert len(bronze_files) == 1

    # silver: 3 linhas válidas, 0 rejeitadas
    assert result["silver"]["row_count"] == 3
    assert result["silver"]["rejected"] == 0
    assert Path(result["silver"]["path"]).suffix == ".parquet"

    # gold: 3 habilidades, run registrado
    assert count_skills(gold_db) == 3
    assert result["gold"]["skills"] == 3
    assert result["gold"]["run_id"] > 0

    ef = query.get_skill("EF05MA03")
    assert ef is not None
    assert {a["type"] for a in ef["ancestors"]} == {
        "stage",
        "area",
        "component",
        "thematic_unit",
        "knowledge_object",
    }
    assert ef["year"] == 5

    ei = query.get_skill("EI03A01")
    assert ei is not None
    assert {a["type"] for a in ei["ancestors"]} == {"stage", "area", "component"}

    em = query.get_skill("EM13CHS101")
    assert em is not None
    assert em["year"] == 13
    assert em["stage"] == "medio"


def test_fonte_indisponivel_nao_altera_nada(
    lake_root: Path,
    gold_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cenário 'Fonte indisponível': erro HTTP propaga, tudo intacto."""

    def _raise(url: str, timeout: float, client: Any = None) -> str:
        raise httpx.ConnectError("conexão recusada", request=httpx.Request("GET", url))

    monkeypatch.setattr(source, "fetch_raw", _raise)
    with pytest.raises(httpx.ConnectError):
        pipeline.run_ingest(url="http://indisponivel")

    assert not (lake_root / "bronze").exists()
    assert not (lake_root / "silver").exists()
    assert count_skills(gold_db) == 0
    assert count_nodes(gold_db) == 0


def test_fonte_vazia_nao_altera_nada(
    lake_root: Path,
    gold_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cenário 'Fonte vazia': payload sem itens propaga ValueError."""

    def _empty(url: str, timeout: float, client: Any = None) -> str:
        return "[]"

    monkeypatch.setattr(source, "fetch_raw", _empty)
    with pytest.raises(ValueError, match="vazio"):
        pipeline.run_ingest(url="http://vazia")

    assert not (lake_root / "bronze").exists()
    assert not (lake_root / "silver").exists()
    assert count_skills(gold_db) == 0


def test_load_sem_silver_marca_todas_ausentes(
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """Load sem silver (sem arquivos) não quebra: skills viram ausentes."""
    p1 = write_payload(tmp_path / "v1.json", minimal_items)
    pipeline.run_ingest(raw_file=p1)
    assert count_skills(gold_db) == 3

    result = load_to_gold(root=tmp_path / "lake_sem_silver")
    assert result["nodes"] == 0
    assert result["skills"] == 0
    assert result["absent"] == 3
    assert count_skills(gold_db) == 3
    assert set(skill_statuses(gold_db).values()) == {"absent"}
