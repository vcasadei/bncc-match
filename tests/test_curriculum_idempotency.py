"""Testes de idempotência, chave de código e reingestão (spec 'curriculum')."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from util import count_nodes, count_skills, skill_field, skill_statuses, write_payload

from bncc_match.curriculum import pipeline
from bncc_match.data_lake import manifest


def test_duas_execucoes_nao_duplicam(
    lake_root: Path,
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """Cenário 'Duas execuções não duplicam a árvore'.

    COUNT idêntico no gold após a 2ª execução; bronze acumula arquivos
    imutáveis distintos, registrados no manifesto.
    """
    payload = write_payload(tmp_path / "bncc.json", minimal_items)
    pipeline.run_ingest(raw_file=payload, source_version="v1")
    skills_1 = count_skills(gold_db)
    nodes_1 = count_nodes(gold_db)
    assert skills_1 == 3

    pipeline.run_ingest(raw_file=payload, source_version="v2")
    assert count_skills(gold_db) == skills_1
    assert count_nodes(gold_db) == nodes_1

    bronze_files = sorted((lake_root / "bronze" / "bncc").glob("*/*.jsonl"))
    assert len(bronze_files) == 2  # imutáveis, um por execução

    entries = manifest.list_files()
    bronze_entries = [e for e in entries if e["layer"] == "bronze"]
    silver_entries = [e for e in entries if e["layer"] == "silver"]
    assert len(bronze_entries) == 2
    assert len(silver_entries) == 2
    assert len({e["path"] for e in bronze_entries}) == 2  # arquivos distintos


def test_interrupcao_entre_silver_e_gold(
    lake_root: Path,
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cenário 'Interrupção no meio da ingestão'.

    Falha entre silver e gold: a reexecução completa sem duplicar o gold.
    """
    payload = write_payload(tmp_path / "bncc.json", minimal_items)
    pipeline.run_ingest(raw_file=payload, source_version="v1")
    skills_1 = count_skills(gold_db)
    assert skills_1 == 3

    def _falha(**kwargs: Any) -> Any:
        raise RuntimeError("falha simulada no load")

    with monkeypatch.context() as m:
        m.setattr("bncc_match.curriculum.pipeline.load_to_gold", _falha)
        with pytest.raises(RuntimeError, match="falha simulada"):
            pipeline.run_ingest(raw_file=payload, source_version="v2")

    # silver foi escrito (2 arquivos), gold intacto
    silver_files = list((lake_root / "silver" / "curriculum").glob("*.parquet"))
    assert len(silver_files) == 2
    assert count_skills(gold_db) == skills_1

    # reexecução completa sem duplicar
    pipeline.run_ingest(raw_file=payload, source_version="v3")
    assert count_skills(gold_db) == skills_1
    assert count_nodes(gold_db) > 0


def test_removida_da_fonte_fica_ausente(
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """Habilidade removida da fonte é marcada 'absent' (sem órfão sem tratamento)."""
    p1 = write_payload(tmp_path / "v1.json", minimal_items)
    pipeline.run_ingest(raw_file=p1, source_version="v1")
    assert skill_statuses(gold_db)["EF05MA03"] == "active"

    v2 = [item for item in minimal_items if item["code"] != "EF05MA03"]
    p2 = write_payload(tmp_path / "v2.json", v2)
    result = pipeline.run_ingest(raw_file=p2, source_version="v2")

    statuses = skill_statuses(gold_db)
    assert statuses["EF05MA03"] == "absent"
    assert statuses["EI03A01"] == "active"
    assert statuses["EM13CHS101"] == "active"
    assert count_skills(gold_db) == 3  # preservada como histórico
    assert skill_field(gold_db, "EF05MA03", "node_id") is None
    assert result["gold"]["absent"] == 1


def test_mesmo_code_mantem_um_registro(
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """Cenário 'Código oficial é a chave de identidade': upsert, 1 registro."""
    duplicado = dict(minimal_items[1])  # EF05MA03
    duplicado["text"] = "texto atualizado da fonte v2"
    payload = write_payload(tmp_path / "dup.json", minimal_items + [duplicado])
    result = pipeline.run_ingest(raw_file=payload)

    assert result["silver"]["row_count"] == 4  # 4 linhas válidas na silver
    assert count_skills(gold_db) == 3  # UNIQUE(code) intacto
    assert skill_field(gold_db, "EF05MA03", "text") == "texto atualizado da fonte v2"


def test_item_sem_code_e_rejeitado(
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """Cenário 'Habilidade sem código': rejeitado com relatório, fora do gold."""
    sem_code: dict[str, Any] = {
        "text": "objeto sem código oficial",
        "year": None,
        "stage": "fundamental",
        "path": [["stage", "Ensino Fundamental"]],
        "extras": {},
    }
    payload = write_payload(tmp_path / "nocode.json", minimal_items + [sem_code])
    result = pipeline.run_ingest(raw_file=payload)

    assert result["silver"]["rejected"] == 1
    assert result["silver"]["row_count"] == 3
    assert count_skills(gold_db) == 3
    assert skill_field(gold_db, "SEM-CODIGO", "text") is None
