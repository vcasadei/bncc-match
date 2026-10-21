"""Testes da camada bronze (JSONL imutável e particionado)."""

import json
from pathlib import Path

import duckdb
import pytest

from bncc_match.data_lake import bronze, manifest


def test_payload_bruto_ingerido_em_bronze(lake_root: Path) -> None:
    """Cenário: payload bruto ingerido em bronze."""
    raw1 = json.dumps({"code": "EF05MA03", "text": "habilidade"})
    raw2 = json.dumps({"code": "EF06MA01", "text": "outra"})

    path = bronze.write("bncc", [raw1, raw2])

    lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln]
    assert len(lines) == 2
    assert json.loads(lines[0]) == json.loads(raw1)
    assert json.loads(lines[1]) == json.loads(raw2)

    entries = [e for e in manifest.list_files() if e["path"] == str(path)]
    assert len(entries) == 1
    entry = entries[0]
    assert entry["layer"] == "bronze"
    assert entry["source"] == "bncc"
    assert entry["row_count"] == 2
    assert entry["content_hash"]


def test_reingestao_nao_sobrescreve_bronze_anterior(lake_root: Path) -> None:
    """Cenário: reingestão da mesma fonte não sobrescreve bronze anterior."""
    first = bronze.write("bncc", [json.dumps({"code": "A"})])
    second = bronze.write("bncc", [json.dumps({"code": "B"})])

    assert first != second
    assert first.name != second.name
    assert first.parent == second.parent
    assert first.exists() and second.exists()

    paths = {e["path"] for e in manifest.list_files() if e["source"] == "bncc"}
    assert str(first) in paths and str(second) in paths


def test_jsonl_e_queryavel_via_duckdb(lake_root: Path) -> None:
    """Cenário: o JSONL é legível por humano/queryável via read_json_auto."""
    bronze.write(
        "bncc",
        [
            json.dumps({"code": "A", "text": "x"}),
            json.dumps({"code": "B", "text": "y"}),
        ],
    )

    con = duckdb.connect()
    try:
        pattern = str(lake_root / "bronze" / "bncc" / "*" / "*.jsonl")
        rows = con.execute(
            f"SELECT code, text FROM read_json_auto('{pattern}')"
        ).fetchall()
    finally:
        con.close()

    assert sorted(r[0] for r in rows) == ["A", "B"]


def test_read_bronze_retorna_relacao(lake_root: Path) -> None:
    """``bronze.read`` retorna uma relação com ``.fetchall()``."""
    bronze.write("bncc", [json.dumps({"code": "A"}), json.dumps({"code": "B"})])

    rows = bronze.read("bncc").fetchall()
    assert len(rows) == 2


def test_write_com_payload_vazio_lanca_erro(lake_root: Path) -> None:
    """``write`` com ``payload_lines`` vazio levanta ``ValueError``."""
    with pytest.raises(ValueError):
        bronze.write("bncc", [])


def test_write_respeita_root_explicito(tmp_path: Path) -> None:
    """O parâmetro ``root`` é respeitado (sem depender da variável de ambiente)."""
    root = tmp_path / "lake_custom"
    path = bronze.write("bncc", [json.dumps({"code": "A"})], root=root)

    assert path.exists()
    assert root in path.parents
    assert manifest.list_files(root=root)[0]["source"] == "bncc"
