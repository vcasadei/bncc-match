"""Testes da transformação bronze → silver (Parquet validado e deduplicado)."""

import json
from pathlib import Path
from typing import Any

from bncc_match.data_lake import bronze, manifest, silver


def _transform(rec: dict[str, Any]) -> dict[str, Any]:
    return {"code": rec["code"], "text": rec["text"].strip(), "year": rec["year"]}


def _valid(rec: dict[str, Any]) -> bool:
    return rec.get("code") is not None and rec.get("year") is not None


def test_bronze_valido_transformado_em_silver(lake_root: Path) -> None:
    """Cenário: bronze válido é transformado em silver."""
    records = [
        {"code": f"C{i:02d}", "text": f"  habilidade {i} ", "year": 5 + i}
        for i in range(10)
    ]
    bronze.write("bncc", [json.dumps(r) for r in records])

    result = silver.transform("curriculum", "bncc", _transform, _valid)

    assert result["row_count"] == 10
    assert result["rejected"] == 0
    assert Path(result["path"]).exists()
    assert len(silver.read("curriculum").fetchall()) == 10

    entry = next(e for e in manifest.list_files() if e["path"] == result["path"])
    assert entry["layer"] == "silver"
    assert entry["row_count"] == 10


def test_registros_invalidos_excluidos_da_silver(lake_root: Path) -> None:
    """Cenário: registros inválidos são excluídos da silver."""
    records = []
    for i in range(10):
        if i % 2 == 0:
            records.append({"code": f"C{i:02d}", "text": f"t{i}", "year": 5})
        else:
            records.append({"code": None, "text": f"t{i}", "year": None})
    bronze.write("bncc", [json.dumps(r) for r in records])

    result = silver.transform("curriculum", "bncc", _transform, _valid)

    assert result["row_count"] == 5
    assert result["rejected"] == 5
    assert len(silver.read("curriculum").fetchall()) == 5


def test_transform_com_todos_invalidos_retorna_vazio(lake_root: Path) -> None:
    """Sem registros válidos, não há arquivo silver nem rejeição de validação."""
    bronze.write("bncc", [json.dumps({"code": None, "text": "x", "year": None})])

    result = silver.transform("curriculum", "bncc", _transform, _valid)

    assert result["path"] is None
    assert result["row_count"] == 0
    assert result["rejected"] == 1


def test_silver_com_tipos_variados(lake_root: Path) -> None:
    """Tipos numéricos e booleanos são inferidos corretamente no Parquet."""
    records = [
        {"code": "A", "text": "x", "score": 0.9, "flag": True},
        {"code": "B", "text": "y", "score": 0.1, "flag": False},
    ]
    bronze.write("bncc", [json.dumps(r) for r in records])

    result = silver.transform(
        "video", "bncc", lambda r: r, lambda r: r["code"] is not None
    )

    info = manifest.inspect(result["path"])
    types = {c["name"]: c["type"] for c in info["columns"]}
    assert types["score"] == "DOUBLE"
    assert types["flag"] == "BOOLEAN"
    assert types["code"] == "VARCHAR"


def test_read_silver_retorna_dados(lake_root: Path) -> None:
    """``silver.read`` retorna uma relação com ``.fetchall()``."""
    records = [
        {"code": "A1", "text": "x", "year": 5},
        {"code": "A2", "text": "y", "year": 6},
    ]
    bronze.write("bncc", [json.dumps(r) for r in records])
    silver.transform("curriculum", "bncc", _transform, _valid)

    rows = silver.read("curriculum").fetchall()
    assert len(rows) == 2


def test_transform_fn_retorna_none_eh_rejeitado(lake_root: Path) -> None:
    """Se ``transform_fn`` retorna ``None``, o registro conta como rejeitado."""
    records = [
        {"code": "A", "text": "x", "year": 5},
        {"code": "B", "text": "y", "year": 6},
    ]
    bronze.write("bncc", [json.dumps(r) for r in records])

    def drop_first(rec: dict[str, Any]) -> dict[str, Any] | None:
        return (
            None if rec["code"] == "A" else {"code": rec["code"], "year": rec["year"]}
        )

    result = silver.transform("d", "bncc", drop_first, lambda r: r["code"] is not None)
    assert result["row_count"] == 1
    assert result["rejected"] == 1


def test_inferencia_de_tipos_duckdb() -> None:
    """Mapeadores de tipo puros (cobrem ramos defensivos de ``_column_type``)."""
    assert silver._duckdb_type(None) == "VARCHAR"
    assert silver._duckdb_type(True) == "BOOLEAN"
    assert silver._duckdb_type(3) == "BIGINT"
    assert silver._duckdb_type(3.5) == "DOUBLE"
    assert silver._duckdb_type("s") == "VARCHAR"
    assert silver._duckdb_type([1]) == "JSON"

    assert silver._column_type("v", [{"v": 1}, {"v": 2.5}]) == "DOUBLE"
    assert silver._column_type("v", [{"v": 1}, {"v": 2}]) == "BIGINT"
    assert silver._column_type("v", [{"v": "a"}, {"v": "b"}]) == "VARCHAR"
    assert silver._column_type("v", []) == "VARCHAR"
