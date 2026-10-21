"""Testes do manifesto (catálogo DuckDB da lake)."""

import json
from pathlib import Path

import duckdb

from bncc_match.data_lake import bronze, manifest, silver


def test_listagem_retorna_todos_os_arquivos(lake_root: Path) -> None:
    """Cenário: listagem do manifesto retorna todos os arquivos."""
    bronze.write("bncc", [json.dumps({"code": "A", "text": "t", "year": 5})])
    result = silver.transform(
        "curriculum",
        "bncc",
        lambda r: {"code": r["code"], "text": r["text"], "year": r["year"]},
        lambda r: r["code"] is not None,
    )

    entries = manifest.list_files()
    bronze_entries = [e for e in entries if e["layer"] == "bronze"]
    silver_entries = [e for e in entries if e["layer"] == "silver"]
    assert len(bronze_entries) == 1
    assert len(silver_entries) == 1
    assert result["path"] in {e["path"] for e in silver_entries}


def test_inspecao_retorna_schema_e_contagem(lake_root: Path) -> None:
    """Cenário: inspeção de um arquivo retorna schema e contagem."""
    bronze_path = bronze.write(
        "bncc",
        [json.dumps({"code": "A", "year": 5}), json.dumps({"code": "B", "year": 6})],
    )
    bronze_info = manifest.inspect(bronze_path)
    assert bronze_info["row_count"] == 2
    assert {"code", "year"} <= {c["name"] for c in bronze_info["columns"]}

    result = silver.transform(
        "curriculum",
        "bncc",
        lambda r: {"code": r["code"], "year": r["year"]},
        lambda r: r["code"] is not None,
    )
    silver_info = manifest.inspect(result["path"])
    assert silver_info["row_count"] == 2
    assert {"code", "year"} <= {c["name"] for c in silver_info["columns"]}


def test_init_cria_tabela_de_manifesto(lake_root: Path) -> None:
    """Tarefa 4.1: ``init`` cria o catálogo com a tabela ``manifest``."""
    catalog = manifest.init()
    assert catalog.exists()
    con = duckdb.connect(str(catalog))
    try:
        tables = con.execute("SHOW TABLES").fetchall()
    finally:
        con.close()
    assert any(t[0] == "manifest" for t in tables)
