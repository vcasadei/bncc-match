"""Testes da CLI ``data`` (ls, inspect, head)."""

import json
from pathlib import Path
from typing import Any

import pytest

from bncc_match.data_lake import bronze, silver
from bncc_match.data_lake.cli import main


def _transform(rec: dict[str, Any]) -> dict[str, Any]:
    return {"code": rec["code"], "text": rec["text"], "year": rec["year"]}


def _valid(rec: dict[str, Any]) -> bool:
    return rec.get("code") is not None


def test_data_ls_lista_arquivos(
    capsys: pytest.CaptureFixture[str], lake_root: Path
) -> None:
    """Cenário: ``data ls`` lista arquivos da lake."""
    bronze.write("bncc", [json.dumps({"code": "A", "text": "t", "year": 5})])
    silver.transform("curriculum", "bncc", _transform, _valid)

    assert main(["ls"]) == 0
    out = capsys.readouterr().out
    assert "bronze" in out
    assert "silver" in out
    assert "bncc" in out


def test_data_ls_vazia(capsys: pytest.CaptureFixture[str], lake_root: Path) -> None:
    """Com a lake vazia, ``data ls`` avisa e não quebra."""
    assert main(["ls"]) == 0
    assert "(lake vazia)" in capsys.readouterr().out


def test_data_inspect_mostra_schema_e_contagem(
    capsys: pytest.CaptureFixture[str], lake_root: Path
) -> None:
    """Cenário: ``data inspect`` mostra schema e contagem."""
    bronze_path = bronze.write("bncc", [json.dumps({"code": "A", "year": 5})])

    assert main(["inspect", str(bronze_path)]) == 0
    out = capsys.readouterr().out
    assert "row_count: 1" in out
    assert "code" in out


def test_data_head_mostra_primeiras_linhas(
    capsys: pytest.CaptureFixture[str], lake_root: Path
) -> None:
    """Cenário: ``data head`` mostra as primeiras linhas."""
    bronze_path = bronze.write(
        "bncc",
        [json.dumps({"code": f"C{i}", "text": f"t{i}"}) for i in range(5)],
    )

    assert main(["head", str(bronze_path), "3"]) == 0
    out = capsys.readouterr().out
    assert "C0" in out and "C1" in out and "C2" in out
    assert "C3" not in out


def test_end_to_end_bronze_silver_cli(
    capsys: pytest.CaptureFixture[str], lake_root: Path
) -> None:
    """Cenário: fluxo completo write bronze → silver → ls/inspect/head."""
    records = [{"code": f"C{i}", "text": f"t{i}", "year": 5 + i} for i in range(5)]
    bronze_path = bronze.write("bncc", [json.dumps(r) for r in records])
    result = silver.transform("curriculum", "bncc", _transform, _valid)

    assert main(["ls"]) == 0
    ls_out = capsys.readouterr().out
    assert "bronze" in ls_out and "silver" in ls_out

    assert main(["inspect", result["path"]]) == 0
    assert "row_count: 5" in capsys.readouterr().out

    assert main(["head", str(bronze_path), "2"]) == 0
    head_out = capsys.readouterr().out
    assert "C0" in head_out and "C1" in head_out and "C2" not in head_out
