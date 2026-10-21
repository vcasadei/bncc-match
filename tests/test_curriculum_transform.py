"""Testes de validação/normalização bronze → silver (curriculum.transform)."""

from __future__ import annotations

import json
from typing import Any

from bncc_match.curriculum import transform


def _item(**overrides: Any) -> dict[str, Any]:
    """Monta um item bruto padrão com overrides.

    Args:
        overrides: Campos a sobrescrever.

    Returns:
        Dicionário do item bruto.
    """
    item: dict[str, Any] = {
        "code": "EF05MA03",
        "text": "  Comparar e ordenar números naturais. ",
        "year": 5,
        "stage": "fundamental",
        "path": [["stage", "Ensino Fundamental"], ["area", "Matemática"]],
        "extras": {},
    }
    item.update(overrides)
    return item


def test_validate_raw_exige_code() -> None:
    """Item é válido apenas com code não vazio."""
    assert transform.validate_raw(_item()) is True
    assert transform.validate_raw(_item(code="")) is False
    assert transform.validate_raw(_item(code="   ")) is False
    assert transform.validate_raw({"text": "x"}) is False


def test_normalize_linha_silver() -> None:
    """Item válido vira linha flat com path/extras serializados."""
    row = transform.normalize_raw(_item())
    assert row is not None
    assert row["code"] == "EF05MA03"
    assert row["text"] == "Comparar e ordenar números naturais."
    assert row["year"] == 5
    assert row["stage"] == "fundamental"
    assert json.loads(row["path_json"]) == [
        ["stage", "Ensino Fundamental"],
        ["area", "Matemática"],
    ]
    assert json.loads(row["extras_json"]) == {}


def test_normalize_sem_code_retorna_none() -> None:
    """Item sem code é descartado na normalização."""
    assert transform.normalize_raw({"text": "x"}) is None
    assert transform.normalize_raw(_item(code="")) is None


def test_normalize_campos_ausentes() -> None:
    """Campos ausentes viram padrões (texto/etapa vazios, sem ano)."""
    row = transform.normalize_raw({"code": "EF01LP01"})
    assert row is not None
    assert row["text"] == ""
    assert row["stage"] == ""
    assert row["year"] is None
    assert json.loads(row["path_json"]) == []
    assert json.loads(row["extras_json"]) == {}


def test_normalize_camina_json_serializado() -> None:
    """path/extras chegam como string JSON (ex.: via DuckDB)."""
    row = transform.normalize_raw(
        _item(
            path='[["stage", "Ensino Fundamental"]]',
            extras='{"a": 1}',
        )
    )
    assert row is not None
    assert json.loads(row["path_json"]) == [["stage", "Ensino Fundamental"]]
    assert json.loads(row["extras_json"]) == {"a": 1}
