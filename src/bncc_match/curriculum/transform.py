"""Validação e normalização dos itens brutos da BNCC (bronze → silver).

O item bruto (uma linha do JSONL bronze) segue o contrato documentado em
``docs/bncc-source-structure.md``. A normalização produz a linha flat da
silver, serializando ``path``/``extras`` como JSON em colunas VARCHAR.
"""

from __future__ import annotations

import json
from typing import Any


def validate_raw(record: dict[str, Any]) -> bool:
    """Um item é válido se tem ``code`` oficial não vazio.

    Args:
        record: Registro bruto (linha do bronze).

    Returns:
        ``True`` se o registro é válido.
    """
    code = record.get("code")
    return isinstance(code, str) and bool(code.strip())


def _to_json(value: Any, default: Any) -> str:
    """Serializa ``value`` para JSON, aceitando valor já serializado.

    Args:
        value: Valor a serializar (dict/list ou string JSON).
        default: Usado quando ``value`` é ``None`` ou não é JSON.

    Returns:
        String JSON.
    """
    if value is None:
        value = default
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return json.dumps(default, ensure_ascii=False)
    return json.dumps(value, ensure_ascii=False)


def normalize_raw(record: dict[str, Any]) -> dict[str, Any] | None:
    """Normaliza o item bruto para a linha flat da silver.

    Args:
        record: Registro bruto (linha do bronze).

    Returns:
        Linha silver (``code``, ``text``, ``year``, ``stage``,
        ``path_json``, ``extras_json``) ou ``None`` se sem ``code``.
    """
    code = record.get("code")
    if not isinstance(code, str) or not code.strip():
        return None
    text = record.get("text")
    stage = record.get("stage")
    return {
        "code": code.strip(),
        "text": text.strip() if isinstance(text, str) else "",
        "year": record.get("year"),
        "stage": stage.strip() if isinstance(stage, str) else "",
        "path_json": _to_json(record.get("path"), []),
        "extras_json": _to_json(record.get("extras"), {}),
        "ingest_run": str(record.get("ingest_run") or ""),
    }
