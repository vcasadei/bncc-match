"""Fetch e parsing da fonte oficial da BNCC.

A fonte primária é o JSON estruturado servido pelo site oficial
(basenacionalcomum.mec.gov.br). O contrato do payload — chaves, formato
dos códigos e exemplos por etapa — está documentado em
``docs/bncc-source-structure.md``.
"""

from __future__ import annotations

import json
from typing import Any

import httpx

#: Endpoint documentado da fonte (ver ``docs/bncc-source-structure.md``).
DEFAULT_SOURCE_URL = "https://basenacionalcomum.mec.gov.br/api/habilidades"

_USER_AGENT = "bncc-match/0.1 (+https://github.com/vcasadei/bncc-match)"


def fetch_raw(
    url: str = DEFAULT_SOURCE_URL,
    timeout: float = 30.0,
    client: httpx.Client | None = None,
) -> str:
    """Baixa o payload bruto (JSON) da fonte da BNCC.

    Args:
        url: Endpoint a consultar.
        timeout: Timeout da requisição em segundos.
        client: Cliente httpx opcional (injeção de transporte em testes).

    Returns:
        Corpo da resposta como string (JSON bruto).

    Raises:
        httpx.HTTPError: Falha de rede ou timeout.
        httpx.HTTPStatusError: Resposta com status de erro HTTP.
    """
    if client is None:
        resp = httpx.get(url, timeout=timeout, headers={"User-Agent": _USER_AGENT})
    else:
        resp = client.get(url, timeout=timeout, headers={"User-Agent": _USER_AGENT})
    resp.raise_for_status()
    return resp.text


def parse_raw(raw: str) -> list[dict[str, Any]]:
    """Extrai a lista de itens brutos de habilidade do payload.

    Aceita um JSON que é uma lista de itens, ou um objeto com a lista sob
    uma das chaves ``items``, ``habilidades`` ou ``data``.

    Args:
        raw: Payload JSON bruto.

    Returns:
        Lista de itens brutos (dicionários), na ordem do payload.

    Raises:
        ValueError: Payload com JSON inválido, vazio ou sem itens de objeto.
    """
    try:
        data: Any = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"payload não é um JSON válido: {exc}") from exc
    if isinstance(data, dict):
        for key in ("items", "habilidades", "data"):
            candidate = data.get(key)
            if isinstance(candidate, list):
                data = candidate
                break
    if not isinstance(data, list):
        # ValueError é o contrato de parsing (payload inválido), não de tipo.
        raise ValueError("payload não é uma lista de itens")  # noqa: TRY004
    if not data:
        raise ValueError("payload vazio (nenhum item)")
    items = [item for item in data if isinstance(item, dict)]
    if not items:
        raise ValueError("payload não contém itens de objeto")
    return items
