"""Testes de fetch e parsing da fonte BNCC (curriculum.source)."""

from __future__ import annotations

import json

import httpx
import pytest

from bncc_match.curriculum import source


def _client_for(status: int, text: str) -> httpx.Client:
    """Cria um client httpx com transporte mockado.

    Args:
        status: Status da resposta.
        text: Corpo da resposta.

    Returns:
        Cliente com ``MockTransport``.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text=text)

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_fetch_raw_retorna_corpo() -> None:
    """Fetch bem-sucedido retorna o corpo JSON bruto."""
    client = _client_for(200, '[{"code": "EF05MA03"}]')
    raw = source.fetch_raw("http://exemplo/bncc", client=client)
    assert json.loads(raw) == [{"code": "EF05MA03"}]


def test_fetch_raw_erro_http_propaga() -> None:
    """Status 5xx propaga HTTPStatusError."""
    client = _client_for(500, "erro interno")
    with pytest.raises(httpx.HTTPStatusError):
        source.fetch_raw("http://exemplo/bncc", client=client)


def test_fetch_raw_sem_client_usa_httpx_global(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sem client, usa httpx.get (ponto único de injeção)."""

    def fake_get(url: str, timeout: float, headers: dict[str, str]) -> httpx.Response:
        return httpx.Response(200, text="[{}]", request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx, "get", fake_get)
    assert source.fetch_raw("http://exemplo") == "[{}]"


def test_parse_raw_lista() -> None:
    """Lista de itens é retornada na ordem."""
    items = source.parse_raw('[{"code": "EF05MA03"}, {"code": "EM13CHS101"}]')
    assert [item["code"] for item in items] == ["EF05MA03", "EM13CHS101"]


def test_parse_raw_objeto_com_chave_items() -> None:
    """Objeto com a lista sob 'items' é aceito."""
    items = source.parse_raw('{"items": [{"code": "EF05MA03"}]}')
    assert len(items) == 1
    assert items[0]["code"] == "EF05MA03"


@pytest.mark.parametrize(
    ("payload", "erro"),
    [
        ("isso não é json", "JSON"),
        ("[]", "vazio"),
        ("[1, 2, 3]", "objeto"),
        ('{"outra_chave": 1}', "lista"),
    ],
)
def test_parse_raw_rejeita_invalido(payload: str, erro: str) -> None:
    """Payloads inválidos/vazios levantam ValueError."""
    with pytest.raises(ValueError, match=erro):
        source.parse_raw(payload)
