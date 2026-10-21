"""Testes da CLI ``bncc`` (ingest, validate, get)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pytest
from _pytest.capture import CaptureFixture
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas as pdfcanvas
from util import count_skills, write_payload

from bncc_match.curriculum import cli, source


def _make_pdf(path: Path, lines: list[str]) -> Path:
    """Gera um PDF simples com as linhas dadas.

    Args:
        path: Caminho de destino.
        lines: Linhas de texto do PDF.

    Returns:
        O caminho do PDF gerado.
    """
    c = pdfcanvas.Canvas(str(path), pagesize=letter)
    y = 750
    for line in lines:
        c.drawString(72, y, line)
        y -= 14
    c.save()
    return path


def _ingest_minima(
    capsys: CaptureFixture[str],
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """Ingesta a fixture mínima via CLI.

    Args:
        capsys: Captura de saída do pytest.
        gold_db: Caminho do SQLite do gold.
        minimal_items: Itens da fixture.
        tmp_path: Diretório temporário.
    """
    payload = write_payload(tmp_path / "bncc.json", minimal_items)
    assert cli.main(["ingest", "--file", str(payload)]) == 0
    capsys.readouterr()


def test_cli_ingest_file(
    capsys: CaptureFixture[str],
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """`bncc ingest --file` executa o pipeline e imprime o resumo."""
    payload = write_payload(tmp_path / "bncc.json", minimal_items)
    rc = cli.main(["ingest", "--file", str(payload)])
    out = capsys.readouterr().out
    assert rc == 0
    assert "bronze:" in out
    assert "silver: 3 linhas" in out
    assert "gold:" in out
    assert count_skills(gold_db) == 3


def test_cli_ingest_erro_fonte(
    capsys: CaptureFixture[str],
    gold_db: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`bncc ingest` com fonte fora do ar sai com 1 e mensagem no stderr."""

    def _raise(url: str, timeout: float, client: Any = None) -> str:
        raise httpx.ConnectError("fora do ar", request=httpx.Request("GET", url))

    monkeypatch.setattr(source, "fetch_raw", _raise)
    rc = cli.main(["ingest"])
    err = capsys.readouterr().err
    assert rc == 1
    assert "erro na ingestão" in err


def test_cli_get(
    capsys: CaptureFixture[str],
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """`bncc get EF05MA03` imprime a hierarquia."""
    _ingest_minima(capsys, gold_db, minimal_items, tmp_path)
    rc = cli.main(["get", "EF05MA03"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "EF05MA03" in out
    assert "Matemática" in out
    assert "Números naturais" in out


def test_cli_get_inexistente(
    capsys: CaptureFixture[str],
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """`bncc get` com código ausente sai com 1."""
    _ingest_minima(capsys, gold_db, minimal_items, tmp_path)
    rc = cli.main(["get", "EF99ZZ99"])
    err = capsys.readouterr().err
    assert rc == 1
    assert "não encontrado" in err


def test_cli_validate_identico(
    capsys: CaptureFixture[str],
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """`bncc validate` com PDF igual ao gold sai com 0."""
    _ingest_minima(capsys, gold_db, minimal_items, tmp_path)
    pdf = _make_pdf(
        tmp_path / "bncc.pdf", ["EI03A01 a.", "EF05MA03 b.", "EM13CHS101 c."]
    )
    rc = cli.main(["validate", str(pdf)])
    out = capsys.readouterr().out
    assert rc == 0
    assert "sem divergências" in out


def test_cli_validate_divergente(
    capsys: CaptureFixture[str],
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
    tmp_path: Path,
) -> None:
    """`bncc validate` com PDF incompleto sai com 2 e lista divergências."""
    _ingest_minima(capsys, gold_db, minimal_items, tmp_path)
    pdf = _make_pdf(tmp_path / "parcial.pdf", ["EF05MA03 b."])
    rc = cli.main(["validate", str(pdf)])
    out = capsys.readouterr().out
    assert rc == 2
    assert "somente no gold" in out


def test_cli_validate_pdf_ausente(
    capsys: CaptureFixture[str],
    gold_db: Path,
    tmp_path: Path,
) -> None:
    """PDF inexistente sai com 1."""
    rc = cli.main(["validate", str(tmp_path / "nao-existe.pdf")])
    err = capsys.readouterr().err
    assert rc == 1
    assert "não encontrado" in err
