"""Testes da validação contra o PDF oficial (curriculum.validate)."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas as pdfcanvas
from util import write_payload

from bncc_match.curriculum import pipeline, validate


def _make_pdf(path: Path, lines: Sequence[str]) -> Path:
    """Gera um PDF simples com as linhas dadas (fixture sintética).

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


def test_extrai_codigos_do_pdf_sintetico(tmp_path: Path) -> None:
    """Task 4.1: PDF de fixture sintética retorna os códigos esperados."""
    pdf = _make_pdf(
        tmp_path / "bncc.pdf",
        [
            "EF05MA03 Comparar e ordenar numeros naturais.",
            "EM13CHS101 Analisar e interpretar o trabalho humano.",
            "EI03A01 Apropriar-se da escuta e da fala.",
            "EF05MA03 (repetido intencionalmente).",
            "linha sem nenhum codigo oficial",
        ],
    )
    assert validate.extract_codes(pdf) == ["EF05MA03", "EI03A01", "EM13CHS101"]


def test_pdf_sem_codigos(tmp_path: Path) -> None:
    """PDF sem códigos oficiais retorna lista vazia."""
    pdf = _make_pdf(tmp_path / "vazio.pdf", ["documento sem habilidades"])
    assert validate.extract_codes(pdf) == []


def test_relatorio_aponta_divergencias(
    tmp_path: Path,
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
) -> None:
    """Task 4.2: relatório aponta divergências (a mais no banco / ausentes)."""
    payload = write_payload(tmp_path / "bncc.json", minimal_items)
    pipeline.run_ingest(raw_file=payload)

    pdf = _make_pdf(
        tmp_path / "parcial.pdf",
        [
            "EI03A01 apropriar-se da escuta.",
            "EF07LP99 codigo que nao esta no gold.",
        ],
    )
    report = validate.run_validation(pdf)
    assert report["identical"] is False
    assert report["count_pdf"] == 2
    assert report["count_gold"] == 3
    assert report["only_in_gold"] == ["EF05MA03", "EM13CHS101"]
    assert report["only_in_pdf"] == ["EF07LP99"]


def test_relatorio_identico(
    tmp_path: Path,
    gold_db: Path,
    minimal_items: list[dict[str, Any]],
) -> None:
    """PDF com exatamente os códigos do gold: sem divergências."""
    payload = write_payload(tmp_path / "bncc.json", minimal_items)
    pipeline.run_ingest(raw_file=payload)
    pdf = _make_pdf(
        tmp_path / "completo.pdf",
        ["EI03A01 a.", "EF05MA03 b.", "EM13CHS101 c."],
    )
    report = validate.run_validation(pdf)
    assert report["identical"] is True
    assert report["only_in_pdf"] == []
    assert report["only_in_gold"] == []
