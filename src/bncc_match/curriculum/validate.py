"""Validação do gold contra o PDF oficial da BNCC.

O PDF oficial (publicado pelo MEC) é a referência canônica de integridade:
o script extrai os códigos de habilidade do PDF e compara com os códigos
ativos do banco, gerando um relatório de divergências. Não faz parte do
caminho de ingestão — é um script separado (``bncc validate``).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pdfplumber
from sqlalchemy.orm import Session, sessionmaker

from bncc_match.curriculum.query import list_skill_codes
from bncc_match.db import default_session_factory

#: Códigos oficiais: ``EF<ano><disc><n>`` (Fundamental), ``EM13<área><n>``
#: (Médio) e ``EI<faixa><campo><n>`` (Infantil).
CODE_RE = re.compile(
    r"\b(?:EF\d{2}[A-Z]{2}\d{2,3}|EM13[A-Z]{2,3}\d{2,3}|EI\d{2}[A-Z]\d{2,3})\b"
)


def extract_codes(pdf_path: str | Path) -> list[str]:
    """Extrai os códigos de habilidade presentes em um PDF.

    Args:
        pdf_path: Caminho do PDF (oficial ou fixture sintética).

    Returns:
        Códigos únicos encontrados, em ordem alfabética.
    """
    found: set[str] = set()
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            found.update(CODE_RE.findall(text))
    return sorted(found)


def compare_codes(pdf_codes: list[str], gold_codes: list[str]) -> dict[str, Any]:
    """Compara os códigos do PDF com os do gold.

    Args:
        pdf_codes: Códigos extraídos do PDF.
        gold_codes: Códigos ativos no gold.

    Returns:
        Relatório com ``identical``, contagens e divergências
        (``only_in_pdf`` / ``only_in_gold``).
    """
    pdf_set = set(pdf_codes)
    gold_set = set(gold_codes)
    return {
        "identical": pdf_set == gold_set,
        "count_pdf": len(pdf_set),
        "count_gold": len(gold_set),
        "only_in_pdf": sorted(pdf_set - gold_set),
        "only_in_gold": sorted(gold_set - pdf_set),
    }


def run_validation(
    pdf_path: str | Path,
    session_factory: sessionmaker[Session] | None = None,
) -> dict[str, Any]:
    """Executa a validação completa (PDF x gold) e devolve o relatório.

    Args:
        pdf_path: Caminho do PDF oficial.
        session_factory: Fábrica de sessões; ``None`` usa o gold padrão.

    Returns:
        Relatório de :func:`compare_codes` com ``pdf_path`` incluído.
    """
    factory = (
        session_factory if session_factory is not None else default_session_factory()
    )
    report = compare_codes(
        extract_codes(pdf_path), list_skill_codes(session_factory=factory)
    )
    report["pdf_path"] = str(pdf_path)
    return report
