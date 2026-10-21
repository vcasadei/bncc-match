"""Capability de currículo BNCC: ingestão, consulta e validação.

Ingesta os dados estruturados da BNCC (fonte: site oficial do MEC) para o
gold SQLite (árvore curricular + habilidades por código oficial), com
validação de integridade contra o PDF oficial.
"""

from bncc_match.curriculum.load import load_to_gold
from bncc_match.curriculum.pipeline import run_ingest
from bncc_match.curriculum.query import get_skill, list_skill_codes
from bncc_match.curriculum.source import DEFAULT_SOURCE_URL, fetch_raw, parse_raw
from bncc_match.curriculum.transform import normalize_raw, validate_raw
from bncc_match.curriculum.validate import compare_codes, extract_codes, run_validation

__all__ = [
    "DEFAULT_SOURCE_URL",
    "compare_codes",
    "extract_codes",
    "fetch_raw",
    "get_skill",
    "list_skill_codes",
    "load_to_gold",
    "normalize_raw",
    "parse_raw",
    "run_ingest",
    "run_validation",
    "validate_raw",
]
