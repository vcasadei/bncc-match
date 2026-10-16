"""Testes de smoke: o pacote importa e o pyproject.toml é válido."""

import tomllib
from pathlib import Path

import bncc_match

_PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_package_imports() -> None:
    """O pacote importa e expõe uma versão semântica."""
    assert bncc_match.__version__ == "0.1.0"


def test_pyproject_is_valid_toml() -> None:
    """O pyproject.toml é TOML válido e declara o nome do pacote."""
    data = tomllib.loads((_PROJECT_ROOT / "pyproject.toml").read_text())
    assert data["project"]["name"] == "bncc_match"
