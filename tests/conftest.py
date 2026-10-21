"""Fixtures compartilhadas dos testes da data lake."""

from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def lake_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isola cada teste em um diretório temporário para a data lake.

    Define ``BNCC_LAKE_ROOT`` para um ``tmp_path`` único, de modo que
    ``write``/``transform``/``manifest``/CLI usem um raiz limpo por teste.

    Args:
        tmp_path: Diretório temporário do pytest.
        monkeypatch: Ferramenta de patch do pytest.

    Returns:
        Path do raiz da lake temporário.
    """
    root = tmp_path / "data"
    monkeypatch.setenv("BNCC_LAKE_ROOT", str(root))
    return root
