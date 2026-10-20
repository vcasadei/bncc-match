"""Resolução do caminho raiz da data lake."""

import os
from pathlib import Path


def lake_root() -> Path:
    """Retorna o diretório raiz da data lake.

    O raiz é controlado pela variável de ambiente ``BNCC_LAKE_ROOT``;
    na ausência, usa ``data`` relativo ao diretório de trabalho atual.

    Returns:
        Path do diretório raiz da data lake.
    """
    return Path(os.environ.get("BNCC_LAKE_ROOT", "data"))


def resolve_root(root: Path | None) -> Path:
    """Retorna ``root`` se informado, senão o raiz padrão da lake.

    Args:
        root: Raiz explícito; ``None`` usa :func:`lake_root`.

    Returns:
        Path a ser usado como raiz da lake.
    """
    return Path(root) if root is not None else lake_root()
