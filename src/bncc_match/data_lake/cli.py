"""CLI ``data`` para inspeção da data lake via DuckDB.

Subcomandos:
    ls       lista os arquivos registrados no manifesto.
    inspect  mostra schema e row count de um arquivo.
    head     mostra as primeiras linhas de um arquivo.
"""

import argparse

from bncc_match.data_lake import manifest


def _ls(_args: argparse.Namespace) -> int:
    """Lista os arquivos da lake.

    Args:
        _args: Argumentos do subcomando (não utilizado).

    Returns:
        0 em caso de sucesso.
    """
    rows = manifest.list_files()
    if not rows:
        print("(lake vazia)")
        return 0
    print(f"{'layer':<8} {'source':<16} {'partition':<12} {'rows':>6}  path")
    for row in rows:
        print(
            f"{row['layer']:<8} {row['source']:<16} {row['partition_date']:<12} "
            f"{row['row_count']:>6}  {row['path']}"
        )
    return 0


def _inspect(args: argparse.Namespace) -> int:
    """Inspeciona um arquivo da lake (schema + row count).

    Args:
        args: Argumentos do subcomando (``path``).

    Returns:
        0 em caso de sucesso.
    """
    info = manifest.inspect(args.path)
    print(f"path: {info['path']}")
    print(f"row_count: {info['row_count']}")
    print("columns:")
    for col in info["columns"]:
        print(f"  {col['name']}: {col['type']}")
    return 0


def _head(args: argparse.Namespace) -> int:
    """Exibe as primeiras linhas de um arquivo da lake.

    Args:
        args: Argumentos do subcomando (``path``, ``n``).

    Returns:
        0 em caso de sucesso.
    """
    for row in manifest.head(args.path, args.n):
        print(row)
    return 0


def main(argv: list[str] | None = None) -> int:
    """Ponto de entrada da CLI ``data``.

    Args:
        argv: Lista de argumentos (``None`` usa ``sys.argv``).

    Returns:
        Código de saída (0 = sucesso).
    """
    parser = argparse.ArgumentParser(prog="data", description="Inspeção da data lake")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("ls", help="lista os arquivos da lake")

    inspect_p = subparsers.add_parser(
        "inspect", help="schema e row count de um arquivo"
    )
    inspect_p.add_argument("path", help="caminho do arquivo")

    head_p = subparsers.add_parser("head", help="primeiras linhas de um arquivo")
    head_p.add_argument("path", help="caminho do arquivo")
    head_p.add_argument("n", nargs="?", type=int, default=5, help="número de linhas")

    args = parser.parse_args(argv)
    if args.command == "ls":
        return _ls(args)
    if args.command == "inspect":
        return _inspect(args)
    return _head(args)
