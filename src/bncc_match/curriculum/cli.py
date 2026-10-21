"""CLI ``bncc`` para ingestão, validação e consulta do currículo BNCC.

Subcomandos:
    ingest    fetch → bronze → silver → gold.
    validate  valida o gold contra o PDF oficial.
    get       consulta uma habilidade por código.
"""

import argparse
import sys
from pathlib import Path

import httpx

from bncc_match.curriculum import query, validate
from bncc_match.curriculum.pipeline import run_ingest
from bncc_match.curriculum.source import DEFAULT_SOURCE_URL


def _ingest(args: argparse.Namespace) -> int:
    """Executa a ingestão e imprime o resumo.

    Args:
        args: Argumentos do subcomando (``url``, ``version``, ``file``).

    Returns:
        0 em caso de sucesso; 1 em falha.
    """
    try:
        result = run_ingest(
            url=args.url,
            source_version=args.version,
            raw_file=args.file,
        )
    except (httpx.HTTPError, ValueError, OSError) as exc:
        print(f"erro na ingestão: {exc}", file=sys.stderr)
        return 1
    silver_result = result["silver"]
    gold_result = result["gold"]
    print(f"bronze: {result['bronze_path']}")
    print(
        f"silver: {silver_result['row_count']} linhas"
        f" ({silver_result['rejected']} rejeitadas)"
    )
    print(
        f"gold: {gold_result['nodes']} nós, {gold_result['skills']} habilidades"
        f" ({gold_result['absent']} marcadas ausentes), run_id={gold_result['run_id']}"
    )
    return 0


def _validate(args: argparse.Namespace) -> int:
    """Valida o gold contra um PDF e imprime o relatório.

    Args:
        args: Argumentos do subcomando (``pdf``).

    Returns:
        0 sem divergências; 2 com divergências; 1 em erro.
    """
    pdf_path = Path(args.pdf)
    if not pdf_path.is_file():
        print(f"PDF não encontrado: {pdf_path}", file=sys.stderr)
        return 1
    report = validate.run_validation(pdf_path)
    print(f"códigos no PDF: {report['count_pdf']} | no gold: {report['count_gold']}")
    if report["identical"]:
        print("OK: sem divergências entre PDF e gold")
        return 0
    only_pdf = report["only_in_pdf"]
    only_gold = report["only_in_gold"]
    print(f"somente no PDF ({len(only_pdf)}): {', '.join(only_pdf) or '-'}")
    print(f"somente no gold ({len(only_gold)}): {', '.join(only_gold) or '-'}")
    return 2


def _get(args: argparse.Namespace) -> int:
    """Consulta uma habilidade por código e imprime a hierarquia.

    Args:
        args: Argumentos do subcomando (``code``).

    Returns:
        0 se encontrada; 1 se o código não existir.
    """
    result = query.get_skill(args.code)
    if result is None:
        print(f"código não encontrado: {args.code}", file=sys.stderr)
        return 1
    print(f"código: {result['code']}")
    print(
        f"etapa: {result['stage']} | ano: {result['year']} | status: {result['status']}"
    )
    print("hierarquia (raiz → nó-pai):")
    for ancestor in result["ancestors"]:
        print(f"  {ancestor['type']}: {ancestor['name']}")
    print(f"texto: {result['text']}")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Ponto de entrada da CLI ``bncc``.

    Args:
        argv: Lista de argumentos (``None`` usa ``sys.argv``).

    Returns:
        Código de saída.
    """
    parser = argparse.ArgumentParser(
        prog="bncc",
        description="Ingestão, validação e consulta do currículo BNCC",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_p = subparsers.add_parser(
        "ingest", help="ingestão completa (fetch → bronze → silver → gold)"
    )
    ingest_p.add_argument("--url", default=DEFAULT_SOURCE_URL, help="endpoint da fonte")
    ingest_p.add_argument(
        "--version", default=None, help="versão da fonte (proveniência)"
    )
    ingest_p.add_argument(
        "--file", default=None, help="arquivo JSON local (no lugar do fetch)"
    )

    validate_p = subparsers.add_parser(
        "validate", help="valida o gold contra o PDF oficial"
    )
    validate_p.add_argument("pdf", help="caminho do PDF oficial da BNCC")

    get_p = subparsers.add_parser("get", help="consulta uma habilidade por código")
    get_p.add_argument("code", help="código oficial (ex.: EF05MA03)")

    args = parser.parse_args(argv)
    if args.command == "ingest":
        return _ingest(args)
    if args.command == "validate":
        return _validate(args)
    return _get(args)


if __name__ == "__main__":
    raise SystemExit(main())
