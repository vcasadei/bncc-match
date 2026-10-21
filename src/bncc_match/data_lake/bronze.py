"""Camada bronze: payloads brutos em JSONL, imutáveis e particionados por data.

Cada escrita cria um novo arquivo (uuid) — nunca sobrescreve — e registra
seus metadados no manifesto (source, hash, row_count, partição).
"""

import hashlib
import uuid
from datetime import date
from pathlib import Path

import duckdb

from bncc_match.data_lake import manifest
from bncc_match.data_lake.paths import resolve_root


def _content_hash(path: Path) -> str:
    """Calcula o SHA-256 do conteúdo de um arquivo.

    Args:
        path: Caminho do arquivo.

    Returns:
        Hex digest SHA-256 do conteúdo.
    """
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _bronze_pattern(source: str, root: Path | None = None) -> str:
    """Gera o glob de arquivos bronze de uma fonte.

    Args:
        source: Identificador da fonte.
        root: Raiz da lake; ``None`` usa o raiz padrão.

    Returns:
        Padrão de caminho (com aspas escapadas) para funções de leitura.
    """
    r = resolve_root(root)
    return str(r / "bronze" / source / "*" / "*.jsonl").replace("'", "''")


def write(
    source: str,
    payload_lines: list[str],
    root: Path | None = None,
) -> Path:
    """Escreve um payload bruto em JSONL na camada bronze e registra no manifesto.

    O arquivo é imutável e particionado por data:
    ``data/bronze/<source>/<YYYY-MM-DD>/<uuid>.jsonl``.

    Args:
        source: Identificador da fonte (ex.: ``bncc``).
        payload_lines: Objetos JSON (strings), um por linha.
        root: Raiz da lake; ``None`` usa o raiz padrão.

    Returns:
        Path do arquivo JSONL escrito.

    Raises:
        ValueError: Se ``payload_lines`` estiver vazio.
    """
    if not payload_lines:
        raise ValueError("payload_lines não pode ser vazio")

    r = resolve_root(root)
    partition = date.today().strftime("%Y-%m-%d")
    out_dir = r / "bronze" / source / partition
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{uuid.uuid4()}.jsonl"

    with out_path.open("w", encoding="utf-8") as fh:
        for line in payload_lines:
            fh.write(line if line.endswith("\n") else line + "\n")

    manifest.register(
        layer="bronze",
        path=str(out_path),
        source=source,
        partition_date=partition,
        schema_json=manifest.schema_json(out_path),
        row_count=len(payload_lines),
        content_hash=_content_hash(out_path),
        root=r,
    )
    return out_path


def read(source: str, root: Path | None = None) -> duckdb.DuckDBPyConnection:
    """Retorna a relação DuckDB com todos os registros da fonte no bronze.

    Args:
        source: Identificador da fonte.
        root: Raiz da lake; ``None`` usa o raiz padrão.

    Returns:
        Resultado da consulta DuckDB (suporta ``.fetchall()``) sobre os JSONL.
    """
    con = duckdb.connect()
    return con.execute(
        f"SELECT * FROM read_json_auto('{_bronze_pattern(source, root)}')"
    )


def describe(source: str, root: Path | None = None) -> list[str]:
    """Retorna os nomes das colunas inferidas do bronze de uma fonte.

    Args:
        source: Identificador da fonte.
        root: Raiz da lake; ``None`` usa o raiz padrão.

    Returns:
        Lista de nomes de colunas, na ordem do JSON.
    """
    con = duckdb.connect()
    try:
        cols = con.execute(
            f"DESCRIBE SELECT * FROM read_json_auto('{_bronze_pattern(source, root)}')"
        ).fetchall()
    finally:
        con.close()
    return [c[0] for c in cols]
