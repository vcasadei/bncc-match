"""Catálogo (manifesto) da data lake, armazenado em DuckDB.

O manifesto indexa cada arquivo da lake (bronze/silver) com metadados —
layer, path, source, partição, schema, row_count, hash — de modo que os
arquivos sejam localizáveis e inspecionáveis sem varredura de diretório.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from bncc_match.data_lake.paths import resolve_root

_CATALOG_SUBDIR = ".catalog"
_CATALOG_FILE = "lake.duckdb"

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS manifest (
    layer TEXT NOT NULL,
    path TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    partition_date TEXT NOT NULL,
    schema_json TEXT,
    row_count INTEGER NOT NULL,
    content_hash TEXT,
    created_at TEXT NOT NULL
)
"""


def catalog_path(root: Path | None = None) -> Path:
    """Caminho do arquivo de catálogo DuckDB da lake.

    Args:
        root: Raiz da lake; ``None`` usa o raiz padrão.

    Returns:
        Path do arquivo ``lake.duckdb``.
    """
    return resolve_root(root) / _CATALOG_SUBDIR / _CATALOG_FILE


def init(root: Path | None = None) -> Path:
    """Cria o catálogo DuckDB e a tabela ``manifest`` (idempotente).

    Args:
        root: Raiz da lake; ``None`` usa o raiz padrão.

    Returns:
        Path do catálogo criado.
    """
    catalog = catalog_path(root)
    catalog.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(catalog))
    try:
        con.execute(_CREATE_TABLE)
    finally:
        con.close()
    return catalog


def _connect(root: Path | None) -> duckdb.DuckDBPyConnection:
    """Abre conexão com o catálogo, inicializando-o se necessário."""
    if not catalog_path(root).exists():
        init(root)
    return duckdb.connect(str(catalog_path(root)))


def register(
    layer: str,
    path: str,
    source: str,
    partition_date: str,
    schema_json: str,
    row_count: int,
    content_hash: str,
    root: Path | None = None,
) -> None:
    """Registra (ou atualiza) um arquivo da lake no manifesto.

    Args:
        layer: Camada (``bronze`` ou ``silver``).
        path: Caminho do arquivo.
        source: Fonte de origem do dado.
        partition_date: Partição (``YYYY-MM-DD``).
        schema_json: Schema serializado em JSON.
        row_count: Número de linhas/registros do arquivo.
        content_hash: Hash (SHA-256) do conteúdo do arquivo.
        root: Raiz da lake; ``None`` usa o raiz padrão.
    """
    created_at = datetime.now(UTC).isoformat()
    con = _connect(root)
    try:
        con.execute(
            """
            INSERT INTO manifest
                (layer, path, source, partition_date, schema_json,
                 row_count, content_hash, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (path) DO UPDATE SET
                layer = excluded.layer,
                source = excluded.source,
                partition_date = excluded.partition_date,
                schema_json = excluded.schema_json,
                row_count = excluded.row_count,
                content_hash = excluded.content_hash,
                created_at = excluded.created_at
            """,
            [
                layer,
                path,
                source,
                partition_date,
                schema_json,
                row_count,
                content_hash,
                created_at,
            ],
        )
    finally:
        con.close()


def list_files(root: Path | None = None) -> list[dict[str, Any]]:
    """Lista todos os arquivos registrados no manifesto.

    Args:
        root: Raiz da lake; ``None`` usa o raiz padrão.

    Returns:
        Lista de dicionários com os metadados de cada arquivo.
    """
    con = _connect(root)
    try:
        rows = con.execute(
            """
            SELECT layer, path, source, partition_date, row_count,
                   content_hash, created_at
            FROM manifest ORDER BY created_at, path
            """
        ).fetchall()
    finally:
        con.close()
    return [
        {
            "layer": row[0],
            "path": row[1],
            "source": row[2],
            "partition_date": row[3],
            "row_count": row[4],
            "content_hash": row[5],
            "created_at": row[6],
        }
        for row in rows
    ]


def _table_expr(path: Path) -> str:
    """Monta a expressão de leitura DuckDB para um arquivo da lake.

    Args:
        path: Caminho do arquivo (JSONL ou Parquet).

    Returns:
        Expressão SQL (``read_json_auto``/``read_parquet``/literal).
    """
    quoted = str(path).replace("'", "''")
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        return f"read_json_auto('{quoted}')"
    if suffix == ".parquet":
        return f"read_parquet('{quoted}')"
    return f"'{quoted}'"


def _describe(path: Path) -> list[dict[str, str]]:
    """Descreve o schema (colunas + tipos) de um arquivo da lake.

    Args:
        path: Caminho do arquivo (JSONL ou Parquet).

    Returns:
        Lista de dicionários ``{"name", "type"}``.
    """
    con = duckdb.connect()
    try:
        cols = con.execute(f"DESCRIBE SELECT * FROM {_table_expr(path)}").fetchall()
    finally:
        con.close()
    return [{"name": c[0], "type": c[1]} for c in cols]


def schema_json(path: Path | str) -> str:
    """Retorna o schema de um arquivo da lake serializado em JSON.

    Args:
        path: Caminho do arquivo (JSONL ou Parquet).

    Returns:
        String JSON com a lista de colunas (nome + tipo).
    """
    return json.dumps(_describe(Path(path)), ensure_ascii=False)


def inspect(path: Path | str) -> dict[str, Any]:
    """Inspeciona um arquivo da lake: schema e contagem de linhas.

    Args:
        path: Caminho do arquivo (JSONL ou Parquet).

    Returns:
        Dicionário com ``path``, ``columns`` e ``row_count``.
    """
    p = Path(path)
    table = _table_expr(p)
    con = duckdb.connect()
    try:
        cols = con.execute(f"DESCRIBE SELECT * FROM {table}").fetchall()
        count_row = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        count = count_row[0] if count_row is not None else 0
    finally:
        con.close()
    return {
        "path": str(p),
        "columns": [{"name": c[0], "type": c[1]} for c in cols],
        "row_count": count,
    }


def head(path: Path | str, n: int = 5) -> list[dict[str, Any]]:
    """Retorna as primeiras ``n`` linhas de um arquivo da lake via DuckDB.

    Args:
        path: Caminho do arquivo (JSONL ou Parquet).
        n: Número de linhas a retornar (default 5).

    Returns:
        Lista de dicionários (uma por linha) com as primeiras linhas.
    """
    p = Path(path)
    table = _table_expr(p)
    con = duckdb.connect()
    try:
        cols = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM {table}").fetchall()]
        rows = con.execute(f"SELECT * FROM {table} LIMIT {int(n)}").fetchall()
    finally:
        con.close()
    return [dict(zip(cols, row)) for row in rows]
