"""Camada silver: transformação do bronze em Parquet validado e deduplicado.

Lê o JSONL de bronze via DuckDB, aplica funções de validação e transformação
fornecidas pelo domínio, e grava Parquet colunar em ``data/silver/<domain>/``,
registrando o resultado no manifesto.
"""

import hashlib
import uuid
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from bncc_match.data_lake import bronze, manifest
from bncc_match.data_lake.paths import resolve_root

TransformFn = Callable[[dict[str, Any]], dict[str, Any] | None]
ValidateFn = Callable[[dict[str, Any]], bool]


def _content_hash(path: Path) -> str:
    """Calcula o SHA-256 do conteúdo de um arquivo.

    Args:
        path: Caminho do arquivo.

    Returns:
        Hex digest SHA-256 do conteúdo.
    """
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _duckdb_type(value: Any) -> str:
    """Mapeia o tipo de um valor Python para um tipo DuckDB.

    Args:
        value: Valor a inspecionar.

    Returns:
        Nome do tipo DuckDB correspondente.
    """
    if value is None:
        return "VARCHAR"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE"
    if isinstance(value, str):
        return "VARCHAR"
    return "JSON"


def _column_type(column: str, records: list[dict[str, Any]]) -> str:
    """Inferência do tipo de uma coluna a partir de todos os registros.

    Args:
        column: Nome da coluna.
        records: Lista de registros (mesmas chaves).

    Returns:
        Nome do tipo DuckDB da coluna.
    """
    types = {
        _duckdb_type(record.get(column))
        for record in records
        if record.get(column) is not None
    }
    if not types:
        return "VARCHAR"
    if {"BIGINT", "DOUBLE"} <= types:
        return "DOUBLE"
    return types.pop() if len(types) == 1 else "VARCHAR"


def _write_parquet(records: list[dict[str, Any]], path: Path) -> list[str]:
    """Escreve registros (dicionários) em um arquivo Parquet via DuckDB.

    Cria uma tabela temporária com tipos inferidos, insere os registros e
    exporta para Parquet.

    Args:
        records: Lista de dicionários (mesmas chaves), não vazia.
        path: Caminho de destino do arquivo Parquet.

    Returns:
        Lista de nomes de colunas, na ordem gravada.
    """
    columns = list(records[0].keys())
    defs = ", ".join(f'"{c}" {_column_type(c, records)}' for c in columns)
    con = duckdb.connect()
    try:
        con.execute(f"CREATE TABLE _staging ({defs})")
        placeholders = ", ".join("?" for _ in columns)
        rows = [tuple(record.get(c) for c in columns) for record in records]
        con.executemany(f"INSERT INTO _staging VALUES ({placeholders})", rows)
        con.execute(f"COPY _staging TO '{path}' (FORMAT PARQUET)")
    finally:
        con.close()
    return columns


def transform(
    domain: str,
    source: str,
    transform_fn: TransformFn,
    validate_fn: ValidateFn,
    root: Path | None = None,
) -> dict[str, Any]:
    """Transforma o bronze de ``source`` em silver Parquet para ``domain``.

    Lê os registros do bronze via DuckDB, descarta os que falham em
    ``validate_fn``, aplica ``transform_fn`` aos válidos, e grava Parquet em
    ``data/silver/<domain>/<...>.parquet``, registrando no manifesto.

    Args:
        domain: Domínio de destino (ex.: ``curriculum``).
        source: Fonte no bronze (ex.: ``bncc``).
        transform_fn: Normaliza um registro válido; pode retornar ``None``.
        validate_fn: Decide se um registro é válido.
        root: Raiz da lake; ``None`` usa o raiz padrão.

    Returns:
        Dicionário com ``path``, ``row_count``, ``rejected`` e ``columns``.
    """
    r = resolve_root(root)

    # Fonte bronze sem arquivos: nada a transformar (não é erro).
    if not list((r / "bronze" / source).glob("*/*.jsonl")):
        return {"path": None, "row_count": 0, "rejected": 0, "columns": []}

    names = bronze.describe(source, root=r)
    rows = bronze.read(source, root=r).fetchall()
    records = [dict(zip(names, row)) for row in rows]

    valid_records: list[dict[str, Any]] = []
    rejected = 0
    for record in records:
        if not validate_fn(record):
            rejected += 1
            continue
        transformed = transform_fn(record)
        if transformed is None:
            rejected += 1
            continue
        valid_records.append(transformed)

    if not valid_records:
        return {"path": None, "row_count": 0, "rejected": rejected, "columns": []}

    partition = date.today().strftime("%Y-%m-%d")
    out_dir = r / "silver" / domain
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{partition}-{uuid.uuid4()}.parquet"
    columns = _write_parquet(valid_records, out_path)

    manifest.register(
        layer="silver",
        path=str(out_path),
        source=source,
        partition_date=partition,
        schema_json=manifest.schema_json(out_path),
        row_count=len(valid_records),
        content_hash=_content_hash(out_path),
        root=r,
    )
    return {
        "path": str(out_path),
        "row_count": len(valid_records),
        "rejected": rejected,
        "columns": columns,
    }


def read(domain: str, root: Path | None = None) -> duckdb.DuckDBPyConnection:
    """Retorna a relação DuckDB sobre o silver de um domínio.

    Args:
        domain: Domínio no silver (ex.: ``curriculum``).
        root: Raiz da lake; ``None`` usa o raiz padrão.

    Returns:
        Resultado da consulta DuckDB (suporta ``.fetchall()``) sobre o Parquet.
    """
    r = resolve_root(root)
    pattern = str(r / "silver" / domain / "*.parquet").replace("'", "''")
    con = duckdb.connect()
    return con.execute(f"SELECT * FROM read_parquet('{pattern}')")
