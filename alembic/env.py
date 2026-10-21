"""Ambiente de migração Alembic para o gold SQLite do bncc-match.

A URL do banco vem da variável de ambiente ``BNCC_DB_PATH`` (mesma fonte
de ``bncc_match.db``), de modo que ``alembic upgrade head`` atue no mesmo
SQLite usado pela aplicação e pelos testes.
"""

import os
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

import bncc_match.models  # noqa: F401  # registra as tables no Base.metadata
from alembic import context
from bncc_match.db import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _db_path() -> str:
    """Caminho do SQLite (override por ``BNCC_DB_PATH``)."""
    return os.environ.get("BNCC_DB_PATH", "data/gold/curriculum.db")


def run_migrations_offline() -> None:
    """Executa migrações em modo offline (só emite SQL)."""
    context.configure(
        url=f"sqlite:///{_db_path()}",
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Executa migrações conectando ao SQLite do gold."""
    path = _db_path()
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        url=f"sqlite:///{path}",
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
