"""Configuração do banco de dados (gold SQLite) do bncc-match.

Centraliza o caminho do banco (configurável por variável de ambiente),
o engine SQLAlchemy e a ``Base`` declarativa compartilhada pelos models
e pelas migrações Alembic.
"""

import os
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.pool import StaticPool

_ENV_DB_PATH = "BNCC_DB_PATH"
_DEFAULT_DB_PATH = "data/gold/curriculum.db"


class Base(DeclarativeBase):
    """Base declarativa comum a todos os models do gold."""


def db_path() -> Path:
    """Retorna o caminho do arquivo SQLite do gold.

    Controlado pela variável de ambiente ``BNCC_DB_PATH``; na ausência,
    usa ``data/gold/curriculum.db`` relativo ao diretório de trabalho.

    Returns:
        Path do arquivo SQLite.
    """
    return Path(os.environ.get(_ENV_DB_PATH, _DEFAULT_DB_PATH))


def make_engine(db_file: str | Path | None = None, in_memory: bool = False) -> Engine:
    """Cria um engine SQLAlchemy para o gold.

    Args:
        db_file: Caminho do SQLite; ``None`` usa :func:`db_path`.
        in_memory: Se ``True``, usa SQLite em memória (uma única conexão).

    Returns:
        Engine configurado (SQLite com WAL para leitura durante escrita).
    """
    if in_memory:
        # StaticPool mantém UMA conexão: memória compartilhada no in-memory.
        engine = create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
    else:
        path = Path(db_file) if db_file is not None else db_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        engine = create_engine(f"sqlite:///{path}")
    _enable_sqlite_wal(engine)
    return engine


def _enable_sqlite_wal(engine: Engine) -> None:
    """Habilita WAL no SQLite (leituras durante escrita, processo único).

    Args:
        engine: Engine SQLite a configurar.
    """
    if engine.dialect.name != "sqlite":
        return

    @event.listens_for(engine, "connect")
    def _on_connect(dbapi_conn: Any, _record: Any) -> None:
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Cria um ``sessionmaker`` vinculado a um engine.

    Args:
        engine: Engine SQLAlchemy a usar.

    Returns:
        Fábrica de sessões (``expire_on_commit=False`` para leitura pós-commit).
    """
    return sessionmaker(bind=engine, expire_on_commit=False)


def default_session_factory() -> sessionmaker[Session]:
    """Fábrica de sessões padrão para o gold configurado.

    Returns:
        Fábrica de sessões ligada ao engine de :func:`db_path`.
    """
    return make_session_factory(make_engine())
