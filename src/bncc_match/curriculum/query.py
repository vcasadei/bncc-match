"""Consulta da árvore curricular por código de habilidade.

``get_skill`` é a API interna usada pelas capabilities seguintes
(descoberta/classificação de vídeos): recebe um código oficial e devolve
a habilidade com a cadeia completa de ancestrais, da etapa até o nó-pai.
"""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from bncc_match.db import default_session_factory
from bncc_match.models import CurriculumNode, Skill


def _ancestors(session: Session, node_id: int | None) -> list[dict[str, Any]]:
    """Caminha a árvore de um nó até a raiz.

    Args:
        session: Sessão do gold.
        node_id: Nó a partir do qual caminhar (nó da habilidade).

    Returns:
        Lista de ancestrais em ordem raiz → folha (exclui o próprio nó).
        Cada entrada tem ``type``, ``name``, ``stage`` e ``extras``.
    """
    chain: list[dict[str, Any]] = []
    current: int | None = node_id
    while current is not None:
        node = session.get(CurriculumNode, current)
        if node is None:
            break
        extras: Any = {}
        if node.extras_json:
            try:
                extras = json.loads(node.extras_json)
            except json.JSONDecodeError:
                extras = {}
        chain.append(
            {
                "type": node.type,
                "name": node.name,
                "stage": node.stage,
                "extras": extras if isinstance(extras, dict) else {},
            }
        )
        current = node.parent_id
    chain.reverse()
    return chain


def get_skill(
    code: str,
    session_factory: sessionmaker[Session] | None = None,
) -> dict[str, Any] | None:
    """Retorna a habilidade pelo código oficial, com a hierarquia completa.

    Args:
        code: Código oficial (ex.: ``EF05MA03``, ``EM13CHS101``,
            ``EI03A01``).
        session_factory: Fábrica de sessões; ``None`` usa o gold padrão.

    Returns:
        Dicionário com ``code``, ``text``, ``year``, ``stage``, ``status``
        e ``ancestors`` (da raiz até o nó-pai); ``None`` se o código não
        existir no gold (consulta inexistente não levanta erro).
    """
    factory = (
        session_factory if session_factory is not None else default_session_factory()
    )
    with factory() as session:
        skill = session.get(Skill, code)
        if skill is None:
            return None
        # Caminha a partir do PAI do nó da habilidade: os ancestrais
        # excluem o nó próprio (type == "skill").
        parent_id: int | None = None
        if skill.node_id is not None:
            skill_node = session.get(CurriculumNode, skill.node_id)
            if skill_node is not None:
                parent_id = skill_node.parent_id
        ancestors = _ancestors(session, parent_id)
        stage = ancestors[0]["stage"] if ancestors else ""
        return {
            "code": skill.code,
            "text": skill.text,
            "year": skill.year,
            "stage": stage,
            "status": skill.status,
            "ancestors": ancestors,
        }


def list_skill_codes(
    session_factory: sessionmaker[Session] | None = None,
) -> list[str]:
    """Lista os códigos das habilidades ativas no gold.

    Args:
        session_factory: Fábrica de sessões; ``None`` usa o gold padrão.

    Returns:
        Códigos em ordem alfabética.
    """
    factory = (
        session_factory if session_factory is not None else default_session_factory()
    )
    with factory() as session:
        stmt = select(Skill.code).where(Skill.status == "active").order_by(Skill.code)
        return list(session.execute(stmt).scalars().all())
