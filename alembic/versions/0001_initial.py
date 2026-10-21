"""Initial curriculum schema.

Cria as tabelas do gold: ``curriculum_node``, ``skill`` (``code`` único
como chave natural) e ``ingestion_run``.

Revision ID: 0001
Revises:
Create Date: 2026-10-20
"""

import sqlalchemy as sa

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "curriculum_node",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "parent_id",
            sa.Integer(),
            sa.ForeignKey("curriculum_node.id"),
            nullable=True,
        ),
        sa.Column("stage", sa.String(), nullable=False),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("extras_json", sa.Text(), nullable=True),
    )
    op.create_table(
        "skill",
        sa.Column("code", sa.String(), primary_key=True),
        sa.Column(
            "node_id",
            sa.Integer(),
            sa.ForeignKey("curriculum_node.id"),
            nullable=True,
        ),
        sa.Column("text", sa.Text(), nullable=False, server_default=""),
        sa.Column("year", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(), nullable=False, server_default="active"),
    )
    op.create_table(
        "ingestion_run",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("source_url", sa.String(), nullable=False),
        sa.Column("source_version", sa.String(), nullable=True),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("ingestion_run")
    op.drop_table("skill")
    op.drop_table("curriculum_node")
