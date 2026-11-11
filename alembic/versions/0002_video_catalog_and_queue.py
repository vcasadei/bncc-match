"""Video catalog and job queue schema.

Cria as tabelas do catálogo de vídeos (``video``, ``video_topic``,
``transcript``) e da fila persistente (``job_queue``, ``quota_state``).

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-20
"""

import sqlalchemy as sa

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "video",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("youtube_id", sa.String(), nullable=False, unique=True),
        sa.Column("title", sa.String(), nullable=False, server_default=""),
        sa.Column("channel_name", sa.String(), nullable=False, server_default=""),
        sa.Column("description", sa.Text(), nullable=False, server_default=""),
        sa.Column("duration_seconds", sa.Integer(), nullable=True),
        sa.Column("published_at", sa.DateTime(), nullable=True),
        sa.Column("url", sa.String(), nullable=False, server_default=""),
        sa.Column("thumbnail_url", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=False, server_default="discovered"),
    )
    op.create_table(
        "video_topic",
        sa.Column(
            "video_id", sa.Integer(), sa.ForeignKey("video.id"), primary_key=True
        ),
        sa.Column(
            "curriculum_node_id",
            sa.Integer(),
            sa.ForeignKey("curriculum_node.id"),
            primary_key=True,
        ),
    )
    op.create_table(
        "transcript",
        sa.Column(
            "video_id", sa.Integer(), sa.ForeignKey("video.id"), primary_key=True
        ),
        sa.Column("text", sa.Text(), nullable=False, server_default=""),
        sa.Column("language", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
    )
    op.create_table(
        "job_queue",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("job_type", sa.String(), nullable=False),
        sa.Column("reference_key", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("last_error", sa.String(), nullable=True),
        sa.Column("next_retry_at", sa.DateTime(), nullable=True),
        sa.Column("quota_consumed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("job_type", "reference_key", name="uq_job_type_ref"),
    )
    op.create_table(
        "quota_state",
        sa.Column("day", sa.String(), primary_key=True),
        sa.Column("units_used", sa.Integer(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    op.drop_table("quota_state")
    op.drop_table("job_queue")
    op.drop_table("transcript")
    op.drop_table("video_topic")
    op.drop_table("video")
