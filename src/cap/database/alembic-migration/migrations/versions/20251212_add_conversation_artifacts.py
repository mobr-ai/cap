"""add conversation artifacts

Revision ID: 20251212_add_conversation_artifacts
Revises: 20251211_add_user_conversations
Create Date: 2025-12-12
"""

from alembic import op
import sqlalchemy as sa


revision = "20251212_add_conversation_artifacts"
down_revision = "20251211_add_user_conversations"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "conversation_artifact",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "conversation_id",
            sa.Integer(),
            sa.ForeignKey("conversation.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "nl_query_id",
            sa.Integer(),
            sa.ForeignKey("query_metrics.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "conversation_message_id",
            sa.Integer(),
            sa.ForeignKey("conversation_message.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("artifact_type", sa.String(length=50), nullable=False),
        sa.Column("kv_type", sa.String(length=50), nullable=True),
        sa.Column("config", sa.JSON(), nullable=False),
        sa.Column("artifact_hash", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("NOW()")),
    )

    op.create_index(
        "idx_conversation_artifact_convo_created",
        "conversation_artifact",
        ["conversation_id", "created_at"],
        unique=False,
    )

    op.create_index(
        "uq_conversation_artifact_convo_hash",
        "conversation_artifact",
        ["conversation_id", "artifact_hash"],
        unique=True,
    )

    op.create_index(
        "ix_conversation_artifact_conversation_id",
        "conversation_artifact",
        ["conversation_id"],
        unique=False,
    )

    op.create_index(
        "ix_conversation_artifact_nl_query_id",
        "conversation_artifact",
        ["nl_query_id"],
        unique=False,
    )

    op.create_index(
        "ix_conversation_artifact_conversation_message_id",
        "conversation_artifact",
        ["conversation_message_id"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_conversation_artifact_conversation_message_id", table_name="conversation_artifact")
    op.drop_index("ix_conversation_artifact_nl_query_id", table_name="conversation_artifact")
    op.drop_index("ix_conversation_artifact_conversation_id", table_name="conversation_artifact")
    op.drop_index("uq_conversation_artifact_convo_hash", table_name="conversation_artifact")
    op.drop_index("idx_conversation_artifact_convo_created", table_name="conversation_artifact")
    op.drop_table("conversation_artifact")
