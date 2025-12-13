"""add user conversations and link dashboard items

Revision ID: 20251211_add_user_conversations
Revises: 20251205_admin_settings
Create Date: 2025-12-11
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20251211_add_user_conversations"
down_revision = "20251205_admin_settings"
branch_labels = None
depends_on = None


def upgrade():
    # -------------------------
    # conversation
    # -------------------------
    op.create_table(
        "conversation",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "user_id",
            sa.Integer,
            sa.ForeignKey("user.user_id"),
            nullable=False,
        ),
        sa.Column("title", sa.String(length=255), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime,
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime,
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )
    op.create_index(
        "idx_conversation_user",
        "conversation",
        ["user_id"],
    )

    # -------------------------
    # conversation_message
    # -------------------------
    op.create_table(
        "conversation_message",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "conversation_id",
            sa.Integer,
            sa.ForeignKey("conversation.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "user_id",
            sa.Integer,
            sa.ForeignKey("user.user_id"),
            nullable=True,
        ),
        sa.Column("role", sa.String(length=50), nullable=False),
        sa.Column("content", sa.Text, nullable=False),
        sa.Column(
            "nl_query_id",
            sa.Integer,
            sa.ForeignKey("query_metrics.id"),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime,
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
    )

    op.create_index(
        "idx_conversation_message_conversation",
        "conversation_message",
        ["conversation_id"],
    )
    op.create_index(
        "idx_conversation_message_user",
        "conversation_message",
        ["user_id"],
    )
    op.create_index(
        "idx_conversation_message_nl_query",
        "conversation_message",
        ["nl_query_id"],
    )

    # -------------------------
    # dashboard_item → conversation_message
    # -------------------------
    op.add_column(
        "dashboard_item",
        sa.Column("conversation_message_id", sa.Integer, nullable=True),
    )

    op.create_foreign_key(
        "fk_dashboard_item_conversation_message",
        source_table="dashboard_item",
        referent_table="conversation_message",
        local_cols=["conversation_message_id"],
        remote_cols=["id"],
        ondelete="SET NULL",
    )

    op.create_index(
        "idx_dashboard_item_conversation_message",
        "dashboard_item",
        ["conversation_message_id"],
    )


def downgrade():
    # dashboard_item link
    op.drop_index(
        "idx_dashboard_item_conversation_message",
        table_name="dashboard_item",
    )
    op.drop_constraint(
        "fk_dashboard_item_conversation_message",
        "dashboard_item",
        type_="foreignkey",
    )
    op.drop_column("dashboard_item", "conversation_message_id")

    # conversation_message
    op.drop_index(
        "idx_conversation_message_nl_query",
        table_name="conversation_message",
    )
    op.drop_index(
        "idx_conversation_message_user",
        table_name="conversation_message",
    )
    op.drop_index(
        "idx_conversation_message_conversation",
        table_name="conversation_message",
    )
    op.drop_table("conversation_message")

    # conversation
    op.drop_index("idx_conversation_user", table_name="conversation")
    op.drop_table("conversation")

