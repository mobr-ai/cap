"""add created_at to waiting_list

Revision ID: 20251213_add_created_at_to_waiting_list
Revises: 20251205_add_admin_settings_table
Create Date: 2025-12-13
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20251213_add_created_at_to_waiting_list"
down_revision = "20251205_admin_settings"
branch_labels = None
depends_on = None


def upgrade():
    # Add column with default
    op.add_column(
        "waiting_list",
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )

    # Optional: index for admin ordering / analytics
    op.create_index(
        "ix_waiting_list_created_at",
        "waiting_list",
        ["created_at"],
    )


def downgrade():
    op.drop_index("ix_waiting_list_created_at", table_name="waiting_list")
    op.drop_column("waiting_list", "created_at")
