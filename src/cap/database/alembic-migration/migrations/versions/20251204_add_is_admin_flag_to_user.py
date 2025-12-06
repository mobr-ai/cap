"""Add is_admin flag to user table

Revision ID: 20251204_add_is_admin_flag_to_user
Revises: 20251127_add_metrics_tables
Create Date: 2025-12-04 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# Revision identifiers, used by Alembic.
revision = "20251204_add_is_admin_flag"
down_revision = "20251127_add_metrics_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1) Add column with a server default so existing rows get a value
    op.add_column(
        "user",
        sa.Column(
            "is_admin",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )

    # 2) Optionally remove the default at the schema level
    #    so future inserts must be explicit in the ORM (or rely on model default).
    op.alter_column(
        "user",
        "is_admin",
        server_default=None,
        existing_type=sa.Boolean(),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.drop_column("user", "is_admin")
