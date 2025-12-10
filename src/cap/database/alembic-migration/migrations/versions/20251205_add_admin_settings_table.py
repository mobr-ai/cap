"""add admin_setting table

Revision ID: 20251205_add_admin_settings_table
Revises: 20251204_add_is_admin_flag_to_user
Create Date: 2025-12-05

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "20251205_admin_settings"
down_revision = "20251204_add_is_admin_flag"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "admin_setting",
        sa.Column("key", sa.String(length=64), primary_key=True),
        sa.Column("value", sa.JSON(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("NOW()")),
    )


def downgrade():
    op.drop_table("admin_setting")
