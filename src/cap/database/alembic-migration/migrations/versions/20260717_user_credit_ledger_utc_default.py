"""Use UTC default for user credit ledger timestamps.

Revision ID: 20260717_credit_ledger_utc
Revises: 202606300001
"""

from alembic import op
import sqlalchemy as sa


revision = "20260717_credit_ledger_utc"
down_revision = "202606300001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "user_credit_ledger",
        "created_at",
        existing_type=sa.DateTime(),
        nullable=False,
        server_default=sa.text("TIMEZONE('utc', NOW())"),
    )


def downgrade() -> None:
    op.alter_column(
        "user_credit_ledger",
        "created_at",
        existing_type=sa.DateTime(),
        nullable=False,
        server_default=sa.text("NOW()"),
    )
