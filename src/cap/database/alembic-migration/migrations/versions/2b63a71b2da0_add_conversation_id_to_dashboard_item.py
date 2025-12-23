"""add conversation_id to dashboard_item

Revision ID: 2b63a71b2da0
Revises: 3ea19d5efaa6
Create Date: 2025-12-19 17:22:52.749588
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "2b63a71b2da0"
down_revision: Union[str, Sequence[str], None] = "3ea19d5efaa6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1) add nullable conversation_id column
    op.add_column(
        "dashboard_item",
        sa.Column("conversation_id", sa.Integer(), nullable=True),
    )

    # 2) index for faster lookups / joins
    op.create_index(
        "ix_dashboard_item_conversation_id",
        "dashboard_item",
        ["conversation_id"],
    )

    # 3) FK to conversation.id
    op.create_foreign_key(
        "fk_dashboard_item_conversation_id",
        "dashboard_item",
        "conversation",
        ["conversation_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_dashboard_item_conversation_id",
        "dashboard_item",
        type_="foreignkey",
    )
    op.drop_index(
        "ix_dashboard_item_conversation_id",
        table_name="dashboard_item",
    )
    op.drop_column("dashboard_item", "conversation_id")
