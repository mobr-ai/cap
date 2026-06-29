from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "add_telegram_guest_usage"
down_revision: str | Sequence[str] | None = "add_telegram_integration"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "telegram_guest_usage_period",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("telegram_user_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "feature_code",
            sa.String(64),
            server_default=sa.text("'telegram_guest_nl_query'"),
            nullable=False,
        ),
        sa.Column("period_start", sa.DateTime(), nullable=False),
        sa.Column("period_end", sa.DateTime(), nullable=False),
        sa.Column("used_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("limit_count", sa.Integer(), server_default=sa.text("3"), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("NOW()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("NOW()"), nullable=False),
        sa.UniqueConstraint(
            "telegram_user_id",
            "feature_code",
            "period_start",
            "period_end",
            name="uq_telegram_guest_usage_user_feature_window",
        ),
    )

    op.create_index(
        "idx_telegram_guest_usage_user_feature",
        "telegram_guest_usage_period",
        ["telegram_user_id", "feature_code"],
    )
    op.create_index(
        "idx_telegram_guest_usage_period_start",
        "telegram_guest_usage_period",
        ["period_start"],
    )
    op.create_index(
        "idx_telegram_guest_usage_period_end",
        "telegram_guest_usage_period",
        ["period_end"],
    )


def downgrade() -> None:
    op.drop_index("idx_telegram_guest_usage_period_end", table_name="telegram_guest_usage_period")
    op.drop_index("idx_telegram_guest_usage_period_start", table_name="telegram_guest_usage_period")
    op.drop_index("idx_telegram_guest_usage_user_feature", table_name="telegram_guest_usage_period")
    op.drop_table("telegram_guest_usage_period")
