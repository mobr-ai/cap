"""extend query_metrics for telegram and sql metrics

Revision ID: <new_revision>
Revises: 1a4dda3a21c3
Create Date: 2026-06-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "202606300001"
down_revision: str | Sequence[str] | None = (
    "20260626225720_merge_alembic_heads",
    "add_telegram_guest_usage",
)
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "query_metrics",
        sa.Column("telegram_account_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "query_metrics",
        sa.Column("telegram_user_id", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "query_metrics",
        sa.Column("telegram_chat_id", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "query_metrics",
        sa.Column(
            "request_source",
            sa.String(length=32),
            server_default=sa.text("'cap_web'"),
            nullable=False,
        ),
    )
    op.add_column(
        "query_metrics",
        sa.Column(
            "sql_query",
            sa.Text(),
            server_default=sa.text("''"),
            nullable=False,
        ),
    )
    op.add_column(
        "query_metrics",
        sa.Column("query_source", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "query_metrics",
        sa.Column(
            "has_sparql",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
    )
    op.add_column(
        "query_metrics",
        sa.Column(
            "has_sql",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
    )
    op.add_column(
        "query_metrics",
        sa.Column(
            "sql_valid",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
    )
    op.add_column(
        "query_metrics",
        sa.Column("sql_latency_ms", sa.Integer(), nullable=True),
    )

    op.create_foreign_key(
        "fk_query_metrics_telegram_account_id",
        "query_metrics",
        "telegram_account",
        ["telegram_account_id"],
        ["id"],
        ondelete="SET NULL",
    )

    op.create_index(
        "idx_query_metrics_telegram_user_date",
        "query_metrics",
        ["telegram_user_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "idx_query_metrics_telegram_account_date",
        "query_metrics",
        ["telegram_account_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "idx_query_metrics_request_source_date",
        "query_metrics",
        ["request_source", "created_at"],
        unique=False,
    )
    op.create_index(
        "idx_query_metrics_query_source_date",
        "query_metrics",
        ["query_source", "created_at"],
        unique=False,
    )

    op.execute("""
        UPDATE query_metrics
        SET
            has_sparql = COALESCE(NULLIF(TRIM(sparql_query), ''), '') <> '',
            has_sql = false,
            sql_query = ''
    """)


def downgrade() -> None:
    op.drop_index("idx_query_metrics_query_source_date", table_name="query_metrics")
    op.drop_index("idx_query_metrics_request_source_date", table_name="query_metrics")
    op.drop_index("idx_query_metrics_telegram_account_date", table_name="query_metrics")
    op.drop_index("idx_query_metrics_telegram_user_date", table_name="query_metrics")

    op.drop_constraint(
        "fk_query_metrics_telegram_account_id",
        "query_metrics",
        type_="foreignkey",
    )

    op.drop_column("query_metrics", "sql_latency_ms")
    op.drop_column("query_metrics", "sql_valid")
    op.drop_column("query_metrics", "has_sql")
    op.drop_column("query_metrics", "has_sparql")
    op.drop_column("query_metrics", "query_source")
    op.drop_column("query_metrics", "sql_query")
    op.drop_column("query_metrics", "request_source")
    op.drop_column("query_metrics", "telegram_chat_id")
    op.drop_column("query_metrics", "telegram_user_id")
    op.drop_column("query_metrics", "telegram_account_id")
