# cap/src/cap/database/alembic-migration/migrations/versions/20251221_add_shared_image_table.py
"""
add shared_image table (idempotent)

Revision ID: 20251221_add_shared_image_table
Revises: 2b63a71b2da0
Create Date: 2025-12-21
"""

from alembic import op
import sqlalchemy as sa

revision = "20251221_add_shared_image_table"
down_revision = "2b63a71b2da0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create table only if it doesn't exist
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS shared_image (
            id VARCHAR(36) PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES "user"(user_id) ON DELETE CASCADE,
            access_token VARCHAR(64) NOT NULL,
            content_sha256 VARCHAR(64) NOT NULL,
            mime VARCHAR(64) NOT NULL,
            bytes INTEGER NOT NULL,
            etag VARCHAR(64) NOT NULL,
            storage_path VARCHAR(512) NOT NULL,
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW() NOT NULL,
            expires_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
        )
        """
    )

    # Indexes (IF NOT EXISTS is supported by Postgres)
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_shared_image_user_id ON shared_image (user_id)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_shared_image_access_token ON shared_image (access_token)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_shared_image_created_at ON shared_image (created_at)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_shared_image_expires_at ON shared_image (expires_at)"
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_shared_image_user_sha
        ON shared_image (user_id, content_sha256)
        """
    )


def downgrade() -> None:
    op.drop_index("uq_shared_image_user_sha", table_name="shared_image")
    op.drop_index("ix_shared_image_expires_at", table_name="shared_image")
    op.drop_index("ix_shared_image_created_at", table_name="shared_image")
    op.drop_index("ix_shared_image_access_token", table_name="shared_image")
    op.drop_index("ix_shared_image_user_id", table_name="shared_image")
    op.drop_table("shared_image")
