"""add dashboard and dashboard_item tables

Revision ID: 20251111_add_dashboard_tables
Revises: 20251030_add_avatar_blob
Create Date: 2025-11-11

"""
from sqlalchemy import text
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20251111_add_dashboard_tables"
down_revision = "20251030_add_avatar_blob"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()

    # create dashboard if not exists
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dashboard (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES "user"(user_id),
            name VARCHAR(100) NOT NULL,
            description VARCHAR(255),
            is_default BOOLEAN DEFAULT false NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS dashboard_item (
            id SERIAL PRIMARY KEY,
            dashboard_id INTEGER NOT NULL REFERENCES dashboard(id) ON DELETE CASCADE,
            artifact_type VARCHAR(50) NOT NULL,
            title VARCHAR(150) NOT NULL,
            source_query VARCHAR(1000),
            config JSON NOT NULL,
            position INTEGER DEFAULT 0 NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """))


def downgrade() -> None:
    op.drop_table("dashboard_item")
    op.drop_table("dashboard")

