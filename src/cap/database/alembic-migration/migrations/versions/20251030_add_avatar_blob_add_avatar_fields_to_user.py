"""add avatar fields to user

Revision ID: 20251030_add_avatar_blob
Revises: base_20251030
Create Date: 2025-10-30 21:52:11.130898

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '20251030_add_avatar_blob'
down_revision: Union[str, Sequence[str], None] = 'base_20251030'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None



def upgrade():
    # op.add_column("user", sa.Column("avatar_blob", sa.LargeBinary(), nullable=True))
    # op.add_column("user", sa.Column("avatar_mime", sa.String(length=64), nullable=True))
    # op.add_column("user", sa.Column("avatar_etag", sa.String(length=64), nullable=True))
    op.execute(sa.text('ALTER TABLE "user" ADD COLUMN IF NOT EXISTS avatar_blob BYTEA'))
    op.execute(sa.text('ALTER TABLE "user" ADD COLUMN IF NOT EXISTS avatar_mime VARCHAR(64)'))
    op.execute(sa.text('ALTER TABLE "user" ADD COLUMN IF NOT EXISTS avatar_etag VARCHAR(64)'))

def downgrade():
    op.drop_column("user", "avatar_etag")
    op.drop_column("user", "avatar_mime")
    op.drop_column("user", "avatar_blob")
