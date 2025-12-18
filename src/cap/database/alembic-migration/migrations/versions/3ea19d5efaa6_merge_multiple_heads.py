"""merge multiple heads

Revision ID: 3ea19d5efaa6
Revises: 20251212_add_conversation_artifacts, 20251213_add_created_at_to_waiting_list
Create Date: 2025-12-13 17:47:36.462469

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3ea19d5efaa6'
down_revision: Union[str, Sequence[str], None] = ('20251212_add_conversation_artifacts', '20251213_add_created_at_to_waiting_list')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
