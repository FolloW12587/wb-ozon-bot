"""Remove lat and lon from wb_punkts

Revision ID: da3da5e982cc
Revises: 67f00888d2d2
Create Date: 2025-02-26 16:15:41.314902

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'da3da5e982cc'
down_revision: Union[str, None] = '67f00888d2d2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column('wb_punkts', 'lat')
    op.drop_column('wb_punkts', 'lon')
    pass


def downgrade() -> None:
    op.add_column('wb_punkts', sa.Column('lat', sa.Float(), nullable=True))
    op.add_column('wb_punkts', sa.Column('lon', sa.Float(), nullable=True))
    pass
