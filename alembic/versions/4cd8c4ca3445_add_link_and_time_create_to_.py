"""Add link and time_create to PopularProduct model

Revision ID: 4cd8c4ca3445
Revises: cc5bf10fe49b
Create Date: 2025-05-20 19:56:17.978597

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4cd8c4ca3445'
down_revision: Union[str, None] = 'cc5bf10fe49b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('popular_products', sa.Column('link', sa.String(), nullable=True))
    op.add_column('popular_products', sa.Column('time_create', sa.TIMESTAMP(timezone=True), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('popular_products', 'time_create')
    op.drop_column('popular_products', 'link')
    # ### end Alembic commands ###
