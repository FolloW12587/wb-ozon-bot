"""alter_add_last_notificated_price_to_popular_product

Revision ID: b740c88f8d8d
Revises: 4cd8c4ca3445
Create Date: 2025-06-05 18:21:21.783374

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b740c88f8d8d'
down_revision: Union[str, None] = '4cd8c4ca3445'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('popular_products', sa.Column('last_notificated_price', sa.Integer(), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('popular_products', 'last_notificated_price')
    # ### end Alembic commands ###
