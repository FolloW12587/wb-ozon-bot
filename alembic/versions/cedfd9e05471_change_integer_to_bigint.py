"""change integer to bigint

Revision ID: cedfd9e05471
Revises: b5453149b690
Create Date: 2025-02-08 16:27:19.550223

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'cedfd9e05471'
down_revision: Union[str, None] = 'b5453149b690'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        'users',  # имя вашей таблицы
        'tg_id',  # имя столбца, который нужно изменить
        type_=sa.BigInteger(),  # новый тип
        postgresql_using='tg_id::bigint'  # преобразование данных
    )


def downgrade() -> None:
    op.alter_column(
        'users',
        'tg_id',
        type_=sa.Integer(),
        postgresql_using='tg_id::integer'
    )
