"""change user_id to bigint

Revision ID: 51760031e1b5
Revises: cedfd9e05471
Create Date: 2025-02-08 17:13:55.118277

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '51760031e1b5'
down_revision: Union[str, None] = 'cedfd9e05471'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Wb
    # Удаляем внешний ключ
    op.drop_constraint('wb_products_user_id_fkey', 'wb_products', type_='foreignkey')
    
    # Изменяем тип столбца
    op.alter_column(
        'wb_products',  # имя вашей таблицы
        'user_id',  # имя столбца, который нужно изменить
        type_=sa.BigInteger(),  # новый тип
        postgresql_using='user_id::bigint'  # преобразование данных
    )
    # Добавляем внешний ключ обратно
    op.create_foreign_key('wb_products_user_id_fkey', 'wb_products', 'users', ['user_id'], ['tg_id'])

    # Ozon
    # Удаляем внешний ключ
    op.drop_constraint('ozon_products_user_id_fkey', 'ozon_products', type_='foreignkey')
    
    # Изменяем тип столбца
    op.alter_column(
        'ozon_products',  # имя вашей таблицы
        'user_id',  # имя столбца, который нужно изменить
        type_=sa.BigInteger(),  # новый тип
        postgresql_using='user_id::bigint'  # преобразование данных
    )
    # Добавляем внешний ключ обратно
    op.create_foreign_key('ozon_products_user_id_fkey', 'ozon_products', 'users', ['user_id'], ['tg_id'])


def downgrade() -> None:
    # Wb
    # Удаляем внешний ключ
    op.drop_constraint('wb_products_user_id_fkey', 'wb_products', type_='foreignkey')

    # Изменяем тип столбца обратно
    op.alter_column(
        'wb_products',
        'user_id',
        type_=sa.Integer(),
        postgresql_using='user_id::integer'
    )

    # Добавляем внешний ключ обратно
    op.create_foreign_key('wb_products_user_id_fkey', 'wb_products', 'users', ['user_id'], ['tg_id'])

    # Ozon
    # Удаляем внешний ключ
    op.drop_constraint('ozon_products_user_id_fkey', 'ozon_products', type_='foreignkey')

    # Изменяем тип столбца обратно
    op.alter_column(
        'ozon_products',
        'user_id',
        type_=sa.Integer(),
        postgresql_using='user_id::integer'
    )

    # Добавляем внешний ключ обратно
    op.create_foreign_key('ozon_products_user_id_fkey', 'ozon_products', 'users', ['user_id'], ['tg_id'])
