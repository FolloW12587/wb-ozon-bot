"""add on_delete SET NULL for WB and Ozon product models

Revision ID: ecb2e00d0e8f
Revises: da3da5e982cc
Create Date: 2025-02-26 16:59:28.273580

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'ecb2e00d0e8f'
down_revision: Union[str, None] = 'da3da5e982cc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Удаляем старый внешний ключ, если он существует
    op.drop_constraint('wb_products_wb_punkt_id_fkey', 'wb_products', type_='foreignkey')

    op.alter_column('wb_products', 'wb_punkt_id', nullable=True)

    # Создаем новый внешний ключ с ondelete='SET NULL'
    op.create_foreign_key(
        'wb_products_wb_punkt_id_fkey',  # Название внешнего ключа
        'wb_products',  # Таблица, в которой создается внешний ключ
        'wb_punkts',  # Таблица, на которую ссылается внешний ключ
        ['wb_punkt_id'],  # Столбцы в таблице wb_products
        ['id'],  # Столбцы в таблице wb_punkts
        ondelete='SET NULL'  # Поведение при удалении
    )

    # Удаляем старый внешний ключ, если он существует
    op.drop_constraint('ozon_products_ozon_punkt_id_fkey', 'ozon_products', type_='foreignkey')

    op.alter_column('ozon_products', 'ozon_punkt_id', nullable=True)

    # Создаем новый внешний ключ с ondelete='SET NULL'
    op.create_foreign_key(
        'ozon_products_ozon_punkt_id_fkey',  # Название внешнего ключа
        'ozon_products',  # Таблица, в которой создается внешний ключ
        'ozon_punkts',  # Таблица, на которую ссылается внешний ключ
        ['ozon_punkt_id'],  # Столбцы в таблице wb_products
        ['id'],  # Столбцы в таблице wb_punkts
        ondelete='SET NULL'  # Поведение при удалении
    )
    pass


def downgrade() -> None:
    # Удаляем внешний ключ
    op.drop_constraint('wb_products_wb_punkt_id_fkey', 'wb_products', type_='foreignkey')

    # Восстанавливаем старый внешний ключ без ondelete
    op.create_foreign_key(
        'wb_products_wb_punkt_id_fkey',  # Название внешнего ключа
        'wb_products',  # Таблица, в которой создается внешний ключ
        'wb_punkts',  # Таблица, на которую ссылается внешний ключ
        ['wb_punkt_id'],  # Столбцы в таблице wb_products
        ['id']  # Столбцы в таблице wb_punkts
    )

    # Удаляем старый внешний ключ, если он существует
    op.drop_constraint('ozon_products_ozon_punkt_id_fkey', 'ozon_products', type_='foreignkey')

    # Создаем новый внешний ключ с ondelete='SET NULL'
    op.create_foreign_key(
        'ozon_products_ozon_punkt_id_fkey',  # Название внешнего ключа
        'ozon_products',  # Таблица, в которой создается внешний ключ
        'ozon_punkts',  # Таблица, на которую ссылается внешний ключ
        ['ozon_punkt_id'],  # Столбцы в таблице wb_products
        ['id'],  # Столбцы в таблице wb_punkts
    )
    pass
