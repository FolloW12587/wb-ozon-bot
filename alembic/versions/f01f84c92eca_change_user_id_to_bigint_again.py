"""change user_id to bigint again

Revision ID: f01f84c92eca
Revises: 51760031e1b5
Create Date: 2025-02-08 17:38:21.924835

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f01f84c92eca'
down_revision: Union[str, None] = '51760031e1b5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Wb punkt
    # Удаляем внешний ключ
    op.drop_constraint('wb_punkts_user_id_fkey', 'wb_punkts', type_='foreignkey')

    # Изменяем тип столбца обратно
    op.alter_column(
        'wb_punkts',
        'user_id',
        type_=sa.BigInteger(),
        postgresql_using='user_id::integer'
    )

    # Добавляем внешний ключ обратно
    op.create_foreign_key('wb_punkts_user_id_fkey', 'wb_punkts', 'users', ['user_id'], ['tg_id'])

    # UserJob
    # Удаляем внешний ключ
    op.drop_constraint('user_job_user_id_fkey', 'user_job', type_='foreignkey')

    # Изменяем тип столбца обратно
    op.alter_column(
        'user_job',
        'user_id',
        type_=sa.BigInteger(),
        postgresql_using='user_id::integer'
    )

    # Добавляем внешний ключ обратно
    op.create_foreign_key('user_job_user_id_fkey', 'user_job', 'users', ['user_id'], ['tg_id'])


def downgrade() -> None:
    # Wb punkt
    # Удаляем внешний ключ
    op.drop_constraint('wb_punkts_user_id_fkey', 'wb_punkts', type_='foreignkey')

    # Изменяем тип столбца обратно
    op.alter_column(
        'wb_punkts',
        'user_id',
        type_=sa.Integer(),
        postgresql_using='user_id::integer'
    )

    # Добавляем внешний ключ обратно
    op.create_foreign_key('wb_punkts_user_id_fkey', 'wb_punkts', 'users', ['user_id'], ['tg_id'])

    # UserJob
    # Удаляем внешний ключ
    op.drop_constraint('user_job_user_id_fkey', 'user_job', type_='foreignkey')

    # Изменяем тип столбца обратно
    op.alter_column(
        'user_job',
        'user_id',
        type_=sa.Integer(),
        postgresql_using='user_id::integer'
    )

    # Добавляем внешний ключ обратно
    op.create_foreign_key('user_job_user_id_fkey', 'user_job', 'users', ['user_id'], ['tg_id'])

