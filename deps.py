from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from db.base import get_session
from db.repository.order import OrderRepository
from db.repository.transaction import TransactionRepository

from services.yoomoney.yoomoney_service import YoomoneyService

import config


DbSessionDep = Annotated[AsyncSession, Depends(get_session)]


def get_order_repository(
    db_session: DbSessionDep,
) -> OrderRepository:
    return OrderRepository(db_session)


OrderRepositoryDep = Annotated[OrderRepository, Depends(get_order_repository)]


def get_transaction_repository(
    db_session: DbSessionDep,
) -> TransactionRepository:
    return TransactionRepository(db_session)


TransactionRepositoryDep = Annotated[
    TransactionRepository, Depends(get_transaction_repository)
]


def get_yoomoney_service_repository(
    order_repository: OrderRepositoryDep,
    transaction_repository: TransactionRepositoryDep,
) -> YoomoneyService:
    return YoomoneyService(
        config.YOOMONEY_NOTIFICATION_SECRET,
        config.YOOMONEY_RECEIVER,
        transaction_repository,
        order_repository,
    )


YoomoneyServiceDep = Annotated[
    YoomoneyService, Depends(get_yoomoney_service_repository)
]
