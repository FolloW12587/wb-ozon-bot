from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import Order, Transaction, PaymentProvider

from services.yoomoney.yoomoney_dto import YoomoneyNotificationData


class TransactionRepository(BaseRepository[Transaction]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Transaction)

    async def save_yoomoney_transaction(
        self, data: YoomoneyNotificationData, order: Order, raw_data: dict
    ) -> Transaction:
        transaction = Transaction(
            user_id=order.user_id,
            order_id=order.id,
            provider=PaymentProvider.YOOMONEY.value,
            provider_txn_id=data.operation_id,
            amount=data.amount,
            currency=data.currency,
            status="success",
            transaction_datetime=data.datetime,
            raw_data=raw_data,
        )
        return await self.create(transaction)
