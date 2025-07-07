from hashlib import sha1
from uuid import UUID

from db.base import Order, Subscription, Transaction, User
from db.repository.order import OrderRepository
from db.repository.transaction import TransactionRepository

from services.yoomoney.yoomoney_dto import YoomoneyNotificationData
from services.yoomoney.errors import (
    HashValidationError,
    OrderNotExists,
    YoomoneyServiceError,
)


HASH_STRING_KEYS = [
    "notification_type",
    "operation_id",
    "amount",
    "currency",
    "datetime",
    "sender",
    "codepro",
    "notification_secret",
    "label",
]


class YoomoneyService:
    def __init__(
        self,
        notification_secret: str,
        wallet_id: str,
        transaction_repo: TransactionRepository,
        order_repo: OrderRepository,
    ):
        self._notification_secret = notification_secret
        self._wallet_id = wallet_id
        self._transaction_repo = transaction_repo
        self._order_repo = order_repo

    async def process_transaction_data(self, data: dict) -> Transaction:
        order = await self.__validate_transaction(data)
        try:
            notification_data = YoomoneyNotificationData(**data)
            return await self._transaction_repo.save_yoomoney_transaction(
                notification_data, order, data
            )
        except Exception as e:
            raise YoomoneyServiceError from e

    async def generate_payment_url(self, subscription: Subscription, user: User) -> str:
        order = await self._order_repo.generate_order(subscription, user)
        return (
            "https://yoomoney.ru/quickpay/confirm?"
            f"receiver={self._wallet_id}&sum={subscription.price_rub}&"
            f"quickpay-form=button&label={order.id}"
        )

    async def __validate_transaction(self, data: dict) -> Order:
        order_id = UUID(data.get("label"))
        order = await self._order_repo.find_by_id(order_id)
        if not order:
            raise OrderNotExists(f"Order with id {order_id} doesn't exist")

        s = ""
        for key in HASH_STRING_KEYS:
            if key != "notification_secret":
                s += f"{data.get(key)}&"
                continue
            s += f"{self._notification_secret}&"

        if data.get("label"):
            s = s[:-1]

        sha1_hash = sha1(s.encode("utf-8"))
        sha1_hash_hex = sha1_hash.hexdigest()
        if sha1_hash_hex != data.get("sha1_hash"):
            raise HashValidationError()

        return order
