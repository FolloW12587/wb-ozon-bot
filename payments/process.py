from uuid import UUID

from dateutil.relativedelta import relativedelta

from commands.send_message import notify_admins
from db.base import Order, OrderStatus, Transaction, User, UserSubscription, get_session
from db.repository.transaction import TransactionRepository
from db.repository.order import OrderRepository
from db.repository.user_subscription import UserSubscriptionRepository
from db.repository.user import UserRepository

from payments.errors import TransactionProcessError
from payments.notifications import (
    notify_user_about_fail,
    notify_user_about_purchsed_subscription,
)
from payments.utils import (
    give_user_subscription,
)


from logger import logger
from schemas import MessageInfo


async def process_transaction(_, transaction_id: int):
    try:
        _ = await __process_transaction(transaction_id)
    except Exception:
        logger.error(
            "Transaction %s was not processed correctly", transaction_id, exc_info=True
        )
        await __transaction_process_failed(transaction_id)


async def __transaction_process_failed(transaction_id: int):
    await notify_admins(
        MessageInfo(text=f"Ошибка при обработке транзакции {transaction_id}"),
    )
    async for session in get_session():
        transaction_repo = TransactionRepository(session)
        transaction = await transaction_repo.find_by_id(transaction_id)
        if not transaction:
            return

        await notify_user_about_fail(transaction.user_id)


async def __process_transaction(transaction_id: int) -> Transaction:
    logger.info("Proccessing transaction %s", transaction_id)
    async for session in get_session():
        transaction_repo = TransactionRepository(session)
        order_repo = OrderRepository(session)
        user_subscription_repo = UserSubscriptionRepository(session)
        user_repo = UserRepository(session)

        transaction = await transaction_repo.find_by_id(transaction_id)
        if not transaction:
            raise TransactionProcessError(
                f"Can't find transaction {transaction_id} to process"
            )

        order = await order_repo.find_by_id(transaction.order_id)
        if not order:
            raise TransactionProcessError(
                f"Can't find order {transaction.order_id} "
                f"for transaction {transaction_id} to process"
            )

        if order.status != OrderStatus.PENDING.value:
            raise TransactionProcessError(
                f"Order {order.id} status is not pending: {order.status}"
            )

        logger.info("Found order %s for transaction %s", order.id, transaction_id)
        if order.user_id != transaction.user_id:
            raise TransactionProcessError(
                f"Order.user_id {order.user_id} and "
                f"transaction.user_id {transaction.user_id} are not the same"
            )

        user_id = transaction.user_id
        user = await user_repo.find_by_id(user_id)
        if not user:
            raise TransactionProcessError(f"Can't find user with tg_id {user_id}")

        if await __is_order_processed(user_subscription_repo, transaction.order_id):
            logger.info("Order %s is already processed", order.id)
            return

        user_subscription = await __process_order(
            user_subscription_repo, user_repo, order_repo, order, user
        )

        await notify_user_about_purchsed_subscription(user_subscription, user_id)

        logger.info("Transaction %s processed successfully", transaction.id)
        return transaction


async def __is_order_processed(
    repo: UserSubscriptionRepository, order_id: UUID
) -> bool:
    return await repo.subscription_by_order(order_id) is not None


async def __process_order(
    us_repo: UserSubscriptionRepository,
    user_repo: UserRepository,
    order_repo: OrderRepository,
    order: Order,
    user: User,
) -> UserSubscription:
    active_from = await us_repo.get_start_date_for_new_subscription(user.tg_id)
    active_to = active_from + relativedelta(months=1) - relativedelta(days=1)

    user_subscription = await give_user_subscription(
        us_repo=us_repo,
        user_repo=user_repo,
        user=user,
        subscription_id=order.subscription_id,
        active_from=active_from,
        active_to=active_to,
        order_id=order.id,
    )
    await order_repo.update_old(order.id, status=OrderStatus.SUCCESS.value)

    return user_subscription
