from uuid import UUID

from dateutil.relativedelta import relativedelta

from db.base import Order, OrderStatus, User, get_session
from db.repository.transaction import TransactionRepository
from db.repository.order import OrderRepository
from db.repository.user_subscription import UserSubscriptionRepository
from db.repository.user import UserRepository

from payments.errors import TransactionProcessError


from logger import logger


async def process_transaction(cxt, transaction_id: int):
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

        if order.status != OrderStatus.PENDING:
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

        order = await __process_order(user_subscription_repo, order_repo, order, user)
        logger.info("Transaction %s processed successfully", transaction.id)


async def __is_order_processed(
    repo: UserSubscriptionRepository, order_id: UUID
) -> bool:
    return await repo.subscription_by_order(order_id) is not None


async def __process_order(
    us_repo: UserSubscriptionRepository,
    order_repo: OrderRepository,
    order: Order,
    user: User,
) -> Order:
    active_from = await us_repo.get_start_date_for_new_subscription(user.tg_id)
    active_to = active_from + relativedelta(months=1) - relativedelta(days=1)

    user_subscription = await us_repo.new_subscription(
        user_id=user.tg_id,
        order_id=order.id,
        subscription_id=order.subscription_id,
        active_from=active_from,
        active_to=active_to,
    )
    logger.info("Created new user subscription %s", user_subscription.id)

    order = await order_repo.update(order.id, {"status": OrderStatus.SUCCESS})
    if not order:
        raise TransactionProcessError("Error while updating order occured!")

    return order
