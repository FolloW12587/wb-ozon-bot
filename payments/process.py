from datetime import datetime, timezone
from uuid import UUID

from aiogram import types
from dateutil.relativedelta import relativedelta

from commands.send_message import send_message
from db.base import Order, OrderStatus, Transaction, User, UserSubscription, get_session
from db.repository.transaction import TransactionRepository
from db.repository.order import OrderRepository
from db.repository.user_subscription import UserSubscriptionRepository
from db.repository.user import UserRepository

from payments.errors import TransactionProcessError
import config


from logger import logger


async def process_transaction(cxt, transaction_id: int):
    try:
        _ = await __process_transaction(transaction_id)
    except Exception:
        logger.error(
            "Transaction %s was not processed correctly", transaction_id, exc_info=True
        )
        await __transaction_process_failed(transaction_id)


async def __transaction_process_failed(transaction_id: int):
    await send_message(
        config.PAYMENTS_CHAT_ID,
        f"Ошибка при обработке транзакции {transaction_id}",
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

        await __notify_user_about_purchsed_subscription(user_subscription, user_id)

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

    user_subscription = await us_repo.new_subscription(
        user_id=user.tg_id,
        order_id=order.id,
        subscription_id=order.subscription_id,
        active_from=active_from,
        active_to=active_to,
    )
    logger.info("Created new user subscription %s", user_subscription.id)

    await __set_subscription_to_user_if_needed(user_repo, user, user_subscription)
    await order_repo.update(order.id, status=OrderStatus.SUCCESS.value)

    return user_subscription


async def __set_subscription_to_user_if_needed(
    user_repo: UserRepository, user: User, user_subscription: UserSubscription
):
    now = datetime.now(timezone.utc).date()
    if user_subscription.active_from <= now <= user_subscription.active_to:
        logger.info("User subscription is set to %s", user_subscription.subscription_id)
        await user_repo.update(
            user.tg_id, subscription_id=user_subscription.subscription_id
        )


async def __notify_user_about_purchsed_subscription(
    user_subscription: UserSubscription, user_id: int
):
    logger.info(
        "Notifying user %s about new purchased subscription %s",
        user_id,
        user_subscription.id,
    )
    text = f"""
*🎉 Подписка успешно оформлена!*

Спасибо за оплату — вы получили доступ ко всем функциям:

✔️ Безлимит на отслеживаемые товары
✔️ График изменения цен
✔️ Выбор пункта выдачи

*🗓 Подписка активна до {user_subscription.active_to}*

_Мы заранее напомним вам за 5 дней до окончания, чтобы вы могли продлить без перерыва в работе._

Приятных покупок и выгодных скидок! 💸"""
    try:
        await send_message(user_id, text)
    except Exception:
        logger.error("Error in notifying user about new sdubscription", exc_info=True)
        raise


async def notify_user_about_fail(user_id: int):
    logger.info("Notifying user %s about transaction processing fail", user_id)
    text = f"""
*❌ Не удалось оформить подписку*

Пожалуйста, напишите [нам в поддержку]({config.SUPPORT_BOT_URL}) и мы обязательно вам поможем.
"""
    btn = types.InlineKeyboardButton(text="Тех. поддержка", url=config.SUPPORT_BOT_URL)

    markup = types.InlineKeyboardMarkup(inline_keyboard=[[btn]])
    try:
        await send_message(user_id, text, markup)
    except Exception:
        logger.error(
            "Error in notifying user abput failed transaction processing", exc_info=True
        )
