from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession

from commands.send_message import send_message
from db.base import get_session
from db.repository.order import OrderRepository
from db.repository.transaction import TransactionRepository
from payments.process import notify_user_about_fail
from services.yoomoney.yoomoney_service import YoomoneyService
from background.base import get_redis_background_pool, _redis_pool

import config
from logger import logger


async def yoomoney_payment_notification_handler(data: dict, service: YoomoneyService):
    global _redis_pool

    logger.info("YooMoney webhook: %s", data)
    try:
        transaction = await service.process_transaction_data(data)
    except Exception:
        await __yoomoney_payment_notificaiton_handler_failed(data)
        raise

    logger.info("Successfully processed and created transaction %s", transaction.id)
    await send_message(config.PAYMENTS_CHAT_ID, "Новый платежное уведомление из юмани")

    if not _redis_pool:
        _redis_pool = await get_redis_background_pool()

    await _redis_pool.enqueue_job(
        "process_transaction", transaction_id=transaction.id, _queue_name="arq:high"
    )


def get_yoomoney_service(session: AsyncSession) -> YoomoneyService:
    transaction_repo = TransactionRepository(session)
    order_repo = OrderRepository(session)

    return YoomoneyService(
        config.YOOMONEY_NOTIFICATION_SECRET,
        config.YOOMONEY_RECEIVER,
        transaction_repo,
        order_repo,
    )


async def __yoomoney_payment_notificaiton_handler_failed(data: dict):
    await send_message(
        config.PAYMENTS_CHAT_ID,
        "Ошибка при обработке платежного уведомления из юмани",
    )
    order_id = data.get("label")
    if not order_id:
        return
    try:
        order_id = UUID(order_id)
    except Exception:
        return

    async for session in get_session():
        order_repo = OrderRepository(session)
        order = await order_repo.find_by_id(order_id)
        if not order:
            return

        await notify_user_about_fail(order.user_id)
