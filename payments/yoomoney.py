from services.yoomoney.yoomoney_service import YoomoneyService
from background.base import get_redis_background_pool, _redis_pool

from logger import logger


async def yoomoney_payment_notification_handler(data: dict, service: YoomoneyService):
    global _redis_pool

    logger.info("YooMoney webhook: %s", data)
    transaction = await service.process_transaction_data(data)
    logger.info("Successfully processed and created transaction %s", transaction.id)
    if not _redis_pool:
        _redis_pool = await get_redis_background_pool()

    await _redis_pool.enqueue_job(
        "process_transaction", transaction_id=transaction.id, _queue_name="arq:high"
    )
