from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

from background.tasks import (
    update_user_product_prices,
    new_add_product_task,
    add_popular_product,
    add_punkt_by_user,
)
from payments.process import process_transaction
from background.base import redis_settings, _redis_pool, get_redis_background_pool


from config import JOB_STORE_URL


async def startup(ctx):
    global _redis_pool
    jobstores = {
        "sqlalchemy": SQLAlchemyJobStore(url=JOB_STORE_URL),
    }

    # Создание и настройка планировщика
    scheduler = AsyncIOScheduler(jobstores=jobstores)

    if not _redis_pool:
        _redis_pool = await get_redis_background_pool()

    scheduler.start()
    ctx["scheduler"] = scheduler
    print("Worker is starting up...")


async def shutdown(ctx):
    print("Worker is shutting down...")


class WorkerSettings:
    functions = [
        update_user_product_prices,
        new_add_product_task,
        add_popular_product,
        add_punkt_by_user,
        process_transaction,
    ]
    on_startup = startup
    on_shutdown = shutdown
    queue_name = "arq:high"
    redis_settings = redis_settings
    keep_result = 0
    job_defaults = {
        "max_tries": 1,
    }
