from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore


from background.base import _redis_pool, get_redis_background_pool, redis_settings
from background.tasks import (
    new_push_check_ozon_price,
    new_push_check_wb_price,
    periodic_delete_old_message,
)
from background.subscriptions import (
    search_users_for_ended_subscription,
    notify_users_about_subscription_ending,
)
from background.messaging import process_message_sendings


from config import JOB_STORE_URL


async def startup(ctx):
    global _redis_pool
    jobstores = {
        "sqlalchemy": SQLAlchemyJobStore(url=JOB_STORE_URL),
    }

    scheduler = AsyncIOScheduler(jobstores=jobstores)
    if not _redis_pool:
        _redis_pool = await get_redis_background_pool()

    scheduler.start()
    ctx["scheduler"] = scheduler
    print("Worker is starting up...")


async def shutdown(ctx):
    ctx.pop("scheduler")
    print("Worker is shutting down...")


class WorkerSettings:
    functions = [
        new_push_check_ozon_price,
        new_push_check_wb_price,
        periodic_delete_old_message,
        search_users_for_ended_subscription,
        notify_users_about_subscription_ending,
        process_message_sendings,
    ]
    on_startup = startup
    on_shutdown = shutdown
    queue_name = "arq:low"
    redis_settings = redis_settings
    keep_result = 0
    job_defaults = {
        "max_tries": 1,
    }
