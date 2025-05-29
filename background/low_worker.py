from background.tasks import (new_push_check_ozon_price,
                              new_push_check_wb_price,
                              push_check_ozon_popular_product,
                              push_check_wb_popular_product,
                              periodic_delete_old_message,
                              add_popular_product)
from background.base import redis_settings, _redis_pool, get_redis_background_pool

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

from config import REDIS_HOST, REDIS_PASSWORD, JOB_STORE_URL


async def startup(ctx):
    jobstores = {
        'sqlalchemy': SQLAlchemyJobStore(url=JOB_STORE_URL),
    }

    scheduler = AsyncIOScheduler(jobstores=jobstores)

    ctx['scheduler'] = scheduler
    print("Worker is starting up...")


async def shutdown(ctx):
    ctx.pop('scheduler')
    print("Worker is shutting down...")


class WorkerSettings:
    functions = [
        new_push_check_ozon_price,
        new_push_check_wb_price,
        # push_check_ozon_popular_product,
        # push_check_wb_popular_product,
        periodic_delete_old_message,
        # add_popular_product,
    ]
    on_startup = startup
    on_shutdown = shutdown
    queue_name = "arq:low"
    redis_settings = redis_settings
    keep_result = 0
    job_defaults = {
        'max_tries': 1, 
    }

