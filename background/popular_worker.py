from background.tasks import push_check_popular_product
from background.base import redis_settings, _redis_pool, get_redis_background_pool


async def startup(ctx):
    print("Worker is starting up...")


async def shutdown(ctx):
    print("Worker is shutting down...")


class WorkerSettings:
    functions = [push_check_popular_product]
    on_startup = startup
    on_shutdown = shutdown
    queue_name = "arq:popular"
    redis_settings = redis_settings
    keep_result = 0
    job_defaults = {
        "max_tries": 1,
    }
