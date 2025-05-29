from arq import create_pool
from arq.connections import RedisSettings

from config import REDIS_HOST, REDIS_PASSWORD


redis_settings = RedisSettings(
    host=REDIS_HOST,
    password=REDIS_PASSWORD,
    )

_redis_pool = None


async def get_redis_background_pool():
    global _redis_pool
    
    if _redis_pool is None:
        # print(22)
        _redis_pool = await create_pool(settings_=redis_settings) 
        # print(_redis_pool)
    
    return _redis_pool



def get_redis_pool():
    if _redis_pool is None:
        raise RuntimeError("Redis pool not initialized. Call init_redis_pool() first.")
    return _redis_pool