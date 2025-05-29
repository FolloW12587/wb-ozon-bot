import redis

import redis.asyncio
import redis.asyncio.client

from aiogram.fsm.storage.redis import RedisStorage

from config import REDIS_HOST, REDIS_PASSWORD


redis_client = redis.asyncio.client.Redis(host=REDIS_HOST,
                                          password=REDIS_PASSWORD)
storage = RedisStorage(redis=redis_client)