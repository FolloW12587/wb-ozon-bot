from typing import Callable, Awaitable, Dict, Any

from arq.connections import ArqRedis

from aiogram import BaseMiddleware
from aiogram.types import TelegramObject

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from sqlalchemy.ext.asyncio import async_sessionmaker

from utils.pics import ImageManager


class DbSessionMiddleware(BaseMiddleware):
    def __init__(
        self,
        session_pool: async_sessionmaker,
        scheduler: AsyncIOScheduler,
        redis_pool: ArqRedis,
        image_manager: ImageManager,
    ):
        super().__init__()
        self.session_pool = session_pool
        self.scheduler = scheduler
        self.redis_pool = redis_pool
        self.image_manager = image_manager

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:

        async with self.session_pool() as session:
            data["session"] = session

        data["scheduler"] = self.scheduler
        data["redis_pool"] = self.redis_pool
        data["image_manager"] = self.image_manager

        return await handler(event, data)
