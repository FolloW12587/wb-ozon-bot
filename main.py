import asyncio

from uvicorn import Config, Server
from starlette.middleware.cors import CORSMiddleware

from fastapi import FastAPI

from aiogram import Dispatcher, types

from background.base import get_redis_background_pool
from db.base import engine, session, Base, get_session

from middlewares.db import DbSessionMiddleware

from utils.pics import ImageManager
from utils.storage import storage
from utils.scheduler import (
    scheduler,
    send_fake_price,
)
from utils.utm import add_utm_to_db

from schemas import UTMSchema

from config import PUBLIC_URL, FAKE_NOTIFICATION_SECRET, HOST, PORT

from handlers.base import main_router

from bot22 import bot


dp = Dispatcher(storage=storage)
dp.include_router(main_router)


# #Add session and database connection in handlers

# #Initialize web server
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

event_loop = asyncio.new_event_loop()
asyncio.set_event_loop(event_loop)
config = Config(app=app, loop=event_loop, workers=2, host=HOST, port=int(PORT))
server = Server(config)


# #For set webhook
WEBHOOK_PATH = "/webhook_"


async def init_db():
    async with engine.begin() as conn:
        # Создаем таблицы
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)


# #Set webhook and create database on start
@app.on_event("startup")
async def on_startup():
    await bot.delete_webhook()
    await bot.set_webhook(f"{PUBLIC_URL}{WEBHOOK_PATH}", drop_pending_updates=True)

    redis_pool = await get_redis_background_pool()
    image_manager = ImageManager(bot)
    scheduler.start()

    dp.update.middleware(
        DbSessionMiddleware(
            session_pool=session,
            scheduler=scheduler,
            redis_pool=redis_pool,
            image_manager=image_manager,
        )
    )


@app.on_event("shutdown")
async def on_shutdown():
    await bot.delete_webhook(drop_pending_updates=True)
    try:
        scheduler.shutdown()
    except Exception as ex:
        print(ex)


# #Endpoint for incoming updates
@app.post(WEBHOOK_PATH)
async def bot_webhook(update: dict):
    # print('UPDATE FROM TG',update)
    tg_update = types.Update(**update)
    # print('TG UPDATE', tg_update, tg_update.__dict__)
    await dp.feed_update(bot=bot, update=tg_update)


@app.post("/send_utm_data")
async def send_utm_data(data: UTMSchema):
    print("CATCH UTM", data.__dict__)
    await add_utm_to_db(data)


@app.get("/send_fake_notification")
async def send_fake_notification_by_user(
    user_id: int, product_id: int, fake_price: int, secret: str
):

    if secret == FAKE_NOTIFICATION_SECRET:
        # print('CATCH UTM', data.__dict__)
        async for session in get_session():
            await send_fake_price(user_id, product_id, fake_price, session)


if __name__ == "__main__":
    event_loop.run_until_complete(server.serve())
