import asyncio

from fastapi.responses import JSONResponse
from fastapi import FastAPI, Request
from uvicorn import Config, Server
from starlette.middleware.cors import CORSMiddleware


from aiogram import Dispatcher, types

from background.base import get_redis_background_pool
from db.base import engine, session, Base

from middlewares.db import DbSessionMiddleware

from utils.pics import ImageManager
from utils.storage import storage
from utils.scheduler import (
    scheduler,
    setup_subscription_end_job,
    sync_popular_product_jobs,
    setup_subscription_is_about_to_end_job,
    setup_messages_sendigns_job,
)

from payments.yoomoney import yoomoney_payment_notification_handler
from deps import YoomoneyServiceDep

import config

from handlers.base import main_router
from handlers.subscription import router as payments_router
from handlers.punkt import router as punkt_router

from bot22 import bot
from logger import logger


dp = Dispatcher(storage=storage)


@dp.errors()
async def aiogram_global_error_handler(event: types.ErrorEvent):
    print("AIOGRAM ERROR")
    logger.exception("Unhandled aiogram exception", exc_info=event.exception)


dp.include_router(payments_router)
dp.include_router(punkt_router)
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
app_config = Config(
    app=app, loop=event_loop, workers=2, host=config.HOST, port=int(config.PORT)
)
server = Server(app_config)


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
    logger.info("Setting webhook at url %s%s", config.PUBLIC_URL, WEBHOOK_PATH)
    await bot.set_webhook(
        f"{config.PUBLIC_URL}{WEBHOOK_PATH}", drop_pending_updates=True
    )

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

    asyncio.create_task(sync_popular_product_jobs(scheduler))
    asyncio.create_task(setup_subscription_end_job(scheduler))
    asyncio.create_task(setup_subscription_is_about_to_end_job(scheduler))
    asyncio.create_task(setup_messages_sendigns_job(scheduler))


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
    # print("UPDATE FROM TG", update)
    tg_update = types.Update(**update)
    # print('TG UPDATE', tg_update, tg_update.__dict__)
    await dp.feed_update(bot=bot, update=tg_update)


@app.post("/payments/yoomoney_payment_notification")
async def yoomoney_webhook(request: Request, yoomoney_service: YoomoneyServiceDep):
    form = await request.form()
    data = dict(form)

    try:
        await yoomoney_payment_notification_handler(data, yoomoney_service)
    except Exception:
        logger.error(
            "Error happened while processing yoomoney payment notification",
            exc_info=True,
        )
        return {"status": "error"}

    return {"status": "ok"}


if __name__ == "__main__":
    event_loop.run_until_complete(server.serve())


@app.exception_handler(Exception)
def global_exception_handler(_: Request, exc: Exception):
    logger.exception(
        "Unhandled exception occurred",
        exc_info=exc,
        extra={
            "error": str(exc),
        },
    )

    return JSONResponse(
        status_code=500,
        content={"error": {"message": "Unhandled exception occured"}},
    )
