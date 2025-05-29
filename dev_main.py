import asyncio

# import redis

import uvicorn

# import redis.asyncio
# import redis.asyncio.client
from uvicorn import Config, Server

# from aiogram.fsm.storage.redis import RedisStorage

from pyrogram import Client

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

from starlette.middleware.cors import CORSMiddleware

from fastapi import FastAPI, APIRouter

from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.storage.redis import RedisStorage

from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import select, and_

from sqlalchemy.ext.automap import automap_base

from db.base import UserJob, engine, session, Base, db_url, get_session

from middlewares.db import DbSessionMiddleware

from utils.storage import redis_client, storage
from utils.scheduler import create_new_punkts_from_old, scheduler, startup_update_scheduler_jobs
from utils.utm import add_utm_to_db

from background.base import get_redis_background_pool

from schemas import UTMSchema

from config import (TOKEN,
                    db_url,
                    PUBLIC_URL,
                    # API_ID,
                    # API_HASH,
                    REDIS_HOST,
                    REDIS_PASSWORD,
                    JOB_STORE_URL)
# from handlers import main_router

from handlers.base import main_router

from bot22 import bot


# #Initialize Redis storage
# # redis_client = redis.asyncio.client.Redis(host=REDIS_HOST,
# #                                           password=REDIS_PASSWORD)
# # storage = RedisStorage(redis=redis_client)

# ### WEBHOOK ###

# #TG BOT
# # bot = Bot(TOKEN, parse_mode="HTML")

# # #####
# # # api_client = Client('my_account',
# # #                     api_id=API_ID,
# # #                     api_hash=API_HASH)
# # #####

# dp = Dispatcher(storage=storage)
# dp.include_router(ozon_router)
# dp.include_router(wb_router)
# dp.include_router(main_router)


# # #Add session and database connection in handlers 

# # #Initialize web server
# app = FastAPI()
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )
# # event_loop = asyncio.get_event_loop()
# event_loop = asyncio.new_event_loop()
# asyncio.set_event_loop(event_loop)
# config = Config(app=app,
#                 loop=event_loop,
#                 workers=4,
#                 host='0.0.0.0',
#                 port=8001)
# server = Server(config)


# # fast_api_router = APIRouter(prefix='/bot_api')
# # # app.include_router(fast_api_router)

# # #For set webhook
# WEBHOOK_PATH = f'/webhook_'


# # Настройка хранилища задач
# # jobstores = {
# #     'sqlalchemy': SQLAlchemyJobStore(url=JOB_STORE_URL),
# # }

# # Создание и настройка планировщика
# # scheduler = AsyncIOScheduler(jobstores=jobstores)

# # scheduler = AsyncIOScheduler()

# # scheduler.add_jobstore('sqlalchemy', 'sqlalchemy', url=JOB_STORE_URL)


# dp.update.middleware(DbSessionMiddleware(session_pool=session,
#                                          scheduler=scheduler))

# async def init_db():
#     async with engine.begin() as conn:
#         # Создаем таблицы
#         await conn.run_sync(Base.metadata.drop_all)
#         await conn.run_sync(Base.metadata.create_all)


# # async def update_scheduler_jobs():
# #     query = (
# #         select(
# #             UserJob.job_id
# #         )
# #     )

# #     async with session() as _session:
# #         async with _session as __session:
# #             res = await __session.execute(query)

# #     job_ids = res.scalars().all()


# # #Set webhook and create database on start
# @app.on_event('startup')
# async def on_startup():
#     await bot.delete_webhook()
#     await bot.set_webhook(f"{PUBLIC_URL}{WEBHOOK_PATH}",
#                           drop_pending_updates=True)
#                         #   allowed_updates=['message', 'callback_query'])
#     # await init_db()
#     scheduler.start()

#     startup_update_scheduler_jobs(scheduler)
#     # await add_task_to_delete_old_message_for_users()


# @app.on_event('shutdown')
# async def on_shutdown():
#     await bot.delete_webhook(drop_pending_updates=True)
#     # await bot.set_webhook(f"{PUBLIC_URL}{WEBHOOK_PATH}",
#     #                       drop_pending_updates=True)
#                         #   allowed_updates=['message', 'callback_query'])
#     try:
#         scheduler.shutdown()
#     except Exception as ex:
#         print(ex)
#     # await init_db()
#     # Base.metadata.reflect(bind=engine)
    
# #     # Base.prepare(engine, reflect=True)
# #

# # #Endpoint for checking
# # @app.get(WEBHOOK_PATH)
# # async def any():
# #     return {'status': 'ok'}


# # #Endpoint for incoming updates
# @app.post(WEBHOOK_PATH)
# async def bot_webhook(update: dict):
#     # print('UPDATE FROM TG',update)
#     tg_update = types.Update(**update)
#     # print('TG UPDATE', tg_update, tg_update.__dict__)
#     await dp.feed_update(bot=bot, update=tg_update)


# @app.post('/send_utm_data')
# async def send_utm_data(data: UTMSchema):
#     print('CATCH UTM', data.__dict__)
#     await add_utm_to_db(data)


# if __name__ == '__main__':
#     event_loop.run_until_complete(server.serve())


################


# @app.get('/send_to_tg_group')
# async def send_to_tg_group(user_id: int,
#                            order_id: int,
#                            marker: str):
#     await test_send(user_id=user_id,
#                     order_id=order_id,
#                     marker=marker,
#                     session=session(),
#                     bot=bot)
#Endpoint for mass send message
# @app.get('/send_mass_message')
# async def send_mass_message_for_all_users(name_send: str):
#     await send_mass_message(bot=bot,
#                             session=session(),
#                             name_send=name_send)
    

# app.include_router(fast_api_router)
# fast_api_router = APIRouter()

# @fast_api_router.get('/test')
# async def test_api():
#     Guest = Base.classes.general_models_guest

#     # with session() as conn:
#     #     conn: Session
#     #     conn.query(Guest)
#     await bot.send_message('686339126', 'what`s up')
    
# app = FastAPI()

# bot = Bot(TOKEN, parse_mode="HTML")

###

# fast_api_router = APIRouter()

# @fast_api_router.get('/test')
# async def test_api():
#     Guest = Base.classes.general_models_guest

#     # with session() as conn:
#     #     conn: Session
#     #     conn.query(Guest)
#     await send_mass_message(bot=bot,
#                             session=session())
    # await bot.send_message('686339126', 'what`s up')

# app.include_router(fast_api_router)
    ###


# ### LONG POOLING ###


# # Настройка хранилища
# # jobstores = {
# #     'default': SQLAlchemyJobStore(url=db_url)  # Используйте SQLite или другую БД
# # }

# # scheduler = AsyncIOScheduler(jobstores=jobstores)


async def main():
    bot = Bot(TOKEN, parse_mode="HTML")
#     # w = await bot.get_my_commands()
#     # print(w)
#     # await bot.set_my_commands([
#     #     types.BotCommand(command='send',description='send mass message'),
#     # ])
#     # w = await bot.get_my_commands()
#     # print(w)


#     # api_client = Client('my_account',
#     #                     api_id=API_ID,
#     #                     api_hash=API_HASH)
#     async def init_db():
#         async with engine.begin() as conn:
#             # Создаем таблицы
#             # await conn.run_sync(Base.metadata.drop_all)
#             await conn.run_sync(Base.metadata.create_all)

#     # await init_db()


    dp = Dispatcher()
#     dp.include_router(ozon_router)
#     dp.include_router(wb_router)
    dp.include_router(main_router)

#     # DATABASE_URL = "postgresql+asyncpg://postgres:22222@psql_db2/postgres"


#     # jobstores = {
#     #     'default': SQLAlchemyJobStore(engine=engine)
#     # }
    # DATABASE_URL = "postgresql+psycopg2://postgres:22222@psql_db2/postgres"

    # scheduler = AsyncIOScheduler()

    # scheduler.add_jobstore('sqlalchemy', url=JOB_STORE_URL)
    # await create_new_punkts_from_old()
    redis_pool = await get_redis_background_pool()

    scheduler.start()

    # await test_migrate_on_new_sctucture_db()
    # await test_add_photo_to_exist_products()
    # startup_update_scheduler_jobs(scheduler)

#     # #Add session and database connection in handlers
    dp.update.middleware(DbSessionMiddleware(session_pool=session,
                                         scheduler=scheduler,
                                         redis_pool=redis_pool))

#     # engine = create_engine(db_url,
#     #                        echo=True)

#     # Base.prepare(engine, reflect=True)
    

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)
#     # await event_loop.run_until_complete(server.serve())
#     # uvicorn.run('main:app', host='0.0.0.0', port=8001)


if __name__ == '__main__':
    asyncio.run(main())
# # if __name__ == '__main__':
# #     event_loop.run_until_complete(server.serve())