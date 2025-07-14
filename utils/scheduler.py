import json
import re
import pytz
import aiohttp
import asyncio
import aiofiles

from datetime import datetime, timedelta
from typing import Literal


from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from sqlalchemy import insert, select, and_, text, update, func, desc

import config
from db.base import (
    Category,
    ChannelLink,
    OzonPunkt,
    PopularProduct,
    Product,
    Punkt,
    Subscription,
    WbProduct,
    WbPunkt,
    User,
    get_session,
    UserJob,
    OzonProduct,
    UTM,
    UserProduct,
    UserProductJob,
    ProductPrice,
)

from background.base import get_redis_background_pool, _redis_pool, get_redis_pool

from db.repository.product import ProductRepository
from db.repository.popular_product_sale_range import PopularProductSaleRangeRepository
from keyboards import (
    add_or_create_close_kb,
    create_remove_and_edit_sale_kb,
    new_create_remove_and_edit_sale_kb,
)

from bot22 import bot

from services.ozon_api_service import OzonAPIService
from services.wb_api_service import WbAPIService
from utils.pics import ImageManager
from utils.storage import redis_client
from utils.any import (
    generate_pretty_amount,
    generate_sale_for_price,
    add_message_to_delete_dict,
    # generate_sale_for_price_popular_product,
    send_data_to_yandex_metica,
)

from utils.exc import (
    OzonAPICrashError,
    OzonProductExistsError,
    WbAPICrashError,
    WbProductExistsError,
)

from config import (
    DEV_ID,
    SUB_DEV_ID,
    JOB_STORE_URL,
)


# Настройка хранилища задач
jobstores = {
    "sqlalchemy": SQLAlchemyJobStore(url=JOB_STORE_URL),
}

# Создание и настройка планировщика
scheduler = AsyncIOScheduler(jobstores=jobstores)


timezone = pytz.timezone("Europe/Moscow")

# scheduler_cron = CronTrigger(minute=1,
#                              timezone=timezone)

scheduler_cron = IntervalTrigger(minutes=15, timezone=timezone)


scheduler_interval = IntervalTrigger(hours=1, timezone=timezone)
image_manager = ImageManager(bot)


async def add_task_to_delete_old_message_for_users(user_id: int = None):
    print("add task to delete old message...")

    async for session in get_session():
        try:
            if user_id is not None:
                query = select(
                    User.tg_id,
                ).where(
                    User.tg_id == user_id,
                )
            else:
                query = select(
                    User.tg_id,
                )

            res = await session.execute(query)

            res = res.fetchall()
        finally:
            try:
                await session.close()
            except Exception:
                pass

    for user in res:
        user_id = user[0]
        job_id = f"delete_msg_task_{user_id}"

        job = scheduler.add_job(
            func=background_task_wrapper,
            trigger=scheduler_interval,
            id=job_id,
            coalesce=True,
            args=(f"periodic_delete_old_message", int(user_id)),  # func_name, *args
            kwargs={"_queue_name": "arq:low"},
            jobstore="sqlalchemy",
        )  # _queue_name


async def periodic_delete_old_message(user_id: int):
    print(f"TEST SCHEDULER TASK DELETE OLD MESSAGE USER {user_id}")
    key = f"fsm:{user_id}:{user_id}:data"

    async with redis_client.pipeline(transaction=True) as pipe:
        user_data: bytes = await pipe.get(key)
        results = await pipe.execute()
        # Извлекаем результат из выполненного pipeline
    # print('RESULTS', results)
    # print('USER DATA (BYTES)', user_data)

    json_user_data: dict = json.loads(results[0])
    # print('USER DATA', json_user_data)

    dict_msg_on_delete: dict = json_user_data.get("dict_msg_on_delete")
    if not dict_msg_on_delete:
        return

    for _key in list(dict_msg_on_delete.keys()):
        chat_id, message_date = dict_msg_on_delete.get(_key)
        date_now = datetime.now()
        # тестовый вариант, удаляем сообщения старше 1 часа
        print(
            (
                datetime.fromtimestamp(date_now.timestamp())
                - datetime.fromtimestamp(message_date)
            )
            > timedelta(hours=36)
        )
        if (
            datetime.fromtimestamp(date_now.timestamp())
            - datetime.fromtimestamp(message_date)
        ) > timedelta(hours=36):
            try:
                await bot.delete_message(chat_id=chat_id, message_id=_key)
                await asyncio.sleep(0.1)
                # await bot.delete_messages() # что будет если какое то сообщение не сможет удалиться и произойдет ошибка ???
            except Exception as ex:
                del dict_msg_on_delete[_key]
                print(ex)
            else:
                del dict_msg_on_delete[_key]


async def new_check_product_by_user_in_db(
    user_id: int, short_link: str, session: AsyncSession
):
    query = (
        select(UserProduct.id)
        .join(Product, UserProduct.product_id == Product.id)
        .where(
            and_(
                Product.short_link == short_link,
                UserProduct.user_id == user_id,
            )
        )
    )
    async with session as _session:
        res = await _session.execute(query)

    _check_product = res.scalar_one_or_none()

    return bool(_check_product)


async def new_check_subscription_limit(
    user_id: int, marker: Literal["wb", "ozon"], session: AsyncSession
):
    # product_model = OzonProduct if marker == 'ozon' else WbProduct
    # print(marker)
    marker = marker.lower()

    if marker == "wb":
        subscription_limit = Subscription.wb_product_limit
    else:
        subscription_limit = Subscription.ozon_product_limit

    # pylint: disable=not-callable
    query = (
        select(
            func.count(UserProduct.id),
            subscription_limit,
        )
        .join(User, UserProduct.user_id == User.tg_id)
        .join(Subscription, User.subscription_id == Subscription.id)
        .join(Product, UserProduct.product_id == Product.id)
        .where(
            and_(
                # product_model.short_link == short_link,
                Product.product_marker == marker,
                UserProduct.user_id == user_id,
            )
        )
        .group_by(subscription_limit)
    )

    async with session as _session:
        res = await _session.execute(query)

    _check_limit = res.fetchall()

    if _check_limit:
        _check_limit = _check_limit[0]

        product_count, subscription_limit = _check_limit

        print("SUBSCRIPTION TEST", product_count, subscription_limit)

        if product_count >= subscription_limit:
            return subscription_limit


# async def update_sale_for_popular_products():
#     query = select(
#         PopularProduct,
#     )

#     async for session in get_session():
#         async with session as _session:
#             res = await _session.execute(query)

#             popular_products = res.scalars().all()

#             for popular_product in popular_products:
#                 start_price = popular_product.start_price
#                 popular_product.sale = await generate_sale_for_price_popular_product(
#                     session,
#                     start_price
#                 )

#             try:
#                 await _session.commit()
#                 print("UDPATE POPULAR PRODUCTS SUCCESSFULLY")
#             except Exception:
#                 await _session.rollback()
#                 print("UDPATE POPULAR PRODUCTS WITH ERROR")


async def add_product_to_db_popular_product(
    data: dict, session: AsyncSession, scheduler: AsyncIOScheduler
):
    short_link = data.get("short_link")
    name = data.get("name")
    photo_id = data.get("photo_id")
    high_category = data.get("high_category")
    low_category = data.get("low_category")
    marker: str = data.get("product_marker")

    check_product_query = select(Product).where(
        Product.short_link == short_link,
    )

    async with session as _session:
        res = await _session.execute(check_product_query)

        _product = res.scalar_one_or_none()

        if not _product:
            insert_data = {
                "product_marker": marker,
                "name": name,
                "short_link": short_link,
                "photo_id": photo_id,
            }

            _product = Product(**insert_data)
            _session.add(_product)

            await _session.flush()
            await _session.commit()

    product_id = _product.id
    print("product_id", product_id)

    check_high_category_query = select(Category).where(
        Category.name == high_category,
    )

    check_low_category_query = select(Category).where(
        Category.name == low_category,
    )

    default_channel_query = select(ChannelLink).where(
        ChannelLink.name == "Общий",
    )

    public_default_channel_query = select(ChannelLink).where(
        ChannelLink.name == "Общий публичный",
    )

    async with session as _session:
        high_res = await _session.execute(check_high_category_query)
        low_res = await _session.execute(check_low_category_query)
        default_channel_res = await _session.execute(default_channel_query)
        public_default_channel_res = await _session.execute(
            public_default_channel_query
        )

        high_category_obj = high_res.scalar_one_or_none()
        low_category_obj = low_res.scalar_one_or_none()
        default_channel_obj = default_channel_res.scalar_one_or_none()
        public_default_channel_obj = public_default_channel_res.scalar_one_or_none()

        if not high_category_obj:
            insert_data = {
                "name": high_category,
            }

            high_category_obj = Category(**insert_data)
            high_category_obj.channel_links.append(default_channel_obj)
            high_category_obj.channel_links.append(public_default_channel_obj)

            session.add(high_category_obj)
            await _session.flush()

            await _session.commit()

        if not low_category_obj:
            insert_data = {
                "name": low_category,
                "parent_id": high_category_obj.id,
            }

            low_category_obj = Category(**insert_data)
            low_category_obj.channel_links.append(default_channel_obj)
            low_category_obj.channel_links.append(public_default_channel_obj)

            _session.add(low_category_obj)
            await _session.flush()

            await _session.commit()

    popular_product_data = {
        "link": data.get("link"),
        "product_id": product_id,
        "start_price": data.get("start_price"),
        "actual_price": data.get("actual_price"),
        "sale": data.get("sale"),
        "time_create": datetime.now(),
        "category_id": low_category_obj.id,
    }

    popular_product = PopularProduct(**popular_product_data)

    print("pop", popular_product)

    async with session as _session:
        _session.add(popular_product)
        await _session.flush()

        await _session.commit()
        print("added!!!!!")

        job_id = f"popular_{marker}_{popular_product.id}"
        job = scheduler.add_job(
            func=background_task_wrapper,
            trigger="interval",
            hours=2,
            id=job_id,
            coalesce=True,
            args=(
                f"push_check_{marker}_popular_product",
                popular_product.id,
            ),  # func_name, *args
            kwargs={"_queue_name": "arq:popular"},  # _queue_name
            jobstore="sqlalchemy",
        )
        print("jobbb", job)


async def add_product_to_db(
    data: dict,
    marker: str,
    is_first_product: bool,
    session: AsyncSession,
    scheduler: AsyncIOScheduler,
):
    short_link = data.get("short_link")
    name = data.get("name")
    user_id = data.get("user_id")
    photo_id = data.get("photo_id")

    check_product_query = select(Product).where(
        Product.short_link == short_link,
    )

    async with session as _session:
        res = await _session.execute(check_product_query)

    _product = res.scalar_one_or_none()

    if not _product:
        insert_data = {
            "product_marker": marker,
            "name": name,
            "short_link": short_link,
            "photo_id": photo_id,
        }

        _product = Product(**insert_data)
        _session.add(_product)

        await session.flush()

    product_id = _product.id

    user_product_data = {
        "link": data.get("link"),
        "product_id": product_id,
        "user_id": user_id,
        "start_price": data.get("start_price"),
        "actual_price": data.get("actual_price"),
        "sale": data.get("sale"),
        "time_create": datetime.now(),
    }

    user_product = UserProduct(**user_product_data)

    session.add(user_product)

    await session.flush()

    user_product_id = user_product.id

    #          user_id | marker | product_id
    job_id = f"{user_id}:{marker}:{user_product_id}"
    # job_id = 'test_job_id'

    # if user_id == int(DEV_ID):

    if marker == "wb":
        func_name = "new_push_check_wb_price"
    else:
        func_name = "new_push_check_ozon_price"

    job = scheduler.add_job(
        background_task_wrapper,
        trigger="interval",
        minutes=15,
        id=job_id,
        jobstore="sqlalchemy",
        coalesce=True,
        args=(
            func_name,
            user_id,
            user_product_id,
        ),
        kwargs={"_queue_name": "arq:low"},
    )
    # else:
    #     if marker == 'wb':
    #         scheduler_func = new_push_check_wb_price
    #     else:
    #         scheduler_func = new_push_check_ozon_price

    #     job = scheduler.add_job(scheduler_func,
    #                             trigger='interval',
    #                             minutes=15,
    #                             id=job_id,
    #                             jobstore='sqlalchemy',
    #                             coalesce=True,
    #                             kwargs={'user_id': user_id,
    #                                     'product_id': user_product_id})

    _data = {
        "user_product_id": user_product_id,
        "job_id": job.id,
        # 'job_id': job_id,
    }

    user_job = UserProductJob(**_data)

    session.add(user_job)

    if marker == "wb":
        update_count_query = (
            update(User)
            .values(
                wb_total_count=User.wb_total_count + 1,
            )
            .where(
                User.tg_id == user_id,
            )
        )
    else:
        update_count_query = (
            update(User)
            .values(
                ozon_total_count=User.ozon_total_count + 1,
            )
            .where(
                User.tg_id == user_id,
            )
        )

    async with session as _session:
        try:
            await _session.execute(update_count_query)
            await _session.commit()
            _text = f"{marker} товар успешно добавлен"
            print(_text)
        except Exception as ex:
            print(ex)
            await _session.rollback()
            _text = f"{marker} товар не был добавлен"
            print(_text)
        else:
            if is_first_product:
                # get request to yandex metrika
                utm_query = select(UTM.client_id).where(UTM.user_id == int(user_id))

                utm_res = await _session.execute(utm_query)

                client_id = utm_res.scalar_one_or_none()

                if client_id:
                    await send_data_to_yandex_metica(client_id, goal_id="add_product")


async def try_update_ozon_product_photo(
    product_id: int, short_link: str, session: AsyncSession
):
    photo_id = None
    try:
        api_service = OzonAPIService()
        text_data = await api_service.get_product_data(short_link)

        photo_url_pattern = r'images\\":\[{\\"src\\":\\"https:\/\/cdn1\.ozone\.ru\/s3\/multimedia-[a-z0-9]*(-\w*)?(\/*[a-z0-9]*\/*)?\/\d+\.jpg'

        match = re.search(photo_url_pattern, text_data)

        if match:
            # print('search',match.group())
            photo_url_match = re.search(r"https.*\.jpg?", match.group())
            if photo_url_match:
                photo_url = photo_url_match.group()

                photo_id = await image_manager.generate_photo_id_for_url(photo_url)
    except Exception as ex:
        print(ex)
    finally:
        if not photo_id:
            photo_id = await image_manager.get_default_product_photo_id()

    repo = ProductRepository(session)
    repo.update(product_id, photo_id=photo_id)


async def try_get_ozon_product_photo(
    short_link: str, text_data: str, session: AsyncSession
):
    repo = ProductRepository(session)
    product = await repo.find_by_short_link(short_link)
    if product:
        return product.photo_id

    # photo_url_pattern = r'image\\":\\"https:\/\/cdn1\.ozone\.ru\/s3\/multimedia-\d+(-\w+)?\/\d+\.jpg'

    photo_url_pattern = r'images\\":\[{\\"src\\":\\"https:\/\/cdn1\.ozone\.ru\/s3\/multimedia-[a-z0-9]*(-\w*)?\/\d+\.jpg'

    match = re.search(photo_url_pattern, text_data)

    if match:
        # print('search',match.group())
        photo_url_match = re.search(r"https.*\.jpg?", match.group())
        if photo_url_match:
            photo_url = photo_url_match.group()
            # print('RESULT URL',photo_url)
            return await image_manager.generate_photo_id_for_url(url=photo_url)
    else:
        print("URL не найден")
    return await image_manager.get_default_product_photo_id()


async def save_popular_ozon_product(
    product_data: dict, session: AsyncSession, scheduler: AsyncIOScheduler
):
    link: str = product_data.get("link")
    name: str = product_data.get("name")

    if link.startswith("https://ozon.ru/t/"):
        _idx = link.find("/t/")
        _prefix = "/t/"
        ozon_short_link = "croppedLink|" + link[_idx + len(_prefix) :]
        print(ozon_short_link)
    else:
        _prefix = "product/"
        _idx = link.rfind("product/")
        ozon_short_link = link[(_idx + len(_prefix)) :]

    api_service = OzonAPIService()
    res = await api_service.get_product_data(ozon_short_link)

    _new_short_link = res.split("|")[0]
    print(_new_short_link)

    response_data = res.split("|", maxsplit=1)[-1]

    json_data: dict = json.loads(response_data)

    photo_id = await try_get_ozon_product_photo(
        short_link=_new_short_link, text_data=res, session=session
    )

    if not photo_id:
        photo_id = await image_manager.get_default_product_photo_id()

    w = re.findall(r"\"cardPrice.*currency?", res)

    if w:
        w = w[0].split(",")[:3]

        _d = {
            "price": None,
            "originalPrice": None,
            "cardPrice": None,
        }

        for k in _d:
            if not all(v for v in _d.values()):
                for q in w:
                    if q.find(k) != -1:
                        _name, price = q.split(":")
                        price = price.replace("\\", "").replace('"', "")
                        price = float("".join(price.split()[:-1]))
                        print(price)
                        _d[k] = price
                        break
            else:
                break

        print(_d)
        start_price = int(_d.get("cardPrice", 0))
        actual_price = int(_d.get("cardPrice", 0))
        basic_price = int(_d.get("price", 0))

    else:
        # try:
        script_list = json_data.get("seo").get("script")

        inner_html = script_list[0].get("innerHTML")  # .get('offers').get('price')

        # try:
        inner_html_json: dict = json.loads(inner_html)
        offers = inner_html_json.get("offers")

        _price = offers.get("price")

        start_price = int(_price)
        actual_price = int(_price)
        basic_price = int(_price)

        print("Price", _price)

    # _sale = generate_sale_for_price(start_price)
    _sale = await generate_sale_for_price_popular_product(session, start_price)  # new !

    _data = {
        "link": link,
        "short_link": _new_short_link,
        "name": name,
        "actual_price": actual_price,
        "start_price": start_price,
        "basic_price": basic_price,
        "sale": _sale,
        "photo_id": photo_id,
        "product_marker": product_data.get("product_marker"),
        "high_category": product_data.get("high_category"),
        "low_category": product_data.get("low_category"),
    }

    await add_product_to_db_popular_product(_data, session, scheduler)


async def generate_sale_for_price_popular_product(
    session: AsyncSession, price: float
) -> float:
    repo = PopularProductSaleRangeRepository(session)
    coef = await repo.get_sale_coefficient(price)
    return price * coef


async def save_popular_wb_product(
    product_data: dict, session: AsyncSession, scheduler: AsyncIOScheduler
):
    link = product_data.get("link")
    name = product_data.get("name")

    _prefix = "catalog/"

    _idx_prefix = link.find(_prefix)

    short_link = link[_idx_prefix + len(_prefix) :].split("/")[0]

    api_service = WbAPIService()
    res = await api_service.get_product_data(
        short_link, config.WB_DEFAULT_DELIVERY_ZONE
    )

    photo_id = await try_get_wb_product_photo(short_link=short_link, session=session)

    if not photo_id:
        photo_id = await image_manager.get_default_product_photo_id()

    d = res.get("data")

    sizes = d.get("products")[0].get("sizes")

    _product_name = d.get("products")[0].get("name")

    _basic_price = _product_price = None

    for size in sizes:
        _price = size.get("price")
        if _price:
            _basic_price = size.get("price").get("basic")
            _product_price = size.get("price").get("product")

            _basic_price = str(_basic_price)[:-2]
            _product_price = str(_product_price)[:-2]

            _product_price = float(_product_price)

    print("WB price", _product_price)

    _sale = await generate_sale_for_price_popular_product(
        session, float(_product_price)
    )

    _data_name = name if name else _product_name

    _data = {
        "link": link,
        "short_link": short_link,
        "start_price": _product_price,
        "actual_price": _product_price,
        "sale": _sale,
        "name": _data_name,
        "photo_id": photo_id,
    }

    await add_product_to_db_popular_product(_data, session, scheduler)


async def save_ozon_product(
    user_id: int,
    link: str,
    name: str | None,
    is_first_product: bool,
    session: AsyncSession,
    scheduler: AsyncIOScheduler,
):
    if link.startswith("https://ozon.ru/t/"):
        _idx = link.find("/t/")
        _prefix = "/t/"
        ozon_short_link = "croppedLink|" + link[_idx + len(_prefix) :]
        print(ozon_short_link)
    else:
        _prefix = "product/"
        _idx = link.rfind("product/")
        ozon_short_link = link[(_idx + len(_prefix)) :]

    query = select(
        UserProduct.id,
    ).where(
        UserProduct.user_id == user_id,
        UserProduct.link == link,
    )
    async with session as _session:
        res = await _session.execute(query)

    res = res.scalar_one_or_none()

    if res:
        raise OzonProductExistsError()

    query = (
        select(
            Punkt.ozon_zone,
        )
        .join(User, Punkt.user_id == User.tg_id)
        .where(User.tg_id == user_id)
    )
    async with session as _session:
        res = await _session.execute(query)

    del_zone = res.scalar_one_or_none()

    print("do request on OZON API (new version)")

    api_service = OzonAPIService()
    res = await api_service.get_product_data(ozon_short_link, del_zone)

    _new_short_link = res.split("|")[0]
    print(_new_short_link)

    check_product_by_user = await new_check_product_by_user_in_db(
        user_id=user_id, short_link=_new_short_link, session=session
    )

    if check_product_by_user:
        raise OzonProductExistsError()

    response_data = res.split("|", maxsplit=1)[-1]

    json_data: dict = json.loads(response_data)

    photo_id = await try_get_ozon_product_photo(
        short_link=_new_short_link, text_data=res, session=session
    )

    if not photo_id:
        photo_id = await image_manager.get_default_product_photo_id()
        # print('Не удалось спарсить фото OZON товара')
        # raise Exception()

    w = re.findall(r"\"cardPrice.*currency?", res)

    if w:
        w = w[0].split(",")[:3]

        _d = {
            "price": None,
            "originalPrice": None,
            "cardPrice": None,
        }

        for k in _d:
            if not all(v for v in _d.values()):
                for q in w:
                    if q.find(k) != -1:
                        _name, price = q.split(":")
                        price = price.replace("\\", "").replace('"', "")
                        price = float("".join(price.split()[:-1]))
                        print(price)
                        _d[k] = price
                        break
            else:
                break

        print(_d)
        start_price = int(_d.get("cardPrice", 0))
        actual_price = int(_d.get("cardPrice", 0))
        basic_price = int(_d.get("price", 0))

    else:
        script_list = json_data.get("seo").get("script")

        inner_html = script_list[0].get("innerHTML")  # .get('offers').get('price')

        inner_html_json: dict = json.loads(inner_html)
        offers = inner_html_json.get("offers")

        _price = offers.get("price")

        start_price = int(_price)
        actual_price = int(_price)
        basic_price = int(_price)

        print("Price", _price)

    # if not name:
    name = " ".join(json_data.get("seo").get("title").split()[:4])

    print("NAMEEE FROM SEO", name)

    _sale = generate_sale_for_price(start_price)

    _data = {
        "link": link,
        "short_link": _new_short_link,
        "name": name,
        "actual_price": actual_price,
        "start_price": start_price,
        "basic_price": basic_price,
        "sale": _sale,
        "user_id": user_id,
        "photo_id": photo_id,
    }

    await add_product_to_db(_data, "ozon", is_first_product, session, scheduler)


async def try_update_wb_product_photo(
    product_id: int, short_link: str, session: AsyncSession
):
    try:
        api_service = WbAPIService()
        image_data = await api_service.get_product_image(short_link)

        image_name = "test_image.png"

        async with aiofiles.open(image_name, "wb") as file:
            await file.write(image_data)

        photo_id = await image_manager.generate_photo_id_for_file(f"./{image_name}")

        if not photo_id:
            photo_id = await image_manager.get_default_product_photo_id()

        repo = ProductRepository(session)
        await repo.update(product_id, photo_id=photo_id)
    except Exception as e:
        print(e)


async def try_get_wb_product_photo(short_link: str, session: AsyncSession):
    async with session as _session:
        repo = ProductRepository(session)
        product = await repo.find_by_short_link(short_link)
        if product:
            return product.photo_id

    api_service = WbAPIService()
    image_data = await api_service.get_product_image(short_link)

    image_name = "test_image.png"

    async with aiofiles.open(image_name, "wb") as file:
        await file.write(image_data)

    return await image_manager.generate_photo_id_for_file(f"./{image_name}")


async def save_wb_product(
    user_id: int,
    link: str,
    name: str | None,
    is_first_product: bool,
    session: AsyncSession,
    scheduler: AsyncIOScheduler,
):
    _prefix = "catalog/"

    _idx_prefix = link.find(_prefix)

    short_link = link[_idx_prefix + len(_prefix) :].split("/")[0]

    query = select(
        UserProduct.id,
    ).where(
        UserProduct.user_id == user_id,
        UserProduct.link == link,
    )
    async with session as _session:
        res = await _session.execute(query)

    res = res.scalar_one_or_none()

    if res:
        raise WbProductExistsError()

    query = (
        select(
            Punkt.wb_zone,
        )
        .join(User, Punkt.user_id == User.tg_id)
        .where(User.tg_id == user_id)
    )
    async with session as _session:
        res = await _session.execute(query)

    del_zone = res.scalar_one_or_none()

    if not del_zone:
        del_zone = config.WB_DEFAULT_DELIVERY_ZONE

    check_product_by_user = await new_check_product_by_user_in_db(
        user_id=user_id, short_link=short_link, session=session
    )

    if check_product_by_user:
        raise WbProductExistsError()

    api_service = WbAPIService()
    res = await api_service.get_product_data(short_link, del_zone)

    photo_id = await try_get_wb_product_photo(short_link=short_link, session=session)

    if not photo_id:
        photo_id = await image_manager.get_default_product_photo_id()

    d = res.get("data")

    sizes = d.get("products")[0].get("sizes")

    _product_name = d.get("products")[0].get("name")

    _basic_price = _product_price = None

    for size in sizes:
        _price = size.get("price")
        if _price:
            _basic_price = size.get("price").get("basic")
            _product_price = size.get("price").get("product")

            _basic_price = str(_basic_price)[:-2]
            _product_price = str(_product_price)[:-2]

            _product_price = float(_product_price)

    print("WB price", _product_price)

    _sale = generate_sale_for_price(float(_product_price))

    _data_name = name if name else _product_name

    _data = {
        "link": link,
        "short_link": short_link,
        "start_price": _product_price,
        "actual_price": _product_price,
        "sale": _sale,
        "name": _data_name,
        "user_id": user_id,
        "photo_id": photo_id,
    }

    await add_product_to_db(_data, "wb", is_first_product, session, scheduler)


async def save_popular_product(
    product_data: dict, session: AsyncSession, scheduler: AsyncIOScheduler
):
    link: str = product_data.get("link")
    name: str = product_data.get("name")

    if link.find("ozon") > 0:
        # save popular ozon product
        await save_popular_ozon_product(
            product_data=product_data, session=session, scheduler=scheduler
        )

    elif link.find("wildberries") > 0:
        # save popular wb product
        await save_popular_wb_product(
            product_data=product_data, session=session, scheduler=scheduler
        )


async def new_save_product(
    user_data: dict, session: AsyncSession, scheduler: AsyncIOScheduler
):
    msg = user_data.get("msg")
    _name = user_data.get("name")
    link: str = user_data.get("link")
    link = link.split("?")[0]

    print("NAMEEE", _name)

    query = select(UserProduct.id).where(UserProduct.user_id == msg[0])

    async with session as _session:
        res = await _session.execute(query)

    products_by_user = res.scalars().all()

    product_count_by_user = len(products_by_user)

    is_first_product = not bool(product_count_by_user)

    print(f"PRODUCT COUNT BY USER {msg[0]} {product_count_by_user}")

    if link.find("ozon") > 0:
        # save ozon product
        await save_ozon_product(
            user_id=msg[0],
            link=link,
            name=_name,
            is_first_product=is_first_product,
            session=session,
            scheduler=scheduler,
        )

    elif link.find("wildberries") > 0:
        # save wb product
        await save_wb_product(
            user_id=msg[0],
            link=link,
            name=_name,
            is_first_product=is_first_product,
            session=session,
            scheduler=scheduler,
        )


async def test_add_photo_to_exist_products():
    product_query = select(
        Product.id,
        Product.product_marker,
        Product.short_link,
        Product.photo_id,
    ).where(
        Product.photo_id.is_(None),
    )

    async for session in get_session():
        async with session as _session:
            res = await _session.execute(product_query)

            for product in res:
                _id, marker, short_link, photo_id = product
                print("PRODUCT", product)

                if marker == "wb":
                    if not photo_id:
                        await try_update_wb_product_photo(
                            product_id=_id, short_link=short_link, session=_session
                        )
                        await asyncio.sleep(1.5)

                elif marker == "ozon":
                    if not photo_id:
                        await try_update_ozon_product_photo(
                            product_id=_id, short_link=short_link, session=_session
                        )
                        await asyncio.sleep(1.5)

            try:
                await _session.commit()
            except Exception as ex:
                print(ex)
                await _session.rollback()


async def send_fake_price(
    user_id: int, product_id: int, fake_price: int, session: AsyncSession
):
    async with session as _session:
        try:
            query = (
                select(
                    Product.id,
                    UserProduct.id,
                    UserProduct.link,
                    Product.short_link,
                    Product.product_marker,
                    UserProduct.actual_price,
                    UserProduct.start_price,
                    Product.name,
                    UserProduct.sale,
                    Punkt.ozon_zone,
                    Punkt.city,
                    UserProductJob.job_id,
                    Product.photo_id,
                    UserProduct.last_send_price,
                )
                .select_from(UserProduct)
                .join(Product, UserProduct.product_id == Product.id)
                .outerjoin(Punkt, Punkt.user_id == int(user_id))
                .outerjoin(
                    UserProductJob, UserProductJob.user_product_id == UserProduct.id
                )
                .where(
                    and_(
                        UserProduct.id == int(product_id),
                        UserProduct.user_id == int(user_id),
                    )
                )
            )

            res = await _session.execute(query)

            res = res.fetchall()
        finally:
            try:
                await _session.close()
            except Exception:
                pass
    if res:
        (
            main_product_id,
            _id,
            link,
            short_link,
            product_marker,
            actual_price,
            start_price,
            name,
            sale,
            zone,
            city,
            job_id,
            photo_id,
            last_send_price,
        ) = res[0]

        _waiting_price = start_price - sale

        pretty_product_price = generate_pretty_amount(fake_price)
        pretty_actual_price = generate_pretty_amount(actual_price)
        pretty_sale = generate_pretty_amount(sale)
        pretty_start_price = generate_pretty_amount(start_price)

        if _waiting_price >= fake_price:

            # проверка, отправлялось ли уведомление с такой ценой в прошлый раз
            # if last_send_price is not None and (last_send_price == _product_price):
            #     print(f'LAST SEND PRICE VALIDATION STOP {last_send_price} | {_product_price}')
            #     return

            if actual_price < fake_price:
                _text = f'🔄 Цена повысилась, но всё ещё входит в выставленный диапазон скидки на товар <a href="{link}">{name}</a>\n\nМаркетплейс: Ozon\n\n🔄Отслеживаемая скидка: {pretty_sale}\n\n⬇️Цена по карте: {pretty_product_price} (дешевле на {start_price - fake_price}₽)\n\nНачальная цена: {pretty_start_price}\n\nПредыдущая цена: {pretty_actual_price}'
                _disable_notification = True
            else:
                _text = f'🚨 Изменилась цена на <a href="{link}">{name}</a>\n\nМаркетплейс: {product_marker}\n\n🔄Отслеживаемая скидка: {pretty_sale}\n\n⬇️Цена по карте: {pretty_product_price} (дешевле на {start_price - fake_price}₽)\n\nНачальная цена: {pretty_start_price}\n\nПредыдущая цена: {pretty_actual_price}'
                _disable_notification = False

            _kb = new_create_remove_and_edit_sale_kb(
                user_id=user_id,
                product_id=product_id,
                marker=product_marker,
                job_id=job_id,
                with_redirect=False,
            )

            _kb = add_or_create_close_kb(_kb)

            msg = await bot.send_photo(
                chat_id=user_id,
                photo=photo_id,
                caption=_text,
                disable_notification=_disable_notification,
                reply_markup=_kb.as_markup(),
            )

            # await update_last_send_price_by_user_product(last_send_price=_product_price,
            #                                                 user_product_id=_id)

            await add_message_to_delete_dict(msg)
            return


# для планировании задачи в APScheduler и выполнения в ARQ worker`e
async def background_task_wrapper(func_name, *args, _queue_name):

    _redis_pool = get_redis_pool()

    _args_str = ".".join([f"{arg}" for arg in args])

    _job_id = f"{func_name}_{_args_str}"

    await _redis_pool.enqueue_job(
        func_name, *args, _queue_name=_queue_name, _job_id=_job_id
    )


async def sync_popular_product_jobs(scheduler: AsyncIOScheduler):
    async for session in get_session():
        result = await session.execute(
            text(r"SELECT id FROM apscheduler_jobs where id like '%popular%';")
        )

        existing_job_ids = [r[0] for r in result]
        existing_pp_ids = list(
            map(lambda job_id: int(job_id.split("_")[-1]), existing_job_ids)
        )

        # Получаем список актуальных ID популярных продуктов
        popular_product_result = await session.execute(select(PopularProduct.id))
        actual_pp_ids = set(row[0] for row in popular_product_result)

        # --- Удаление задач, для которых больше нет популярных продуктов ---
        obsolete_job_ids = [
            job_id
            for job_id in existing_job_ids
            if int(job_id.split("_")[-1]) not in actual_pp_ids
        ]
        for job_id in obsolete_job_ids:
            scheduler.remove_job(job_id)

        # --- Добавление задач, которые отсутствуют, но должны быть ---
        popular_products_stmt = (
            select(PopularProduct.id, Product.product_marker)
            .join(Product, PopularProduct.product_id == Product.id)
            .where(~PopularProduct.id.in_(existing_pp_ids))
        )

        result = await session.execute(popular_products_stmt)
        popular_products_list = result.all()

        for popular_product_data in popular_products_list:
            pp_id, marker = popular_product_data

            job_id = f"popular_{marker}_{pp_id}"
            scheduler.add_job(
                func=background_task_wrapper,
                trigger="interval",
                hours=2,
                id=job_id,
                coalesce=True,
                args=(
                    f"push_check_{marker}_popular_product",
                    pp_id,
                ),  # func_name, *args
                kwargs={"_queue_name": "arq:popular"},  # _queue_name
                jobstore="sqlalchemy",
            )


async def push_check_wb_price(user_id: str, product_id: str):

    print(f"фоновая задача {user_id}")

    async for session in get_session():
        try:
            subquery = (
                select(UserJob.job_id, UserJob.user_id, UserJob.product_id).where(
                    UserJob.user_id == user_id
                )
            ).subquery()

            query = (
                select(
                    User.username,
                    WbProduct.link,
                    WbProduct.short_link,
                    WbProduct.actual_price,
                    WbProduct.start_price,
                    WbProduct.name,
                    WbProduct.sale,
                    WbPunkt.zone,
                    subquery.c.job_id,
                )
                .select_from(WbProduct)
                .outerjoin(WbPunkt, WbProduct.wb_punkt_id == WbPunkt.id)
                .join(User, WbProduct.user_id == User.tg_id)
                .outerjoin(subquery, subquery.c.product_id == WbProduct.id)
                .where(
                    and_(
                        User.tg_id == user_id,
                        WbProduct.id == product_id,
                    )
                )
                .distinct(WbProduct.id)
            )

            res = await session.execute(query)

            res = res.fetchall()
        finally:
            try:
                await session.close()
            except Exception:
                pass
    if res:
        (
            username,
            link,
            short_link,
            actual_price,
            start_price,
            _name,
            sale,
            zone,
            job_id,
        ) = res[0]

        if not zone:
            zone = config.WB_DEFAULT_DELIVERY_ZONE

        name = _name if _name is not None else "Отсутствует"
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession() as aiosession:
                _url = f"http://172.18.0.7:8080/product/{zone}/{short_link}"

                async with aiosession.get(url=_url, timeout=timeout) as response:
                    res = await response.json()

            d = res.get("data")

            sizes = d.get("products")[0].get("sizes")

            _basic_price = _product_price = None

            for size in sizes:
                _price = size.get("price")
                if _price:
                    _basic_price = size.get("price").get("basic")
                    _product_price = size.get("price").get("product")

                    _basic_price = str(_basic_price)[:-2]
                    _product_price = str(_product_price)[:-2]

                    print("основная:", _basic_price)
                    print("актупльная:", _product_price)

            _product_price = float(_product_price)

            print(f"TEST PRICE PROBLEM {_product_price} | {actual_price}")

            check_price = _product_price == actual_price

            if check_price:
                _text = "цена не изменилась"
                print(f"{_text} user {user_id} product {_name}")
                return
            else:

                query = (
                    update(WbProduct)
                    .values(actual_price=_product_price)
                    .where(WbProduct.id == product_id)
                )
                async for session in get_session():
                    try:
                        await session.execute(query)
                        await session.commit()
                    except Exception as ex:
                        await session.rollback()
                        print(ex)

                _waiting_price = start_price - sale

                pretty_product_price = generate_pretty_amount(_product_price)
                pretty_actual_price = generate_pretty_amount(actual_price)
                pretty_sale = generate_pretty_amount(sale)
                pretty_waiting_price = generate_pretty_amount(_waiting_price)
                pretty_start_price = generate_pretty_amount(start_price)

                if _waiting_price >= _product_price:

                    if actual_price < _product_price:
                        _text = f'🔄 Цена повысилась, но всё ещё входит в выставленный диапазон скидки на товар <a href="{link}">{_name}</a>\n\nМаркетплейс: Wb\n🔄Отслеживаемая скидка: {pretty_sale}\n\n⬇️Цена по карте: {pretty_product_price} (дешевле на {start_price - _product_price}₽)\n\nНачальная цена: {pretty_start_price}\n\nПредыдущая цена: {pretty_actual_price}'
                        _disable_notification = True
                    else:
                        _text = f'🚨 Изменилась цена на <a href="{link}">{_name}</a>\n\nМаркетплейс: Wb\n🔄Отслеживаемая скидка: {pretty_sale}\n\n⬇️Цена по карте: {pretty_product_price} (дешевле на {start_price - _product_price}₽)\n\nНачальная цена: {pretty_start_price}\n\nПредыдущая цена: {pretty_actual_price}'
                        _disable_notification = False

                    _kb = create_remove_and_edit_sale_kb(
                        user_id=user_id,
                        product_id=product_id,
                        marker="wb",
                        job_id=job_id,
                        with_redirect=False,
                    )

                    _kb = add_or_create_close_kb(_kb)

                    msg = await bot.send_message(
                        chat_id=user_id,
                        text=_text,
                        disable_notification=_disable_notification,
                        reply_markup=_kb.as_markup(),
                    )
                    await add_message_to_delete_dict(msg)
                    return

        except Exception as ex:
            print(ex)
            pass


async def push_check_ozon_price(user_id: str, product_id: str):

    print(f"фоновая задача {user_id}")

    async for session in get_session():
        async with session as _session:
            try:
                subquery = (
                    select(UserJob.job_id, UserJob.user_id, UserJob.product_id).where(
                        UserJob.user_id == user_id
                    )
                ).subquery()

                query = (
                    select(
                        User.username,
                        OzonProduct.link,
                        OzonProduct.short_link,
                        OzonProduct.actual_price,
                        OzonProduct.start_price,
                        OzonProduct.name,
                        OzonProduct.sale,
                        OzonPunkt.zone,
                        subquery.c.job_id,
                    )
                    .select_from(OzonProduct)
                    .join(User, OzonProduct.user_id == User.tg_id)
                    .outerjoin(OzonPunkt, OzonProduct.ozon_punkt_id == OzonPunkt.id)
                    .outerjoin(subquery, subquery.c.product_id == OzonProduct.id)
                    .where(
                        and_(
                            User.tg_id == user_id,
                            OzonProduct.id == product_id,
                        )
                    )
                    .distinct(OzonProduct.id)
                )

                res = await _session.execute(query)

                res = res.fetchall()
            finally:
                try:
                    await _session.close()
                except Exception:
                    pass
    if res:
        (
            username,
            link,
            short_link,
            actual_price,
            start_price,
            _name,
            sale,
            zone,
            job_id,
        ) = res[0]

        _name = _name if _name is not None else "Отсутствует"
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession() as aiosession:
                if zone:
                    _url = f"http://172.18.0.6:8080/product/{zone}/{short_link}"
                # _url = f"http://5.61.53.235:1441/product/{message.text}"
                else:
                    _url = f"http://172.18.0.6:8080/product/{short_link}"
                async with aiosession.get(url=_url, timeout=timeout) as response:

                    print(response.status)

                    if response.status == 408:
                        print("OZON TIMEOUT")
                        return

                    res = await response.text()

            w = re.findall(r"\"cardPrice.*currency?", res)

            if w:
                w = w[0].split(",")[:3]

                _d = {
                    "price": None,
                    "originalPrice": None,
                    "cardPrice": None,
                }

                for k in _d:
                    if not all(v for v in _d.values()):
                        for q in w:
                            if q.find(k) != -1:
                                name, price = q.split(":")
                                price = price.replace("\\", "").replace('"', "")
                                price = float("".join(price.split()[:-1]))
                                print(price)
                                _d[k] = price
                                break
                    else:
                        break

                print(_d)

                _product_price = _d.get("cardPrice", 0)
            else:
                try:
                    response_data = res.split("|")[-1]

                    json_data: dict = json.loads(response_data)

                    script_list = json_data.get("seo").get("script")

                    inner_html = script_list[0].get(
                        "innerHTML"
                    )  # .get('offers').get('price')

                    inner_html_json: dict = json.loads(inner_html)
                    offers = inner_html_json.get("offers")

                    _price = offers.get("price")

                    _product_price = _price

                    print("Price", _price)
                except Exception as ex:
                    print("scheduler parse inner html error", ex)
                    return

            #
            _product_price = float(_product_price)

            print(f"TEST PRICE PROBLEM {_product_price} | {actual_price}")

            check_price = _product_price == actual_price

            if check_price:
                _text = "цена не изменилась"
                print(f"{_text} user {user_id} product {_name}")
                return
            else:
                _waiting_price = start_price - sale

                query = (
                    update(OzonProduct)
                    .values(actual_price=_product_price)
                    .where(OzonProduct.id == product_id)
                )
                async for session in get_session():
                    async with session as _session:
                        try:
                            await session.execute(query)
                            await session.commit()
                        except Exception as ex:
                            await session.rollback()
                            print(ex)

                pretty_product_price = generate_pretty_amount(_product_price)
                pretty_actual_price = generate_pretty_amount(actual_price)
                pretty_sale = generate_pretty_amount(sale)
                pretty_waiting_price = generate_pretty_amount(_waiting_price)
                pretty_start_price = generate_pretty_amount(start_price)

                if _waiting_price >= _product_price:

                    if actual_price < _product_price:
                        _text = f'🔄 Цена повысилась, но всё ещё входит в выставленный диапазон скидки на товар <a href="{link}">{_name}</a>\n\nМаркетплейс: Ozon\n🔄Отслеживаемая скидка: {pretty_sale}\n\n⬇️Цена по карте: {pretty_product_price} (дешевле на {start_price - _product_price}₽)\n\nНачальная цена: {pretty_start_price}\n\nПредыдущая цена: {pretty_actual_price}'
                        _disable_notification = True
                    else:
                        _text = f'🚨 Изменилась цена на <a href="{link}">{_name}</a>\n\nМаркетплейс: Ozon\n🔄Отслеживаемая скидка: {pretty_sale}\n\n⬇️Цена по карте: {pretty_product_price} (дешевле на {start_price - _product_price}₽)\n\nНачальная цена: {pretty_start_price}\n\nПредыдущая цена: {pretty_actual_price}'
                        _disable_notification = False

                    _kb = create_remove_and_edit_sale_kb(
                        user_id=user_id,
                        product_id=product_id,
                        marker="ozon",
                        job_id=job_id,
                        with_redirect=False,
                    )

                    _kb = add_or_create_close_kb(_kb)

                    msg = await bot.send_message(
                        chat_id=user_id,
                        text=_text,
                        disable_notification=_disable_notification,
                        reply_markup=_kb.as_markup(),
                    )
                    await add_message_to_delete_dict(msg)
                    return

        except Exception as ex:
            print("OZON SCHEDULER ERROR", ex)


async def try_add_product_price_to_db(product_id: int, city: str | None, price: float):

    city = city if city else "МОСКВА"

    check_monitoring_price_query = (
        select(
            ProductPrice.time_price,
        )
        .where(
            and_(
                ProductPrice.product_id == product_id,
                ProductPrice.city == city,
            )
        )
        .order_by(desc(ProductPrice.time_price))
    )

    async for session in get_session():
        async with session as _session:
            res = await _session.execute(check_monitoring_price_query)

    first_element_date = res.scalars().first()

    if first_element_date:
        print("first_element_date", first_element_date)
        check_date = datetime.now().astimezone(tz=timezone) - timedelta(hours=12)

        if first_element_date > check_date:
            print("early yet")
            return

    monitoring_price_data = {
        "product_id": product_id,
        "city": city,
        "price": price,
        "time_price": datetime.now(),
    }

    monitoring_price_query = insert(ProductPrice).values(**monitoring_price_data)

    async for session in get_session():
        async with session as _session:
            try:
                await session.execute(monitoring_price_query)
                await session.commit()
            except Exception as ex:
                await session.rollback()
                print(ex)


async def update_last_send_price_by_user_product(
    last_send_price: float, user_product_id: int
):
    update_query = (
        update(UserProduct)
        .values(last_send_price=last_send_price)
        .where(
            UserProduct.id == user_product_id,
        )
    )
    async for session in get_session():
        async with session as _session:
            await _session.execute(update_query)
            try:
                await _session.commit()
            except Exception as ex:
                print("UPDATE LAST SEND PRICE ERROR", ex)
                await _session.rollback()


async def create_new_punkts_from_old():
    counter = 0
    user_query = select(User.tg_id)
    async for session in get_session():
        async with session as _session:
            user_res = await _session.execute(user_query)

            user_ids = user_res.scalars().all()

            for user_id in user_ids:

                if user_id in (int(DEV_ID), int(SUB_DEV_ID)):
                    continue

                punkt_query = (
                    select(
                        WbPunkt.city,
                        WbPunkt.index,
                        WbPunkt.time_create,
                        WbPunkt.zone,
                        OzonPunkt.zone,
                    )
                    .select_from(User)
                    .join(WbPunkt, WbPunkt.user_id == User.tg_id)
                    .join(OzonPunkt, OzonPunkt.user_id == User.tg_id)
                    .where(
                        User.tg_id == user_id,
                    )
                )

                # async with session as _session:
                punkt_res = await _session.execute(punkt_query)

                punkt_data = punkt_res.fetchall()
                print(punkt_data, user_id)

                if punkt_data:
                    counter += 1

                    city, index, time_create, wb_zone, ozon_zone = punkt_data[0]

                    insert_data = {
                        "city": city,
                        "index": index,
                        "time_create": time_create,
                        "wb_zone": wb_zone,
                        "ozon_zone": ozon_zone,
                        "user_id": user_id,
                    }

                    insert_query = insert(Punkt).values(**insert_data)

                    await _session.execute(insert_query)
                    # await _session.commit()
            # print(_session.new)
            # print("Dirty objects before commit:", _session.dirty)
            try:
                await _session.commit()
            except Exception as ex:
                print(ex)
                await _session.rollback()

    print("COUNTER", counter)
