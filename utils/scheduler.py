from datetime import datetime, timedelta

import aiofiles
import pytz


from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from sqlalchemy.ext.asyncio import AsyncSession

import config
from background.base import get_redis_background_pool, _redis_pool, get_redis_pool

from db.base import (
    Category,
    PopularProduct,
    Product,
    get_session,
    UserProduct,
    UserProductJob,
    ProductPrice,
)
from db.repository.apscheduler_job import ApschedulerJobRepository
from db.repository.category import CategoryRepository
from db.repository.channel_link import ChannelLinkRepository
from db.repository.popular_product import PopularProductRepository
from db.repository.product import ProductRepository
from db.repository.popular_product_sale_range import PopularProductSaleRangeRepository
from db.repository.product_price import ProductPriceRepository
from db.repository.punkt import PunktRepository
from db.repository.user import UserRepository
from db.repository.user_product import UserProductRepository
from db.repository.user_product_job import UserProductJobRepository
from db.repository.utm import UTMRepository

from bot22 import bot

from services.ozon.ozon_api_service import OzonAPIService
from services.wb.wb_api_service import WbAPIService
from utils.pics import ImageManager
from utils.any import generate_sale_for_price, send_data_to_yandex_metica

from utils.exc import OzonProductExistsError, WbProductExistsError

from config import JOB_STORE_URL
from logger import logger


# Настройка хранилища задач
jobstores = {
    "sqlalchemy": SQLAlchemyJobStore(url=JOB_STORE_URL),
}

# Создание и настройка планировщика
scheduler = AsyncIOScheduler(jobstores=jobstores)


timezone = pytz.timezone("Europe/Moscow")

scheduler_cron = IntervalTrigger(minutes=15, timezone=timezone)

scheduler_interval = IntervalTrigger(hours=1, timezone=timezone)
image_manager = ImageManager(bot)


async def add_task_to_delete_old_message_for_users(user_id: int):
    print("add task to delete old message...")
    job_id = f"delete_msg_task_{user_id}"

    _ = scheduler.add_job(
        func=background_task_wrapper,
        trigger=scheduler_interval,
        id=job_id,
        coalesce=True,
        args=("periodic_delete_old_message", user_id),  # func_name, *args
        kwargs={"_queue_name": "arq:low"},
        jobstore="sqlalchemy",
    )  # _queue_name


async def check_product_by_user_in_db(
    user_id: int, short_link: str, session: AsyncSession
):
    async with session as _session:
        up_repo = UserProductRepository(_session)
        product = up_repo.get_user_product_by_product_short_link(user_id, short_link)

    return bool(product)


async def add_product_to_db_popular_product(
    data: dict, session: AsyncSession, scheduler: AsyncIOScheduler
):
    short_link = data.get("short_link")
    name = data.get("name")
    photo_id = data.get("photo_id")
    high_category = data.get("high_category")
    low_category = data.get("low_category")
    marker: str = data.get("product_marker")

    async with session as _session:
        product_repo = ProductRepository(_session)
        popular_product_repo = PopularProductRepository(_session)
        category_repo = CategoryRepository(_session)
        channel_link_repo = ChannelLinkRepository(_session)

        product = await product_repo.find_by_short_link(short_link)

        if not product:
            product = Product(
                product_marker=marker,
                name=name,
                short_link=short_link,
                photo_id=photo_id,
            )

            product = await product_repo.create(product)
            logger.info("Created product for popular product %s", product.id)
        else:
            logger.info("Product for popular product already exists: %s", product.id)

            if await popular_product_repo.get_by_product_id(product.id):
                logger.info("Popular product already exists!")
                return

        high_category_obj = await category_repo.get_by_name(high_category)
        low_category_obj = await category_repo.get_by_name(low_category)

        default_channel_obj = await channel_link_repo.get_common_private_channel_link()
        public_default_channel_obj = (
            await channel_link_repo.get_common_private_channel_link
        )

        if not high_category_obj:
            high_category_obj = Category(name=high_category)
            high_category_obj.channel_links.append(default_channel_obj)
            high_category_obj.channel_links.append(public_default_channel_obj)
            await category_repo.create(high_category_obj)

        if not low_category_obj:
            low_category_obj = Category(
                name=low_category, parent_id=high_category_obj.id
            )
            low_category_obj.channel_links.append(default_channel_obj)
            low_category_obj.channel_links.append(public_default_channel_obj)

            await category_repo.create(low_category_obj)

        popular_product = PopularProduct(
            link=data.get("link"),
            product_id=product.id,
            start_price=data.get("start_price"),
            actual_price=data.get("actual_price"),
            sale=data.get("sale"),
            time_create=datetime.now(),
            category_id=low_category_obj.id,
        )

        popular_product = await popular_product_repo.create(popular_product)

    logger.info("Created popular product %s", popular_product.id)

    job_id = f"popular_{popular_product.id}"
    job = scheduler.add_job(
        func=background_task_wrapper,
        trigger="interval",
        hours=2,
        id=job_id,
        coalesce=True,
        args=(
            "push_check_popular_product",
            popular_product.id,
        ),  # func_name, *args
        kwargs={"_queue_name": "arq:popular"},  # _queue_name
        jobstore="sqlalchemy",
    )
    print("Created job for popular product: %s", job)


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

    async with session:
        product_repo = ProductRepository(session)
        user_repo = UserRepository(session)
        user_product_repo = UserProductRepository(session)
        user_product_job_repo = UserProductJobRepository(session)

        product = await product_repo.find_by_short_link(short_link)
        if not product:
            product = Product(
                product_marker=marker,
                name=name,
                short_link=short_link,
                photo_id=photo_id,
            )
            product = await product_repo.create(product)

        user_product = UserProduct(
            link=data.get("link"),
            product_id=product.id,
            user_id=user_id,
            start_price=data.get("start_price"),
            actual_price=data.get("actual_price"),
            sale=data.get("sale"),
            time_create=datetime.now(),
        )
        user_product = await user_product_repo.create(user_product)

        user_product_id = user_product.id

        job_id = f"{user_id}:{marker}:{user_product_id}"

        _ = scheduler.add_job(
            background_task_wrapper,
            trigger="interval",
            minutes=15,
            id=job_id,
            jobstore="sqlalchemy",
            coalesce=True,
            args=(
                "push_check_price",
                user_id,
                user_product_id,
            ),
            kwargs={"_queue_name": "arq:low"},
        )

        user_job = UserProductJob(user_product_id=user_product_id, job_id=job_id)
        await user_product_job_repo.create(user_job)

        await user_repo.increase_product_count_for_user(user_id, marker)

        if is_first_product:
            # get request to yandex metrika
            utm_repo = UTMRepository(session)
            utm = await utm_repo.get_by_user_id(user_id)

            if utm and utm.client_id:
                await send_data_to_yandex_metica(utm.client_id, goal_id="add_product")


async def try_update_ozon_product_photo(
    product_id: int, short_link: str, session: AsyncSession
):
    try:
        api_service = OzonAPIService()
        text_data = await api_service.get_product_data(short_link)
        product_data = api_service.parse_product_data(text_data)
        if product_data.photo_url:
            photo_id = await image_manager.generate_photo_id_for_url(
                url=product_data.photo_url
            )
        else:
            photo_id = await image_manager.get_default_product_photo_id()
    except Exception:
        photo_id = await image_manager.get_default_product_photo_id()

    repo = ProductRepository(session)
    repo.update_old(product_id, photo_id=photo_id)


async def get_product_photo_id(
    short_link: str, photo_url: str | None, session: AsyncSession
) -> str:
    repo = ProductRepository(session)
    product = await repo.find_by_short_link(short_link)
    if product:
        return product.photo_id

    if photo_url:
        return await image_manager.generate_photo_id_for_url(url=photo_url)

    return await image_manager.get_default_product_photo_id()


async def save_popular_ozon_product(
    product_data: dict, session: AsyncSession, scheduler: AsyncIOScheduler
):
    link: str = product_data.get("link")
    name: str = product_data.get("name")

    api_service = OzonAPIService()
    ozon_short_link = api_service.shorten_link(link)
    res = await api_service.get_product_data(ozon_short_link)

    data = api_service.parse_product_data(res)

    photo_id = await get_product_photo_id(
        short_link=data.short_link, photo_url=data.photo_url, session=session
    )

    sale = await generate_sale_for_price_popular_product(session, data.start_price)

    _data = {
        "link": link,
        "short_link": data.short_link,
        "name": name,
        "actual_price": data.actual_price,
        "start_price": data.start_price,
        "basic_price": data.basic_price,
        "sale": sale,
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
    is_first_product: bool,
    session: AsyncSession,
    scheduler: AsyncIOScheduler,
):
    api_service = OzonAPIService()
    ozon_short_link = api_service.shorten_link(link)

    up_repo = UserProductRepository(session)
    product = await up_repo.get_user_product(user_id, link)

    if product:
        raise OzonProductExistsError()

    punkt_repo = PunktRepository(session)
    punkt = await punkt_repo.get_users_punkt(user_id)
    del_zone = punkt.ozon_zone if punkt else None

    print("do request on OZON API (new version)")

    res = await api_service.get_product_data(ozon_short_link, del_zone)
    data = api_service.parse_product_data(res)

    product = await up_repo.get_user_product_by_product_short_link(
        user_id, data.short_link
    )

    if product:
        raise OzonProductExistsError()

    photo_id = await get_product_photo_id(
        short_link=data.short_link, photo_url=data.photo_url, session=session
    )

    sale = generate_sale_for_price(data.start_price)

    _data = {
        "link": link,
        "short_link": data.short_link,
        "name": data.name,
        "actual_price": data.actual_price,
        "start_price": data.start_price,
        "basic_price": data.basic_price,
        "sale": sale,
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
        await repo.update_old(product_id, photo_id=photo_id)
    except Exception as e:
        print(e)


async def try_get_wb_product_photo(short_link: str, session: AsyncSession):
    try:
        async with session:
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
    except Exception:
        return None


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

    up_repo = UserProductRepository(session)
    punkt_repo = PunktRepository(session)
    product = await up_repo.get_user_product(user_id, link)

    if product:
        raise WbProductExistsError()

    product = await up_repo.get_user_product_by_product_short_link(user_id, short_link)

    if product:
        raise WbProductExistsError()

    punkt = await punkt_repo.get_users_punkt(user_id)
    del_zone = punkt.wb_zone if punkt else config.WB_DEFAULT_DELIVERY_ZONE
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

    async with session as _session:
        up_repo = UserProductRepository(_session)
        products = await up_repo.get_user_products(msg[0])
        is_first_product = len(products) == 0

    if link.find("ozon") > 0:
        # save ozon product
        await save_ozon_product(
            user_id=msg[0],
            link=link,
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


# для планировании задачи в APScheduler и выполнения в ARQ worker`e
async def background_task_wrapper(func_name, *args, _queue_name):

    _redis_pool = await get_redis_background_pool()

    _args_str = ".".join([f"{arg}" for arg in args])

    _job_id = f"{func_name}_{_args_str}"

    await _redis_pool.enqueue_job(
        func_name, *args, _queue_name=_queue_name, _job_id=_job_id
    )


async def sync_popular_product_jobs(scheduler: AsyncIOScheduler):
    async for session in get_session():
        apscheduler_repo = ApschedulerJobRepository(session)

        existing_job_ids = await apscheduler_repo.get_existing_job_ids()
        existing_pp_ids = list(
            map(lambda job_id: int(job_id.split("_")[-1]), existing_job_ids)
        )

        # Получаем список актуальных ID популярных продуктов
        popular_product_repo = PopularProductRepository(session)

        actual_pp_ids = await popular_product_repo.get_ids_that_not_in_list([])
        actual_pp_ids = set(actual_pp_ids)

        # --- Удаление задач, для которых больше нет популярных продуктов ---
        obsolete_job_ids = [
            job_id
            for job_id in existing_job_ids
            if int(job_id.split("_")[-1]) not in actual_pp_ids
        ]
        for job_id in obsolete_job_ids:
            scheduler.remove_job(job_id)

        # --- Добавление задач, которые отсутствуют, но должны быть ---
        popular_products_list = await popular_product_repo.get_ids_that_not_in_list(
            existing_pp_ids
        )

        for pp_id in popular_products_list:
            job_id = f"popular_{pp_id}"
            scheduler.add_job(
                func=background_task_wrapper,
                trigger="interval",
                hours=2,
                id=job_id,
                coalesce=True,
                args=(
                    "push_check_popular_product",
                    pp_id,
                ),  # func_name, *args
                kwargs={"_queue_name": "arq:popular"},  # _queue_name
                jobstore="sqlalchemy",
            )


async def setup_subscription_end_job(scheduler: AsyncIOScheduler):
    logger.info("Setup subscription_end job")

    scheduler.add_job(
        func=background_task_wrapper,
        trigger=CronTrigger(hour=9, minute=0, second=0),
        id="subscription_end",
        coalesce=True,
        args=("search_users_for_ended_subscription",),
        kwargs={"_queue_name": "arq:low"},  # _queue_name
        jobstore="sqlalchemy",
        replace_existing=True,
    )


async def setup_subscription_is_about_to_end_job(scheduler: AsyncIOScheduler):
    logger.info("Setup subscription_is_about_to_end job")

    scheduler.add_job(
        func=background_task_wrapper,
        trigger=CronTrigger(hour=8, minute=0, second=0),
        id="subscription_is_about_to_end",
        coalesce=True,
        args=("notify_users_about_subscription_ending",),
        kwargs={"_queue_name": "arq:low"},  # _queue_name
        jobstore="sqlalchemy",
        replace_existing=True,
    )


async def setup_messages_sendigns_job(scheduler: AsyncIOScheduler):
    logger.info("Setup messages_sendigns job")

    scheduler.add_job(
        func=background_task_wrapper,
        trigger=IntervalTrigger(minutes=5),
        id="messages_sendigns",
        coalesce=True,
        args=("process_message_sendings",),
        kwargs={"_queue_name": "arq:low"},  # _queue_name
        jobstore="sqlalchemy",
        replace_existing=True,
    )


async def try_add_product_price_to_db(product_id: int, city: str | None, price: float):

    city = city if city else "МОСКВА"

    async for session in get_session():
        async with session as _session:
            pp_repo = ProductPriceRepository(_session)
            first_element_date = await pp_repo.get_last_for_product_and_city(
                product_id, city
            )

            if first_element_date:
                print("first_element_date", first_element_date)
                check_date = datetime.now().astimezone(tz=timezone) - timedelta(
                    hours=12
                )

                if first_element_date > check_date:
                    print("Too early")
                    return

            product_price = ProductPrice(
                product_id=product_id,
                city=city,
                price=price,
                time_price=datetime.now(),
            )

            await pp_repo.create(product_price)


async def update_last_send_price_by_user_product(
    last_send_price: float, user_product_id: int
):
    async for session in get_session():
        async with session as _session:
            repo = UserProductRepository(_session)
            await repo.update_old(user_product_id, last_send_price=last_send_price)
