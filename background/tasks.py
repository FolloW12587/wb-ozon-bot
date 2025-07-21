import json
import asyncio
from math import ceil
from datetime import datetime, timedelta

import aiohttp

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import insert, select, and_, update
from sqlalchemy.orm import selectinload

from commands.subscription_mass_sending import subscription_is_about_to_end
import config
from db.base import (
    Category,
    PopularProduct,
    Product,
    Punkt,
    Subscription,
    User,
    get_session,
    UserProduct,
    UserProductJob,
)

from db.repository.popular_product import PopularProductRepository
from db.repository.punkt import PunktRepository
from db.repository.subscription import SubscriptionRepository
from db.repository.user import UserRepository
from db.repository.user_product import UserProductRepository
from keyboards import (
    add_or_create_close_kb,
    create_remove_popular_kb,
    new_create_remove_and_edit_sale_kb,
    create_go_to_subscription_kb,
)

from bot22 import bot

from services.ozon.ozon_api_service import OzonAPIService
from services.wb_api_service import WbAPIService
from utils.storage import redis_client
from utils.any import (
    generate_pretty_amount,
    add_message_to_delete_dict,
    generate_percent_to_popular_product,
)
from utils.exc import (
    OzonAPICrashError,
    OzonProductExistsError,
    WbAPICrashError,
    WbProductExistsError,
)
from utils.scheduler import (
    new_save_product,
    save_popular_product,
    try_add_product_price_to_db,
    update_last_send_price_by_user_product,
)
from utils.subscription import get_user_subscription_limit
from logger import logger


async def new_add_product_task(cxt, user_data: dict):
    try:
        scheduler = cxt.get("scheduler")
        product_marker: str = user_data.get("product_marker")
        _add_msg_id: int = user_data.get("_add_msg_id")
        msg: tuple = user_data.get("msg")

        async for session in get_session():
            # check_product_limit = await new_check_subscription_limit(
            #     user_id=msg[0], marker=product_marker, session=session
            # )
            try:
                limits, used = await get_user_subscription_limit(msg[0], session)
                limits_tuple_key = 0 if product_marker.lower() == "ozon" else 1
            except Exception:
                logger.error(
                    "Can't check user %s subscription product limits",
                    msg[0],
                    exc_info=True,
                )
                await bot.edit_message_text(
                    chat_id=msg[0],
                    message_id=_add_msg_id,
                    text=f"{product_marker.upper()} не удалось добавить",
                )
                return

        if used[limits_tuple_key] >= limits[limits_tuple_key]:
            _text = f"""
*🚫 Достигнут лимит товаров*

На бесплатной версии можно отслеживать только {limits[0]} товара с Ozon и {limits[1]} с WB.

*🔓 Хотите больше? Оформите подписку и отслеживайте товары без ограничений👇*"""
            kb = create_go_to_subscription_kb()
            msg = await bot.edit_message_text(
                chat_id=msg[0],
                message_id=_add_msg_id,
                text=_text,
                reply_markup=kb.as_markup(resize_keyboard=True),
                parse_mode="markdown",
            )
            await add_message_to_delete_dict(msg)
            return
        try:
            async for session in get_session():
                await new_save_product(
                    user_data=user_data, session=session, scheduler=scheduler
                )
        except (OzonProductExistsError, WbProductExistsError) as ex:
            print("PRODUCT EXISTS", ex)
            _text = f"❗️ {product_marker} товар уже есть в Вашем списке"
        except OzonAPICrashError as ex:
            print("OZON API CRASH", ex)
        except aiohttp.ClientError as ex:
            print("Таймаут по запросу к OZON API", ex)
        except Exception as ex:
            print(ex)
            _text = (
                f"‼️ Возникла ошибка при добавлении {product_marker} товара\n\n"
                f"Попробуйте повторить позже"
            )
        else:
            _text = f"{product_marker} товар добавлен к отслеживанию✅"

        await bot.edit_message_text(chat_id=msg[0], message_id=_add_msg_id, text=_text)

    except Exception as ex:
        print("SCHEDULER ADD ERROR", ex)
        await bot.edit_message_text(
            chat_id=msg[0],
            message_id=_add_msg_id,
            text=f"{product_marker.upper()} не удалось добавить",
        )


async def new_push_check_ozon_price(cxt, user_id: str, product_id: str):
    print(f'qwe {cxt["job_id"]}')
    print(f"new 222 фоновая задача ozon {user_id}")

    async for session in get_session():
        query = (
            select(
                Product.id,
                UserProduct.id,
                UserProduct.link,
                Product.short_link,
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
                UserProductJob,
                UserProductJob.user_product_id == UserProduct.id,
            )
            .where(
                and_(
                    UserProduct.id == int(product_id),
                    UserProduct.user_id == int(user_id),
                )
            )
        )

        res = await session.execute(query)

        res = res.fetchall()

    if not res:
        return

    (
        main_product_id,
        _id,
        link,
        short_link,
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

    name = name if name is not None else "Отсутствует"
    try:
        api_service = OzonAPIService()
        res = await api_service.get_product_data(short_link, zone)
        data = api_service.parse_product_data(res)

        _product_price = float(data.actual_price)

        await try_add_product_price_to_db(
            product_id=main_product_id, city=city, price=_product_price
        )

        check_price = _product_price == actual_price

        if check_price:
            print(f"Цена не изменилась user {user_id} product {name}")
            return

        _waiting_price = start_price - sale

        async for session in get_session():
            async with session as _session:
                up_repo = UserProductRepository(_session)
                await up_repo.update(product_id, actual_price=_product_price)

        pretty_product_price = generate_pretty_amount(_product_price)
        pretty_actual_price = generate_pretty_amount(actual_price)
        pretty_sale = generate_pretty_amount(sale)
        pretty_start_price = generate_pretty_amount(start_price)

        if _waiting_price < _product_price:
            return

        # проверка, отправлялось ли уведомление с такой ценой в прошлый раз
        if last_send_price is not None and (last_send_price == _product_price):
            print(
                f"LAST SEND PRICE VALIDATION STOP {last_send_price} | {_product_price}"
            )
            return

        if actual_price < _product_price:
            _text = (
                f"🔄 Цена повысилась, но всё ещё входит в выставленный диапазон "
                f'скидки на товар <a href="{link}">{name}</a>\n\n'
                f"Маркетплейс: Ozon\n\n"
                f"🔄Отслеживаемая скидка: {pretty_sale}\n\n"
                f"⬇️Цена по карте: {pretty_product_price} "
                f"(дешевле на {start_price - _product_price}₽)\n\n"
                f"Начальная цена: {pretty_start_price}\n\n"
                f"Предыдущая цена: {pretty_actual_price}"
            )
            _disable_notification = True
        else:
            _text = (
                f'🚨 Изменилась цена на <a href="{link}">{name}</a>\n\n'
                f"Маркетплейс: Ozon\n\n"
                f"🔄Отслеживаемая скидка: {pretty_sale}\n\n"
                f"⬇️Цена по карте: {pretty_product_price} "
                f"(дешевле на {start_price - _product_price}₽)\n\n"
                f"Начальная цена: {pretty_start_price}\n\n"
                f"Предыдущая цена: {pretty_actual_price}"
            )
            _disable_notification = False

        _kb = new_create_remove_and_edit_sale_kb(
            user_id=user_id,
            product_id=product_id,
            marker="ozon",
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

        await update_last_send_price_by_user_product(
            last_send_price=_product_price, user_product_id=_id
        )

        await add_message_to_delete_dict(msg)
        return

    except OzonAPICrashError as ex:
        print("SCHEDULER OZON API CRUSH", ex)

    except Exception as ex:
        print("OZON SCHEDULER ERROR", ex, ex.args)


async def new_push_check_wb_price(cxt, user_id: str, product_id: str):
    print(f'qwe {cxt["job_id"]}')
    print(f"new 222 фоновая задача wb {user_id}")

    async for session in get_session():
        async with session as _session:
            query = (
                select(
                    Product.id,
                    UserProduct.id,
                    UserProduct.link,
                    Product.short_link,
                    UserProduct.actual_price,
                    UserProduct.start_price,
                    Product.name,
                    UserProduct.sale,
                    Punkt.wb_zone,
                    Punkt.city,
                    UserProductJob.job_id,
                    Product.photo_id,
                    UserProduct.last_send_price,
                )
                .select_from(UserProduct)
                .join(Product, UserProduct.product_id == Product.id)
                .outerjoin(Punkt, Punkt.user_id == int(user_id))
                .outerjoin(
                    UserProductJob,
                    UserProductJob.user_product_id == UserProduct.id,
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

    if not res:
        return

    (
        main_product_id,
        _id,
        link,
        short_link,
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

    name = name if name is not None else "Отсутствует"

    if not zone:
        zone = config.WB_DEFAULT_DELIVERY_ZONE

    try:
        api_service = WbAPIService()
        res = await api_service.get_product_data(short_link, zone)

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

        _product_price = float(_product_price)

        print("Wb price", _product_price)

        await try_add_product_price_to_db(
            product_id=main_product_id, city=city, price=_product_price
        )

        check_price = _product_price == actual_price

        if check_price:
            print(f"Цена не изменилась user {user_id} product {name}")
            return

        async for session in get_session():
            async with session as _session:
                up_repo = UserProductRepository(_session)
                await up_repo.update(product_id, actual_price=_product_price)

        _waiting_price = start_price - sale

        pretty_product_price = generate_pretty_amount(_product_price)
        pretty_actual_price = generate_pretty_amount(actual_price)
        pretty_sale = generate_pretty_amount(sale)
        pretty_start_price = generate_pretty_amount(start_price)

        if _waiting_price < _product_price:
            return

        # проверка, отправлялось ли уведомление с такой ценой в прошлый раз
        if last_send_price is not None and (last_send_price == _product_price):
            print(
                f"LAST SEND PRICE VALIDATION STOP {last_send_price} | {_product_price}"
            )
            return

        if actual_price < _product_price:
            _text = (
                f"🔄 Цена повысилась, но всё ещё входит в выставленный "
                f'диапазон скидки на товар <a href="{link}">{name}</a>\n\n'
                f"Маркетплейс: Wb\n\n"
                f"🔄Отслеживаемая скидка: {pretty_sale}\n\n"
                f"⬇️Цена по карте: {pretty_product_price} "
                f"(дешевле на {start_price - _product_price}₽)\n\n"
                f"Начальная цена: {pretty_start_price}\n\nПредыдущая цена: {pretty_actual_price}"
            )
            _disable_notification = True
        else:
            _text = (
                f'🚨 Изменилась цена на <a href="{link}">{name}</a>\n\n'
                f"Маркетплейс: Wb\n\n"
                f"🔄Отслеживаемая скидка: {pretty_sale}\n\n"
                f"⬇️Цена по карте: {pretty_product_price} "
                f"(дешевле на {start_price - _product_price}₽)\n\n"
                f"Начальная цена: {pretty_start_price}\n\n"
                f"Предыдущая цена: {pretty_actual_price}"
            )
            _disable_notification = False

        _kb = new_create_remove_and_edit_sale_kb(
            user_id=user_id,
            product_id=product_id,
            marker="wb",
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

        await update_last_send_price_by_user_product(
            last_send_price=_product_price, user_product_id=_id
        )

        await add_message_to_delete_dict(msg)

    except WbAPICrashError as ex:
        print("SCHEDULER WB API CRUSH", ex)

    except Exception as ex:
        print(ex)


async def add_popular_product(cxt, product_data: dict):
    scheduler = cxt.get("scheduler")
    product_marker: str = product_data.get("product_marker")
    print(f"from task {product_data}")

    try:
        async for session in get_session():
            await save_popular_product(
                product_data=product_data, session=session, scheduler=scheduler
            )

    except (OzonProductExistsError, WbProductExistsError) as ex:
        print("PRODUCT EXISTS", ex)
        _text = f"❗️ {product_marker} товар уже есть в Вашем списке"
    except OzonAPICrashError as ex:
        print("OZON API CRASH", ex)
    except aiohttp.ClientError as ex:
        print("Таймаут по запросу к OZON API", ex)
    except Exception as ex:
        print(ex)
        _text = (
            f"‼️ Возникла ошибка при добавлении {product_marker} товара\n\n"
            f"Попробуйте повторить позже"
        )
    else:
        _text = f"{product_marker} популярный товар добавлен к отслеживанию✅"
        print(_text)


async def push_check_ozon_popular_product(cxt, product_id: int):
    async for session in get_session():
        async with session as _session:
            await __push_check_ozon_popular_product(_session, product_id)
            try:
                await _session.close()
            except Exception:
                pass


async def __push_check_ozon_popular_product(session: AsyncSession, product_id: int):
    print("new фоновая задача ozon (популярный товар)")
    popular_product_repo = PopularProductRepository(session)

    query = (
        select(PopularProduct)
        .options(
            selectinload(PopularProduct.product),
            selectinload(PopularProduct.category).selectinload(Category.channel_links),
        )
        .where(PopularProduct.id == int(product_id))
    )

    res = await session.execute(query)

    popular_product = res.scalar_one_or_none()

    if not popular_product:
        print("wtf!@!@!@!#!")
        return

    link = popular_product.link
    short_link = popular_product.product.short_link
    actual_price = popular_product.actual_price
    start_price = popular_product.start_price
    last_notificated_price = popular_product.last_notificated_price
    name = popular_product.product.name
    sale = popular_product.sale
    photo_id = popular_product.product.photo_id

    try:
        api_service = OzonAPIService()
        res = await api_service.get_product_data(short_link)
        data = api_service.parse_product_data(res)
        _product_price = float(data.actual_price)

        if _product_price == actual_price:
            print(f"цена не изменилась (популярный товар) product {name}")
            return

        _waiting_price = start_price - sale
        update_kwargs = {"actual_price": _product_price}

        if _waiting_price < _product_price:
            update_kwargs["last_notificated_price"] = None

        await popular_product_repo.update(product_id, **update_kwargs)

        # текущая цена выше, чем скидочный порог
        if _waiting_price < _product_price:
            return

        # последняя фиксированная цена не определена
        if last_notificated_price is None:
            # фиксируем и оповещаем
            await popular_product_repo.update(
                product_id, last_notificated_price=_product_price
            )

            await notify_channels_about_popular_product_sale(
                popular_product.id,
                name,
                link,
                _product_price,
                start_price,
                photo_id,
                popular_product.category,
                popular_product.product,
            )
            return

        # последняя фиксированная цена ниже текущей цены
        if last_notificated_price < _product_price:
            # фиксируем новую цену
            await popular_product_repo.update(
                product_id, last_notificated_price=_product_price
            )
            return

        price_diff = last_notificated_price - _product_price
        # разница последней фикс цены и текущей цены ниже трех процентов
        if price_diff / start_price < 0.03:
            return

        # новая цена больше, чем на 3 процента ниже предыдущей фиксированной цены
        # фиксируем и оповещаем
        await popular_product_repo.update(
            product_id, last_notificated_price=_product_price
        )

        await notify_channels_about_popular_product_sale(
            popular_product.id,
            name,
            link,
            _product_price,
            start_price,
            photo_id,
            popular_product.category,
            popular_product.product,
        )

    except OzonAPICrashError as ex:
        print("SCHEDULER OZON API CRUSH", ex)

    except Exception as ex:
        print("OZON SCHEDULER ERROR", ex, ex.args)


async def notify_channels_about_popular_product_sale(
    popular_product_id: int,
    name: str,
    link: str,
    product_price: int,
    start_price: int,
    photo_id: str,
    category: Category | None,
    product: Product,
):
    pretty_product_price = generate_pretty_amount(product_price)
    pretty_start_price = generate_pretty_amount(start_price)
    percent = generate_percent_to_popular_product(start_price, product_price)
    _text = (
        f"🔥 {name} <b>-{percent}%</b> 🔥\n\n📉Было {pretty_start_price} -> <b>"
        f'<u>Стало {pretty_product_price}</u></b>\n\n➡️<a href="{link}">Ссылка на товар</a>'
    )

    if category:
        category_name = category.name
        _text += f"\n\n#{category_name.lower()}"

    _disable_notification = False

    # print(channel_links)

    _kb = create_remove_popular_kb(
        marker=product.product_marker, popular_product_id=popular_product_id
    )

    # channel_links = [channel.channel_id for channel in category.channel_links]
    # for channel_link in channel_links:
    for channel in category.channel_links:
        if not channel.is_active:
            continue

        markup = _kb.as_markup() if channel.is_admin else None
        _ = await bot.send_photo(
            chat_id=channel.channel_id,
            photo=photo_id,
            caption=_text,
            disable_notification=_disable_notification,
            reply_markup=markup,
        )

        await asyncio.sleep(0.2)


async def push_check_wb_popular_product(cxt, product_id: str):
    print("new фоновая задача wb (популярные товары)")

    async for session in get_session():
        async with session as _session:
            query = (
                select(PopularProduct)
                .options(
                    selectinload(PopularProduct.product),
                    selectinload(PopularProduct.category).selectinload(
                        Category.channel_links
                    ),
                )
                .where(PopularProduct.id == int(product_id))
            )

            res = await _session.execute(query)

            popular_product = res.scalar_one_or_none()

    if not popular_product:
        return

    link = popular_product.link
    short_link = popular_product.product.short_link
    actual_price = popular_product.actual_price
    start_price = popular_product.start_price
    name = popular_product.product.name
    sale = popular_product.sale
    photo_id = popular_product.product.photo_id

    try:
        api_service = WbAPIService()
        res = await api_service.get_product_data(
            short_link, config.WB_DEFAULT_DELIVERY_ZONE
        )

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

        _product_price = float(_product_price)

        print("Wb price", _product_price)

        check_price = _product_price == actual_price

        if check_price:
            _text = "цена не изменилась"
            print(f"{_text} popular product {name}")
            return

        else:
            update_query = (
                update(PopularProduct)
                .values(actual_price=_product_price)
                .where(PopularProduct.id == product_id)
            )

            async for session in get_session():
                async with session as _session:
                    try:
                        await _session.execute(update_query)
                        await _session.commit()
                    except Exception as ex:
                        await _session.rollback()
                        print(ex)

            _waiting_price = start_price - sale

            pretty_product_price = generate_pretty_amount(_product_price)
            pretty_actual_price = generate_pretty_amount(actual_price)
            pretty_sale = generate_pretty_amount(sale)
            pretty_start_price = generate_pretty_amount(start_price)

            if _waiting_price >= _product_price:

                percent = generate_percent_to_popular_product(
                    start_price, _product_price
                )
                _text = f'🔥 {name} <b>-{percent}%</b> 🔥\n\n📉Было {pretty_start_price} -> <b><u>Стало {pretty_product_price}</u></b>\n\n➡️<a href="{link}">Ссылка на товар</a>'

                if popular_product.category:
                    category_name = popular_product.category.name
                    _text += f"\n\n#{category_name.lower()}"
                _disable_notification = False

                channel_links = [
                    channel.channel_id
                    for channel in popular_product.category.channel_links
                ]

                _kb = create_remove_popular_kb(
                    marker=popular_product.product.product_marker,
                    popular_product_id=popular_product.id,
                )

                for channel_link in channel_links:
                    msg = await bot.send_photo(
                        chat_id=channel_link,
                        photo=photo_id,
                        caption=_text,
                        disable_notification=_disable_notification,
                        reply_markup=_kb.as_markup(),
                    )

                return

    except WbAPICrashError as ex:
        print("SCHEDULER WB API CRUSH", ex)

    except Exception as ex:
        print(ex)


async def periodic_delete_old_message(cxt, user_id: int):
    print(f"ARQ TASK DELETE OLD MESSAGE USER {user_id}")
    key = f"fsm:{user_id}:{user_id}:data"

    async with redis_client.pipeline(transaction=True) as pipe:
        user_data: bytes = await pipe.get(key)
        results = await pipe.execute()

    if results[0] is not None:
        json_user_data: dict = json.loads(results[0])

        dict_msg_on_delete: dict = json_user_data.get("dict_msg_on_delete")

        message_id_on_delete_list = []

        if dict_msg_on_delete:
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
                    message_id_on_delete_list.append(_key)
                    del dict_msg_on_delete[_key]

        async with redis_client.pipeline(transaction=True) as pipe:
            bytes_data = json.dumps(json_user_data)
            await pipe.set(key, bytes_data)
            results = await pipe.execute()

        if message_id_on_delete_list:
            iterator_count = ceil(len(message_id_on_delete_list) / 100)

            for i in range(iterator_count):
                idx = i * 100
                _messages_on_delete = message_id_on_delete_list[idx : idx + 100]

                await bot.delete_messages(
                    chat_id=chat_id, message_ids=_messages_on_delete
                )
                await asyncio.sleep(0.2)


async def add_punkt_by_user(cxt, punkt_data: dict):
    punkt_action: str = punkt_data.get("punkt_action")
    city: str = punkt_data.get("city")
    city_index: str = punkt_data.get("index")
    settings_msg: tuple = punkt_data.get("settings_msg")
    user_id: int = punkt_data.get("user_id")

    try:
        ozon_api_service = OzonAPIService()
        ozon_del_zone = await ozon_api_service.get_delivery_zone(city_index)
        print("OZON DEL ZONE", ozon_del_zone)

        wb_api_service = WbAPIService()
        wb_del_zone = await wb_api_service.get_delivery_zone(city_index)
        print("WB DEL ZONE", wb_del_zone)

    except Exception as ex:
        print("DEL ZONE REQUEST ERRROR", ex)
        await bot.edit_message_text(
            text="Что то пошло не так, просим прощения\n\nПопробуйте повторить позже",
            chat_id=settings_msg[0],
            message_id=settings_msg[-1],
        )
        return

    try:
        wb_del_zone = int(wb_del_zone)
        ozon_del_zone = int(ozon_del_zone)
    except Exception as ex:
        print("RESPONSE ERROR WITH CONVERT DEL ZONE", ex)
        await bot.edit_message_text(
            text="Что то пошло не так, просим прощения\n\nПопробуйте повторить позже",
            chat_id=settings_msg[0],
            message_id=settings_msg[-1],
        )
        return

    if punkt_action == "add":
        check_query = select(Punkt.id).where(Punkt.user_id == user_id)

        async for session in get_session():
            async with session as _session:
                res = await _session.execute(check_query)

        has_punkt = res.scalar_one_or_none()

        if has_punkt:
            print("PUNKT ADD ERROR, PUNKT BY USER EXISTS")
            return

        insert_data = {
            "user_id": user_id,
            "index": int(city_index),
            "city": city,
            "ozon_zone": ozon_del_zone,
            "wb_zone": wb_del_zone,
            "time_create": datetime.now(),
        }

        query = insert(Punkt).values(**insert_data)
        success_text = (
            f"✅ Пункт выдачи успешно добавлен (Установленный город - {city})."
        )
        error_text = (
            f"❌ Не получилось добавить пункт выдачи (Переданный город - {city})"
        )

    elif punkt_action == "edit":
        update_data = {
            "city": city,
            "index": int(city_index),
            "ozon_zone": ozon_del_zone,
            "wb_zone": wb_del_zone,
            "time_create": datetime.now(),
        }
        query = update(Punkt).values(**update_data).where(Punkt.user_id == user_id)

        success_text = (
            f"✅ Пункт выдачи успешно изменён (Новый установленный город - {city})."
        )
        error_text = (
            f"❌ Не получилось изменить пункт выдачи (Переданный город - {city})"
        )

    else:
        print("!!!!!!!!Такого не должно быть!!!!!!!!")
        return

    async for session in get_session():
        try:
            await session.execute(query)
            await session.commit()
        except Exception as ex:
            await session.rollback()
            print("ADD/EDIT PUNKT BY USER ERRROR", ex)
            await bot.edit_message_text(
                text=error_text, chat_id=settings_msg[0], message_id=settings_msg[-1]
            )
        else:
            await bot.edit_message_text(
                text=success_text, chat_id=settings_msg[0], message_id=settings_msg[-1]
            )


async def notify_users_about_subscription_ending(ctx):
    logger.info("Started notify users about subscription ending")
    async for session in get_session():
        repo = UserRepository(session)

        for days in [5, 1]:
            logger.info("Searching for users which subscription ends in %s days", days)
            users_to_notify = await repo.get_users_which_subscription_ends(days)
            if not users_to_notify:
                logger.info("No one to notify")
                continue

            logger.info("Found %s users to notify", len(users_to_notify))

            user_ids = [user.tg_id for user in users_to_notify]
            await subscription_is_about_to_end(user_ids, session, days)


async def search_users_for_ended_subscription(ctx):
    logger.info("Started searching users for ended subscription")
    async for session in get_session():
        repo = UserRepository(session)
        subscription_repo = SubscriptionRepository(session)

        paid_subscriptions = await subscription_repo.get_paid_subscriptions()
        free_subscription = await subscription_repo.get_subscription_by_name("Free")
        if not paid_subscriptions or not free_subscription:
            logger.error("No paid or free subscriptinos in database. Aborting...")
            return

        paid_subscription_ids = [
            paid_subscription.id for paid_subscription in paid_subscriptions
        ]
        users = await repo.get_users_with_ended_subscription(paid_subscription_ids)

        for user in users:
            await drop_users_subscription(user, free_subscription, session)


async def drop_users_subscription(
    user: User, free_subscription: Subscription, session: AsyncSession
):
    logger.info("Dropping subscription for user %s [%s]", user.username, user.tg_id)
    async with session:
        repo = UserRepository(session)
        up_repo = UserProductRepository(session)

        user.subscription_id = free_subscription.id
        await repo.update(user.tg_id, subscription_id=free_subscription.id)

        for marker in ["ozon", "wb"]:
            products = await up_repo.get_marker_products(user.tg_id, marker)
            marker_limit = getattr(free_subscription, f"{marker}_product_limit", 0)
            if len(products) <= marker_limit:
                continue

            products.sort(key=lambda product: product.time_create, reverse=True)
            logger.info(
                "Deleting %s user products for marker %s",
                len(products[marker_limit:]),
                marker,
            )
            for product in products[marker_limit:]:
                await up_repo.delete(product)

    await drop_users_punkt(user.tg_id, session)
    # TODO: notify user about his subscriptions is being dropped


async def drop_users_punkt(user: User, session: AsyncSession):
    logger.info("Dropping punkt for user %s [%s]", user.username, user.tg_id)
    async with session:
        repo = PunktRepository(session)
        await repo.delete_users_punkt(user.tg_id)
