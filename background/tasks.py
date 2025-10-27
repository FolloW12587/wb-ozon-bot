import json
import asyncio
from math import ceil
from datetime import datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.ext.asyncio import AsyncSession

from commands.send_message import modify_message, send_message
from db.base import Category, Product, Punkt, get_session, UserProduct

from db.repository.popular_product import PopularProductRepository
from db.repository.product import ProductRepository
from db.repository.punkt import PunktRepository
from db.repository.user import UserRepository
from db.repository.user_product import UserProductRepository
from keyboards import (
    add_or_create_close_kb,
    create_remove_popular_kb,
    new_create_remove_and_edit_sale_kb,
    create_go_to_subscription_kb,
)

from background.base import get_redis_background_pool
from bot22 import bot

from schemas import MessageInfo
from services.ozon.ozon_api_service import OzonAPIService
from services.wb.wb_api_service import WbAPIService
from utils.escape import escape_markdown
from utils.prices import get_product_price
from utils.storage import redis_client
from utils.any import (
    generate_pretty_amount,
    add_message_to_delete_dict,
    generate_percent_to_popular_product,
)
from utils.exc import OzonProductExistsError, WbProductExistsError
from utils.scheduler import (
    new_save_product,
    save_popular_product,
    try_add_product_price_to_db,
    update_last_send_price_by_user_product,
)
from utils.subscription import get_user_subscription_limit
from logger import logger


DELETE_THRESHOLD_HOURS = 36
DELETE_BATCH_SIZE = 100
BOT_BATCH_ACTION_DELAY = 0.2


async def new_add_product_task(ctx, user_data: dict):
    try:
        scheduler: AsyncIOScheduler = ctx.get("scheduler")
        product_marker: str = user_data.get("product_marker")
        _add_msg_id: int = user_data.get("_add_msg_id")
        msg: tuple = user_data.get("msg")

        async for session in get_session():
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
        except Exception as ex:
            print(ex)
            logger.error("Про добавлении товара %s произошла ошибка", exc_info=True)
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


async def push_check_price(ctx, user_id, product_id: str):
    logger.info("Новая фоновая задача %s", ctx["job_id"])

    async for session in get_session():
        product_repo = ProductRepository(session)
        user_product_repo = UserProductRepository(session)
        punkt_repo = PunktRepository(session)

        user_product = await user_product_repo.find_by_id(product_id)
        if not user_product or user_id != user_product.user_id:
            logger.error(
                "Can't find user product %s or user_id not matching", product_id
            )
            # TODO: drop job
            return

        product = await product_repo.find_by_id(user_product.product_id)
        if not product:
            logger.error("Can't find product for user product %s", product_id)
            # TODO: drop job
            return

        punkt = await punkt_repo.get_users_punkt(user_id)

    city = punkt.city if punkt else None
    _product_price = get_product_price(product, punkt)
    if not _product_price:
        logger.info(
            "Can't get product price for user_porduct %s user %s", product_id, user_id
        )
        return

    _product_price = float(_product_price)

    product_name = product.name if product.name else "Отсутствует"

    await try_add_product_price_to_db(
        product_id=product.id, city=city, price=_product_price
    )

    if _product_price == user_product.actual_price:
        print(f"Цена не изменилась user {user_id} product {product_name}")
        return

    async for session in get_session():
        async with session as _session:
            up_repo = UserProductRepository(_session)
            await up_repo.update_old(product_id, actual_price=_product_price)

    _waiting_price = user_product.start_price - user_product.sale

    pretty_product_price = generate_pretty_amount(_product_price)
    pretty_actual_price = generate_pretty_amount(user_product.actual_price)
    pretty_sale = generate_pretty_amount(user_product.sale)
    pretty_start_price = generate_pretty_amount(user_product.start_price)

    if _waiting_price < _product_price:
        return

    # проверка, отправлялось ли уведомление с такой ценой в прошлый раз
    if user_product.last_send_price is not None and (
        user_product.last_send_price == _product_price
    ):
        print(
            f"LAST SEND PRICE VALIDATION STOP {user_product.last_send_price} | {_product_price}"
        )
        return

    if user_product.actual_price < _product_price:
        _text = (
            f"🔄 Цена повысилась, но всё ещё входит в выставленный диапазон "
            f'скидки на товар <a href="{user_product.link}">{product_name}</a>\n\n'
            f"Маркетплейс: {str(product.product_marker).capitalize()}\n\n"
            f"🔄Отслеживаемая скидка: {pretty_sale}\n\n"
            f"⬇️Цена по карте: {pretty_product_price} "
            f"(дешевле на {user_product.start_price - _product_price}₽)\n\n"
            f"Начальная цена: {pretty_start_price}\n\n"
            f"Предыдущая цена: {pretty_actual_price}"
        )
        _disable_notification = True
    else:
        _text = (
            f'🚨 Изменилась цена на <a href="{user_product.link}">{product_name}</a>\n\n'
            f"Маркетплейс: {str(product.product_marker).capitalize()}\n\n"
            f"🔄Отслеживаемая скидка: {pretty_sale}\n\n"
            f"⬇️Цена по карте: {pretty_product_price} "
            f"(дешевле на {user_product.start_price - _product_price}₽)\n\n"
            f"Начальная цена: {pretty_start_price}\n\n"
            f"Предыдущая цена: {pretty_actual_price}"
        )
        _disable_notification = False

    _kb = new_create_remove_and_edit_sale_kb(
        user_id=user_id,
        product_id=product_id,
        marker=product.product_marker,
        job_id=ctx["job_id"],
        with_redirect=False,
    )

    _kb = add_or_create_close_kb(_kb)

    msg = await bot.send_photo(
        chat_id=user_id,
        photo=product.photo_id,
        caption=_text,
        disable_notification=_disable_notification,
        reply_markup=_kb.as_markup(),
    )

    await update_last_send_price_by_user_product(
        last_send_price=_product_price, user_product_id=product_id
    )

    await add_message_to_delete_dict(msg)


async def add_popular_product(cxt, product_data: dict):
    scheduler: AsyncIOScheduler = cxt.get("scheduler")
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
    except Exception as ex:
        print(ex)
        _text = (
            f"‼️ Возникла ошибка при добавлении {product_marker} товара\n\n"
            f"Попробуйте повторить позже"
        )
    else:
        _text = f"{product_marker} популярный товар добавлен к отслеживанию✅"
        print(_text)


async def push_check_popular_product(_, product_id: int):
    async for session in get_session():
        async with session as _session:
            await __push_check_popular_product(_session, product_id)


async def __push_check_popular_product(session: AsyncSession, product_id: int):
    logger.info("New popular product %s task", product_id)

    popular_product_repo = PopularProductRepository(session)
    product_repo = ProductRepository(session)

    popular_product = await popular_product_repo.find_by_id(product_id)
    if not popular_product:
        # TODO: drop job
        logger.error("Can't find popular product for id %s", product_id)
        return

    product = await product_repo.find_by_id(popular_product.product_id)
    if not product:
        # TODO: drop job
        logger.error(
            "Can't find product %s for popular product with id %s",
            popular_product.product_id,
            product_id,
        )
        return

    _product_price = await get_product_price(product, None)
    if not _product_price:
        logger.error("Can't get price for product %s", product.id)
        return

    _product_price = float(_product_price)

    if _product_price == popular_product.actual_price:
        print(f"цена не изменилась (популярный товар) product {product.name}")
        return

    _waiting_price = popular_product.start_price - popular_product.sale
    update_kwargs = {"actual_price": _product_price}

    if _waiting_price < _product_price:
        update_kwargs["last_notificated_price"] = None

    await popular_product_repo.update_old(product_id, **update_kwargs)

    # текущая цена выше, чем скидочный порог
    if _waiting_price < _product_price:
        return

    # последняя фиксированная цена не определена
    if popular_product.last_notificated_price is None:
        # фиксируем и оповещаем
        await popular_product_repo.update_old(
            product_id, last_notificated_price=_product_price
        )

        await notify_channels_about_popular_product_sale(
            popular_product.id,
            product.name,
            popular_product.link,
            _product_price,
            popular_product.start_price,
            product.photo_id,
            popular_product.category,
            popular_product.product,
        )
        return

    # последняя фиксированная цена ниже текущей цены
    if popular_product.last_notificated_price < _product_price:
        # фиксируем новую цену
        await popular_product_repo.update_old(
            product_id, last_notificated_price=_product_price
        )
        return

    price_diff = popular_product.last_notificated_price - _product_price
    # разница последней фикс цены и текущей цены ниже трех процентов
    if price_diff / popular_product.start_price < 0.03:
        return

    # новая цена больше, чем на 3 процента ниже предыдущей фиксированной цены
    # фиксируем и оповещаем
    await popular_product_repo.update_old(
        product_id, last_notificated_price=_product_price
    )

    await notify_channels_about_popular_product_sale(
        popular_product.id,
        product.name,
        popular_product.link,
        _product_price,
        popular_product.start_price,
        product.photo_id,
        popular_product.category,
        popular_product.product,
    )


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

        await asyncio.sleep(BOT_BATCH_ACTION_DELAY)


async def periodic_delete_old_message(_, user_id: int):
    """Удаляет старые сообщения пользователя из Redis и Telegram."""
    logger.info("Arq task delete old message user %s", user_id)

    key = f"fsm:{user_id}:{user_id}:data"
    now = datetime.now()

    # --- Читаем данные из Redis ---
    user_data = await redis_client.get(key)
    if not user_data:
        logger.debug("No user data found for %s", user_id)
        return

    json_user_data = json.loads(user_data)
    dict_msg_on_delete: dict = json_user_data.get("dict_msg_on_delete") or {}

    if not dict_msg_on_delete:
        logger.debug("No messages to delete for %s", user_id)
        return

    # --- Фильтруем старые сообщения ---
    expired_messages: dict[int, list[int]] = {}  # {chat_id: [msg_id, ...]}
    for msg_id, (chat_id, message_date) in list(dict_msg_on_delete.items()):
        if now - datetime.fromtimestamp(message_date) > timedelta(
            hours=DELETE_THRESHOLD_HOURS
        ):
            expired_messages.setdefault(chat_id, []).append(int(msg_id))
            del dict_msg_on_delete[msg_id]

    # --- Сохраняем обновленные данные ---
    json_user_data["dict_msg_on_delete"] = dict_msg_on_delete
    await redis_client.set(key, json.dumps(json_user_data))

    # --- Удаляем старые сообщения ---
    if not expired_messages:
        logger.debug("No expired messages for %s", user_id)
        return

    # --- Удаляем сообщения батчами по chat_id ---
    for chat_id, msg_ids in expired_messages.items():
        total_batches = ceil(len(msg_ids) / DELETE_BATCH_SIZE)
        logger.info(
            "Deleting %s messages from chat %s in %s batches for user %s",
            len(msg_ids),
            chat_id,
            total_batches,
            user_id,
        )

        for i in range(total_batches):
            batch = msg_ids[i * DELETE_BATCH_SIZE : (i + 1) * DELETE_BATCH_SIZE]
            try:
                await bot.delete_messages(chat_id=chat_id, message_ids=batch)
                await asyncio.sleep(BOT_BATCH_ACTION_DELAY)
            except Exception:
                logger.warning(
                    "Failed to delete messages for %s", user_id, exc_info=True
                )


async def add_punkt_by_user(punkt_data: dict):
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

    async for session in get_session():
        punkt_repo = PunktRepository(session)
        punkt = await punkt_repo.get_users_punkt(user_id)

        if punkt_action not in ["add", "edit"]:
            logger.error("Unexpected punkt action %s", punkt_action)
            return

        if not punkt:
            punkt = Punkt(
                user_id=user_id,
                index=int(city_index),
                city=city,
                ozon_zone=ozon_del_zone,
                wb_zone=wb_del_zone,
                time_create=datetime.now(),
            )
            await punkt_repo.create(punkt)

            text = f"✅ Пункт выдачи успешно добавлен (Установленный город - {city})."
        else:
            punkt.city = city
            punkt.index = int(city_index)
            punkt.ozon_zone = ozon_del_zone
            punkt.wb_zone = wb_del_zone
            punkt.time_create = datetime.now()

            await punkt_repo.update(punkt)

            text = (
                f"✅ Пункт выдачи успешно изменён (Новый установленный город - {city})."
            )

    await bot.edit_message_text(
        text=text, chat_id=settings_msg[0], message_id=settings_msg[-1]
    )

    redis_pool = await get_redis_background_pool()
    await redis_pool.enqueue_job(
        "update_user_product_prices", user_id, _queue_name="arq:high"
    )


async def update_user_product_prices(user_id: int):
    logger.info("Updating user %s product prices", user_id)

    async for session in get_session():
        user_repo = UserRepository(session)
        user_prod_repo = UserProductRepository(session)
        prod_repo = ProductRepository(session)
        punkt_repo = PunktRepository(session)

        user = await user_repo.find_by_id(user_id)
        if not user:
            logger.error("User with id %s was not found", user_id)
            return

        punkt = await punkt_repo.get_users_punkt(user_id)

        message_id = await send_message(
            user_id,
            MessageInfo(
                text="Обновляем цены на товары в связи со сменой пункта выдачи. "
                "Это может занять некоторое время..."
            ),
        )

        user_products = await user_prod_repo.get_user_products(user_id)
        not_updated_products: list[Product] = []
        for user_product in user_products:
            product = prod_repo.find_by_id(user_product.product_id)
            if not product:
                logger.error(
                    "Ошибка при получении данных продукта пользователя %s",
                    user_product.id,
                )
                continue

            if not await __update_product_price(
                user_product, product, punkt, user_prod_repo
            ):
                not_updated_products.append(product)

        text = "Обновление цен на товары завершено!\n\n"
        if not_updated_products:
            text += "Цены на следующие товары не удалось обновить:"
            for not_updated_product in not_updated_products:
                escaped_name = escape_markdown(not_updated_product.name)
                text += f"\n*{escaped_name}*"
        else:
            text += "*Цены на все товары обновлены успешно*"
        await modify_message(user_id, message_id, MessageInfo(text=text.strip()))


async def __update_product_price(
    user_product: UserProduct,
    product: Product,
    punkt: Punkt,
    user_product_repo: UserProductRepository,
) -> bool:
    try:
        product_price = await get_product_price(product, punkt)
        if not product_price:
            return False

        user_product.start_price = product_price
        user_product.actual_price = product_price
        user_product.last_send_price = None

        await user_product_repo.update(user_product)
    except Exception:
        logger.error(
            "Error in updating product price for user_product %s",
            user_product.id,
            exc_info=True,
        )
        return False
