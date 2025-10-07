from asyncio import sleep
from datetime import datetime
import os

from aiogram import types
from aiogram.fsm.context import FSMContext
from aiogram.utils.text_decorations import markdown_decoration as md
from arq import ArqRedis

import pandas as pd
import pytz

import plotly.graph_objects as go

from sqlalchemy import update, select, and_, insert, Subquery, func
from sqlalchemy.ext.asyncio import AsyncSession

from bot22 import bot

from commands.send_message import notify_admins, send_message

import config
from db.base import (
    Punkt,
    User,
    ProductCityGraphic,
    ProductPrice,
    Product,
    UserProduct,
)
from db.repository.subscription import SubscriptionRepository
from db.repository.user import UserRepository
from db.repository.user_subscription import UserSubscriptionRepository
from db.repository.utm import UTMRepository
from payments.notifications import notify_user_about_referal_free_subscription
from payments.utils import give_users_free_referal_trial
from schemas import MessageInfo
from utils.pics import ImageManager

from utils.exc import NotEnoughGraphicData
from utils.scheduler import (
    add_task_to_delete_old_message_for_users,
)

from utils.any import send_data_to_yandex_metica

from keyboards import (
    create_back_to_product_btn,
    create_or_add_exit_btn,
    new_add_pagination_btn,
    new_create_product_list_for_page_kb,
)

from logger import logger


DEFAULT_PAGE_ELEMENT_COUNT = 5

image_manager = ImageManager(bot)


async def state_clear(state: FSMContext):
    data = await state.get_data()

    dict_msg_on_delete: dict = data.get("dict_msg_on_delete")

    await state.clear()

    if dict_msg_on_delete:
        await state.update_data(dict_msg_on_delete=dict_msg_on_delete)


async def add_message_to_delete_dict(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    message_date = message.date.timestamp()
    message_id = message.message_id

    # test on myself
    # if chat_id in (int(DEV_ID), 311364517):
    data = await state.get_data()

    dict_msg_on_delete: dict = data.get("dict_msg_on_delete")

    if not dict_msg_on_delete:
        dict_msg_on_delete = dict()

    dict_msg_on_delete[message_id] = (chat_id, message_date)

    await state.update_data(dict_msg_on_delete=dict_msg_on_delete)


def check_input_link(link: str):
    if (
        (link.startswith("https://ozon"))
        or (link.startswith("https://www.ozon"))
        or (link.startswith("https://www.wildberries"))
        or (link.startswith("https://wildberries"))
    ):

        return "WB" if link.find("wildberries") > 0 else "OZON"


def generate_sale_for_price(price: float):
    price = float(price)
    if 0 <= price <= 100:
        _sale = 10
    elif 100 < price <= 500:
        _sale = 50
    elif 500 < price <= 2000:
        _sale = 100
    elif 2000 < price <= 5000:
        _sale = 300
    else:
        _sale = 500

    return _sale


def generate_pretty_amount(price: str | float):
    _sign = "₽"
    price = int(price)

    pretty_price = f"{price:,}".replace(",", " ") + f" {_sign}"

    return pretty_price


def filter_price(price_data: list):
    current_price = None
    current_idx = None

    new_data = []

    for idx, data in enumerate(price_data):
        # _price, _date, _city, main_product_id, name, product_marker = data
        _price = data[0]

        if current_price is None:
            new_data.append(data)
            current_price = _price
            current_idx = idx
        else:
            if _price != current_price:

                prev_idx = idx - 1
                if (
                    idx > 1
                    and current_idx != prev_idx
                    and current_price == price_data[prev_idx][0]
                ):
                    new_data.append(price_data[prev_idx])

                new_data.append(data)
                current_price = _price
                current_idx = idx

    if new_data[-1][0] == price_data[-1][0]:

        if new_data[-1][1] == price_data[-1][1]:
            new_data.pop()

        new_data.append(price_data[-1])

    return new_data


def generate_date_view_list(date_list: list[datetime]):
    first = date_list[0]
    last = date_list[-1]
    len_date_list = len(date_list)

    if 10 < len_date_list <= 14:
        step = 2
    else:
        step = round(len(date_list) / 10)
        step = 1 if step == 0 else step

    filtered_list = date_list[1:-1][::step]

    new_date_list = [
        first,
    ]

    for el in filtered_list:
        if new_date_list[-1].day != el.day:
            new_date_list.append(el)

    if new_date_list[-1].day == last.day:
        new_date_list.pop()

    new_date_list.append(last)

    return new_date_list
    # return date_list[::step]
    # return [first, ] + filtered_list + [last, ]


async def generate_graphic(
    user_id: int,
    product_id: int,
    city_subquery: Subquery,
    message_id: int,
    session: AsyncSession,
    state: FSMContext,
    is_background: bool = False,
):
    moscow_tz = pytz.timezone("Europe/Moscow")
    default_value = "МОСКВА"

    query = (
        select(
            ProductPrice.price,
            ProductPrice.time_price,
            func.coalesce(ProductPrice.city, default_value),
            Product.id,
            Product.name,
            Product.product_marker,
        )
        .select_from(ProductPrice)
        .join(Product, ProductPrice.product_id == Product.id)
        .join(UserProduct, UserProduct.product_id == Product.id)
        .outerjoin(Punkt, Punkt.user_id == user_id)
        .where(
            and_(
                UserProduct.id == product_id,
                UserProduct.user_id == user_id,
                ProductPrice.city == func.coalesce(city_subquery, default_value),
            )
        )
        .order_by(ProductPrice.time_price)
    )

    async with session as _session:
        res = await _session.execute(query)

    res = res.fetchall()

    if not (res and len(res) >= 3):
        raise NotEnoughGraphicData()

    price_list = []
    date_list = []

    price_data = filter_price(res)

    for el in price_data:
        _price, _date, _city, main_product_id, name, product_marker = el
        # print(_city)
        _date: datetime
        price_list.append(_price)
        # date_list.append(_date.astimezone(tz=moscow_tz).strftime('%d-%m-%y'))
        date_list.append(_date.astimezone(tz=moscow_tz))

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=date_list, y=price_list, mode="lines+markers"))

    title_name = f"{name}<br>{product_marker.upper()} | {_city}"

    date_view_list = generate_date_view_list(date_list)

    fig.update_layout(
        title={"text": title_name, "x": 0.5, "xanchor": "center"},
        xaxis_title="Дата",
        #   xaxis_tickformat='%d-%m-%y',
        yaxis_title="Цена",
    )

    fig.update_xaxes(
        tickvals=date_view_list, tickformat="%d-%m-%y", dtick="D1", tickangle=-45
    )

    # fig.update_yaxes(tickvals=price_list,
    #                  ticktext=[f'{price:,}'.replace(',', ' ') for price in price_list])

    fig.update_yaxes(ticktext=[f"{price:,}".replace(",", " ") for price in price_list])

    # fig.update_layout(
    #     yaxis=dict(
    #         tickvals=y_data,  # Указываем значения для отображения
    #         ticktext=[f"{price:.5f}" for price in y_data]  # Форматируем текст для отображения
    #     )
    # )

    # Сохраняем график как изображение
    filename = "plot.png"
    fig.write_image("plot.png")

    _kb = create_back_to_product_btn(
        user_id=user_id, product_id=product_id, is_background_task=is_background
    )
    _kb = create_or_add_exit_btn(_kb)

    # photo_msg = await bot.send_photo(chat_id=user_id,
    #                                  photo=types.FSInputFile(path=f'./{filename}'),
    #                                  reply_markup=_kb.as_markup())
    photo_msg = await bot.edit_message_media(
        chat_id=user_id,
        message_id=message_id,
        media=types.InputMediaPhoto(media=types.FSInputFile(path=f"./{filename}")),
        reply_markup=_kb.as_markup(),
    )

    await add_message_to_delete_dict(photo_msg, state)

    if photo_msg.photo:
        photo_id = photo_msg.photo[0].file_id

        check_graphic_query = select(ProductCityGraphic.id).where(
            and_(
                ProductCityGraphic.city == _city,
                ProductCityGraphic.product_id == main_product_id,
            )
        )
        async with session as _session:
            check_res = await _session.execute(check_graphic_query)

        graphic_id = check_res.scalar_one_or_none()

        if not graphic_id:

            insert_data = {
                "product_id": main_product_id,
                "city": _city,
                "photo_id": photo_id,
                "time_create": datetime.now(),
            }

            final_query = insert(ProductCityGraphic).values(**insert_data)
        else:
            final_query = (
                update(ProductCityGraphic)
                .values(
                    photo_id=photo_id,
                    time_create=datetime.now(),
                )
                .where(
                    ProductCityGraphic.id == graphic_id,
                )
            )

        async with session as _session:
            await _session.execute(final_query)
            try:
                await _session.commit()
                print("add success")
                return True
            except Exception as ex:
                await _session.rollback()
                print("add error", ex)


async def add_user(
    message: types.Message, session: AsyncSession, utm_source: str | None
):
    user_repo = UserRepository(session)
    subscription_repo = SubscriptionRepository(session)
    free_subscription = await subscription_repo.get_subscription_by_name("Free")

    if not free_subscription:
        return False

    user = User(
        tg_id=message.from_user.id,
        username=message.from_user.username,
        first_name=message.from_user.first_name,
        last_name=message.from_user.last_name,
        time_create=datetime.now(),
        subscription_id=free_subscription.id,
        utm_source=utm_source,
    )
    user = await user_repo.create(user)

    await add_task_to_delete_old_message_for_users(user_id=message.from_user.id)
    print("user added")

    if not utm_source or utm_source.startswith("direct"):
        return True

    if utm_source.startswith("inviter"):
        await handle_referal_invitation(user, utm_source, session)
        return True

    if utm_source == "prev_user":
        await handle_prev_user(user)
        return True

    utm_repo = UTMRepository(session)
    utms = await utm_repo.get_by_keitaro_id(utm_source)

    if not utms:
        return True

    utm = utms[0]
    await utm_repo.update(utm.id, user_id=message.from_user.id)
    # send csv to yandex API
    await send_data_to_yandex_metica(utm.client_id, goal_id="bot_start")
    return True


async def check_user(
    message: types.Message, session: AsyncSession, utm_source: str | None
):
    async with session as _session:
        repo = UserRepository(_session)
        user = await repo.find_by_id(message.from_user.id)
        if user:
            await repo.update(user.tg_id, is_active=True)
            return True

        return await add_user(message, _session, utm_source)


async def handle_referal_invitation(
    invited_user: User, utm_source: str, session: AsyncSession
):
    logger.info(
        "Handling referal invitation for user %s, source: %s",
        invited_user.tg_id,
        utm_source,
    )
    if invited_user.invited_by_user is not None:
        logger.error(
            "User can't be invited because he is already invited by %s",
            invited_user.invited_by_user,
        )
        return

    user_repo = UserRepository(session)
    us_repo = UserSubscriptionRepository(session)
    subscription_repo = SubscriptionRepository(session)

    subscriptions = await subscription_repo.get_paid_subscriptions()
    if len(subscriptions) < 0:
        logger.error("Can't find any paid subscriptions")
        return
    subscription = subscriptions[0]

    inviter_id = utm_source.split("_")[-1]
    try:
        inviter_id = int(inviter_id)
    except ValueError:
        logger.error("Inviter id should be valid integer %s", inviter_id)
        return

    if inviter_id == invited_user.tg_id:
        logger.error("User can't invite himself")
        return

    inviter = await user_repo.find_by_id(inviter_id)
    if not inviter:
        logger.error("Can't find inviter user with id %s", inviter_id)
        return

    await user_repo.update(invited_user.tg_id, invited_by_user=inviter_id)
    await session.refresh(invited_user)
    try:
        await give_users_free_referal_trial(
            us_repo=us_repo,
            user_repo=user_repo,
            invited_user=invited_user,
            inviter=inviter,
            subscription_id=subscription.id,
        )
    except Exception:
        logger.error("Error in giving free trial subscriptions to users", exc_info=True)
        await notify_admins(
            MessageInfo(
                text=(
                    "Произошла ошибка при попытке дать пользователям триал за рефералку"
                )
            )
        )
        return

    try:
        await notify_user_about_referal_free_subscription(invited_user.tg_id, True)
        await notify_user_about_referal_free_subscription(inviter.tg_id, False)
    except Exception:
        logger.error(
            "Error in notifying users about free referal subscription", exc_info=True
        )

    await notify_admins(
        MessageInfo(
            text=(
                f"Пользователь @{md.quote(invited_user.username or str(invited_user.tg_id))} "
                f"пришел по рефералке от @{md.quote(inviter.username or str(inviter.tg_id))}"
            )
        )
    )


async def handle_prev_user(user: User):
    logger.info("User returned after server crash %s", user.tg_id)
    message = f"""
Приветствуем!

Недавно наш сервер, на котором администрировался бот, подвергся взлому.
📅 С *3 по 4 октября* бот работал с перебоями, а *5 октября* — полностью вышел из строя.

Сейчас работа бота полностью восстановлена, однако, к сожалению, *сохранить добавленные товары и активные подписки не удалось.*
__Пожалуйста, добавьте товары заново__ 🙏.

Мы *усилили защиту* и внедрили дополнительные меры, чтобы подобная ситуация больше не повторилась.

Если у вас была *активная подписка*, напишите в [техническую поддержку]({config.SUPPORT_BOT_URL})\
 — мы восстановим её и *добавим +1 месяц в подарок.*

Приносим искренние извинения за неудобства и благодарим, что остаётесь с нами 🙌.
"""
    await send_message(user.tg_id, MessageInfo(text=message))
    await notify_admins(
        MessageInfo(
            text=(
                f"Пользователь @{md.quote(user.username or str(user.tg_id))} "
                f"проявил активность после взлома"
            )
        )
    )


async def new_check_has_punkt(user_id: int, session: AsyncSession):

    query = select(
        Punkt.city,
    ).where(Punkt.user_id == user_id)

    res = await session.execute(query)

    city_punkt = res.scalar_one_or_none()

    return city_punkt


# new
async def new_show_product_list(product_dict: dict, user_id: int, state: FSMContext):
    data = await state.get_data()

    # print('data' ,data)
    # print('product_dict', product_dict)

    current_page = product_dict.get("current_page")
    product_list = product_dict.get("product_list")
    len_product_list = product_dict.get("len_product_list")
    wb_product_count = product_dict.get("wb_product_count")
    ozon_product_count = product_dict.get("ozon_product_count")

    list_msg: tuple = product_dict.get("list_msg")

    if not product_list:
        await delete_prev_subactive_msg(data)
        sub_active_msg = await bot.send_message(
            chat_id=user_id, text="Нет добавленных товаров"
        )
        await add_message_to_delete_dict(sub_active_msg, state)

        await state.update_data(
            _add_msg=(sub_active_msg.chat.id, sub_active_msg.message_id)
        )
        return

    start_idx = (current_page - 1) * DEFAULT_PAGE_ELEMENT_COUNT
    end_idx = current_page * DEFAULT_PAGE_ELEMENT_COUNT

    product_list_for_page = product_list[start_idx:end_idx]

    _kb = new_create_product_list_for_page_kb(product_list_for_page)
    _kb = new_add_pagination_btn(_kb, product_dict)
    _kb = create_or_add_exit_btn(_kb)

    product_on_current_page_count = len(product_list_for_page)

    _text = f"Ваши товары\n\nВсего товаров: {len_product_list}\nПоказано {product_on_current_page_count} товар(a/ов)"

    _text = f"📝 Список ваших товаров:\n\n🔽 Всего товаров: {len_product_list}\n\n🔵 Товаров с Ozon: {ozon_product_count}\n🟣 Товаров с Wildberries: {wb_product_count}\n\nПоказано {product_on_current_page_count} товаров на странице, нажмите ▶, чтобы листать список"

    photo_id = await image_manager.get_default_product_list_photo_id()
    if not list_msg:
        # list_msg: types.Message = await bot.send_message(chat_id=user_id,
        #                                                  text=_text,
        #                                                  reply_markup=_kb.as_markup())

        list_msg: types.Message = await bot.send_photo(
            chat_id=user_id,
            photo=photo_id,
            caption=_text,
            reply_markup=_kb.as_markup(),
        )

        await add_message_to_delete_dict(list_msg, state)

        product_dict["list_msg"] = (list_msg.chat.id, list_msg.message_id)

        list_msg_on_delete: list = data.get("list_msg_on_delete")

        if not list_msg_on_delete:
            list_msg_on_delete = list()

        list_msg_on_delete.append(list_msg.message_id)

        await state.update_data(list_msg_on_delete=list_msg_on_delete)

    else:
        # await bot.edit_message_text(chat_id=user_id,
        #                             message_id=list_msg[-1],
        #                             text=_text,
        #                             reply_markup=_kb.as_markup())
        await bot.edit_message_media(
            chat_id=user_id,
            media=types.InputMediaPhoto(media=photo_id, caption=_text),
            message_id=list_msg[-1],
            reply_markup=_kb.as_markup(),
        )

    await state.update_data(view_product_dict=product_dict)


async def try_delete_prev_list_msgs(chat_id: int, state: FSMContext):
    data = await state.get_data()

    list_msg_on_delete: list = data.get("list_msg_on_delete")

    if list_msg_on_delete:
        for msg_id in list_msg_on_delete:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except Exception as ex:
                print(ex)
                continue

    await state.update_data(list_msg_on_delete=None)


async def delete_prev_subactive_msg(data: dict):
    subactive_msg: tuple = data.get("_add_msg")
    try:
        await bot.delete_message(chat_id=subactive_msg[0], message_id=subactive_msg[-1])
    except Exception as ex:
        print(ex)


async def try_delete_faq_messages(data: dict):
    try:
        question_msg_list: list[int] = data.get("question_msg_list", list())
        back_to_faq_msg: tuple = data.get("back_to_faq_msg")
        faq_msg: tuple = data.get("faq_msg")

        _chat_id, _message_id = back_to_faq_msg

        question_msg_list.append(_message_id)

        if faq_msg:
            _, _message_id = faq_msg
            question_msg_list.append(_message_id)

        try:
            await bot.delete_messages(chat_id=_chat_id, message_ids=question_msg_list)
        except Exception:
            print("ERROR WITH DELETE FAQ MESSAGES")
    except Exception as ex:
        print("TRY DELETE PREV FAQ MESSAGES", ex)


def get_excel_data(path: str) -> list:
    df = pd.read_excel(path, header=None)

    data_array = df.values.tolist()

    return data_array[1:]


async def add_popular_product_to_db(
    _redis_pool: ArqRedis, file_path: str, reply_chat_id: int
):
    if not os.path.exists(file_path):
        await bot.send_message(reply_chat_id, "Can't find file. Aborting...")
        return

    data = get_excel_data(path=file_path)
    total_len = len(data)
    i = 0
    step = max(1, total_len // 5)

    msg = await bot.send_message(reply_chat_id, f"Обработано {i} из {total_len} строк")
    for name, link, _, high_category, low_category, *_ in data:

        product_marker = check_input_link(link)

        product_data = {
            "name": name,
            "link": link,
            "high_category": high_category,
            "low_category": low_category,
            "product_marker": product_marker.lower(),
        }

        await _redis_pool.enqueue_job(
            "add_popular_product", product_data=product_data, _queue_name="arq:high"
        )

        # чтобы тг не кидал ошибку о спаме в чат
        await sleep(2)
        i += 1
        if i % step == 0:
            await msg.edit_text(f"Загружено в очередь {i} из {total_len} строк")

    await msg.edit_text(
        "Все строки успешно добавлены в очередь, ожидайте появления в бд"
    )
    os.remove(file_path)
