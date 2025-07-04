import asyncio
from datetime import datetime, timedelta
from math import ceil
import os

import pytz


from arq.connections import ArqRedis

from aiogram import Router, types, Bot, F
from aiogram.filters import Command, and_f
from aiogram.fsm.context import FSMContext
from aiogram.utils.media_group import MediaGroupBuilder

from sqlalchemy import (
    and_,
    select,
    update,
    delete,
    func,
    Integer,
    Float,
    desc,
)
from sqlalchemy.sql.expression import cast

from sqlalchemy.ext.asyncio import AsyncSession

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

import config
from keyboards import (
    create_back_to_product_btn,
    create_or_add_exit_faq_btn,
    create_or_add_return_to_product_list_btn,
    create_pagination_page_kb,
    create_or_add_cancel_btn,
    create_remove_and_edit_sale_kb,
    create_reply_start_kb,
    create_settings_kb,
    create_punkt_settings_block_kb,
    create_faq_kb,
    create_question_faq_kb,
    create_back_to_faq_kb,
    create_or_add_exit_btn,
    new_create_or_add_return_to_product_list_btn,
    new_create_pagination_page_kb,
    new_create_remove_and_edit_sale_kb,
)

from schemas import FAQQuestion
from states import EditSale, LocationState, NewEditSale, PunktState

from utils.exc import NotEnoughGraphicData

from utils.handlers import (
    DEFAULT_PAGE_ELEMENT_COUNT,
    add_popular_product_to_db,
    check_input_link,
    delete_prev_subactive_msg,
    generate_graphic,
    generate_pretty_amount,
    check_user,
    new_check_has_punkt,
    new_show_product_list,
    show_product_list,
    try_delete_faq_messages,
    try_delete_prev_list_msgs,
    state_clear,
    add_message_to_delete_dict,
)

from utils.scheduler import (
    background_task_wrapper,
)

from utils.cities import city_index_dict

from utils.pics import ImageManager

from db.base import (
    OzonProduct as OzonProductModel,
    PopularProduct,
    ProductCityGraphic,
    Punkt,
    User,
    UserJob,
    WbProduct,
    Product,
    UserProduct,
    UserProductJob,
)


main_router = Router()

moscow_tz = pytz.timezone("Europe/Moscow")


SUB_START_TEXT = "🖐Здравствуйте, {}"

START_TEXT = "С помощью этого бота вы сможете отследить изменение цены на понравившиеся товары в маркетплейсах Wildberries и Ozon."


@main_router.message(Command("start"))
async def start(
    message: types.Message | types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    bot: Bot,
    image_manager: ImageManager,
):
    start_pic_id = await image_manager.get_start_pic_id()
    _message = message

    await try_delete_prev_list_msgs(message.chat.id, state)

    await state_clear(state)

    utm_source = None

    if isinstance(message, types.Message):
        query_param = message.text.split()

        if len(query_param) > 1:
            utm_source = query_param[-1]
            print("UTM_SOURCE", utm_source)

    await check_user(message, session, utm_source)

    if isinstance(message, types.CallbackQuery):
        message = message.message

    _kb = create_reply_start_kb()

    faq_kb = create_faq_kb()

    await bot.send_message(
        text=SUB_START_TEXT.format(message.from_user.username),
        chat_id=_message.chat.id,
        reply_markup=_kb.as_markup(resize_keyboard=True),
        disable_notification=True,
    )

    start_msg: types.Message = await bot.send_photo(
        chat_id=message.chat.id,
        photo=start_pic_id,
        caption=START_TEXT,
        reply_markup=faq_kb.as_markup(),
    )
    try:
        await bot.unpin_all_chat_messages(chat_id=message.chat.id)
    except Exception as ex:
        print("unpin error", ex)
    await bot.pin_chat_message(
        chat_id=start_msg.chat.id, message_id=start_msg.message_id
    )

    try:
        await message.delete()

        if isinstance(_message, types.CallbackQuery):
            await _message.answer()

    except Exception as ex:
        print(ex)


@main_router.callback_query(F.data.startswith("popular_product"))
async def delete_popular_product(
    callback: types.Message | types.CallbackQuery,
    session: AsyncSession,
    scheduler: AsyncIOScheduler,
):
    _, marker, popular_product_id = callback.data.split(":")

    scheduler_job_id = f"popular_{marker}_{popular_product_id}"

    delete_popular_product_query = delete(PopularProduct).where(
        PopularProduct.id == int(popular_product_id)
    )

    async with session as _session:
        await _session.execute(delete_popular_product_query)

        try:
            await _session.commit()
            scheduler.remove_job(job_id=scheduler_job_id, jobstore="sqlalchemy")
        except Exception as ex:
            print(ex)
            await _session.rollback()

    try:
        # await bot.delete_message(chat_id=)
        await callback.message.delete()
        await callback.answer(text="Популярный товар удалён ✅", show_alert=True)
    except Exception:
        await callback.answer(text="Что то не так", show_alert=True)


@main_router.message(
    and_f(LocationState.location), F.content_type == types.ContentType.LOCATION
)
async def proccess_location(
    message: types.Message | types.CallbackQuery,
    state: FSMContext,
):
    print(message.__dict__)
    print(message.location)
    await state.set_state()


@main_router.callback_query(F.data == "faq")
async def get_faq(
    callback: types.Message | types.CallbackQuery,
    state: FSMContext,
    bot: Bot,
):
    data = await state.get_data()

    await try_delete_faq_messages(data)

    _kb = create_question_faq_kb()
    _kb = create_or_add_exit_btn(_kb)

    _text = "❓Часто задаваемые вопросы❓\n\n👇 Выберите ниже интересующий вас пункт"

    faq_msg = await bot.send_message(
        chat_id=callback.from_user.id, text=_text, reply_markup=_kb.as_markup()
    )

    await add_message_to_delete_dict(faq_msg, state)

    await state.update_data(faq_msg=(faq_msg.chat.id, faq_msg.message_id))
    await callback.answer()


@main_router.callback_query(F.data == "back_to_faq")
async def back_to_faq(
    callback: types.Message | types.CallbackQuery,
    state: FSMContext,
    bot: Bot,
):
    data = await state.get_data()

    question_msg_list: list[int] = data.get("question_msg_list")
    back_to_faq_msg: tuple = data.get("back_to_faq_msg")

    _, _message_id = back_to_faq_msg

    question_msg_list.append(_message_id)

    print(question_msg_list)

    try:
        await bot.delete_messages(
            chat_id=callback.from_user.id, message_ids=question_msg_list
        )
    except Exception:
        print("ERROR WITH DELETE FAQ MESSAGES")

    await callback.answer()

    await get_faq(callback, state, bot)


@main_router.callback_query(F.data == "exit_faq")
async def exit_faq(
    callback: types.Message | types.CallbackQuery,
    state: FSMContext,
    bot: Bot,
):
    data = await state.get_data()

    question_msg_list: list[int] = data.get("question_msg_list")
    back_to_faq_msg: tuple = data.get("back_to_faq_msg")

    _, _message_id = back_to_faq_msg

    question_msg_list.append(_message_id)

    print(question_msg_list)

    try:
        await bot.delete_messages(
            chat_id=callback.from_user.id, message_ids=question_msg_list
        )
    except Exception as ex:
        print("ERROR WITH DELETE FAQ MESSAGES", ex)

    await callback.answer()


@main_router.callback_query(F.data.startswith("question"))
async def question_callback(
    callback: types.Message | types.CallbackQuery,
    state: FSMContext,
    bot: Bot,
    image_manager: ImageManager,
):
    data = await state.get_data()

    await try_delete_faq_messages(data)

    faq_msg: tuple = data.get("faq_msg")

    callback_data = callback.data

    question_prefix = "question_"

    question = callback_data[len(question_prefix) :]

    _kb = create_back_to_faq_kb()
    _kb = create_or_add_exit_faq_btn(_kb)

    try:
        await bot.delete_message(chat_id=callback.from_user.id, message_id=faq_msg[-1])
    except Exception as ex:
        print("ERROR WITH DELETE FAQ QUESTION LIST MESSAGE", ex)

    images = await image_manager.get_faq_photo_ids(FAQQuestion(question))

    media_group = [types.InputMediaPhoto(media=file_id) for file_id in images]

    image_group = MediaGroupBuilder(media_group)
    question_msg = await bot.send_media_group(
        chat_id=callback.from_user.id, media=image_group.build()
    )

    back_to_faq_msg = await bot.send_message(
        chat_id=callback.from_user.id,
        text="👇Выберите дальнейшие действия",
        reply_markup=_kb.as_markup(),
    )

    question_msg.append(back_to_faq_msg)
    for _msg in question_msg:
        await add_message_to_delete_dict(_msg, state)

    question_msg_list: list[int] = [_msg.message_id for _msg in question_msg]

    await state.update_data(
        question_msg_list=question_msg_list,
        back_to_faq_msg=(callback.from_user.id, back_to_faq_msg.message_id),
        faq_msg=None,
    )

    await callback.answer()


@main_router.message(F.text == "Посмотреть товары")
async def get_all_products_by_user(
    message: types.Message | types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
):
    await try_delete_prev_list_msgs(message.chat.id, state)
    await state.update_data(view_product_dict=None)

    data = await state.get_data()

    # pylint: disable=not-callable
    query = (
        select(
            UserProduct.id,
            UserProduct.link,
            cast(UserProduct.actual_price, Integer).label("actual_price"),
            cast(UserProduct.start_price, Integer).label("start_price"),
            UserProduct.user_id,
            cast(func.extract("epoch", UserProduct.time_create), Float).label(
                "time_create"
            ),
            Product.product_marker,
            Product.name,
            UserProduct.sale,
            UserProductJob.job_id,
        )
        .select_from(UserProduct)
        .join(Product, UserProduct.product_id == Product.id)
        .outerjoin(UserProductJob, UserProductJob.user_product_id == UserProduct.id)
        .where(UserProduct.user_id == message.from_user.id)
        .order_by(desc(UserProduct.time_create))
    )

    async with session as _session:
        res = await _session.execute(query)

    product_list = res.fetchall()

    if not product_list:
        await delete_prev_subactive_msg(data)

        sub_active_msg = await message.answer("Нет добавленных продуктов")

        await add_message_to_delete_dict(sub_active_msg, state)

        await state.update_data(
            _add_msg=(sub_active_msg.chat.id, sub_active_msg.message_id)
        )
        return

    len_product_list = len(product_list)

    product_list = list(map(tuple, product_list))

    try:
        wb_product_count = sum(1 for product in product_list if product[6] == "wb")
        ozon_product_count = len_product_list - wb_product_count
    except Exception as ex:
        print("sum eror", ex)
        wb_product_count = 0
        ozon_product_count = len_product_list

    pages = ceil(len_product_list / DEFAULT_PAGE_ELEMENT_COUNT)
    current_page = 1

    view_product_dict = {
        "len_product_list": len_product_list,
        "pages": pages,
        "current_page": current_page,
        "product_list": product_list,
        "ozon_product_count": ozon_product_count,
        "wb_product_count": wb_product_count,
    }

    await new_show_product_list(view_product_dict, message.from_user.id, state)

    try:
        await message.delete()
    except Exception:
        pass


@main_router.message(F.text == "Настройки")
async def get_settings(
    message: types.Message | types.CallbackQuery,
    state: FSMContext,
    bot: Bot,
):
    _text = "⚙️Ваши настройки⚙️\n\n<b>Выберите нужный раздел</b>"
    _kb = create_settings_kb()

    _kb = create_or_add_exit_btn(_kb)

    data = await state.get_data()

    settings_msg: tuple = data.get("settings_msg")
    faq_msg: tuple = data.get("faq_msg")

    if settings_msg:
        try:
            await bot.delete_message(
                chat_id=settings_msg[0], message_id=settings_msg[-1]
            )
        except Exception:
            pass

    if faq_msg:
        try:
            await bot.delete_message(chat_id=faq_msg[0], message_id=faq_msg[-1])
        except Exception:
            pass

    settings_msg: types.Message = await bot.send_message(
        chat_id=message.from_user.id, text=_text, reply_markup=_kb.as_markup()
    )

    await add_message_to_delete_dict(settings_msg, state)

    await state.update_data(
        settings_msg=(settings_msg.chat.id, settings_msg.message_id)
    )

    if isinstance(message, types.Message):
        try:
            await message.delete()
        except Exception:
            pass


@main_router.callback_query(F.data.startswith("settings"))
async def specific_settings_block(
    callback: types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    bot: Bot,
):
    settings_marker = callback.data.split("_")[-1]

    data = await state.get_data()

    settings_msg: tuple = data.get("settings_msg")

    match settings_marker:
        case "punkt":
            async with session as _session:
                # if callback.from_user.id in (int(DEV_ID), int(SUB_DEV_ID)):
                city_punkt = await new_check_has_punkt(
                    user_id=callback.from_user.id, session=_session
                )
                # else:
                #     city_punkt = await check_has_punkt(user_id=callback.from_user.id,
                #                                        session=_session)

            # _kb = create_specific_settings_block_kb(has_punkt=city_punkt)
            _kb = create_punkt_settings_block_kb(has_punkt=city_punkt)
            _kb = create_or_add_exit_btn(_kb)

            if not city_punkt:
                city_punkt = "Москва (по умолчанию)"

            _sub_text = f"Отслеживание цен по городу: {city_punkt}"

            _text = (
                f"⚙️Раздел настроек: Пункт выдачи⚙️\n\n{_sub_text}\n\nВыберите действие👇"
            )

            await bot.edit_message_text(
                text=_text,
                chat_id=settings_msg[0],
                message_id=settings_msg[-1],
                reply_markup=_kb.as_markup(),
            )
            await callback.answer()
        case "faq":
            _kb = create_question_faq_kb()
            _kb = create_or_add_exit_btn(_kb)

            await try_delete_faq_messages(data)

            _text = (
                "❓Часто задаваемые вопросы❓\n\n👇Выберите ниже интересующий вас пункт"
            )

            faq_msg = await bot.edit_message_text(
                chat_id=callback.from_user.id,
                message_id=settings_msg[-1],
                text=_text,
                reply_markup=_kb.as_markup(),
            )

            await state.update_data(faq_msg=(faq_msg.chat.id, faq_msg.message_id))
            await callback.answer()
        case "company":
            _kb = create_or_add_exit_btn()

            _text = "ИП Марченко Андрей Андреевич\n\n+79124970010\n\n198206, Россия, г. Санкт-Петербург, пр-кт Героев, д 32, стр 1, кв 18\n\nИНН 251116612876"

            await bot.edit_message_text(
                text=_text,
                chat_id=settings_msg[0],
                message_id=settings_msg[-1],
                reply_markup=_kb.as_markup(),
            )
            await callback.answer()


@main_router.callback_query(F.data.startswith("punkt"))
async def specific_punkt_block(
    callback: types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    bot: Bot,
):
    data = await state.get_data()

    settings_msg: tuple = data.get("settings_msg")

    callback_data = callback.data.split("_")
    punkt_action = callback_data[-1]

    punkt_data = {
        "user_id": callback.from_user.id,
        "punkt_action": punkt_action,
        # 'punkt_marker': punkt_marker,
    }

    await state.update_data(punkt_data=punkt_data)

    # await state.set_state(PunktState.city)
    _kb = create_or_add_exit_btn()

    match punkt_action:
        case "add":
            await state.set_state(PunktState.city)
            _text = '🏙 Введите название города, в формате "Город", в котором хотите отслеживать цены.\n\n❗Если ваш город не находит, введите название ближайшего крупного населённого пункта.'

            await bot.edit_message_text(
                text=_text,
                chat_id=settings_msg[0],
                message_id=settings_msg[-1],
                reply_markup=_kb.as_markup(),
            )

        case "edit":
            await state.set_state(PunktState.city)
            _text = '🏙 Введите название <b>нового</b> города, в формате "Город", в котором хотите отслеживать цены.\n\n❗Если ваш город не находит, введите название ближайшего крупного населённого пункта.'

            await bot.edit_message_text(
                text=_text,
                chat_id=settings_msg[0],
                message_id=settings_msg[-1],
                reply_markup=_kb.as_markup(),
            )

        case "delete":
            # if callback.from_user.id in (int(DEV_ID), int(SUB_DEV_ID)):
            query = delete(Punkt).where(
                Punkt.user_id == callback.from_user.id,
            )
            # *на всякий случай
            _success_redirect = False

            async with session as _session:
                try:
                    await _session.execute(query)
                    await _session.commit()
                except Exception as ex:
                    print(ex)
                    await _session.rollback()
                    await callback.answer(
                        text="❌ Не получилось удалить пункт выдачи!", show_alert=True
                    )
                else:
                    await callback.answer(
                        text="✅ Пункт выдачи успешно удалён!", show_alert=True
                    )
                    _success_redirect = True

            if _success_redirect:
                await get_settings(callback, state, bot)


@main_router.message(and_f(PunktState.city), F.content_type == types.ContentType.TEXT)
async def add_punkt_proccess(
    message: types.Message | types.CallbackQuery,
    state: FSMContext,
    bot: Bot,
    scheduler: AsyncIOScheduler,
):
    data = await state.get_data()

    settings_msg: tuple = data.get("settings_msg")

    if not settings_msg:
        sub_active_msg: tuple = data.get("_add_msg")

        if not sub_active_msg:
            sub_active_msg: types.Message = await message.answer(
                "Возникли трудности, попробуйте еще раз"
            )
            await add_message_to_delete_dict(sub_active_msg, state)
            await state.update_data(
                _add_msg=(sub_active_msg.chat.id, sub_active_msg.message_id)
            )
        else:
            await bot.edit_message_text(
                text="Возникли трудности, попробуйте еще раз",
                chat_id=sub_active_msg[0],
                message_id=sub_active_msg[-1],
            )

        await state.set_state()

        try:
            await message.delete()
        except Exception as ex:
            print(ex)

        return

    city = message.text.strip().lower()

    _kb = create_or_add_exit_btn()

    city_index = city_index_dict.get(city)

    if not city_index:
        _text = f'❌ Не удалось найти  - {message.text.strip()}\n\n<b><i>Пожалуйста, проверяйте корректность вводимого значения</i></b>\n\n🏙 Введите название города, в формате "Город", в котором хотите отслеживать цены.\n\n❗Если ваш город не находит, введите название ближайшего крупного населённого пункта.'
        await bot.edit_message_text(
            text=_text,
            chat_id=settings_msg[0],
            message_id=settings_msg[-1],
            reply_markup=_kb.as_markup(),
        )
        try:
            await message.delete()
        except Exception as ex:
            print(ex)

        return

    punkt_data: dict = data.get("punkt_data")

    # punkt_marker: str = punkt_data.get('punkt_marker')

    punkt_data.update(
        {
            "city": city.upper(),
            "index": city_index,
            "settings_msg": settings_msg,
        }
    )

    _text = "⏳ Добавление пункта выдачи...\n\n❗<b><i>Просим Вас не пытаться добавить новый пункт, пока не завершиться текущее добавление</i></b>"

    await bot.edit_message_text(
        text=_text, chat_id=settings_msg[0], message_id=settings_msg[-1]
    )

    await state.set_state()

    scheduler.add_job(
        background_task_wrapper,
        trigger=DateTrigger(run_date=datetime.now()),
        args=(
            "add_punkt_by_user",
            punkt_data,
        ),
        kwargs={"_queue_name": "arq:high"},
        jobstore="sqlalchemy",
    )

    await message.delete()


@main_router.callback_query(F.data == "pagination_page")
async def pagination_page(
    callback: types.Message | types.CallbackQuery,
    state: FSMContext,
    bot: Bot,
):
    data = await state.get_data()

    product_dict: dict = data.get("view_product_dict")

    list_msg: tuple = product_dict.get("list_msg")

    _kb = create_pagination_page_kb(product_dict)
    _kb = create_or_add_return_to_product_list_btn(_kb)

    await bot.edit_message_text(
        chat_id=list_msg[0],
        message_id=list_msg[-1],
        text="Выберите страницу, на которую хотите перейти",
        reply_markup=_kb.as_markup(),
    )
    await callback.answer()


@main_router.callback_query(F.data == "new_pagination_page")
async def new_pagination_page(
    callback: types.Message | types.CallbackQuery,
    state: FSMContext,
    bot: Bot,
):
    data = await state.get_data()

    product_dict: dict = data.get("view_product_dict")

    list_msg: tuple = product_dict.get("list_msg")

    _kb = new_create_pagination_page_kb(product_dict)
    _kb = new_create_or_add_return_to_product_list_btn(_kb)

    await bot.edit_message_caption(
        chat_id=list_msg[0],
        message_id=list_msg[-1],
        caption="Выберите страницу, на которую хотите перейти",
        reply_markup=_kb.as_markup(),
    )

    await callback.answer()


@main_router.callback_query(F.data.startswith("go_to_page"))
async def go_to_selected_page(
    callback: types.Message | types.CallbackQuery,
    state: FSMContext,
):
    data = await state.get_data()

    selected_page = callback.data.split("_")[-1]

    product_dict: dict = data.get("view_product_dict")

    product_dict["current_page"] = int(selected_page)

    await show_product_list(product_dict, callback.from_user.id, state)
    await callback.answer()


@main_router.callback_query(F.data.startswith("new_go_to_page"))
async def new_go_to_selected_page(
    callback: types.Message | types.CallbackQuery,
    state: FSMContext,
):
    data = await state.get_data()

    selected_page = callback.data.split("_")[-1]

    product_dict: dict = data.get("view_product_dict")

    product_dict["current_page"] = int(selected_page)

    await new_show_product_list(product_dict, callback.from_user.id, state)
    await callback.answer()


@main_router.callback_query(F.data.startswith("page"))
async def switch_page(
    callback: types.Message | types.CallbackQuery,
    state: FSMContext,
):
    callback_data = callback.data.split("_")[-1]

    data = await state.get_data()

    product_dict = data.get("view_product_dict")

    if not product_dict:
        await callback.answer(text="Ошибка", show_alert=True)
        return

    if callback_data == "next":
        product_dict["current_page"] += 1
    else:
        product_dict["current_page"] -= 1

    await show_product_list(product_dict, callback.from_user.id, state)
    await callback.answer()


@main_router.callback_query(F.data.startswith("new_page"))
async def new_switch_page(
    callback: types.Message | types.CallbackQuery,
    state: FSMContext,
):
    callback_data = callback.data.split("_")[-1]

    data = await state.get_data()

    product_dict = data.get("view_product_dict")

    if not product_dict:
        await callback.answer(text="Ошибка", show_alert=True)
        return

    if callback_data == "next":
        product_dict["current_page"] += 1
    else:
        product_dict["current_page"] -= 1

    await new_show_product_list(product_dict, callback.from_user.id, state)
    await callback.answer()


@main_router.callback_query(F.data == "cancel")
async def callback_cancel(
    callback: types.Message | types.CallbackQuery,
    state: FSMContext,
):
    await state.set_state()
    try:
        await callback.message.delete()
    except Exception:
        pass
    finally:
        await callback.answer()


@main_router.callback_query(F.data == "exit")
async def callback_to_main(
    callback: types.Message | types.CallbackQuery,
    state: FSMContext,
):
    await state.set_state()
    try:
        await callback.message.delete()
    except Exception:
        pass
    finally:
        await callback.answer()


@main_router.callback_query(F.data == "close")
async def callback_close(
    callback: types.Message | types.CallbackQuery,
):
    try:
        await callback.message.delete()
    except Exception as ex:
        print(ex)
    finally:
        await callback.answer()


@main_router.callback_query(F.data.startswith("back_to_product"))
async def back_to_product(
    callback: types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    bot: Bot,
):
    _callback_data = callback.data.split("_")
    print("from back_to_product", _callback_data)
    _callback_marker = "_".join(_callback_data[:-2])
    # user_id, product_id = _callback_data[-2], _callback_data[-1]
    is_background_message = _callback_marker.endswith("bg")

    await new_view_product(
        callback, state, session, bot, is_background=is_background_message
    )
    await callback.answer()


@main_router.callback_query(F.data == "return_to_product_list")
async def back_to_product_list(
    callback: types.Message | types.CallbackQuery, state: FSMContext
):
    data = await state.get_data()

    product_dict: dict = data.get("view_product_dict")

    if product_dict:
        await show_product_list(
            product_dict=product_dict, user_id=callback.from_user.id, state=state
        )
        await callback.answer()
    else:
        await callback.answer(text="Что то пошло не так", show_alert=True)


@main_router.callback_query(F.data == "new_return_to_product_list")
async def new_back_to_product_list(
    callback: types.Message | types.CallbackQuery, state: FSMContext
):
    data = await state.get_data()

    product_dict: dict = data.get("view_product_dict")

    if product_dict:
        await new_show_product_list(
            product_dict=product_dict, user_id=callback.from_user.id, state=state
        )
        await callback.answer()
    else:
        await callback.answer(text="Что то пошло не так", show_alert=True)


@main_router.callback_query(F.data.startswith("delete.new"))
async def new_delete_callback(
    callback: types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    scheduler: AsyncIOScheduler,
):
    with_redirect = True

    data = await state.get_data()

    _callback_data = callback.data.split("_")

    print(_callback_data)

    callback_prefix = _callback_data[0]

    if callback_prefix.endswith("rd"):
        with_redirect = False

    callback_data = _callback_data[1:]
    _, marker, _, product_id, job_id = callback_data

    print("JOB ID", job_id)

    query1 = delete(UserProductJob).where(
        and_(
            UserProductJob.job_id == job_id,
            UserProductJob.user_product_id == int(product_id),
        )
    )
    query2 = delete(UserProduct).where(
        UserProduct.id == int(product_id),
    )

    async with session.begin():
        await session.execute(query1)
        await session.execute(query2)
        try:
            await session.commit()

            scheduler.remove_job(job_id=job_id, jobstore="sqlalchemy")
        except Exception as ex:
            print(ex)
            await session.rollback()
        else:
            await callback.answer("Товар успешно удален", show_alert=True)

        if with_redirect:
            product_dict: dict = data.get("view_product_dict")

            pages: int = product_dict.get("pages")
            current_page: int = product_dict.get("current_page")
            product_list: list = product_dict.get("product_list")
            ozon_product_count: int = product_dict.get("ozon_product_count")
            wb_product_count: int = product_dict.get("wb_product_count")
            list_msg: tuple = product_dict.get("list_msg")

            for idx, product in enumerate(product_list):
                # print(product)
                # print(product[0], product_id)
                # print(product[6], marker)

                if product[0] == int(product_id) and product[6] == marker:
                    del product_list[idx]

            if marker == "wb":
                wb_product_count -= 1
            else:
                ozon_product_count -= 1

            len_product_list = len(product_list)

            pages = ceil(len_product_list / DEFAULT_PAGE_ELEMENT_COUNT)

            if current_page > pages:
                current_page -= 1

            len_product_list = len(product_list)

            view_product_dict = {
                "len_product_list": len_product_list,
                "pages": pages,
                "current_page": current_page,
                "product_list": product_list,
                "ozon_product_count": ozon_product_count,
                "wb_product_count": wb_product_count,
                "list_msg": list_msg,
            }

            await state.update_data(view_product_dict=view_product_dict)

            await new_back_to_product_list(callback, state)
        else:
            try:
                await callback.message.delete()
            except Exception as ex:
                print(ex)


@main_router.callback_query(F.data.startswith("delete"))
async def delete_callback(
    callback: types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    scheduler: AsyncIOScheduler,
):
    with_redirect = True

    data = await state.get_data()

    _callback_data = callback.data.split("_")

    callback_prefix = _callback_data[0]

    if callback_prefix.endswith("rd"):
        with_redirect = False

    callback_data = _callback_data[1:]
    marker, user_id, product_id, job_id = callback_data

    match marker:
        case "wb":
            query1 = delete(UserJob).where(
                and_(
                    UserJob.user_id == int(user_id),
                    UserJob.product_id == int(product_id),
                )
            )
            query2 = delete(WbProduct).where(
                and_(
                    WbProduct.id == int(product_id),
                )
            )
            async with session.begin():
                await session.execute(query1)
                await session.execute(query2)
                try:
                    await session.commit()

                    scheduler.remove_job(job_id=job_id, jobstore="sqlalchemy")
                except Exception as ex:
                    print(ex)
                    await session.rollback()
                else:
                    await callback.answer("Товар успешно удален", show_alert=True)

            if with_redirect:
                product_dict: dict = data.get("view_product_dict")

                pages: int = product_dict.get("pages")
                current_page: int = product_dict.get("current_page")
                product_list: list = product_dict.get("product_list")
                ozon_product_count: int = product_dict.get("ozon_product_count")
                wb_product_count: int = product_dict.get("wb_product_count")
                list_msg: tuple = product_dict.get("list_msg")

                for idx, product in enumerate(product_list):
                    print(product)
                    print(product[0], product_id)
                    print(product[6], marker)
                    if product[0] == int(product_id) and product[6] == marker:
                        del product_list[idx]

                wb_product_count -= 1

                len_product_list = len(product_list)

                pages = ceil(len_product_list / DEFAULT_PAGE_ELEMENT_COUNT)

                if current_page > pages:
                    current_page -= 1

                len_product_list = len(product_list)

                view_product_dict = {
                    "len_product_list": len_product_list,
                    "pages": pages,
                    "current_page": current_page,
                    "product_list": product_list,
                    "ozon_product_count": ozon_product_count,
                    "wb_product_count": wb_product_count,
                    "list_msg": list_msg,
                }

                await state.update_data(view_product_dict=view_product_dict)

                await back_to_product_list(callback, state)
            else:
                try:
                    await callback.message.delete()
                except Exception as ex:
                    print(ex)
        case "ozon":
            query1 = delete(UserJob).where(
                and_(
                    UserJob.user_id == int(user_id),
                    UserJob.product_id == int(product_id),
                )
            )
            query2 = delete(OzonProductModel).where(
                and_(
                    OzonProductModel.id == int(product_id),
                )
            )
            async with session.begin():
                await session.execute(query1)
                await session.execute(query2)
                try:
                    await session.commit()

                    scheduler.remove_job(job_id=job_id, jobstore="sqlalchemy")
                except Exception as ex:
                    print(ex)
                    await session.rollback()
                else:
                    await callback.answer("Товар успешно удален", show_alert=True)

            if with_redirect:
                product_dict: dict = data.get("view_product_dict")

                pages: int = product_dict.get("pages")
                current_page: int = product_dict.get("current_page")
                product_list: list = product_dict.get("product_list")
                ozon_product_count: int = product_dict.get("ozon_product_count")
                wb_product_count: int = product_dict.get("wb_product_count")
                list_msg: tuple = product_dict.get("list_msg")

                for idx, product in enumerate(product_list):
                    if product[0] == int(product_id) and product[6] == marker:
                        del product_list[idx]

                ozon_product_count -= 1

                len_product_list = len(product_list)

                pages = ceil(len_product_list / DEFAULT_PAGE_ELEMENT_COUNT)

                if current_page > pages:
                    current_page -= 1

                len_product_list = len(product_list)

                view_product_dict = {
                    "len_product_list": len_product_list,
                    "pages": pages,
                    "current_page": current_page,
                    "product_list": product_list,
                    "ozon_product_count": ozon_product_count,
                    "wb_product_count": wb_product_count,
                    "list_msg": list_msg,
                }

                await state.update_data(view_product_dict=view_product_dict)
                await back_to_product_list(callback, state)
            else:
                try:
                    await callback.message.delete()
                except Exception as ex:
                    print(ex)


@main_router.callback_query(F.data.startswith("edit.sale"))
async def edit_sale_callback(
    callback: types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    bot: Bot,
):
    data = await state.get_data()

    callback_data = callback.data.split("_")
    callback_prefix = callback_data[0]

    marker, user_id, product_id = callback_data[1:]

    with_redirect = True

    if callback_prefix.endswith("rd"):
        with_redirect = False

    if with_redirect:
        _sale_data: dict = data.get("sale_data")

        link = _sale_data.get("link")
        sale = _sale_data.get("sale")
        start_price = _sale_data.get("start_price")
    else:
        product_model = WbProduct if marker == "wb" else OzonProductModel
        query = select(
            product_model.link,
            product_model.sale,
            product_model.start_price,
        ).where(
            and_(
                product_model.id == int(product_id),
                product_model.user_id == callback.from_user.id,
            )
        )
        async with session as _session:
            res = await _session.execute(query)

        _sale_data = res.fetchall()
        link, sale, start_price = _sale_data[0]

    await state.update_data(
        sale_data={
            "user_id": user_id,
            "product_id": product_id,
            "marker": marker,
            "link": link,
            "sale": sale,
            "start_price": start_price,
            "with_redirect": with_redirect,
        }
    )
    await state.set_state(EditSale.new_sale)

    _kb = create_or_add_cancel_btn()

    msg = await bot.edit_message_text(
        text=f'<b>Установленная скидка на Ваш {marker.upper()} <a href="{link}">товар</a> {sale}</b>\n\nУкажите новую скидку <b>как число</b> в следующем сообщении',
        chat_id=callback.from_user.id,
        message_id=callback.message.message_id,
        reply_markup=_kb.as_markup(),
    )

    await add_message_to_delete_dict(msg, state)

    await state.update_data(msg=(msg.chat.id, msg.message_id))
    await callback.answer()


@main_router.callback_query(F.data.startswith("edit.new.sale"))
async def new_edit_sale_callback(
    callback: types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    bot: Bot,
):
    data = await state.get_data()

    callback_data = callback.data.split("_")
    callback_prefix = callback_data[0]

    _, marker, user_id, product_id = callback_data[1:]

    with_redirect = True

    if callback_prefix.endswith("rd"):
        with_redirect = False

    if with_redirect:
        _sale_data: dict = data.get("sale_data")

        link = _sale_data.get("link")
        sale = _sale_data.get("sale")
        start_price = _sale_data.get("start_price")
    else:
        # product_model = WbProduct if marker == 'wb' else OzonProductModel
        query = select(
            UserProduct.link,
            UserProduct.sale,
            UserProduct.start_price,
        ).where(
            and_(
                UserProduct.id == int(product_id),
                UserProduct.user_id == callback.from_user.id,
            )
        )
        async with session as _session:
            res = await _session.execute(query)

        _sale_data = res.fetchall()
        link, sale, start_price = _sale_data[0]

    await state.update_data(
        sale_data={
            "user_id": user_id,
            "product_id": product_id,
            "marker": marker,
            "link": link,
            "sale": sale,
            "start_price": start_price,
            "with_redirect": with_redirect,
        }
    )
    await state.set_state(NewEditSale.new_sale)

    _kb = create_or_add_cancel_btn()

    msg = await bot.edit_message_caption(
        caption=f'<b>Установленная скидка на Ваш {marker.upper()} <a href="{link}">товар</a> {sale}</b>\n\nУкажите новую скидку <b>как число</b> в следующем сообщении',
        chat_id=callback.from_user.id,
        message_id=callback.message.message_id,
        reply_markup=_kb.as_markup(),
    )

    await add_message_to_delete_dict(msg, state)

    await state.update_data(msg=(msg.chat.id, msg.message_id))
    await callback.answer()


@main_router.message(and_f(EditSale.new_sale), F.content_type == types.ContentType.TEXT)
async def edit_sale_proccess(
    message: types.Message | types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    bot: Bot,
):
    data = await state.get_data()

    new_sale = message.text.strip()

    await delete_prev_subactive_msg(data)

    if not new_sale.isdigit():
        sub_active_msg = await message.answer(
            text=f"Невалидные данные\nОжидается число, передано: {new_sale}"
        )

        await add_message_to_delete_dict(sub_active_msg, state)

        await state.update_data(
            _add_msg=(sub_active_msg.chat.id, sub_active_msg.message_id)
        )

        try:
            await message.delete()
        except Exception:
            pass

        return

    product_dict: dict = data.get("view_product_dict")

    msg: tuple = data.get("msg")

    # print('edit_sale_msg', edit_sale_msg)

    sale_data: dict = data.get("sale_data")

    if not sale_data:
        sub_active_msg = await message.answer("Ошибка")

        await add_message_to_delete_dict(sub_active_msg, state)

        await state.update_data(
            _add_msg=(sub_active_msg.chat.id, sub_active_msg.message_id)
        )

        try:
            await message.delete()
        except Exception:
            pass

        return

    user_id = sale_data.get("user_id")
    product_id = sale_data.get("product_id")
    marker = sale_data.get("marker")
    start_price = sale_data.get("start_price")
    with_redirect = sale_data.get("with_redirect")

    if start_price <= float(new_sale):
        sub_active_msg = await message.answer(
            text=f"Невалидные данные\nСкидка не может быть больше или равной цене товара\nПередано {new_sale}, Начальная цена товара: {start_price}"
        )

        await add_message_to_delete_dict(sub_active_msg, state)

        await state.update_data(
            _add_msg=(sub_active_msg.chat.id, sub_active_msg.message_id)
        )

        try:
            await message.delete()
        except Exception:
            pass

        return

    product_model = OzonProductModel if marker == "ozon" else WbProduct

    query = (
        update(product_model)
        .values(sale=float(new_sale))
        .where(
            and_(
                product_model.id == int(product_id),
                product_model.user_id == int(user_id),
            )
        )
    )

    async with session as _session:
        try:
            await _session.execute(query)
            await _session.commit()
        except Exception as ex:
            print(ex)
            await session.rollback()
            sub_active_msg = await message.answer("Не удалось обновить скидку")
        else:
            sub_active_msg = await message.answer("Скидка обновлена")

    await add_message_to_delete_dict(sub_active_msg, state)

    await state.update_data(
        sale_data=None, _add_msg=(sub_active_msg.chat.id, sub_active_msg.message_id)
    )
    await state.set_state()

    if with_redirect:
        await show_product_list(
            product_dict=product_dict, user_id=message.from_user.id, state=state
        )
    else:
        try:
            await bot.delete_message(chat_id=msg[0], message_id=msg[-1])
        except Exception as ex:
            print("ERROR WITH TRY DELETE SCHEDULER EDIT SALE MESSAGE", ex)

    try:
        await message.delete()
    except Exception:
        pass


@main_router.message(
    and_f(NewEditSale.new_sale), F.content_type == types.ContentType.TEXT
)
async def new_edit_sale_proccess(
    message: types.Message | types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    bot: Bot,
):
    data = await state.get_data()

    new_sale = message.text.strip()

    await delete_prev_subactive_msg(data)

    if not new_sale.isdigit():
        sub_active_msg = await message.answer(
            text=f"Невалидные данные\nОжидается число, передано: {new_sale}"
        )

        await add_message_to_delete_dict(sub_active_msg, state)

        await state.update_data(
            _add_msg=(sub_active_msg.chat.id, sub_active_msg.message_id)
        )

        try:
            await message.delete()
        except Exception:
            pass

        return

    product_dict: dict = data.get("view_product_dict")

    msg: tuple = data.get("msg")

    sale_data: dict = data.get("sale_data")

    if not sale_data:
        sub_active_msg = await message.answer("Ошибка")

        await add_message_to_delete_dict(sub_active_msg, state)

        await state.update_data(
            _add_msg=(sub_active_msg.chat.id, sub_active_msg.message_id)
        )

        try:
            await message.delete()
        except Exception:
            pass

        return

    user_id = sale_data.get("user_id")
    product_id = sale_data.get("product_id")
    # marker = sale_data.get("marker")
    start_price = sale_data.get("start_price")
    with_redirect = sale_data.get("with_redirect")

    if start_price <= float(new_sale):
        sub_active_msg = await message.answer(
            text=f"Невалидные данные\nСкидка не может быть больше или равной цене товара\nПередано {new_sale}, Начальная цена товара: {start_price}"
        )

        await add_message_to_delete_dict(sub_active_msg, state)

        await state.update_data(
            _add_msg=(sub_active_msg.chat.id, sub_active_msg.message_id)
        )

        try:
            await message.delete()
        except Exception:
            pass

        return

    query = (
        update(UserProduct)
        .values(sale=float(new_sale))
        .where(
            and_(UserProduct.id == int(product_id), UserProduct.user_id == int(user_id))
        )
    )

    async with session as _session:
        try:
            await _session.execute(query)
            await _session.commit()
        except Exception as ex:
            print(ex)
            await session.rollback()
            sub_active_msg = await message.answer("Не удалось обновить скидку")
        else:
            sub_active_msg = await message.answer("Скидка обновлена")

    await add_message_to_delete_dict(sub_active_msg, state)

    await state.update_data(
        sale_data=None, _add_msg=(sub_active_msg.chat.id, sub_active_msg.message_id)
    )
    await state.set_state()

    if with_redirect:
        await new_show_product_list(
            product_dict=product_dict, user_id=message.from_user.id, state=state
        )
    else:
        try:
            await bot.delete_message(chat_id=msg[0], message_id=msg[-1])
        except Exception as ex:
            print("ERROR WITH TRY DELETE SCHEDULER EDIT SALE MESSAGE", ex)

    try:
        await message.delete()
    except Exception:
        pass


# graphic
@main_router.callback_query(F.data.startswith("graphic"))
async def view_graphic(
    callback: types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    bot: Bot,
):
    # chat_id = callback.from_user.id
    message_id = callback.message.message_id

    callback_data = callback.data.split("_")

    callback_marker, user_id, product_id = callback_data

    is_background_message = callback_marker.endswith("bg")

    default_value = "МОСКВА"

    city_subquery = (
        select(Punkt.city).where(Punkt.user_id == int(user_id)).limit(1)
    ).scalar_subquery()

    check_datetime = datetime.now().astimezone(tz=moscow_tz) - timedelta(days=1)

    main_product_subquery = (
        select(Product.id)
        .select_from(UserProduct)
        .join(Product, UserProduct.product_id == Product.id)
        .where(UserProduct.id == int(product_id))
        .limit(1)
    ).scalar_subquery()

    graphic_query = select(
        ProductCityGraphic.photo_id,
    ).where(
        and_(
            ProductCityGraphic.product_id == main_product_subquery,
            ProductCityGraphic.city == func.coalesce(city_subquery, default_value),
            ProductCityGraphic.time_create >= check_datetime,
        )
    )

    async with session as _session:
        res = await _session.execute(graphic_query)

    graphic_photo_id = res.scalar_one_or_none()

    try:
        if not graphic_photo_id:
            try:
                await generate_graphic(
                    user_id=int(user_id),
                    product_id=int(product_id),
                    city_subquery=city_subquery,
                    message_id=message_id,
                    session=session,
                    state=state,
                    is_background=is_background_message,
                )
                await callback.answer()
            except NotEnoughGraphicData as ex:
                print(ex)
                await callback.answer(
                    text="Недостаточно данных для построения графика", show_alert=True
                )
        else:
            _kb = create_back_to_product_btn(
                user_id=user_id,
                product_id=product_id,
                is_background_task=is_background_message,
            )
            _kb = create_or_add_exit_btn(_kb)
            # photo_msg = await bot.send_photo(chat_id=user_id,
            #                                 photo=graphic_photo_id,
            #                                 reply_markup=_kb.as_markup())
            photo_msg = await bot.edit_message_media(
                chat_id=user_id,
                message_id=message_id,
                media=types.InputMediaPhoto(media=graphic_photo_id),
                reply_markup=_kb.as_markup(),
            )

            await add_message_to_delete_dict(photo_msg, state)
            await callback.answer()
    except Exception as ex:
        print(ex)
        await callback.answer(text="Не удалось построить график", show_alert=True)


@main_router.callback_query(F.data.startswith("view-product1"))
async def view_product(
    callback: types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    bot: Bot,
    marker: str = None,
):
    data = await state.get_data()

    product_dict: dict = data.get("view_product_dict")

    list_msg: tuple = product_dict.get("list_msg")

    callback_data = callback.data.split("_")[1:]

    _, marker, product_id = callback_data

    match marker:
        case "wb":
            subquery = (
                select(UserJob.job_id, UserJob.user_id, UserJob.product_id).where(
                    UserJob.user_id == callback.from_user.id
                )
            ).subquery()

            query = (
                select(
                    WbProduct.id,
                    WbProduct.link,
                    WbProduct.actual_price,
                    WbProduct.start_price,
                    WbProduct.user_id,
                    WbProduct.name,
                    WbProduct.sale,
                    func.text("WB").label("product_marker"),
                    subquery.c.job_id,
                )
                .select_from(WbProduct)
                .join(User, WbProduct.user_id == User.tg_id)
                .join(UserJob, UserJob.user_id == User.tg_id)
                .outerjoin(subquery, subquery.c.product_id == WbProduct.id)
                .where(
                    and_(
                        User.tg_id == callback.from_user.id,
                        WbProduct.id == int(product_id),
                    )
                )
                .distinct(WbProduct.id)
            )

            async with session as _session:
                res = await _session.execute(query)

                _data = res.fetchall()

            if _data:
                _product = _data[0]
                (
                    product_id,
                    link,
                    actaul_price,
                    start_price,
                    _,
                    name,
                    sale,
                    product_marker,
                    job_id,
                ) = _product
            else:
                print("No _data")
                await callback.answer()
                return

        case "ozon":
            subquery = (
                select(UserJob.job_id, UserJob.user_id, UserJob.product_id).where(
                    UserJob.user_id == callback.from_user.id
                )
            ).subquery()

            query = (
                select(
                    OzonProductModel.id,
                    OzonProductModel.link,
                    OzonProductModel.actual_price,
                    OzonProductModel.start_price,
                    OzonProductModel.user_id,
                    OzonProductModel.name,
                    OzonProductModel.sale,
                    func.text("OZON").label("product_marker"),
                    subquery.c.job_id,
                )
                .select_from(OzonProductModel)
                .join(User, OzonProductModel.user_id == User.tg_id)
                .join(UserJob, UserJob.user_id == User.tg_id)
                .outerjoin(subquery, subquery.c.product_id == OzonProductModel.id)
                .where(
                    and_(
                        User.tg_id == callback.from_user.id,
                        OzonProductModel.id == int(product_id),
                    )
                )
                .distinct(OzonProductModel.id)
            )

            async with session as _session:
                res = await _session.execute(query)

                _data = res.fetchall()

            if _data:
                _product = _data[0]
                (
                    product_id,
                    link,
                    actaul_price,
                    start_price,
                    _,
                    name,
                    sale,
                    product_marker,
                    job_id,
                ) = _product
            else:
                print("No _data")
                await callback.answer()
                return
        case _:
            print(f"Unexpected marker {marker}")
            await callback.answer()
            return

    _text_start_price = generate_pretty_amount(start_price)
    _text_product_price = generate_pretty_amount(actaul_price)

    _text_sale = generate_pretty_amount(sale)
    _text_price_with_sale = generate_pretty_amount((start_price - sale))

    _text = f'Название: <a href="{link}">{name}</a>\nМаркетплейс: {product_marker}\n\nНачальная цена: {_text_start_price}\nАктуальная цена: {_text_product_price}\n\nОтслеживается изменение цены на: {_text_sale}\nОжидаемая цена: {_text_price_with_sale}'

    await state.update_data(
        sale_data={
            "link": link,
            "sale": sale,
            "start_price": start_price,
        }
    )

    _kb = create_remove_and_edit_sale_kb(
        user_id=callback.from_user.id,
        product_id=product_id,
        marker=marker,
        job_id=job_id,
        with_redirect=True,
    )
    _kb = create_or_add_return_to_product_list_btn(_kb)

    if list_msg:
        await bot.edit_message_text(
            chat_id=list_msg[0],
            message_id=list_msg[-1],
            text=_text,
            reply_markup=_kb.as_markup(),
        )
    else:
        list_msg: types.Message = bot.send_message(
            chat_id=callback.from_user.id, text=_text, reply_markup=_kb.as_markup()
        )

        await add_message_to_delete_dict(list_msg, state)

        await state.update_data(list_msg=(list_msg.chat.id, list_msg.message_id))

    await callback.answer()


# new
@main_router.callback_query(F.data.startswith("view-product"))
async def new_view_product(
    callback: types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    bot: Bot,
    is_background: bool = False,
):
    print(callback.data)

    data = await state.get_data()

    product_dict: dict = data.get("view_product_dict")

    list_msg: tuple = product_dict.get("list_msg")

    callback_data = callback.data.split("_")

    if not is_background:
        if len(callback_data) == 4:
            _, _, product_id = callback_data[1:]
        else:
            _, product_id = callback_data[-2], callback_data[-1]
    else:
        _, product_id = callback_data[-2], callback_data[-1]

    query = (
        select(
            UserProduct.id,
            UserProduct.link,
            UserProduct.actual_price,
            UserProduct.start_price,
            UserProduct.user_id,
            Product.name,
            UserProduct.sale,
            Product.product_marker,
            UserProductJob.job_id,
            Product.photo_id,
        )
        .select_from(UserProduct)
        .join(Product, UserProduct.product_id == Product.id)
        .outerjoin(UserProductJob, UserProductJob.user_product_id == UserProduct.id)
        .where(
            UserProduct.id == int(product_id),
        )
    )

    async with session as _session:
        res = await _session.execute(query)

        _data = res.fetchall()

    if _data:
        _product = _data[0]
        (
            product_id,
            link,
            actaul_price,
            start_price,
            _,
            name,
            sale,
            product_marker,
            job_id,
            photo_id,
        ) = _product

        _text_start_price = generate_pretty_amount(start_price)
        _text_product_price = generate_pretty_amount(actaul_price)

        _text_sale = generate_pretty_amount(sale)
        _text_price_with_sale = generate_pretty_amount((start_price - sale))

        _text = f'Название: <a href="{link}">{name}</a>\n\nМаркетплейс: {product_marker}\n\nНачальная цена: {_text_start_price}\nАктуальная цена: {_text_product_price}\n\nОтслеживается изменение цены на: {_text_sale}\nОжидаемая цена: {_text_price_with_sale}'

        await state.update_data(
            sale_data={
                "link": link,
                "sale": sale,
                "start_price": start_price,
            }
        )

        _kb = new_create_remove_and_edit_sale_kb(
            user_id=callback.from_user.id,
            product_id=product_id,
            marker=product_marker,
            job_id=job_id,
            with_redirect=not is_background,
        )

        if is_background:
            _kb = create_or_add_exit_btn(_kb)

            await bot.edit_message_media(
                chat_id=callback.from_user.id,
                message_id=callback.message.message_id,
                media=types.InputMediaPhoto(media=photo_id, caption=_text),
                reply_markup=_kb.as_markup(),
            )
            await callback.answer()
            return

        _kb = new_create_or_add_return_to_product_list_btn(_kb)

        if list_msg:
            await bot.edit_message_media(
                chat_id=list_msg[0],
                message_id=list_msg[-1],
                media=types.InputMediaPhoto(media=photo_id, caption=_text),
                reply_markup=_kb.as_markup(),
            )

        else:
            list_msg: types.Message = bot.send_message(
                chat_id=callback.from_user.id, text=_text, reply_markup=_kb.as_markup()
            )

            await add_message_to_delete_dict(list_msg, state)

            await state.update_data(list_msg=(list_msg.chat.id, list_msg.message_id))

    await callback.answer()


@main_router.message(F.content_type == types.ContentType.PHOTO)
async def photo_test(
    message: types.Message,
):
    print(message.photo)
    print("*" * 10)


@main_router.message(
    F.content_type == types.ContentType.DOCUMENT,
    F.from_user.id.in_(config.ADMIN_IDS),
)
async def add_excel(message: types.Message, bot: Bot, redis_pool: ArqRedis):
    print("Checking file")
    document: types.Document = message.document
    filename = document.file_name

    # Проверка расширения
    if not filename.lower().endswith(".xlsx"):
        await message.reply("Пожалуйста, отправьте файл с расширением .xlsx")
        return

    # Получение файла от Telegram
    file = await message.bot.get_file(document.file_id)

    # Скачивание
    dest_path = os.path.join(config.DATA_DIR, filename)
    print("downloading file")
    await bot.download_file(file.file_path, destination=dest_path)
    asyncio.create_task(
        add_popular_product_to_db(redis_pool, dest_path, message.chat.id)
    )
    await message.answer(text="run adding...")


@main_router.message(F.content_type == types.ContentType.TEXT)
async def any_input(
    message: types.Message,
    state: FSMContext,
    redis_pool: ArqRedis,
):
    data = await state.get_data()

    await delete_prev_subactive_msg(data)

    _message_text = message.text.strip().split()

    _name = link = None

    if len(_message_text) > 1:
        *_name, link = _message_text
        _name = " ".join(_name)
    else:
        link = message.text.strip()

    check_link = check_input_link(link)

    if check_link:
        sub_active_msg = await message.answer(text=f"{check_link} товар добавляется...")

        user_data = {
            "msg": (message.chat.id, message.message_id),
            "name": _name,
            "link": link,
            "_add_msg_id": sub_active_msg.message_id,
            "product_marker": check_link,
        }

        await redis_pool.enqueue_job(
            "new_add_product_task", user_data, _queue_name="arq:high"
        )
    else:
        sub_active_msg = await message.answer(text="Невалидная ссылка")

    await add_message_to_delete_dict(sub_active_msg, state)

    await state.update_data(
        _add_msg=(sub_active_msg.chat.id, sub_active_msg.message_id)
    )

    try:
        await message.delete()
    except Exception as ex:
        print(ex)
