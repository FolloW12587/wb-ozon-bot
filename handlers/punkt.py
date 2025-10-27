from datetime import datetime
from aiogram import Router, types, Bot, F

from aiogram.filters import and_f
from aiogram.fsm.context import FSMContext

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger

from arq.connections import ArqRedis
from sqlalchemy.ext.asyncio import AsyncSession

# from handlers.base import get_settings
from states import PunktState
from keyboards import create_go_to_subscription_kb, create_or_add_exit_btn

from logger import logger

from utils.cities import city_index_dict
from utils.handlers import add_message_to_delete_dict, check_user
from utils.scheduler import background_task_wrapper
from utils.subscription import get_user_subscription_option

from db.repository.punkt import PunktRepository

router = Router()


async def block_free_access_punkt(
    user_id: int, session: AsyncSession, state: FSMContext, bot: Bot
) -> bool:
    """Returns True if accessed"""
    data = await state.get_data()

    settings_msg: tuple = data.get("settings_msg")

    async with session as _session:
        try:
            subscription = await get_user_subscription_option(_session, user_id)
        except Exception:
            # Не получается получить подписку пользователя
            logger.error(
                "Error in getting subscription for user %s", user_id, exc_info=True
            )
            _kb = create_or_add_exit_btn()
            await bot.edit_message_text(
                text="Произошла ошибка, попробуйте еще раз или обратитесь в наше службу поддержки",
                chat_id=user_id,
                message_id=settings_msg[-1],
                reply_markup=_kb.as_markup(),
            )
            return

        if subscription.name == "Free":
            # Если у пользователя бесплатный план - ему доступна только Москва
            _text = """*🚫 Выбор пункта выдачи доступен по подписке 🏙*

В бесплатной версии цены показываются по Москве.

*🔓 С подпиской вы сможете настроить свой город и видеть актуальные цены для себя👇*"""
            _kb = create_go_to_subscription_kb()
            _kb = create_or_add_exit_btn(_kb)

            await bot.edit_message_text(
                text=_text,
                chat_id=user_id,
                message_id=settings_msg[-1],
                reply_markup=_kb.as_markup(),
                parse_mode="markdown",
            )
            return False

    return True


async def __delete_punkt(session: AsyncSession, user_id: int) -> bool:
    async with session as _session:
        punkt_repo = PunktRepository(_session)
        try:
            await punkt_repo.delete_users_punkt(user_id)
        except Exception:
            await _session.rollback()
            return False

        return True


@router.callback_query(F.data.startswith("punkt"))
async def specific_punkt_block(
    callback: types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    bot: Bot,
    redis_pool: ArqRedis,
):
    await check_user(callback, session, "prev_user")
    data = await state.get_data()

    settings_msg: tuple = data.get("settings_msg")
    user_id = callback.from_user.id

    if not await block_free_access_punkt(user_id, session, state, bot):
        await callback.answer()
        return

    callback_data = callback.data.split("_")
    punkt_action = callback_data[-1]

    punkt_data = {
        "user_id": user_id,
        "punkt_action": punkt_action,
        # 'punkt_marker': punkt_marker,
    }

    await state.update_data(punkt_data=punkt_data)

    # await state.set_state(PunktState.city)
    _kb = create_or_add_exit_btn()

    match punkt_action:
        case "add":
            await state.set_state(PunktState.city)
            _text = (
                '🏙 Введите название города, в формате "Город", '
                "в котором хотите отслеживать цены.\n\n"
                "❗Если ваш город не находит, введите название ближайшего "
                "крупного населённого пункта."
            )

            await bot.edit_message_text(
                text=_text,
                chat_id=settings_msg[0],
                message_id=settings_msg[-1],
                reply_markup=_kb.as_markup(),
            )

        case "edit":
            await state.set_state(PunktState.city)
            _text = (
                '🏙 Введите название <b>нового</b> города, в формате "Город", '
                "в котором хотите отслеживать цены.\n\n"
                "❗Если ваш город не находит, введите название ближайшего "
                "крупного населённого пункта."
            )

            await bot.edit_message_text(
                text=_text,
                chat_id=settings_msg[0],
                message_id=settings_msg[-1],
                reply_markup=_kb.as_markup(),
            )

        case "delete":
            is_success = await __delete_punkt(session, user_id)

            if not is_success:
                await callback.answer(
                    text="❌ Не получилось удалить пункт выдачи!", show_alert=True
                )
                return

            await callback.answer(
                text="✅ Пункт выдачи успешно удалён!", show_alert=True
            )

            await redis_pool.enqueue_job(
                "update_user_product_prices", user_id, _queue_name="arq:high"
            )


@router.message(and_f(PunktState.city), F.content_type == types.ContentType.TEXT)
async def add_punkt_proccess(
    message: types.Message | types.CallbackQuery,
    state: FSMContext,
    session: AsyncSession,
    bot: Bot,
    redis_pool: ArqRedis,
):
    await check_user(message, session, "prev_user")
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

    if not await block_free_access_punkt(message.from_user.id, session, state, bot):
        await message.delete()
        return

    city = message.text.strip().lower()

    _kb = create_or_add_exit_btn()

    city_index = city_index_dict.get(city)

    if not city_index:
        _text = (
            f"❌ Не удалось найти  - {message.text.strip()}\n\n"
            f"<b><i>Пожалуйста, проверяйте корректность вводимого значения</i></b>\n\n"
            f'🏙 Введите название города, в формате "Город", в котором хотите отслеживать цены.\n\n'
            f"❗Если ваш город не находит, введите название ближайшего крупного населённого пункта."
        )
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

    punkt_data.update(
        {
            "city": city.upper(),
            "index": city_index,
            "settings_msg": settings_msg,
        }
    )

    _text = (
        "⏳ Добавление пункта выдачи...\n\n"
        "❗<b><i>Просим Вас не пытаться добавить новый пункт, "
        "пока не завершиться текущее добавление</i></b>"
    )

    await bot.edit_message_text(
        text=_text, chat_id=settings_msg[0], message_id=settings_msg[-1]
    )

    await state.set_state()

    await redis_pool.enqueue_job(
        "update_user_product_prices", punkt_data, _queue_name="arq:high"
    )

    await message.delete()
