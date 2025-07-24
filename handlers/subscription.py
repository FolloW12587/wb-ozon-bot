from datetime import datetime, timezone
from aiogram import Router, types, Bot, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command

from aiogram.fsm.context import FSMContext

from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.order import OrderRepository
from db.repository.subscription import SubscriptionRepository
from db.repository.transaction import TransactionRepository
from db.repository.user import UserRepository
from db.repository.user_subscription import UserSubscriptionRepository
from services.yoomoney.yoomoney_service import YoomoneyService

from utils.handlers import add_message_to_delete_dict
from keyboards import create_subscription_kb, create_or_add_exit_btn
from payments.yoomoney import get_yoomoney_service

import config

from logger import logger

router = Router()


@router.message(Command("pay"))
async def pay(
    message: types.Message | types.CallbackQuery,
    # state: FSMContext,
    session: AsyncSession,
    bot: Bot,
):
    if message.from_user.id not in config.ADMIN_IDS:
        return

    async with session:
        transaction_repository = TransactionRepository(session)
        order_repository = OrderRepository(session)
        user_repo = UserRepository(session)
        subscription_repo = SubscriptionRepository(session)

        service = YoomoneyService(
            config.YOOMONEY_NOTIFICATION_SECRET,
            config.YOOMONEY_RECEIVER,
            transaction_repository,
            order_repository,
        )

        subscription = await subscription_repo.get_subscription_by_name("Unlimit")
        user = await user_repo.find_by_id(message.from_user.id)
        if not subscription or not user:
            await bot.send_message(
                message.chat.id, "Что-то пошло не так, попробуйте позднее"
            )
            return

        payment_url = await service.generate_payment_url(subscription, user)

        text = """
*🔓 С подпиской доступно:*

— Безлимит на отслеживаемые товары (в бесплатной версии добавить можно всего 3 товара WB и 3 товара Ozon)
— График цен (история изменения цен)
— Выбор пункта выдачи (а не только Москва)

*👉 Оформите подписку и экономьте ещё удобнее*"""

        btn = InlineKeyboardButton(text="Оформить подписку", url=payment_url)

        markup = InlineKeyboardMarkup(inline_keyboard=[[btn]])

        await bot.send_message(
            message.chat.id, text, reply_markup=markup, parse_mode="markdown"
        )


@router.message(F.text == "Подписка")
async def get_subscription(
    message: types.Message,
    session: AsyncSession,
    state: FSMContext,
    bot: Bot,
):
    await get_subscription_handler(message, session, state, bot)


@router.callback_query(F.data.startswith("subscription"))
async def get_subscription_qh(
    callback: types.CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    bot: Bot,
):
    await get_subscription_handler(callback, session, state, bot)


async def get_subscription_handler(
    message: types.Message | types.CallbackQuery,
    session: AsyncSession,
    state: FSMContext,
    bot: Bot,
):
    async with session:
        user_repo = UserRepository(session)
        user = await user_repo.find_by_id(message.from_user.id)
        if not user:
            await bot.send_message(
                text="Мы не можем найти Вас. Пожалуйста, попробуйте начать сначала командой /start"
            )
            return

        subscription_repo = SubscriptionRepository(session)
        subscriptions = await subscription_repo.get_paid_subscriptions()
        if not subscriptions:
            await bot.send_message(
                text=(
                    "На данный момент у нас предложений по подпискам для Вас! "
                    "Пожалуйста, попробуйте позднее"
                )
            )
            return

        subscription = subscriptions[0]

        user_subscription_repo = UserSubscriptionRepository(session)
        user_subscription = await user_subscription_repo.get_latest_subscription(
            user.tg_id
        )
        service = get_yoomoney_service(session)
        now = datetime.now(timezone.utc).date()
        has_active_subscription = user_subscription is not None and (
            user_subscription.active_to is None or user_subscription.active_to >= now
        )

        if has_active_subscription:
            _text = f"""
*✅ У вас активна подписка "НаСкидку за {subscription.price_rub} ₽ в месяц"*

Действует до *{user_subscription.active_to}*

• Безлимитное добавление товаров
• История цен в виде графика
• Настройка пункта выдачи

*🔄 Хотите продлить?👇*"""
        else:
            _text = f"""
*📦 Подписка НаСкидку — всего {subscription.price_rub} ₽ в месяц*

*🔓 С подпиской доступно:*

— Безлимит на отслеживаемые товары (в бесплатной версии добавить можно всего 3 товара WB и 3 товара Ozon)
— График цен (история изменения цен)
— Выбор пункта выдачи (а не только Москва)

*👉 Оформите подписку и экономьте ещё удобнее*"""

        payment_url = await service.generate_payment_url(subscription, user)
        _kb = create_subscription_kb(has_active_subscription, payment_url)

        _kb = create_or_add_exit_btn(_kb)

        await delete_main_messages(state, bot)

        subscription_msg: types.Message = await bot.send_message(
            chat_id=message.from_user.id,
            text=_text,
            reply_markup=_kb.as_markup(),
            parse_mode="markdown",
        )

        await add_message_to_delete_dict(subscription_msg, state)

        await state.update_data(
            subscription_msg=(subscription_msg.chat.id, subscription_msg.message_id)
        )

        if isinstance(message, types.Message):
            try:
                await message.delete()
            except Exception:
                logger.error("Can't delete message", exc_info=True)

        if isinstance(message, types.CallbackQuery):
            await message.answer()
            try:
                await bot.delete_message(
                    chat_id=message.from_user.id, message_id=message.message.message_id
                )
            except Exception:
                logger.error("Can't delete message", exc_info=True)


async def delete_main_messages(state: FSMContext, bot: Bot):
    data = await state.get_data()

    keys = ["settings_msg", "faq_msg", "subscription_msg"]
    for key in keys:
        msg_info: tuple = data.get(key)
        if msg_info:
            try:
                await bot.delete_message(chat_id=msg_info[0], message_id=msg_info[-1])
            except Exception:
                pass
