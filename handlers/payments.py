from aiogram import Router, types, Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command

# from aiogram.fsm.context import FSMContext

from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.order import OrderRepository
from db.repository.subscription import SubscriptionRepository
from db.repository.transaction import TransactionRepository
from db.repository.user import UserRepository
from services.yoomoney.yoomoney_service import YoomoneyService

import config

# from logger import logger

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

    async with session as _session:
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
