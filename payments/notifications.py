from aiogram import types
from commands.send_message import send_message

import config
from db.base import UserSubscription
from logger import logger
from schemas import MessageInfo


async def notify_user_about_fail(user_id: int):
    logger.info("Notifying user %s about transaction processing fail", user_id)
    text = f"""
*❌ Не удалось оформить подписку*

Пожалуйста, напишите [нам в поддержку]({config.SUPPORT_BOT_URL}) и мы обязательно вам поможем.
"""
    btn = types.InlineKeyboardButton(text="Тех. поддержка", url=config.SUPPORT_BOT_URL)

    markup = types.InlineKeyboardMarkup(inline_keyboard=[[btn]])
    try:
        await send_message(user_id, MessageInfo(text=text, markup=markup))
    except Exception:
        logger.error(
            "Error in notifying user abput failed transaction processing", exc_info=True
        )


async def notify_user_about_purchsed_subscription(
    user_subscription: UserSubscription, user_id: int
):
    logger.info(
        "Notifying user %s about new purchased subscription %s",
        user_id,
        user_subscription.id,
    )
    text = f"""
*🎉 Подписка успешно оформлена!*

Спасибо за оплату — вы получили доступ ко всем функциям:

✔️ Безлимит на отслеживаемые товары
✔️ График изменения цен
✔️ Выбор пункта выдачи

*🗓 Подписка активна до {user_subscription.active_to}*

_Мы заранее напомним вам за 5 дней до окончания, чтобы вы могли продлить без перерыва в работе._

Приятных покупок и выгодных скидок! 💸"""
    try:
        await send_message(user_id, MessageInfo(text=text))
    except Exception:
        logger.error("Error in notifying user about new subscription", exc_info=True)
        raise


async def notify_user_about_referal_free_subscription(user_id: int, is_invited: bool):
    """Уведомляет пользователя о том, что ему достались бесплатные дни подписки
    по реферальной программе. Если `is_invited == True`, то это приглашенный пользователь.
    Если `is_invited == False`, то это пользователь, который пригласил."""
    logger.info(
        "Notifying user %s about free referal subscription. Is invited %s",
        user_id,
        is_invited,
    )
    text = ""
    if is_invited:
        text = """
*🎉 Мы дарим вам 14 дней бесплатной подписки!*

За участие в нашей реферальной программе☺️
"""
    else:
        text = """
За ваш вклад в развитие нашего сервиса

*🎉 Мы дарим вам 14 дней бесплатной подписки!*
"""

    try:
        await send_message(user_id, MessageInfo(text=text))
    except Exception:
        logger.error(
            "Error in notifying user %s about free referal subscription. Is invited %s",
            user_id,
            is_invited,
            exc_info=True,
        )
        raise
