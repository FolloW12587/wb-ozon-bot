from aiogram import F, Bot, Router, types
from sqlalchemy.ext.asyncio import AsyncSession

from utils.handlers import check_user
import config


router = Router()


@router.message(F.content_type == types.ContentType.TEXT)
async def any_input(
    message: types.Message,
    session: AsyncSession,
    bot: Bot,
):
    if message.chat.type != "private":
        return

    await check_user(message, session, config.PREV_USER_UTM)
    text = """
⏸️ *Проект временно заморожен*

Если у вас была *активная подписка на 18.11*, напишите в \
[техподдержку](https://t.me/NaSkidku_support) — мы *вернём средства*.

О возобновлении проекта сообщим отдельной рассылкой.
"""
    await bot.send_message(message.chat.id, text, parse_mode="markdown")
