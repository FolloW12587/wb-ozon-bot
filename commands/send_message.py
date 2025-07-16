from asyncio import sleep
from typing import Union
from aiogram.types import (
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    ForceReply,
)

# import config
from bot22 import bot
from logger import logger

Markup = Union[
    InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove, ForceReply, None
]


async def send_message(chat_id: int, text: str, markup: Markup = None) -> int:
    msg = await bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=markup,
        parse_mode="markdown",
    )
    return msg.message_id


async def pin_message(chat_id: int, message_id: int) -> bool:
    return await bot.pin_chat_message(chat_id=chat_id, message_id=message_id)


async def mass_sending_message(
    chat_ids: list[int], text: str, markup: Markup = None
) -> list[bool]:
    """Рассылка сообщения пользователям.
    Возвращает список булевых значений, показывающих успешность отправки сообщения"""
    output = []
    for chat_id in chat_ids:
        try:
            await send_message(chat_id, text, markup)
            output.append(True)
            await sleep(0.05)
        except Exception:
            logger.info(
                "Error in sending message to %s. Possibly due to inactivity", chat_id
            )
            output.append(False)

    return output
