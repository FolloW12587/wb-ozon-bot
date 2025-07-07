from aiogram.types import (
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    ForceReply,
)

# import config
from bot22 import bot


async def send_message(
    chat_id: int,
    text: str,
    markup: (
        InlineKeyboardMarkup
        | ReplyKeyboardMarkup
        | ReplyKeyboardRemove
        | ForceReply
        | None
    ) = None,
) -> int:
    msg = await bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=markup,
        parse_mode="markdown",
    )
    return msg.message_id


async def pin_message(chat_id: int, message_id: int) -> bool:
    return await bot.pin_chat_message(chat_id=chat_id, message_id=message_id)
