from asyncio import sleep

# import config
from bot22 import bot
import config
from logger import logger

from schemas import MessageInfo


async def send_message(chat_id: int, message: MessageInfo) -> int:
    if message.photo_id:
        return await __send_photo(chat_id, message)

    return await __send_message(chat_id, message)


async def __send_message(chat_id: int, message: MessageInfo) -> int:
    msg = await bot.send_message(
        chat_id=chat_id,
        text=message.text,
        reply_markup=message.markup,
        parse_mode="markdown",
    )
    return msg.message_id


async def __send_photo(chat_id: int, message: MessageInfo) -> int:
    msg = await bot.send_photo(
        chat_id=chat_id,
        photo=message.photo_id,
        caption=message.text,
        reply_markup=message.markup,
        parse_mode="markdown",
    )
    return msg.message_id


async def pin_message(chat_id: int, message_id: int) -> bool:
    return await bot.pin_chat_message(chat_id=chat_id, message_id=message_id)


async def mass_sending_message(
    chat_ids: list[int], messages: list[MessageInfo]
) -> list[bool]:
    """Рассылка сообщения пользователям.
    Возвращает список булевых значений, показывающих успешность отправки сообщения"""
    output = []
    for chat_id in chat_ids:
        try:
            for message in messages:
                await send_message(chat_id, message)
                await sleep(0.05)

            output.append(True)
        except Exception:
            logger.info(
                "Error in sending message to %s. Possibly due to inactivity", chat_id
            )
            output.append(False)

    return output


async def notify_admins(message: MessageInfo):
    await send_message(config.ADMINS_CHAT_ID, message)
