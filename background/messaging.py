from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from aiogram import types
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder


import config
from commands.send_message import mass_sending_message, send_message
from commands.set_users_as_inactive import set_users_as_inactive
from db.base import (
    get_session,
    MessageSending,
    MessageSendingButton,
    MessageSendingStatus,
    MessageSendingButtonType,
)
from db.repository.message_sending import MessageSendingRepository
from db.repository.message_sending_button import MessageSendingButtonRepository
from db.repository.user import UserRepository

from logger import logger
from schemas import MessageInfo


async def process_message_sendings(ctx):
    async for session in get_session():
        ms_repo = MessageSendingRepository(session)

        sendings = await ms_repo.get_by_status(MessageSendingStatus.UPCOMING)
        if sendings:
            logger.info("Found %s sendings", len(sendings))

        for sending in sendings:
            await __process_message_sending(session, ms_repo, sending, False)

        test_sendings = await ms_repo.get_by_status(MessageSendingStatus.TEST)
        if test_sendings:
            logger.info("Found %s sendings", len(test_sendings))

        for sending in test_sendings:
            await __process_message_sending(session, ms_repo, sending, True)


async def __process_message_sending(
    session: AsyncSession,
    ms_repo: MessageSendingRepository,
    sending: MessageSending,
    is_test: bool,
):
    logger.info("Starting to process %s sending, is_test: %s", sending.id, is_test)
    sending.status = MessageSendingStatus.PROCESSING
    await ms_repo.update(
        sending.id, status=MessageSendingStatus.PROCESSING, error_message=""
    )

    try:
        markup = await __create_message_sending_markup(session, sending)
        logger.info("Markup created %s", markup)
    except Exception as e:
        logger.error(
            "Error in creating keyboard to test sending %s", sending.id, exc_info=True
        )
        await ms_repo.update(
            sending.id,
            status=MessageSendingStatus.FAILED,
            error_message=f"При создании кнопок для рассылки произошла ошибка: {str(e)}",
        )
        return

    user_ids = await __get_user_ids_for_message_sending(session, is_test)
    logger.info("Got %s user ids for sending %s", len(user_ids), sending.id)

    message_info = MessageInfo(text=sending.text, markup=markup)

    await __pre_message_sending(sending)
    if not is_test:
        await ms_repo.update(
            sending.id,
            started_at=datetime.now(),
            users_to_notify=len(user_ids),
        )
    try:
        results = await mass_sending_message(user_ids, [message_info])
        inactive_count = await set_users_as_inactive(user_ids, results, session)
    except Exception as e:
        logger.error("Error in mass sending %s", sending.id, exc_info=True)
        await ms_repo.update(
            sending.id,
            status=MessageSendingStatus.FAILED,
            error_message=f"При выполнении рассылки произошла ошибка: {str(e)}",
        )
        return

    await __post_message_sending(sending, len(user_ids), inactive_count)
    if is_test:
        await ms_repo.update(
            sending.id,
            status=MessageSendingStatus.CREATED,
            error_message="",
        )
        return

    await ms_repo.update(
        sending.id,
        status=MessageSendingStatus.COMPLETED,
        users_notified=len(user_ids) - inactive_count,
        error_message="",
    )


async def __get_user_ids_for_message_sending(
    session: AsyncSession, is_test: bool
) -> list[int]:
    logger.info("Getting user ids")
    if is_test:
        return [config.PAYMENTS_CHAT_ID]

    repo = UserRepository(session)
    users = await repo.get_active()
    return [user.tg_id for user in users]


async def __create_message_sending_markup(
    session: AsyncSession, sending: MessageSending
) -> types.ReplyKeyboardMarkup | types.InlineKeyboardMarkup | None:
    logger.info("Creating keyboard for sending %s", sending.id)
    msb_repo = MessageSendingButtonRepository(session)
    buttons = await msb_repo.get_by_sending_id(sending.id)

    if not buttons:
        logger.info("No buttons found for sending %s", sending.id)
        return None

    match buttons[0].type:
        case MessageSendingButtonType.KEYBOARD:
            kb = __create_reply_kb(sending, buttons)
            markup = kb.as_markup(  # pylint: disable=assignment-from-no-return
                resize_keyboard=True
            )
        case _:
            kb = __create_inline_kb(sending, buttons)
            markup = kb.as_markup()  # pylint: disable=assignment-from-no-return

    return markup


def __create_reply_kb(
    sending: MessageSending, buttons: list[MessageSendingButton]
) -> ReplyKeyboardBuilder:
    kb = ReplyKeyboardBuilder()

    for button in buttons:
        if button.type != MessageSendingButtonType.KEYBOARD:
            raise ValueError(
                "Wrong button type! Keyboard buttons are not compatible with other types. "
                f"Sending {sending.id}, button: {button.id}"
            )
        kb.button(text=button.text)

    return kb


def __create_inline_kb(
    sending: MessageSending, buttons: list[MessageSendingButton]
) -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()

    for button in buttons:
        if button.type == MessageSendingButtonType.KEYBOARD:
            raise ValueError(
                "Wrong button type! Keyboard buttons are not compatible with other types. "
                f"Sending {sending.id}, button: {button.id}"
            )

        args = {}
        match button.type:
            case MessageSendingButtonType.DATA:
                args["callback_data"] = button.data
            case MessageSendingButtonType.URL:
                args["url"] = button.data

        kb.button(text=button.text, **args)
    kb.adjust(1)

    return kb


async def __pre_message_sending(sending: MessageSending):
    logger.info("Starting mass sending")

    await send_message(
        config.PAYMENTS_CHAT_ID,
        MessageInfo(
            text=(
                f"Начинаем рассылку [{sending.id}]"
                f"({config.PUBLIC_URL}/admin/message_sendings/messagesending/1/change/)"
            )
        ),
    )


async def __post_message_sending(
    sending: MessageSending, users_count: int, inactive_count: int
):
    logger.info("Ended mass sending")

    await send_message(
        config.PAYMENTS_CHAT_ID,
        MessageInfo(
            text=(
                f"Рассылка [{sending.id}]"
                f"({config.PUBLIC_URL}/admin/message_sendings/messagesending/1/change/) закончена. "
                f"Пользователей в рассылке {users_count}, из них неактивных {inactive_count}"
            )
        ),
    )
