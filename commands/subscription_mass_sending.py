import os
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.user import UserRepository
from db.base import get_session

from commands.send_message import mass_sending_message, send_message
from keyboards import create_reply_start_kb, create_go_to_subscription_kb

import config

from bot22 import bot
from logger import logger
from schemas import MessageInfo
from utils.pics import ImageManager


async def subscription_mass_sending():
    logger.info("Started subscription mass sending")
    image_manager = ImageManager(bot)

    photo_path = os.path.join(image_manager.images_dir, "subscription_mass_sending.jpg")
    if not os.path.exists(photo_path):
        logger.error("File not exists %s", photo_path)
        return

    photo_id = await image_manager.generate_photo_id_for_file(photo_path)
    logger.info("Photo id is %s", photo_id)

    async for session in get_session():
        repo = UserRepository(session)
        active_users = await repo.get_active()
        active_user_ids = [active_user.tg_id for active_user in active_users]
        logger.info("Found %s active users", len(active_user_ids))
        text = """*⚠️ Важное обновление*

Мы запускаем *подписку за 200 руб. в месяц*, чтобы продолжать развивать бота и добавлять новые возможности.

До этого момента *график цен* и *выбор пункта выдачи* были бесплатны, но через *7 дней* эти функции будут доступны только по подписке.

*💡 Что будет с подпиской:*

— Безлимит на добавление товаров
— Доступ к графику изменения цен
— Возможность выбора пункта выдачи 

*🔁 Что будет с бесплатной версией, через неделю:*

— График цен и выбор пункта выдачи станут недоступны
— Все пункты выдачи, автоматически сбросятся на Москву, а цены и скидки будут пересчитаны на основе московского региона

📈 Эта мера необходима *для поддержания проекта и его развития*. В будущем мы планируем добавить ещё *новые полезные функции* для подписчиков.

Спасибо, что вы с нами! ❤️"""
        kb = create_reply_start_kb()
        message1 = MessageInfo(
            text=text, markup=kb.as_markup(resize_keyboard=True), photo_id=photo_id
        )

        text2 = """Чтобы не потерять доступ к расширенным функциям — *оформите подписку заранее👇*"""
        kb2 = create_go_to_subscription_kb()
        message2 = MessageInfo(text=text2, markup=kb2.as_markup())

        logger.info("Sending...")
        results = await mass_sending_message(active_user_ids, [message1, message2])

        logger.info("Finished sending")
        num_set_as_inactive = await set_users_as_inactive(
            active_user_ids, results, session
        )

    await send_message(
        config.PAYMENTS_CHAT_ID,
        MessageInfo(
            text=(
                f"Рассылка закончена. Пользователей найдено: {len(active_user_ids)}. "
                f"Из них {num_set_as_inactive} неактивных"
            )
        ),
    )


async def subscription_is_about_to_end(
    user_ids: list[int], session: AsyncSession, days=5
):
    days_str = "дней"
    if days == 1:
        days_str = "день"
    elif 2 <= days <= 4:
        days_str = "дня"

    text = f"""*⏳ Подписка заканчивается через {days} {days_str}*

Чтобы не потерять доступ к расширенным функциям — *продлите подписку заранее👇*"""
    kb = create_go_to_subscription_kb()

    results = await mass_sending_message(
        user_ids, [MessageInfo(text=text, markup=kb.as_markup())]
    )
    await set_users_as_inactive(user_ids, results, session)


async def set_users_as_inactive(
    user_ids: list[int], activity_labels: list[bool], session: AsyncSession
) -> int:
    logger.info("Started set users as inactive function")

    inactive_users = []
    for i, user_id in enumerate(user_ids):
        if not activity_labels[i]:
            inactive_users.append(user_id)

    if not inactive_users:
        logger.info("No inactive users")
        return 0

    logger.info("Found %s inactive users out of %s", len(inactive_users), len(user_ids))
    async with session:
        repo = UserRepository(session)
        logger.info("Updating...")
        await repo.set_as_inactive(inactive_users)

    return len(inactive_users)


async def notify_users_that_subscription_ended(
    user_ids: list[int], subscription_price: int, session: AsyncSession
):
    text = f"""*⚠️ Ваши цены пересчитаны по Москве ⚠️*

Подписка не была оформлена, поэтому:

— Пункт выдачи сброшен на *Москву*
— Все цены и скидки теперь рассчитываются по московскому региону

*🔓 Хотите вернуть свой город и другие функции?*

Оформите подписку — это откроет:

• Безлимитное добавление товаров
• Выбор пункта выдачи
• График цен

📦 Подписка стоит всего *{subscription_price} ₽ в месяц*"""
    kb = create_go_to_subscription_kb()
    results = await mass_sending_message(
        user_ids, [MessageInfo(text=text, markup=kb.as_markup())]
    )
    inactive_num = await set_users_as_inactive(user_ids, results, session)

    await send_message(
        config.PAYMENTS_CHAT_ID,
        MessageInfo(
            text=(
                f"У {len(user_ids)} пользователей закончилась подписка. "
                f"Из них {inactive_num} неактивных."
            )
        ),
    )
