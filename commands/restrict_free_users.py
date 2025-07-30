# Команда нужна для разового выполнения, чтобы ограничить бесплатные функции у пользователей
from db.base import get_session

from db.repository.user import UserRepository
from db.repository.subscription import SubscriptionRepository

from background.subscriptions import drop_users_subscription
from commands.subscription_mass_sending import notify_users_that_subscription_ended
from utils.scheduler import scheduler

from logger import logger


async def restrict_free_users():
    logger.info("Started restriction of free users")
    scheduler.start()
    async for session in get_session():
        repo = UserRepository(session)
        subscription_repo = SubscriptionRepository(session)

        paid_subscriptions = await subscription_repo.get_paid_subscriptions()
        if not paid_subscriptions:
            logger.error("No paid subscriptions in database. Aborting...")
            return

        paid_subscription_price = paid_subscriptions[0].price_rub
        free_subscription = await subscription_repo.get_subscription_by_name("Free")
        if not free_subscription:
            logger.error("No free subscriptinos in database. Aborting...")
            return

        users = await repo.get_users_using_subscription(free_subscription.id)
        if not users:
            logger.info("No users with free subscription")
            return

        for user in users:
            await drop_users_subscription(user, free_subscription, session, scheduler)

        user_ids = [user.tg_id for user in users if user.is_active]
        logger.info(
            "Dropped subscription to %s users. %s of them are marked as active and being notified",
            len(users),
            len(user_ids),
        )
        await notify_users_that_subscription_ended(
            user_ids, paid_subscription_price, session
        )
