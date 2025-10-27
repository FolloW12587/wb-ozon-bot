from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.ext.asyncio import AsyncSession


from commands.subscription_mass_sending import (
    notify_users_that_subscription_ended,
    subscription_is_about_to_end,
)

from db.base import get_session, Subscription, User
from db.repository.punkt import PunktRepository
from db.repository.subscription import SubscriptionRepository
from db.repository.user import UserRepository
from db.repository.user_product import UserProductRepository
from db.repository.user_product_job import UserProductJobRepository

from logger import logger


async def notify_users_about_subscription_ending(ctx):
    logger.info("Started notify users about subscription ending")
    async for session in get_session():
        repo = UserRepository(session)

        for days in [5, 1]:
            logger.info("Searching for users which subscription ends in %s days", days)
            users_to_notify = await repo.get_users_which_subscription_ends(days)
            if not users_to_notify:
                logger.info("No one to notify")
                continue

            logger.info("Found %s users to notify", len(users_to_notify))

            user_ids = [user.tg_id for user in users_to_notify]
            await subscription_is_about_to_end(user_ids, session, days)


async def search_users_for_ended_subscription(ctx):
    scheduler = ctx.get("scheduler")
    logger.info("Started searching users for ended subscription")
    async for session in get_session():
        repo = UserRepository(session)
        subscription_repo = SubscriptionRepository(session)

        paid_subscriptions = await subscription_repo.get_paid_subscriptions()
        free_subscription = await subscription_repo.get_subscription_by_name("Free")
        if not paid_subscriptions or not free_subscription:
            logger.error("No paid or free subscriptinos in database. Aborting...")
            return

        paid_subscription_ids = [
            paid_subscription.id for paid_subscription in paid_subscriptions
        ]
        users = await repo.get_users_with_ended_subscription(paid_subscription_ids)
        if not users:
            logger.info("No users with ended subscription")
            return

        for user in users:
            await drop_users_subscription(user, free_subscription, session, scheduler)

        user_ids = [user.tg_id for user in users]
        await notify_users_that_subscription_ended(
            user_ids, paid_subscriptions[0].price_rub, session
        )


async def drop_users_subscription(
    user: User,
    free_subscription: Subscription,
    session: AsyncSession,
    scheduler: AsyncIOScheduler,
):
    logger.info("Dropping subscription for user %s [%s]", user.username, user.tg_id)
    async with session:
        repo = UserRepository(session)
        up_repo = UserProductRepository(session)
        upj_repo = UserProductJobRepository(session)

        user.subscription_id = free_subscription.id
        await repo.update_old(user.tg_id, subscription_id=free_subscription.id)

        for marker in ["ozon", "wb"]:
            products = await up_repo.get_marker_products(user.tg_id, marker)
            marker_limit = getattr(free_subscription, f"{marker}_product_limit", 0)
            if len(products) <= marker_limit:
                continue

            products.sort(key=lambda product: product.time_create, reverse=True)
            logger.info(
                "Deleting %s user products for marker %s",
                len(products[marker_limit:]),
                marker,
            )
            for product in products[marker_limit:]:
                job_id = f"{user.tg_id}:{marker}:{product.id}"
                await upj_repo.delete_by_product_id(product.id)
                await up_repo.delete(product)

                scheduler.remove_job(job_id=job_id, jobstore="sqlalchemy")

    await drop_users_punkt(user, session)


async def drop_users_punkt(user: User, session: AsyncSession):
    logger.info("Dropping punkt for user %s [%s]", user.username, user.tg_id)
    async with session:
        repo = PunktRepository(session)
        await repo.delete_users_punkt(user.tg_id)
