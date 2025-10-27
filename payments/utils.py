from datetime import date, datetime, timedelta, timezone


from db.base import User, UserSubscription
from db.repository.user import UserRepository
from db.repository.user_subscription import UserSubscriptionRepository
from logger import logger


async def set_subscription_to_user_if_needed(
    user_repo: UserRepository, user: User, user_subscription: UserSubscription
):
    now = datetime.now(timezone.utc).date()
    if user_subscription.active_from <= now <= user_subscription.active_to:
        logger.info("User subscription is set to %s", user_subscription.subscription_id)
        await user_repo.update_old(
            user.tg_id, subscription_id=user_subscription.subscription_id
        )


async def give_user_subscription(
    us_repo: UserSubscriptionRepository,
    user_repo: UserRepository,
    user: User,
    subscription_id: int,
    active_from: date,
    active_to: date,
    order_id: int | None = None,
):
    user_subscription = await us_repo.new_subscription(
        user_id=user.tg_id,
        order_id=order_id,
        subscription_id=subscription_id,
        active_from=active_from,
        active_to=active_to,
    )
    logger.info("Created new user subscription %s", user_subscription.id)

    await set_subscription_to_user_if_needed(user_repo, user, user_subscription)
    return user_subscription


async def give_users_free_referal_trial(
    us_repo: UserSubscriptionRepository,
    user_repo: UserRepository,
    invited_user: User,
    inviter: User,
    subscription_id: int,
):
    # give users two weeks of subscription
    active_from = await us_repo.get_start_date_for_new_subscription(invited_user.tg_id)
    active_to = active_from + timedelta(days=13)
    await give_user_subscription(
        us_repo=us_repo,
        user_repo=user_repo,
        user=invited_user,
        subscription_id=subscription_id,
        active_from=active_from,
        active_to=active_to,
    )

    active_from = await us_repo.get_start_date_for_new_subscription(inviter.tg_id)
    active_to = active_from + timedelta(days=13)
    await give_user_subscription(
        us_repo=us_repo,
        user_repo=user_repo,
        user=inviter,
        subscription_id=subscription_id,
        active_from=active_from,
        active_to=active_to,
    )
