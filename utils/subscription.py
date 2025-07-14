from sqlalchemy.ext.asyncio import AsyncSession

from db.base import Subscription
from db.repository.subscription import SubscriptionRepository
from db.repository.user import UserRepository
from db.repository.user_product import UserProductRepository
from utils.exc import Forbidden


async def get_user_subscription_option(
    session: AsyncSession, user_id: int
) -> Subscription:
    # us_repo = UserSubscriptionRepository(session)
    user_repo = UserRepository(session)
    user = await user_repo.find_by_id(user_id)
    if not user:
        raise Forbidden

    subscripion_repo = SubscriptionRepository(session)
    subscription = await subscripion_repo.find_by_id(user.subscription_id)
    if not subscription:
        raise Forbidden

    return subscription


async def get_user_subscription_limit(
    user_id: int, session: AsyncSession
) -> tuple[tuple[int, int], tuple[int, int]]:
    """Returns user subscriptions limit for given `marker`.\n
    Returns tuple[limit[ozon, wb], used[ozon, wb]]"""
    subscription = await get_user_subscription_option(session, user_id)

    products = {}
    repo = UserProductRepository(session)
    for marker in ["wb", "ozon"]:
        products[marker] = await repo.get_marker_products(user_id, marker)
    return (subscription.ozon_product_limit, subscription.wb_product_limit), (
        len(products["ozon"], products["wb"])
    )
