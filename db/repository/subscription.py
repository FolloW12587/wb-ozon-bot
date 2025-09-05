from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import Subscription


class SubscriptionRepository(BaseRepository[Subscription]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Subscription)

    async def get_subscription_by_name(self, name: str) -> Subscription | None:
        result = await self.session.execute(
            select(self.model_class).where(self.model_class.name == name)
        )

        return result.scalars().first()

    async def get_paid_subscriptions(self) -> list[Subscription]:
        result = await self.session.execute(
            select(self.model_class).where(self.model_class.price_rub > 0)
        )

        return result.scalars().all()
