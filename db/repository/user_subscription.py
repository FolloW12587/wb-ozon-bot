from datetime import date, datetime, timezone, timedelta
from uuid import UUID
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import UserSubscription


# TODO: work with timezones more carefully
class UserSubscriptionRepository(BaseRepository[UserSubscription]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, UserSubscription)

    async def get_active_subscription(self, user_id: int) -> UserSubscription | None:
        now = datetime.now(tz=timezone.utc).date()
        result = await self.session.execute(
            select(self.model_class).where(
                self.model_class.active_from <= now,
                self.model_class.active_to >= now,
                self.model_class.user_id == user_id,
            )
        )
        return result.scalar_one_or_none()

    async def get_latest_subscription(self, user_id: int) -> UserSubscription | None:
        result = await self.session.execute(
            select(self.model_class)
            .where(
                self.model_class.user_id == user_id,
            )
            .order_by(self.model_class.active_to.desc().nulls_last())
        )
        return result.scalars().first()

    async def new_subscription(
        self,
        user_id: int,
        order_id: UUID,
        subscription_id: int,
        active_from: date,
        active_to: date,
    ) -> UserSubscription:
        new = UserSubscription(
            user_id=user_id,
            order_id=order_id,
            subscription_id=subscription_id,
            active_from=active_from,
            active_to=active_to,
        )
        return await self.create(new)

    async def subscription_by_order(self, order_id: UUID) -> UserSubscription | None:
        result = await self.session.execute(
            select(self.model_class).where(
                self.model_class.order_id == order_id,
            )
        )
        return result.scalar_one_or_none()

    async def get_start_date_for_new_subscription(self, user_id: int) -> date:
        result = await self.session.execute(
            select(self.model_class.active_to)
            .where(self.model_class.user_id == user_id)
            .order_by(self.model_class.active_to.desc().nulls_last())
        )

        active_to = result.scalars().first()
        if not active_to:
            return datetime.now(tz=timezone.utc).date()

        return active_to + timedelta(days=1)
