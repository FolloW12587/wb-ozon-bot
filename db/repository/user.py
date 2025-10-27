from datetime import date, timedelta
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import User, UserSubscription


class UserRepository(BaseRepository[User]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, User)

    async def find_by_id(self, model_id: int) -> User | None:
        db_model = await self.session.execute(
            select(self.model_class).filter_by(tg_id=model_id)
        )

        return db_model.scalars().first()

    async def find_by_ids(self, model_ids: list[int]) -> list[User]:
        db_models = await self.session.execute(
            select(self.model_class).where(self.model_class.tg_id.in_(model_ids))
        )

        return db_models.scalars().all()

    async def update_old(self, model_id: int, **kwargs):
        await self.session.execute(
            update(self.model_class)
            .where(self.model_class.tg_id == model_id)
            .values(**kwargs)
        )

        await self.session.commit()

    async def delete_by_id(self, model_id: int):
        await self.session.execute(
            delete(self.model_class).where(self.model_class.tg_id == model_id)
        )
        await self.session.commit()

    async def increase_product_count_for_user(self, user_id: int, marker: str):
        stmt = update(self.model_class).where(self.model_class.tg_id == user_id)

        if marker == "ozon":
            stmt = stmt.values(ozon_total_count=self.model_class.ozon_total_count + 1)
        else:
            stmt = stmt.values(wb_total_count=self.model_class.wb_total_count + 1)

        await self.session.execute(stmt)
        await self.session.commit()

    async def get_active(self) -> list[User]:
        db_models = await self.session.execute(
            select(self.model_class).where(self.model_class.is_active.is_(True))
        )

        return db_models.scalars().all()

    async def set_as_inactive(self, user_ids: list[int]):
        stmt = (
            update(self.model_class)
            .values(is_active=False)
            .where(self.model_class.tg_id.in_(user_ids))
        )
        await self.session.execute(stmt)
        await self.session.commit()

    async def get_users_which_subscription_ends(self, n: int) -> list[User]:
        """Получаем пользователей, чья подписка заканчивается через `n` дней"""
        today = date.today()
        # Если подписка заканчивается через n дней, то она длится до n-1 дней
        target_date = today + timedelta(days=n - 1)

        # Подзапрос: максимальная дата окончания среди будущих или текущих подписок
        subq = (
            select(
                UserSubscription.user_id,
                func.max(UserSubscription.active_to).label("latest_active_to"),
            )
            .where(UserSubscription.active_to >= today)
            .group_by(UserSubscription.user_id)
            .subquery()
        )

        # Основной запрос: активные пользователи, у которых подписка заканчивается через `n` дней
        stmt = (
            select(self.model_class)
            .join(subq, self.model_class.tg_id == subq.c.user_id)
            .where(
                self.model_class.is_active.is_(True),
                subq.c.latest_active_to == target_date,
            )
        )

        results = await self.session.execute(stmt)
        return results.scalars().all()

    async def get_users_with_ended_subscription(
        self, paid_subscription_ids: list[int]
    ) -> list[User]:
        today = date.today()

        subq = (
            select(
                UserSubscription.user_id,
                func.max(UserSubscription.active_to).label("latest_active_to"),
            )
            .group_by(UserSubscription.user_id)
            .subquery()
        )

        stmt = (
            select(User)
            .join(subq, User.tg_id == subq.c.user_id)
            .where(
                User.subscription_id.in_(paid_subscription_ids),
                subq.c.latest_active_to < today,
            )
        )

        results = await self.session.execute(stmt)
        return results.scalars().all()

    async def get_users_using_subscription(self, subscription_id: int) -> list[User]:
        stmt = select(self.model_class).where(
            self.model_class.subscription_id == subscription_id
        )

        results = await self.session.execute(stmt)
        return results.scalars().all()
