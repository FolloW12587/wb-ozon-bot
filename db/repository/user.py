from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import User


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

    async def update(self, model_id: int, **kwargs):
        await self.session.execute(
            update(self.model_class)
            .where(self.model_class.tg_id == model_id)
            .values(**kwargs)
        )

        await self.session.commit()

    async def get_active(self) -> list[User]:
        db_models = await self.session.execute(
            select(self.model_class).where(self.model_class.is_active.is_(True))
        )

        return db_models.scalars().all()
