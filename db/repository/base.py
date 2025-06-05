from typing import Generic, Type, TypeVar

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession


M = TypeVar("M")  # SQLAlchemy model type


class BaseRepository(Generic[M]):
    def __init__(self, session: AsyncSession, model_class: Type[M]):
        self.session = session
        self.model_class = model_class

    async def create(self, db_model: M) -> M:
        self.session.add(db_model)
        await self.session.commit()
        await self.session.refresh(db_model)

        return db_model

    async def find_by_id(self, model_id: int) -> M | None:
        db_model = await self.session.execute(
            select(self.model_class).filter_by(id=model_id)
        )

        return db_model.scalars().first()

    async def find_by_ids(self, model_ids: list[int]) -> list[M]:
        db_models = await self.session.execute(
            select(self.model_class).where(self.model_class.id.in_(model_ids))
        )

        return db_models.scalars().all()

    async def update(self, model_id: M, **kwargs):
        await self.session.execute(
            update(self.model_class)
            .where(self.model_class.id == model_id)
            .values(**kwargs)
        )

        await self.session.commit()

    async def delete(self, db_model: M):
        await self.session.delete(db_model)
        await self.session.commit()

    async def list_all(self) -> list[M]:
        db_models = await self.session.execute(select(self.model_class))
        return db_models.scalars().all()

    async def first(self) -> M | None:
        result = await self.session.execute(select(self.model_class))
        return result.scalar_one_or_none()
