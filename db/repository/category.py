from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import Category


class CategoryRepository(BaseRepository[Category]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Category)

    async def get_by_name(self, name: str) -> Category | None:
        stmt = select(self.model_class).where(self.model_class.name == name).limit(1)

        res = await self.session.execute(stmt)
        return res.scalar_one_or_none()
