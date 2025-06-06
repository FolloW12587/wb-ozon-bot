from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import Product


class ProductRepository(BaseRepository[Product]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Product)

    async def find_by_short_link(self, short_link) -> Product | None:
        result = await self.session.execute(
            select(self.model_class).where(Product.short_link == short_link)
        )

        return result.scalar_one_or_none()
