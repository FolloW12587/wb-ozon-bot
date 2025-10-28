from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import ProductCityGraphic


class ProductCityGraphicRepository(BaseRepository[ProductCityGraphic]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, ProductCityGraphic)

    async def get_by_product_id_and_city(
        self, product_id: int, city: str
    ) -> ProductCityGraphic | None:
        stmt = (
            select(self.model_class)
            .where(
                self.model_class.product_id == product_id, self.model_class.city == city
            )
            .limit(1)
        )

        res = await self.session.execute(stmt)
        return res.scalars().first()
