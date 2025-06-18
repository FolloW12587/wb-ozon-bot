from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import PopularProductSaleRange


class PopularProductSaleRangeRepository(BaseRepository[PopularProductSaleRange]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, PopularProductSaleRange)

    async def get_sale_coefficient(self, price: int) -> float:
        result = await self.session.execute(
            select(self.model_class.coefficient).where(
                self.model_class.start_price <= price,
                self.model_class.end_price > price,
            )
        )

        coef = result.scalar_one_or_none()
        return coef or 1
