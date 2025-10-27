from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import ProductPrice


class ProductPriceRepository(BaseRepository[ProductPrice]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, ProductPrice)

    async def get_by_product_and_city(
        self, product_id: int, city: str
    ) -> list[ProductPrice]:
        stmt = (
            select(
                self.model_class.time_price,
            )
            .where(
                self.model_class.product_id == product_id, self.model_class.city == city
            )
            .order_by(desc(self.model_class.time_price))
        )

        res = await self.session.execute(stmt)
        return res.scalars().all()

    async def get_last_for_product_and_city(
        self, product_id: int, city: str
    ) -> ProductPrice | None:
        stmt = (
            select(
                self.model_class.time_price,
            )
            .where(
                self.model_class.product_id == product_id, self.model_class.city == city
            )
            .order_by(desc(self.model_class.time_price))
            .limit(1)
        )

        res = await self.session.execute(stmt)
        return res.scalars().first()
