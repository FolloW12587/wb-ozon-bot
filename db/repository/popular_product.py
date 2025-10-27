from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import PopularProduct


class PopularProductRepository(BaseRepository[PopularProduct]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, PopularProduct)

    async def get_by_product_id(self, product_id: int) -> PopularProduct | None:
        stmt = (
            select(self.model_class)
            .where(self.model_class.product_id == product_id)
            .limit(1)
        )

        res = await self.session.execute(stmt)
        return res.scalar_one_or_none()

    async def get_ids_that_not_in_list(self, list_of_ids: list[int]) -> list[int]:
        if not list_of_ids:
            stmt = select(self.model_class.id)
        else:
            stmt = select(self.model_class.id).where(
                ~self.model_class.id.in_(list_of_ids)
            )

        res = await self.session.execute(stmt)
        return list(res.scalars().all())
