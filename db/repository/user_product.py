from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from db.repository.base import BaseRepository
from db.base import Product, UserProduct


class UserProductRepository(BaseRepository[UserProduct]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, UserProduct)

    async def get_user_products(self, user_id: int) -> list[UserProduct]:
        stmt = select(self.model_class).where(self.model_class.user_id == user_id)
        result = await self.session.execute(stmt)

        return result.scalars().all()

    async def get_user_product(self, user_id: int, link: str) -> UserProduct | None:
        stmt = select(self.model_class).where(
            self.model_class.user_id == user_id, self.model_class.link == link
        )
        result = await self.session.execute(stmt)

        return result.scalars().first()

    async def get_marker_products(self, user_id: int, marker: str) -> list[UserProduct]:
        stmt = (
            select(UserProduct)
            .join(UserProduct.product)  # JOIN с таблицей Product
            .options(selectinload(UserProduct.product))  # для подгрузки product
            .where(UserProduct.user_id == user_id, Product.product_marker == marker)
        )

        result = await self.session.execute(stmt)
        return result.scalars().all()
