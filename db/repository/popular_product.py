from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import PopularProduct


class PopularProductRepository(BaseRepository[PopularProduct]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, PopularProduct)
