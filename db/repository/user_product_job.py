from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import UserProductJob


class UserProductJobRepository(BaseRepository[UserProductJob]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, UserProductJob)

    async def delete_by_job_id(self, job_id: str):
        await self.session.execute(
            delete(self.model_class).where(self.model_class.job_id == job_id)
        )
        await self.session.commit()

    async def delete_by_product_id(self, product_id: int):
        await self.session.execute(
            delete(self.model_class).where(
                self.model_class.user_product_id == product_id
            )
        )
        await self.session.commit()
