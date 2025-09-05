from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import Punkt


class PunktRepository(BaseRepository[Punkt]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Punkt)

    async def delete_users_punkt(self, user_id: int):
        await self.session.execute(
            delete(self.model_class).where(self.model_class.user_id == user_id)
        )
        await self.session.commit()

    async def get_users_punkt(self, user_id: int) -> Punkt | None:
        result = await self.session.execute(
            select(self.model_class).where(self.model_class.user_id == user_id)
        )
        return result.scalars().first()
