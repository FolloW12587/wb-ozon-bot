from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import UTM


class UTMRepository(BaseRepository[UTM]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, UTM)

    async def get_by_keitaro_id(self, keitaro_id: str) -> list[UTM]:
        db_models = await self.session.execute(
            select(self.model_class).where(UTM.keitaro_id == keitaro_id)
        )

        return db_models.scalars().all()
