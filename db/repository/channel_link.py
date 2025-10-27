from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import ChannelLink


class ChannelLinkRepository(BaseRepository[ChannelLink]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, ChannelLink)

    async def get_common_private_channel_link(self) -> ChannelLink | None:
        stmt = select(self.model_class).where(self.model_class.name == "Общий").limit(1)

        res = await self.session.execute(stmt)
        return res.scalar_one_or_none()

    async def get_common_public_channel_link(self) -> ChannelLink | None:
        stmt = (
            select(self.model_class)
            .where(self.model_class.name == "Общий публичный")
            .limit(1)
        )

        res = await self.session.execute(stmt)
        return res.scalar_one_or_none()
