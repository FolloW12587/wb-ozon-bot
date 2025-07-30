from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import MessageSending, MessageSendingStatus


class MessageSendingRepository(BaseRepository[MessageSending]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, MessageSending)

    async def get_by_status(self, status: MessageSendingStatus) -> list[MessageSending]:
        result = await self.session.execute(
            select(self.model_class).where(self.model_class.status == status)
        )

        return result.scalars().all()
