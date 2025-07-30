from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import MessageSendingButton


class MessageSendingButtonRepository(BaseRepository[MessageSendingButton]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, MessageSendingButton)

    async def get_by_sending_id(
        self, message_sending_id: int
    ) -> list[MessageSendingButton]:
        result = await self.session.execute(
            select(self.model_class).where(
                self.model_class.message_sending_id == message_sending_id
            )
        )

        return result.scalars().all()
