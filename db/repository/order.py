from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.base import BaseRepository
from db.base import Order, OrderStatus, Subscription, User


class OrderRepository(BaseRepository[Order]):
    def __init__(self, session: AsyncSession):
        super().__init__(session, Order)

    async def generate_order(self, subscription: Subscription, user: User) -> Order:
        order = Order(
            user_id=user.tg_id,
            subscription_id=subscription.id,
            status=OrderStatus.PENDING.value,
            price=subscription.price_rub,
        )
        self.session.add(order)
        await self.session.commit()
        await self.session.refresh(order)

        return order
