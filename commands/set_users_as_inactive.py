from sqlalchemy.ext.asyncio import AsyncSession

from db.repository.user import UserRepository

from logger import logger


async def set_users_as_inactive(
    user_ids: list[int], activity_labels: list[bool], session: AsyncSession
) -> int:
    logger.info("Started set users as inactive function")

    inactive_users = []
    for i, user_id in enumerate(user_ids):
        if not activity_labels[i]:
            inactive_users.append(user_id)

    if not inactive_users:
        logger.info("No inactive users")
        return 0

    logger.info("Found %s inactive users out of %s", len(inactive_users), len(user_ids))
    async with session:
        repo = UserRepository(session)
        logger.info("Updating...")
        await repo.set_as_inactive(inactive_users)

    return len(inactive_users)
