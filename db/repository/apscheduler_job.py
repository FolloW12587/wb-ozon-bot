from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


class ApschedulerJobRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_existing_job_ids(self) -> list[str]:
        stmt = r"SELECT id FROM apscheduler_jobs where id like '%popular%';"

        result = await self.session.execute(text(stmt))
        return [r[0] for r in result]
