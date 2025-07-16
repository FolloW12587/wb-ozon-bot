from db.base import get_session, UTM
from db.repository.utm import UTMRepository

from schemas import UTMSchema


async def add_utm_to_db(data: UTMSchema):
    utm = UTM(**data.model_dump())
    async for session in get_session():
        repo = UTMRepository(session)
        try:
            repo.create(utm)
            print("UTM ADDED SUCCESSFULLY")
        except Exception as ex:
            await session.rollback()
            print("ADD UTM ERROR", ex)
