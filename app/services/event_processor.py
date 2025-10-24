import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from app.db.db_helper import db_helper
from app.db.models.event import Event
from app.schemas.events import EventSchema

logger = logging.getLogger("uvicorn.app")


@db_helper.connection
async def process_events(
    events: list[EventSchema],
    *,
    session: AsyncSession
) -> int:
    data_to_insert = [event.model_dump() for event in events]
    if not data_to_insert:
        return 0

    insert_stmt = (
        insert(Event)
        .values(data_to_insert)
        .on_conflict_do_nothing(index_elements=['event_id'])
        .returning(Event.event_id)
    )

    result = await session.execute(insert_stmt)
    await session.commit()

    rows_inserted = len(result.fetchall())
    logger.info(f"Inserted {rows_inserted} new events. Duplicates ignored.")

    return rows_inserted
