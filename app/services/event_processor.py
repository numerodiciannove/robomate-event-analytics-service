import uuid
from typing import List
import asyncpg
import json

from loguru import logger

from app.core.config import settings

from app.schemas.events import EventSchema


async def process_events(events: List[EventSchema]) -> int:
    """
    High-performance data ingestion using asyncpg.executemany.
    Explicit JSON serialization is applied for compatibility with asyncpg.
    """

    try:
        dsn = str(settings.db.url).replace("postgresql+asyncpg://", "postgresql://")
        conn = await asyncpg.connect(dsn=dsn)
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return 0

    try:
        data_to_copy = []
        for event in events:
            new_id = uuid.uuid4()
            properties_json_str = json.dumps(event.properties_json)
            data_to_copy.append((
                new_id,
                event.event_id,
                event.user_id,
                event.occurred_at,
                event.event_type,
                properties_json_str,
            ))

        query_insert = """
            INSERT INTO events (id, event_id, user_id, occurred_at, event_type, properties_json)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (event_id) DO NOTHING
        """

        await conn.executemany(query_insert, data_to_copy)

        logger.info(f"Attempted to process {len(data_to_copy)} events via asyncpg.executemany.")

        return len(data_to_copy)

    except Exception as e:
        logger.error(f"Error executing batch asyncpg query: {e}")
        raise

    finally:
        await conn.close()
