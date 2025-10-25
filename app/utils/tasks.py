import asyncio

from loguru import logger

from app.services.analytics_service import analytics_service


async def hourly_sync_task():
    while True:
        try:
            logger.info("--- Starting hourly data synchronization from PostgreSQL to DuckDB ---")
            await analytics_service.sync_data_from_postgres()
        except Exception as e:
            logger.error(f"!!! Synchronization error: {e}")

        await asyncio.sleep(3600)
