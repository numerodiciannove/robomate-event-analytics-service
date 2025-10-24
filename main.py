import asyncio
from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI

from app.api.routers import main_router
from app.core.config import settings
from app.db.db_helper import db_helper as db_lifespan
from app.services.analytics_service import analytics_service


async def hourly_sync_task():
    """Запускає синхронізацію щогодини."""
    while True:
        try:
            print("--- Запуск годинної синхронізації даних з PostgreSQL до DuckDB ---")
            await analytics_service.sync_data_from_postgres()
        except Exception as e:
            print(f"!!! Помилка синхронізації: {e}")

        # Чекаємо 1 годину (3600 секунд)
        await asyncio.sleep(120)

@asynccontextmanager
async def lifespan(app: FastAPI):
    #startapp
    asyncio.create_task(hourly_sync_task())
    yield
    #shutdown
    # print("dispose db engine")
    await db_lifespan.dispose()

main_app = FastAPI(lifespan=lifespan)
main_app.include_router(
    main_router,
    prefix=settings.api.prefix,
    tags=["api"],
    responses={404: {"description": "Not found"}},
)


if __name__ == "__main__":
    uvicorn.run("main:main_app",
                host=settings.run.host,
                port=settings.run.port,
                reload=True
    )