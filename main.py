import asyncio
from contextlib import asynccontextmanager

import redis.asyncio as redis
import uvicorn
from fastapi import FastAPI
from fastapi_limiter import FastAPILimiter
from loguru import logger

from app.api.routers import main_router
from app.core.config import settings
from app.db.db_helper import db_helper as db_lifespan
from app.utils.tasks import hourly_sync_task



@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    redis_client = redis.from_url(
        "redis://redis:6379",
        encoding="utf-8",
        decode_responses=True
    )
    await FastAPILimiter.init(redis_client)

    asyncio.create_task(hourly_sync_task())

    yield

    # shutdown
    logger.info("dispose db engine")
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