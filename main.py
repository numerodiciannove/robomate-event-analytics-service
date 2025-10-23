from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI

from app.api.routers import main_router
from app.core.config import settings
from app.db.db_helper import db_helper as db_lifespan


@asynccontextmanager
async def lifespan(app: FastAPI):
    #startapp
    yield
    #shutdown
    # print("dispose db engine")
    # await db_lifespan.dispose()

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