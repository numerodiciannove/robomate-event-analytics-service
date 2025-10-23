from fastapi import APIRouter

from app.api.urls_analytics import analytics_router
from app.api.urls_events import events_router

main_router = APIRouter()

# Register API routers ---------------------------------------
main_router.include_router(analytics_router)
main_router.include_router(events_router)
