import time

from fastapi import APIRouter, status, Depends
from pydantic import conlist
from app.schemas.events import EventSchema
from app.services import event_processor
from fastapi_limiter.depends import RateLimiter

from app.services.jwt_service import get_current_user
from app.db.models.users import User as DBUser

events_router = APIRouter()

@events_router.post(
    "/events",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(RateLimiter(times=5, seconds=60))]
)
async def ingest_events(
    events: conlist(EventSchema, min_length=1),
    current_user: DBUser = Depends(get_current_user)
):
    """Accepts a JSON array of events and triggers their asynchronous processing."""

    start_time = time.perf_counter()
    rows_processed = await event_processor.process_events(events=events)
    elapsed = time.perf_counter() - start_time

    return {
        "message": "Successfully processed events.",
        "obj_count": rows_processed,
        "response_time_sec": round(elapsed, 4),
        "user_id": current_user.id
    }