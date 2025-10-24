import time

from fastapi import APIRouter, status
from pydantic import conlist
from app.schemas.events import EventSchema
from app.services import event_processor


events_router = APIRouter()

@events_router.post(
    "/events",
    status_code=status.HTTP_202_ACCEPTED,
)
async def ingest_events(
    events: conlist(EventSchema, min_length=1),
):
    """Accepts a JSON array of events and triggers their asynchronous processing."""

    start_time = time.perf_counter()

    rows_processed = await event_processor.process_events(events=events)

    elapsed = time.perf_counter() - start_time

    return {
        "message": "Successfully processed events.",
        "attempted_count": rows_processed,
        "response_time_sec": round(elapsed, 4)
    }
