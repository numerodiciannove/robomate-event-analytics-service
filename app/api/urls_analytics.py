from fastapi import APIRouter, status
from pydantic import conlist
from app.schemas.events import EventSchema
from app.services import event_processor

analytics_router = APIRouter()

@analytics_router.post(
    "/events",
    status_code=status.HTTP_202_ACCEPTED,
    # Rate Limiting
    # dependencies=[Depends(rate_limit_dependency)]
)
async def ingest_events(
    events: conlist(EventSchema, min_length=1),
):

    """Accepts a JSON array of events and triggers their asynchronous processing."""

    rows_processed = await event_processor.process_events(events=events)

    return {"message": f"Successfully processed events.", "attempted_count": rows_processed}