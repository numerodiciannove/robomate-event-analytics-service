from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, Any, Dict
from uuid import UUID


class EventSchema(BaseModel):
    event_id: UUID = Field(..., description="Unique identifier of the event (UUID).")
    occurred_at: datetime = Field(..., description="Timestamp when the event occurred (ISO-8601).")
    user_id: int = Field(..., description="User identifier.")
    event_type: str = Field(..., description="Type of the event (string).")
    properties: Optional[Dict[str, Any]] = Field(None, description="Additional event properties (JSON object).")

    class Config:
        from_attributes = True
