from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from typing import Optional, Any, Dict
from uuid import UUID


class EventSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    event_id: UUID = Field(..., description="Unique identifier of the event (UUID).")
    occurred_at: datetime = Field(..., description="Timestamp when the event occurred (ISO-8601).")
    user_id: int = Field(..., description="User identifier.")
    event_type: str = Field(..., description="Type of the event (string).")
    properties_json: Optional[Dict[str, Any]] = Field(None, description="Additional event properties (JSON object).")
