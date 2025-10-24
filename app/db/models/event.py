from sqlalchemy import Column, String, DateTime, Index, JSON, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
import uuid

BaseORM = declarative_base()


class Event(BaseORM):
    __tablename__ = "events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_id = Column(UUID(as_uuid=True), unique=True, nullable=False)
    occurred_at = Column(DateTime(timezone=True), nullable=False)
    user_id = Column(Integer, nullable=False)
    event_type = Column(String, nullable=False)
    properties_json = Column(JSON, nullable=False)

    __table_args__ = (
        Index("idx_user_time_type", "user_id", "occurred_at", "event_type"),
    )
