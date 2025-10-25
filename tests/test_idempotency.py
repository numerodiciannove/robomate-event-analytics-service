import pytest
from unittest.mock import AsyncMock, patch
from datetime import datetime, timedelta
import uuid
from typing import List

from app.services.event_processor import process_events
from app.schemas.events import EventSchema


@pytest.fixture(scope="session", autouse=True)
def mock_settings():
    with patch('app.core.config.settings') as mock_conf:
        mock_conf.db.url = "postgresql+asyncpg://user:pass@host:5432/db_test"
        yield mock_conf


@pytest.fixture
def sample_events() -> List[EventSchema]:
    now = datetime.now()
    duplicate_id = str(uuid.uuid4())

    return [
        EventSchema(
            event_id=str(uuid.uuid4()),
            user_id=1,
            occurred_at=now,
            event_type="app_opened",
            properties_json={"platform": "iOS"}
        ),
        EventSchema(
            event_id=duplicate_id,
            user_id=2,
            occurred_at=now + timedelta(seconds=1),
            event_type="item_view",
            properties_json={"item_id": 100}
        ),
        EventSchema(
            event_id=duplicate_id,
            user_id=3,
            occurred_at=now + timedelta(seconds=2),
            event_type="item_view",
            properties_json={"item_id": 101}
        ),
        EventSchema(
            event_id=str(uuid.uuid4()),
            user_id=4,
            occurred_at=now + timedelta(seconds=3),
            event_type="purchase",
            properties_json={"amount": 99}
        )
    ]


@pytest.mark.asyncio
async def test_event_idempotency_counting(sample_events, mocker):
    mock_conn = AsyncMock()
    mock_connect = mocker.patch('asyncpg.connect', return_value=mock_conn)

    events_list = sample_events
    result_count = await process_events(events_list)

    mock_connect.assert_called_once()
    mock_conn.close.assert_called_once()

    query_insert_call = mock_conn.executemany.call_args[0][0]
    expected_clause = "ON CONFLICT (event_id) DO NOTHING"
    assert expected_clause in query_insert_call, "Запит INSERT повинен містити ON CONFLICT (event_id) DO NOTHING"

    assert result_count == len(events_list), "Функція повинна повертати загальну кількість подій, надісланих для обробки"
