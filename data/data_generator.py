import json
import uuid
import random
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

EVENT_TYPES = ["purchase", "app_open", "view_item", "add_to_cart", "search"]
COUNTRIES = ["US", "DE", "PL", "IT", "FR", "UA", "GB"]
CURRENCIES = ["USD", "EUR", "UAH"]
OS = ["Android", "iOS", "Web"]
APP_VERSIONS = ["1.8.9", "2.0.1", "1.9.0", "3.1.0"]
ITEM_IDS = [f"SKU{i:04d}" for i in range(100, 150)]
MAX_USER_ID = 10000
DATE_RANGE_DAYS = 90
TIMEZONE_OFFSET = timezone(timedelta(hours=3))


def random_date(days_back: int = DATE_RANGE_DAYS) -> str:
    end = datetime.now(TIMEZONE_OFFSET)
    start = end - timedelta(days=days_back)
    delta_seconds = int((end - start).total_seconds())
    rand_dt = start + timedelta(seconds=random.randint(0, delta_seconds))
    return rand_dt.isoformat()


def generate_properties(event_type: str) -> Dict[str, Any]:
    props = {
        "country": random.choice(COUNTRIES),
        "session_id": uuid.uuid4().hex[:8],
    }

    if event_type in ["purchase", "view_item"]:
        props.update({
            "item_id": random.choice(ITEM_IDS),
            "price": round(random.uniform(5.0, 500.0), 2),
            "currency": random.choice(CURRENCIES)
        })
        if event_type == "purchase":
            props["transaction_id"] = str(uuid.uuid4())

    elif event_type == "add_to_cart":
        props.update({
            "item_id": random.choice(ITEM_IDS),
            "qty": random.randint(1, 5)
        })

    elif event_type == "app_open":
        props.update({
            "app_version": random.choice(APP_VERSIONS),
            "os": random.choice(OS)
        })

    elif event_type == "search":
        props["query_length"] = random.randint(5, 40)
        props["search_result_count"] = random.choice([0, 0, 5, 10, 20, 50])

    return props


def generate_events_json(count: int) -> List[Dict[str, Any]]:
    events = []
    for _ in range(count):
        event_type = random.choices(EVENT_TYPES, weights=[1, 4, 3, 2, 1], k=1)[0]
        event = {
            "event_id": str(uuid.uuid4()),
            "occurred_at": random_date(),
            "user_id": random.randint(1, MAX_USER_ID),
            "event_type": event_type,
            "properties_json": generate_properties(event_type)
        }
        events.append(event)
    return events


if __name__ == "__main__":
    NUM_EVENTS = 100000
    OUTPUT_FILENAME = "events_100k.json"

    print(f"Generating {NUM_EVENTS} events...")
    data = generate_events_json(NUM_EVENTS)

    try:
        with open(OUTPUT_FILENAME, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
        print(f"✅ Successfully saved {NUM_EVENTS} events to '{OUTPUT_FILENAME}'")
    except Exception as e:
        print(f"❌ Error saving file: {e}")
