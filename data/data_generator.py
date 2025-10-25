import json
import uuid
import random
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

# --- Константи для рандомізації ---
EVENT_TYPES = ["purchase", "app_open", "view_item", "add_to_cart", "search"]
COUNTRIES = ["US", "DE", "PL", "IT", "FR", "UA", "GB"]
CURRENCIES = ["USD", "EUR", "UAH"]
OS = ["Android", "iOS", "Web"]
APP_VERSIONS = ["1.8.9", "2.0.1", "1.9.0", "3.1.0"]
ITEM_IDS = [f"SKU{i:04d}" for i in range(100, 150)]  # 50 унікальних товарів
# Симуляція 10 000 унікальних користувачів
MAX_USER_ID = 10000
# Генерація даних за останні 90 днів
DATE_RANGE_DAYS = 90

# Створення об'єкта часової зони UTC+3 (як у ваших прикладах)
TIMEZONE_OFFSET = timezone(timedelta(hours=3))


def random_date(start_days_ago: int = DATE_RANGE_DAYS) -> str:
    """Генерує випадкову дату і час за останні N днів у форматі ISO 8601 з UTC+3."""
    end_date = datetime.now(TIMEZONE_OFFSET)
    start_date = end_date - timedelta(days=start_days_ago)

    time_diff = end_date - start_date
    random_seconds = random.randint(0, int(time_diff.total_seconds()))

    random_dt = start_date + timedelta(seconds=random_seconds)
    return random_dt.isoformat()


def generate_properties(event_type: str) -> Dict[str, Any]:
    """Генерує випадковий словник properties_json залежно від типу події."""

    # Загальні властивості для всіх подій
    props = {
        "country": random.choice(COUNTRIES),
        # Генеруємо короткий ID сесії для прикладу
        "session_id": uuid.uuid4().hex[:8],
    }

    if event_type == "purchase" or event_type == "view_item":
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
    """
    Основна функція для генерації списку подій.
    """
    events = []

    for _ in range(count):
        # Випадковий вибір типу події
        event_type = random.choices(
            EVENT_TYPES,
            # Надаємо ваги (більше app_open, менше purchase)
            weights=[1, 4, 3, 2, 1],
            k=1
        )[0]

        event = {
            "event_id": str(uuid.uuid4()),
            "occurred_at": random_date(),
            # Генеруємо user_id у діапазоні від 1 до MAX_USER_ID
            "user_id": random.randint(1, MAX_USER_ID),
            "event_type": event_type,
            "properties_json": generate_properties(event_type)
        }
        events.append(event)

    return events


if __name__ == "__main__":
    NUM_EVENTS = 100000
    OUTPUT_FILENAME = "events_100k.json"

    print(f"Початок генерації {NUM_EVENTS} подій...")

    # Генерація даних
    data = generate_events_json(NUM_EVENTS)

    # Збереження даних у JSON файл
    try:
        with open(OUTPUT_FILENAME, "w", encoding="utf-8") as f:
            # Використовуємо ensure_ascii=False для коректного відображення,
            # та compact-формат (без indent) для зменшення розміру файлу
            json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
        print(f"✅ Успішно згенеровано {NUM_EVENTS} подій та збережено у файл '{OUTPUT_FILENAME}'")
    except Exception as e:
        print(f"❌ Помилка при збереженні файлу: {e}")
