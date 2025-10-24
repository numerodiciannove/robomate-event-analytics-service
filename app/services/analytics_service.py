import duckdb
import pandas as pd
import asyncio
from datetime import date
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

from app.core.config import settings


PG_CONN_STRING = str(settings.db.url)
DUCKDB_FILE = "analytics.duckdb"


class AnalyticsService:
    def __init__(self):
        self.duck_conn: duckdb.DuckDBPyConnection = duckdb.connect(database=DUCKDB_FILE, read_only=False)
        self.pg_engine: AsyncEngine = create_async_engine(PG_CONN_STRING)
        self.duck_conn.execute("""
                               CREATE TABLE IF NOT EXISTS synced_events
                               (
                                   event_id
                                   UUID,
                                   occurred_at
                                   TIMESTAMP
                                   WITH
                                   TIME
                                   ZONE,
                                   user_id
                                   INTEGER,
                                   event_type
                                   VARCHAR
                               );
                               """)

    async def sync_data_from_postgres(self):
        """Читає всі дані з PostgreSQL та замінює таблицю у DuckDB."""

        clean_pg_url = PG_CONN_STRING.replace("+asyncpg", "")

        sql_query = f"""
            INSTALL postgres;
            LOAD postgres;

            CREATE OR REPLACE TABLE synced_events AS
            SELECT 
                event_id AS event_id, 
                occurred_at AS occurred_at, 
                CAST(user_id AS INTEGER) AS user_id,
                event_type AS event_type
            FROM postgres_scan('{clean_pg_url}', 'public', 'events');
        """

        await asyncio.to_thread(self.duck_conn.execute, sql_query)

        print(
            f"✅ Синхронізація завершена. Кількість записів: {self.duck_conn.execute('SELECT COUNT(*) FROM synced_events').fetchone()[0]}")


    def get_dau(self, from_date: date, to_date: date) -> pd.DataFrame:
        """GET /stats/dau: Кількість унікальних user_id по днях."""
        query = f"""
            SELECT
                CAST(date_trunc('day', occurred_at) AS DATE) AS date,
                COUNT(DISTINCT user_id) AS dau
            FROM synced_events
            WHERE occurred_at >= CAST('{from_date}' AS DATE) 
              AND occurred_at < CAST('{to_date}' AS DATE) + INTERVAL '1 day'
            GROUP BY 1
            ORDER BY 1;
        """
        return self.duck_conn.execute(query).fetchdf()

    def get_top_events(self, from_date: date, to_date: date, limit: int = 10) -> pd.DataFrame:
        """GET /stats/top-events: Топ event_type за кількістю."""
        query = f"""
            SELECT
                event_type,
                COUNT(*) AS total_count
            FROM synced_events
            WHERE occurred_at >= CAST('{from_date}' AS DATE) 
              AND occurred_at < CAST('{to_date}' AS DATE) + INTERVAL '1 day'
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT {limit};
        """
        return self.duck_conn.execute(query).fetchdf()

    def get_retention(self, start_date: date, windows: int) -> pd.DataFrame:
        """
        GET /stats/retention: Простий когортний ретеншн (тижневі вікна).
        Визначаємо когорту за тижнем першої активності.
        """
        query = f"""
        WITH 
        FirstActivity AS (
            SELECT
                user_id,
                -- Визначаємо тиждень приєднання (когорту)
                date_trunc('week', occurred_at) AS cohort_week
            FROM synced_events
            WHERE occurred_at >= CAST('{start_date}' AS DATE)
            GROUP BY 1, 2
        ),

        WeeklyActivity AS (
            SELECT
                fa.cohort_week,
                -- Тиждень поточної активності
                date_trunc('week', se.occurred_at) AS activity_week,
                se.user_id
            FROM synced_events se
            JOIN FirstActivity fa ON se.user_id = fa.user_id
            WHERE se.occurred_at >= CAST('{start_date}' AS DATE)
            GROUP BY 1, 2, 3
        )

        SELECT
            cohort_week,
            -- Різниця у тижнях: 0 - тиждень приєднання, 1 - наступний тиждень тощо
            date_diff('week', cohort_week, activity_week) AS week_number,
            COUNT(DISTINCT user_id) AS retained_users
        FROM WeeklyActivity
        -- Фільтруємо за кількістю вікон
        WHERE date_diff('week', cohort_week, activity_week) BETWEEN 0 AND {windows - 1}
        GROUP BY 1, 2
        ORDER BY 1, 2;
        """
        return self.duck_conn.execute(query).fetchdf()


analytics_service = AnalyticsService()
