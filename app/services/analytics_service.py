import duckdb
import pandas as pd
import asyncio
from datetime import date

from loguru import logger
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

from app.core.config import settings

PG_CONN_STRING = str(settings.db.url)
DUCKDB_FILE = "analytics.duckdb"


class AnalyticsService:
    def __init__(self):
        self.pg_engine: AsyncEngine = create_async_engine(PG_CONN_STRING)

    @staticmethod
    async def sync_data_from_postgres():
        """
        Reads all data from PostgreSQL and replaces the table in DuckDB.
        Uses a separate connection for writing.
        """

        clean_pg_url = PG_CONN_STRING.replace("+asyncpg", "")

        def execute_sync_query():
            with duckdb.connect(database=DUCKDB_FILE, read_only=False) as write_conn:
                sql_query = f"""
                    INSTALL postgres;
                    LOAD postgres;

                    -- Use postgres_scan to read from PostgreSQL.
                    -- CREATE OR REPLACE TABLE creates the table if it doesn’t exist
                    -- and fully replaces its contents, eliminating the need for
                    -- separate CREATE TABLE IF NOT EXISTS logic.
                    CREATE OR REPLACE TABLE synced_events AS
                    SELECT 
                        event_id AS event_id, 
                        occurred_at AS occurred_at, 
                        CAST(user_id AS INTEGER) AS user_id, 
                        event_type AS event_type
                    FROM postgres_scan('{clean_pg_url}', 'public', 'events');
                """
                write_conn.execute(sql_query)
                count = write_conn.execute('SELECT COUNT(*) FROM synced_events').fetchone()[0]
                return count

        try:
            record_count = await asyncio.to_thread(execute_sync_query)
            logger.success(f"✅ Synchronization completed. Record count: {record_count}")
        except duckdb.IOException as e:
            if "Conflicting lock is held" in str(e):
                logger.warning("⚠️ Synchronization skipped: DuckDB file is locked by another process (Uvicorn worker).")
            else:
                logger.error(f"!!! Synchronization error: {e}")
        except Exception as e:
            logger.error(f"!!! Synchronization error: {e}")

    @staticmethod
    def get_dau(from_date: date, to_date: date) -> pd.DataFrame:
        """GET /stats/dau: Number of unique user_id per day."""
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
        with duckdb.connect(database=DUCKDB_FILE, read_only=True) as read_conn:
            return read_conn.execute(query).fetchdf()

    @staticmethod
    def get_top_events(from_date: date, to_date: date, limit: int = 10) -> pd.DataFrame:
        """GET /stats/top-events: Top event_type by count."""
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
        with duckdb.connect(database=DUCKDB_FILE, read_only=True) as read_conn:
            return read_conn.execute(query).fetchdf()

    @staticmethod
    def get_retention(start_date: date, windows: int) -> pd.DataFrame:
        """
        GET /stats/retention: Simple cohort retention (weekly windows).
        Determines cohorts based on the week of the first activity.
        """
        query = f"""
        WITH 
        FirstActivity AS (
            SELECT
                user_id,
                -- Determine the week of first activity (cohort)
                date_trunc('week', occurred_at) AS cohort_week
            FROM synced_events
            WHERE occurred_at >= CAST('{start_date}' AS DATE)
            GROUP BY 1, 2
        ),

        WeeklyActivity AS (
            SELECT
                fa.cohort_week,
                date_trunc('week', se.occurred_at) AS activity_week,
                se.user_id
            FROM synced_events se
            JOIN FirstActivity fa ON se.user_id = fa.user_id
            WHERE se.occurred_at >= CAST('{start_date}' AS DATE)
            GROUP BY 1, 2, 3
        )

        SELECT
            cohort_week,
            date_diff('week', cohort_week, activity_week) AS week_number,
            COUNT(DISTINCT user_id) AS retained_users
        FROM WeeklyActivity
        WHERE date_diff('week', cohort_week, activity_week) BETWEEN 0 AND {windows - 1}
        GROUP BY 1, 2
        ORDER BY 1, 2;
        """
        with duckdb.connect(database=DUCKDB_FILE, read_only=True) as read_conn:
            return read_conn.execute(query).fetchdf()


analytics_service = AnalyticsService()
