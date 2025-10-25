import pandas as pd
from fastapi import APIRouter, Query, Depends
from fastapi_limiter.depends import RateLimiter
from starlette.responses import JSONResponse
from datetime import date
import asyncio
import json
import time

from app.services.analytics_service import analytics_service
from app.db.models.users import User as DBUser
from app.services.jwt_service import get_current_user

analytics_router = APIRouter(prefix="/stats")


def df_to_json_response(df: pd.DataFrame, elapsed_sec: float):
    """Converts a Pandas DataFrame to a JSON API response with elapsed time."""
    content = {
        "data": json.loads(df.to_json(orient="records", date_format='iso')),
        "response_time_sec": round(elapsed_sec, 3)
    }
    return JSONResponse(content=content)


@analytics_router.get("/dau", dependencies=[Depends(RateLimiter(times=5, seconds=60))])
async def get_dau(
        current_user: DBUser = Depends(get_current_user),
        from_date: date = Query(..., description="Start date (YYYY-MM-DD)"),
        to_date: date = Query(..., description="End date (YYYY-MM-DD)"),
):
    """Number of unique user_id per day (Daily Active Users)."""
    start_time = time.perf_counter()
    df = await asyncio.to_thread(analytics_service.get_dau, from_date, to_date)
    elapsed = time.perf_counter() - start_time
    return df_to_json_response(df, elapsed)


@analytics_router.get("/top-events", dependencies=[Depends(RateLimiter(times=5, seconds=60))])
async def get_top_events(
        current_user: DBUser = Depends(get_current_user),
        from_date: date = Query(..., description="Start date (YYYY-MM-DD)"),
        to_date: date = Query(..., description="End date (YYYY-MM-DD)"),
        limit: int = Query(10, gt=0, description="Limit for the number of events in the top list"),
):
    """Top event_type by count."""
    start_time = time.perf_counter()
    df = await asyncio.to_thread(analytics_service.get_top_events, from_date, to_date, limit)
    elapsed = time.perf_counter() - start_time
    return df_to_json_response(df, elapsed)


@analytics_router.get("/retention", dependencies=[Depends(RateLimiter(times=5, seconds=60))])
async def get_retention(
        current_user: DBUser = Depends(get_current_user),
        start_date: date = Query(..., description="Start date for cohort calculation (YYYY-MM-DD)"),
        windows: int = Query(4, ge=2, description="Number of weekly windows for analysis (including week 0)"),
):
    """Simple cohort retention (weekly cohorts). Week 0 - the week of the first activity."""
    start_time = time.perf_counter()
    df = await asyncio.to_thread(analytics_service.get_retention, start_date, windows)
    elapsed = time.perf_counter() - start_time
    return df_to_json_response(df, elapsed)
