import pandas as pd
from fastapi import APIRouter, Query
from starlette.responses import JSONResponse
from datetime import date
import asyncio
import json

from app.services.analytics_service import analytics_service

analytics_router = APIRouter(prefix="/stats")


def df_to_json_response(df: pd.DataFrame):
    return JSONResponse(content=json.loads(df.to_json(orient="records", date_format='iso')))


@analytics_router.get("/dau")
async def get_dau(
        from_date: date = Query(..., description="Початкова дата (YYYY-MM-DD)"),
        to_date: date = Query(..., description="Кінцева дата (YYYY-MM-DD)"),
):
    """Кількість унікальних user_id по днях (Daily Active Users)."""
    df = await asyncio.to_thread(analytics_service.get_dau, from_date, to_date)
    return df_to_json_response(df)


@analytics_router.get("/top-events")
async def get_top_events(
        from_date: date = Query(..., description="Початкова дата (YYYY-MM-DD)"),
        to_date: date = Query(..., description="Кінцева дата (YYYY-MM-DD)"),
        limit: int = Query(10, gt=0, description="Обмеження кількості подій у топі"),
):
    """Топ event_type за кількістю."""
    df = await asyncio.to_thread(analytics_service.get_top_events, from_date, to_date, limit)
    return df_to_json_response(df)


@analytics_router.get("/retention")
async def get_retention(
        start_date: date = Query(..., description="Дата, з якої починається розрахунок когорт (YYYY-MM-DD)"),
        windows: int = Query(4, ge=2, description="Кількість тижневих вікон для аналізу (включаючи тиждень 0)"),
):
    """
    Простий когортний ретеншн (тижневі когорти).
    Тиждень 0 - тиждень першої активності.
    """
    df = await asyncio.to_thread(analytics_service.get_retention, start_date, windows)
    return df_to_json_response(df)
