"""Scheduler status API (Phase 6)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from schemas import SchedulerStatusResponse
from scheduler import build_scheduler_status

router = APIRouter(tags=["scheduler"])


@router.get("/scheduler")
async def get_scheduler(request: Request) -> dict[str, Any]:
    inner = await build_scheduler_status(request.app)
    return {"data": SchedulerStatusResponse.model_validate(inner).model_dump()}
