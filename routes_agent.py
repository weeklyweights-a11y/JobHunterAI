"""Agent control, status, and SSE (Phase 4)."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, AsyncIterator

from fastapi import APIRouter, HTTPException, Request
from sse_starlette.sse import EventSourceResponse

import db
from agent_runtime import _agent_run_coroutine, validate_start_config
from schemas import AgentStatusResponse, StartAgentData, StopAgentData

logger = logging.getLogger(__name__)

router = APIRouter(tags=["agent"])


@router.post("/start")
async def start_agent(request: Request) -> dict[str, Any]:
    app = request.app
    st = app.state
    lock = st.agent_start_lock
    async with lock:
        if st.agent_task is not None and not st.agent_task.done():
            raise HTTPException(status_code=409, detail="Agent is already running")

        cfg = await db.get_config()
        err = validate_start_config(cfg)
        if err:
            raise HTTPException(status_code=400, detail=err)

        st.last_duration_seconds = None
        run_id = await db.create_run("running")
        st.event_queue = asyncio.Queue()
        st.current_run_id = run_id
        st.last_summary = None
        st.progress_message = "Starting hunt…"

        task = asyncio.create_task(_agent_run_coroutine(app, run_id))
        st.agent_task = task

    return {"data": StartAgentData(run_id=run_id, status="started").model_dump()}


@router.post("/stop")
async def stop_agent(request: Request) -> dict[str, Any]:
    st = request.app.state
    task = st.agent_task
    if task is not None and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("join agent task after cancel")
    return {"data": StopAgentData(status="stopped").model_dump()}


@router.get("/status")
async def agent_status(request: Request) -> dict[str, Any]:
    st = request.app.state
    phase = st.agent_phase
    progress = st.progress_message or ""
    run_id = st.current_run_id
    summary = None
    duration_seconds = None

    if phase == "running" and st.run_started_monotonic is not None:
        duration_seconds = round(time.monotonic() - st.run_started_monotonic, 1)
    elif st.last_duration_seconds is not None:
        duration_seconds = st.last_duration_seconds

    if phase == "completed" and st.last_summary is not None:
        summary = dict(st.last_summary)

    payload = AgentStatusResponse(
        state=phase,
        progress=progress,
        run_id=run_id,
        summary=summary,
        duration_seconds=duration_seconds,
    ).model_dump(exclude_none=True)
    return {"data": payload}


async def _sse_generator(request: Request) -> AsyncIterator[dict[str, Any]]:
    st = request.app.state
    queue: asyncio.Queue = st.event_queue
    while True:
        if await request.is_disconnected():
            break
        try:
            item = await asyncio.wait_for(queue.get(), timeout=15.0)
        except asyncio.TimeoutError:
            yield {"comment": "keep-alive"}
            continue
        if not isinstance(item, dict):
            continue
        ev = item.get("type") or "status"
        try:
            data = json.dumps(item, ensure_ascii=False)
        except (TypeError, ValueError):
            data = json.dumps({"type": ev, "message": str(item)})
        yield {"event": str(ev), "data": data}


@router.get("/events")
async def events_stream(request: Request) -> EventSourceResponse:
    return EventSourceResponse(
        _sse_generator(request),
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
