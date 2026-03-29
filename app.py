"""FastAPI application: dashboard, REST API, SSE shell."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import db
from agent_runtime import init_agent_state
from routes_agent import router as agent_router
from routes_config import router as config_router
from routes_jobs import router as jobs_router
from routes_scheduler import router as scheduler_router
from scheduler import create_scheduler, shutdown_scheduler, sync_scheduler

templates = Jinja2Templates(directory="templates")

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await db.init_db()
    init_agent_state(app)
    create_scheduler(app)
    await sync_scheduler(app, schedule_hours_changed=None)
    try:
        yield
    finally:
        shutdown_scheduler(app)
        task = app.state.agent_task
        if task is not None and not task.done():
            try:
                await asyncio.wait_for(task, timeout=60.0)
            except asyncio.TimeoutError:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logger.exception("join agent after shutdown cancel")
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("join agent during shutdown")
        logger.info("JobHunter AI stopped. Restart with python run.py")


app = FastAPI(title="JobHunter AI", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(config_router, prefix="/api")
app.include_router(agent_router, prefix="/api")
app.include_router(jobs_router, prefix="/api")
app.include_router(scheduler_router, prefix="/api")


@app.get("/")
async def dashboard(request: Request) -> Any:
    return templates.TemplateResponse(
        "index.html",
        {"request": request},
    )


@app.get("/api/health")
async def health() -> Any:
    if await db.check_db_connection():
        return {"status": "ok", "db": "connected"}
    return JSONResponse(
        status_code=503,
        content={"detail": "Database unavailable"},
    )
