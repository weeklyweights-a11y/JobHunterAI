"""FastAPI application: dashboard, REST API, SSE shell."""

from __future__ import annotations

import asyncio
import sys

# Playwright needs subprocess support on Windows; Proactor provides it.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

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

templates = Jinja2Templates(directory="templates")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await db.init_db()
    init_agent_state(app)
    yield


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
