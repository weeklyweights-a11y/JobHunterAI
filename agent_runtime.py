"""Background agent task, config validation, app.state wiring (Phase 4)."""

from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime
from typing import Any

import db
from agents.llm_setup import _env_fallback_key
from agents.orchestrator import run_hunt


def init_agent_state(app: Any) -> None:
    app.state.agent_task = None
    app.state.agent_start_lock = asyncio.Lock()
    app.state.event_queue = asyncio.Queue[dict[str, Any]]()
    app.state.agent_phase = "idle"
    app.state.progress_message = (
        "Agent idle — configure your search and click Start Hunting"
    )
    app.state.last_summary: dict[str, Any] | None = None
    app.state.current_run_id = None
    app.state.run_started_monotonic: float | None = None
    app.state.last_duration_seconds: float | None = None


def config_allows_schedule(cfg: dict[str, Any]) -> bool:
    """True if config is complete enough for Start Hunting / auto-run."""
    return validate_start_config(cfg) is None


async def try_start_hunt(
    app: Any,
    *,
    reason: str = "manual",
) -> dict[str, Any]:
    """
    Start a hunt if idle and config valid. Used by POST /start and the scheduler.
    Returns {"started": bool, "run_id": int | None, "detail": str | None}.
    detail is "already_running", validation message, or None on success.
    """
    st = app.state
    lock = st.agent_start_lock
    async with lock:
        if st.agent_task is not None and not st.agent_task.done():
            return {
                "started": False,
                "run_id": None,
                "detail": "already_running",
            }

        cfg = await db.get_config()
        err = validate_start_config(cfg)
        if err:
            return {"started": False, "run_id": None, "detail": err}

        st.last_duration_seconds = None
        run_id = await db.create_run("running")
        st.event_queue = asyncio.Queue()
        st.current_run_id = run_id
        st.last_summary = None
        st.progress_message = "Starting hunt…"

        task = asyncio.create_task(_agent_run_coroutine(app, run_id))
        st.agent_task = task
        _ = reason  # reserved for logging / future metrics

    return {"started": True, "run_id": run_id, "detail": None}


def validate_start_config(cfg: dict[str, Any]) -> str | None:
    roles = [r for r in (cfg.get("roles") or []) if isinstance(r, str) and r.strip()]
    locs = [l for l in (cfg.get("locations") or []) if isinstance(l, str) and l.strip()]
    if not roles:
        return "Add at least one role in Setup Your Search."
    if not locs:
        return "Add at least one location in Setup Your Search."
    provider = (cfg.get("llm_provider") or "gemini").strip().lower()
    key = (_env_fallback_key(provider) or "").strip()
    if not key:
        key = (cfg.get("llm_api_key") or "").strip()
    gemini_vertex = (
        provider == "gemini"
        and (os.getenv("JOBHUNTER_GEMINI_BACKEND") or "").strip().lower()
        in ("vertex", "vertexai", "gcp")
        and (
            os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or ""
        ).strip()
    )
    if provider != "ollama" and not key and not gemini_vertex:
        return (
            "Set an LLM API key in LLM provider, save settings, or set GOOGLE_API_KEY / "
            "OPENAI_API_KEY / ANTHROPIC_API_KEY in .env. For billed GCP Gemini, set "
            "JOBHUNTER_GEMINI_BACKEND=vertex and GOOGLE_CLOUD_PROJECT (no AI Studio key required)."
        )
    sources = cfg.get("sources") or {}
    board_on = any(sources.get(k, False) for k in ("linkedin", "indeed", "yc"))
    career_on = bool(sources.get("career_page"))
    career_urls = [
        u
        for u in (cfg.get("career_pages") or []) + (cfg.get("custom_sites") or [])
        if isinstance(u, str) and u.strip()
    ]
    if not board_on and not career_on:
        return (
            "Enable at least one job source (LinkedIn, Indeed, YC, or Career Pages). "
            "Start Hunting now saves your current checkboxes automatically."
        )
    if career_on and not career_urls and not board_on:
        return (
            "Career Pages is on but no URLs are saved. Add a career or custom URL, "
            "or enable LinkedIn, Indeed, or YC."
        )
    return None


async def _progress_sink(app: Any, ev: dict[str, Any]) -> None:
    msg = ev.get("message")
    if isinstance(msg, str) and msg.strip():
        app.state.progress_message = msg


def _elapsed_run_seconds(st: Any) -> float:
    start = st.run_started_monotonic
    if start is None:
        return 0.0
    return max(0.0, round(time.monotonic() - start, 2))


async def _agent_run_coroutine(app: Any, run_id: int) -> None:
    st = app.state
    st.agent_phase = "running"
    st.run_started_monotonic = time.monotonic()
    st.last_summary = None

    async def sink(ev: dict[str, Any]) -> None:
        await _progress_sink(app, ev)

    try:
        summary = await run_hunt(
            status_queue=st.event_queue,
            progress_sink=sink,
        )
        now = datetime.now().isoformat()
        elapsed = _elapsed_run_seconds(st)
        await db.update_run(
            run_id,
            finished_at=now,
            status="completed",
            jobs_found=summary["total_new"],
            errors="; ".join(summary["errors"]) if summary["errors"] else None,
            duration_sec=elapsed,
        )
        st.agent_phase = "completed"
        st.last_summary = summary
        st.progress_message = (
            f"Completed: {summary['total_new']} new jobs saved "
            f"({summary['total_found']} collected)."
        )
    except asyncio.CancelledError:
        now = datetime.now().isoformat()
        await db.update_run(
            run_id,
            finished_at=now,
            status="cancelled",
            duration_sec=_elapsed_run_seconds(st),
        )
        st.agent_phase = "idle"
        st.progress_message = "Agent stopped"
        try:
            await st.event_queue.put(
                {
                    "type": "complete",
                    "message": "Agent stopped",
                    "data": {"stopped": True},
                }
            )
        except Exception:
            pass
        raise
    except Exception as e:
        now = datetime.now().isoformat()
        err = str(e)
        await db.update_run(
            run_id,
            finished_at=now,
            status="failed",
            errors=err[:2000],
            duration_sec=_elapsed_run_seconds(st),
        )
        st.agent_phase = "failed"
        st.progress_message = f"Failed: {err}"
        try:
            await st.event_queue.put(
                {"type": "error", "message": err, "data": {}},
            )
            await st.event_queue.put(
                {
                    "type": "complete",
                    "message": "Run ended with errors",
                    "data": {"failed": True},
                },
            )
        except Exception:
            pass
    finally:
        if st.run_started_monotonic is not None:
            st.last_duration_seconds = round(
                time.monotonic() - st.run_started_monotonic, 1
            )
        st.agent_task = None
        st.current_run_id = None
        st.run_started_monotonic = None
