"""Config, career/custom URLs, and download availability routes."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse

import os

import db
from agent_runtime import config_allows_schedule
from llm_key_validate import validate_gemini_api_key
from output_util import OUTPUT_DIR, get_latest_xlsx_info
from schemas import ConfigIn, ConfigOut, LlmKeyValidateIn, UrlPayload
from secrets_util import apply_mask_to_config_dict
from scheduler import sync_scheduler

logger = logging.getLogger(__name__)

router = APIRouter(tags=["config"])


def _merge_config_patch(current: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    merged = {**current}
    for key, val in patch.items():
        if key in ("email_app_password", "llm_api_key", "linkedin_password") and val == "":
            continue
        merged[key] = val
    return merged


@router.get("/config")
async def get_config() -> dict[str, Any]:
    row = await db.get_config()
    masked = apply_mask_to_config_dict(row)
    return {"data": ConfigOut.model_validate(masked).model_dump()}


@router.post("/config")
async def post_config(request: Request, body: ConfigIn) -> dict[str, Any]:
    current = await db.get_config()
    old_ok = bool(current.get("auto_run_enabled", True)) and config_allows_schedule(current)
    patch = body.model_dump(exclude_unset=True, mode="json")
    merged = _merge_config_patch(current, patch)
    schedule_hours_changed = int(merged["schedule_hours"]) != int(
        current["schedule_hours"]
    )
    saved = await db.save_config(merged)
    new_ok = bool(saved.get("auto_run_enabled", True)) and config_allows_schedule(saved)
    await sync_scheduler(request.app, schedule_hours_changed=schedule_hours_changed)
    masked = apply_mask_to_config_dict(saved)
    out: dict[str, Any] = {
        "data": ConfigOut.model_validate(masked).model_dump(),
    }
    if not old_ok and new_ok:
        out["meta"] = {"scheduler_activated": True}
    return out


@router.post("/validate-llm-key")
async def validate_llm_key(body: LlmKeyValidateIn) -> dict[str, Any]:
    """Check Gemini API key against Google (saved key or key from request body)."""
    provider = (body.provider or "gemini").strip().lower()
    backend = (os.getenv("JOBHUNTER_GEMINI_BACKEND") or "").strip().lower()
    if provider == "gemini" and backend in ("vertex", "vertexai", "gcp"):
        proj = (os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or "").strip()
        if not proj:
            raise HTTPException(
                status_code=400,
                detail="Vertex mode: set GOOGLE_CLOUD_PROJECT in .env. "
                "This check is for AI Studio API keys only.",
            )
        return {
            "data": {
                "valid": True,
                "message": "Using Vertex AI (JOBHUNTER_GEMINI_BACKEND=vertex). "
                "Auth is via Application Default Credentials, not the key field.",
            }
        }
    if provider != "gemini":
        raise HTTPException(
            status_code=400,
            detail="Only Gemini key validation is supported here. Use the provider's console for other keys.",
        )
    key = (body.api_key or "").strip()
    if not key:
        key = (os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY") or "").strip()
    if not key:
        cfg = await db.get_config()
        key = (cfg.get("llm_api_key") or "").strip()
    if not key:
        raise HTTPException(
            status_code=400,
            detail="No API key to test. Paste a key in the field or save settings first.",
        )
    ok, message = await asyncio.to_thread(validate_gemini_api_key, key)
    if not ok:
        raise HTTPException(status_code=400, detail=message)
    return {"data": {"valid": True, "message": message}}


@router.post("/career-pages")
async def add_career_page(payload: UrlPayload) -> dict[str, Any]:
    cfg = await db.get_config()
    pages = list(cfg["career_pages"])
    if payload.url not in pages:
        pages.append(payload.url)
    await db.save_config({**cfg, "career_pages": pages})
    return {"data": {"career_pages": pages}}


@router.delete("/career-pages")
async def remove_career_page(payload: UrlPayload) -> dict[str, Any]:
    cfg = await db.get_config()
    pages = [u for u in cfg["career_pages"] if u != payload.url]
    await db.save_config({**cfg, "career_pages": pages})
    return {"data": {"career_pages": pages}}


@router.post("/custom-sites")
async def add_custom_site(payload: UrlPayload) -> dict[str, Any]:
    cfg = await db.get_config()
    sites = list(cfg["custom_sites"])
    if payload.url not in sites:
        sites.append(payload.url)
    await db.save_config({**cfg, "custom_sites": sites})
    return {"data": {"custom_sites": sites}}


@router.delete("/custom-sites")
async def remove_custom_site(payload: UrlPayload) -> dict[str, Any]:
    cfg = await db.get_config()
    sites = [u for u in cfg["custom_sites"] if u != payload.url]
    await db.save_config({**cfg, "custom_sites": sites})
    return {"data": {"custom_sites": sites}}


@router.get("/download/info")
async def download_info() -> dict[str, Any]:
    """JSON: whether an .xlsx exists under output/ (for dashboard button)."""
    return {"data": get_latest_xlsx_info()}


@router.get("/download/latest")
async def download_latest_file() -> FileResponse:
    """Binary: newest jobs_*.xlsx in output/."""
    info = get_latest_xlsx_info()
    if not info.get("available") or not info.get("filename"):
        raise HTTPException(
            status_code=404,
            detail="No Excel report found. Run a hunt with new jobs first.",
        )
    path = OUTPUT_DIR / str(info["filename"])
    if not path.is_file():
        raise HTTPException(
            status_code=404,
            detail="No Excel report found. Run a hunt with new jobs first.",
        )
    return FileResponse(
        path,
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        filename=path.name,
        content_disposition_type="attachment",
    )
