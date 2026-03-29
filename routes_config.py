"""Config, career/custom URLs, and download availability routes."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter

import db
from output_util import get_latest_xlsx_info
from schemas import ConfigIn, ConfigOut, UrlPayload
from secrets_util import apply_mask_to_config_dict

logger = logging.getLogger(__name__)

router = APIRouter(tags=["config"])


def _merge_config_patch(current: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    merged = {**current}
    for key, val in patch.items():
        if key in ("email_app_password", "llm_api_key") and val == "":
            continue
        merged[key] = val
    return merged


@router.get("/config")
async def get_config() -> dict[str, Any]:
    row = await db.get_config()
    masked = apply_mask_to_config_dict(row)
    return {"data": ConfigOut.model_validate(masked).model_dump()}


@router.post("/config")
async def post_config(body: ConfigIn) -> dict[str, Any]:
    current = await db.get_config()
    patch = body.model_dump(exclude_unset=True, mode="json")
    merged = _merge_config_patch(current, patch)
    saved = await db.save_config(merged)
    masked = apply_mask_to_config_dict(saved)
    return {"data": ConfigOut.model_validate(masked).model_dump()}


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


@router.get("/download/latest")
async def download_latest() -> dict[str, Any]:
    return {"data": get_latest_xlsx_info()}
