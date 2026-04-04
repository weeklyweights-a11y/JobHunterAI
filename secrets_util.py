"""Mask sensitive config fields for API responses."""

from __future__ import annotations

from typing import Any


def mask_secret(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 4:
        return "****"
    return "********" + value[-4:]


def apply_mask_to_config_dict(data: dict[str, Any]) -> dict[str, Any]:
    out = dict(data)
    out["email_app_password"] = mask_secret(str(data.get("email_app_password") or ""))
    out["llm_api_key"] = mask_secret(str(data.get("llm_api_key") or ""))
    out["linkedin_password"] = mask_secret(str(data.get("linkedin_password") or ""))
    return out
