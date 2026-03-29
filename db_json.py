"""JSON helpers for SQLite text columns."""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def dump_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"))


def parse_list(text: str) -> list[Any]:
    if not text:
        return []
    try:
        v = json.loads(text)
        return v if isinstance(v, list) else []
    except json.JSONDecodeError:
        logger.warning("Invalid JSON list in database")
        return []


def parse_dict(text: str) -> dict[str, Any]:
    if not text:
        return {}
    try:
        v = json.loads(text)
        return v if isinstance(v, dict) else {}
    except json.JSONDecodeError:
        logger.warning("Invalid JSON object in database")
        return {}
