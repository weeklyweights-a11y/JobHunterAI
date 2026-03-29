"""Parse job listings from browser-use agent output text."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage

logger = logging.getLogger(__name__)


def _coerce_job(obj: Any) -> dict[str, str] | None:
    if not isinstance(obj, dict):
        return None
    title = str(obj.get("title", "")).strip()
    url = str(obj.get("url", "")).strip()
    if not title or not url:
        return None
    company = str(obj.get("company", "") or "").strip()
    return {"title": title, "company": company, "url": url}


def parse_jobs_json_from_text(text: str) -> list[dict[str, str]]:
    """Extract a JSON array of {title, company, url} from model output."""
    if not text or not text.strip():
        return []

    cleaned = text.strip()
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", cleaned)
    if fence:
        cleaned = fence.group(1).strip()

    try:
        data = json.loads(cleaned)
        if isinstance(data, dict) and "jobs" in data:
            data = data["jobs"]
        if not isinstance(data, list):
            return []
        out: list[dict[str, str]] = []
        for item in data:
            row = _coerce_job(item)
            if row:
                out.append(row)
        return out
    except json.JSONDecodeError:
        pass

    start = cleaned.find("[")
    end = cleaned.rfind("]")
    if start != -1 and end > start:
        try:
            data = json.loads(cleaned[start : end + 1])
            if isinstance(data, list):
                out = []
                for item in data:
                    row = _coerce_job(item)
                    if row:
                        out.append(row)
                return out
        except json.JSONDecodeError:
            logger.debug("Bracket JSON slice failed to parse")

    return []


async def format_jobs_with_llm(llm: BaseChatModel, raw_text: str) -> list[dict[str, str]]:
    """Ask the LLM to convert free-form text into a JSON job array."""
    prompt = (
        "Convert the following text into a JSON array only (no markdown), "
        'each element: {"title": string, "company": string, "url": string}. '
        "Use empty string for unknown company. If no jobs, return [].\n\n"
        f"{raw_text[:12000]}"
    )
    msg = HumanMessage(content=prompt)
    resp = await llm.ainvoke([msg])
    content = resp.content if hasattr(resp, "content") else str(resp)
    text = content if isinstance(content, str) else str(content)
    return parse_jobs_json_from_text(text)
