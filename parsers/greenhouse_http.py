"""Greenhouse job-boards (Remix): ``window.__remixContext`` embedded JSON — HTTP-only, no JSON-LD."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

from parsers.ats_posted_time import accept_normalized_posted_string
from parsers.job_description import html_fragment_to_plain_text

logger = logging.getLogger(__name__)

_JSON_DECODER = json.JSONDecoder()


def _extract_remix_context(html: str) -> dict[str, Any] | None:
    m = re.search(r"window\.__remixContext\s*=", html)
    if not m:
        return None
    start = m.end()
    while start < len(html) and html[start] in " \t\n\r":
        start += 1
    if start >= len(html):
        return None
    try:
        data, end = _JSON_DECODER.raw_decode(html, start)
    except json.JSONDecodeError as e:
        logger.debug("Greenhouse __remixContext JSON decode failed: %s", e)
        return None
    if not isinstance(data, dict):
        return None
    # tolerate trailing whitespace / semicolon after JSON
    _ = end
    return data


def _loader_data(root: dict[str, Any]) -> dict[str, Any]:
    st = root.get("state")
    if isinstance(st, dict):
        ld = st.get("loaderData")
        if isinstance(ld, dict):
            return ld
    ld = root.get("loaderData")
    return ld if isinstance(ld, dict) else {}


def _find_job_post_in_remix(root: dict[str, Any]) -> dict[str, Any] | None:
    """Resolve ``jobPost`` from Remix loaderData (job-boards.greenhouse.io job detail)."""
    ld = _loader_data(root)
    keys = list(ld.keys())
    preferred: list[Any] = []
    rest: list[Any] = []
    for k in keys:
        if not isinstance(k, str):
            rest.append(k)
            continue
        kl = k.lower()
        if ".jobs" in kl or "jobs_" in kl or "$job_post" in kl:
            preferred.append(k)
        else:
            rest.append(k)
    for k in preferred + rest:
        val = ld.get(k)
        if not isinstance(val, dict):
            continue
        jp = val.get("jobPost")
        if isinstance(jp, dict) and (
            jp.get("published_at") or jp.get("title") or jp.get("id") is not None
        ):
            return jp
    for k in keys:
        jp = _deep_find_job_post(ld.get(k))
        if jp is not None:
            return jp
    return _deep_find_job_post(root)


def _deep_find_job_post(obj: Any) -> dict[str, Any] | None:
    if isinstance(obj, dict):
        jp = obj.get("jobPost")
        if isinstance(jp, dict) and (
            jp.get("published_at") or jp.get("title") or jp.get("content")
        ):
            return jp
        for v in obj.values():
            found = _deep_find_job_post(v)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for x in obj:
            found = _deep_find_job_post(x)
            if found is not None:
                return found
    return None


def _format_job_post_location(raw: Any) -> str:
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    if isinstance(raw, dict):
        return str(raw.get("name") or raw.get("location") or "").strip()
    if isinstance(raw, list):
        parts: list[str] = []
        for x in raw:
            if isinstance(x, str) and x.strip():
                parts.append(x.strip())
            elif isinstance(x, dict):
                n = str(x.get("name") or "").strip()
                if n:
                    parts.append(n)
        return " | ".join(parts) if parts else ""
    return ""


def _salary_from_job_post(jp: dict[str, Any]) -> str:
    for key in (
        "pay_buckets_display",
        "pay_transparency_display",
        "pay_transparency_description",
        "salary_range",
        "compensation",
    ):
        v = jp.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def merge_greenhouse_remix_page_into_job(html: str, url: str) -> dict[str, Any]:
    """Field updates from Greenhouse Remix job page HTML."""
    out: dict[str, Any] = {}
    root = _extract_remix_context(html)
    if not root:
        return out
    jp = _find_job_post_in_remix(root)
    if not isinstance(jp, dict):
        return out

    t = jp.get("title")
    if isinstance(t, str) and t.strip():
        out["title"] = t.strip()

    cn = jp.get("company_name")
    if isinstance(cn, str) and cn.strip():
        out["company"] = cn.strip()

    pa = jp.get("published_at")
    if isinstance(pa, str) and pa.strip():
        ok = accept_normalized_posted_string(pa.strip())
        if ok:
            out["posted_time"] = ok

    loc = _format_job_post_location(jp.get("job_post_location"))
    if loc:
        out["location"] = loc

    sal = _salary_from_job_post(jp)
    if sal:
        out["salary"] = sal

    content = jp.get("content")
    if isinstance(content, str) and content.strip():
        plain = html_fragment_to_plain_text(content, max_len=14000)
        if plain:
            out["job_description"] = plain

    _ = url
    return out


async def enrich_greenhouse_jobs_via_http(
    jobs: list[dict[str, Any]],
    *,
    emit: Any | None = None,
) -> int:
    """
    For each ``greenhouse.io`` listing URL, HTTP GET and merge ``__remixContext`` jobPost fields.
    Skips Playwright listing loads when date + description are both filled afterward.
    """
    from parsers.ats_posted_time import _fetch_job_page_sync

    n_ok = 0
    for j in jobs:
        url = str(j.get("url") or "").strip()
        host = urlparse(url).netloc.lower()
        if not url or "greenhouse.io" not in host:
            continue
        try:
            html = await asyncio.to_thread(_fetch_job_page_sync, url)
        except (HTTPError, URLError, OSError) as e:
            logger.debug("Greenhouse HTTP fetch failed url=%s err=%s", url[:120], e)
            continue
        except Exception:
            logger.debug("Greenhouse HTTP fetch failed url=%s", url[:120], exc_info=True)
            continue
        patch = merge_greenhouse_remix_page_into_job(html, url)
        if not patch:
            continue
        for k, v in patch.items():
            if v is None:
                continue
            if isinstance(v, str) and not v.strip():
                continue
            j[k] = v
        n_ok += 1
    if n_ok and emit is not None:
        await emit(
            f"ATS: enriched {n_ok} Greenhouse job page(s) via HTTP "
            "(window.__remixContext jobPost; no JSON-LD on new job boards)."
        )
    return n_ok
