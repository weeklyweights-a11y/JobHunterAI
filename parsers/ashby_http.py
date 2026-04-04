"""Ashby job pages: embedded ``window.__appData`` JSON + JSON-LD (HTTP-only, no Playwright)."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from parsers.ats_posted_time import accept_normalized_posted_string

logger = logging.getLogger(__name__)

_JSON_DECODER = json.JSONDecoder()


def _employment_type_label(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    key = s.replace(" ", "").lower()
    mapping = {
        "fulltime": "Full-time",
        "parttime": "Part-time",
        "contract": "Contract",
        "internship": "Internship",
        "temporary": "Temporary",
    }
    return mapping.get(key, s)


def _extract_app_data(html: str) -> dict[str, Any] | None:
    m = re.search(r"window\.__appData\s*=", html)
    if not m:
        return None
    start = m.end()
    while start < len(html) and html[start] in " \t\n\r":
        start += 1
    if start >= len(html):
        return None
    try:
        data, _ = _JSON_DECODER.raw_decode(html, start)
    except json.JSONDecodeError as e:
        logger.debug("Ashby __appData JSON decode failed: %s", e)
        return None
    return data if isinstance(data, dict) else None


def _patch_from_app_data(data: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    posting = data.get("posting")
    if not isinstance(posting, dict):
        return out

    t = posting.get("title")
    if isinstance(t, str) and t.strip():
        out["title"] = t.strip()

    org = data.get("organization")
    if isinstance(org, dict):
        name = org.get("name")
        if isinstance(name, str) and name.strip():
            out["company"] = name.strip()

    pd = posting.get("publishedDate")
    if isinstance(pd, str) and pd.strip():
        ok = accept_normalized_posted_string(pd.strip())
        if ok:
            out["posted_time"] = ok

    loc = posting.get("locationName")
    if isinstance(loc, str) and loc.strip():
        out["location"] = loc.strip()

    comp = posting.get("compensationTierSummary")
    if isinstance(comp, str) and comp.strip():
        out["salary"] = comp.strip()

    et = posting.get("employmentType")
    if et is not None:
        label = _employment_type_label(str(et).strip())
        if label:
            out["employment_type"] = label

    desc = posting.get("descriptionPlainText")
    if isinstance(desc, str) and desc.strip():
        text = " ".join(desc.split())
        if len(text) > 14000:
            text = text[:13999] + "…"
        out["job_description"] = text

    return out


def _walk_ld_job_posting(obj: Any, found: list[dict[str, Any]]) -> None:
    if isinstance(obj, dict):
        types = obj.get("@type")
        names: list[str] = []
        if isinstance(types, str):
            names = [types]
        elif isinstance(types, list):
            names = [str(x) for x in types]
        if any("JobPosting" in str(t) for t in names):
            found.append(obj)
            return
        for v in obj.values():
            _walk_ld_job_posting(v, found)
    elif isinstance(obj, list):
        for x in obj:
            _walk_ld_job_posting(x, found)


def _location_from_ld(job: dict[str, Any]) -> str:
    loc = job.get("jobLocation")
    if isinstance(loc, dict):
        addr = loc.get("address")
        if isinstance(addr, dict):
            parts = [
                addr.get("addressLocality"),
                addr.get("addressRegion"),
                addr.get("addressCountry"),
            ]
            return ", ".join(str(p).strip() for p in parts if p and str(p).strip())
        name = loc.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
    return ""


def _patch_from_ld_json(html: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("script", type=lambda x: x and "ld+json" in str(x).lower()):
        raw = tag.string or tag.get_text() or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        found: list[dict[str, Any]] = []
        if isinstance(data, dict):
            _walk_ld_job_posting(data, found)
        elif isinstance(data, list):
            for item in data:
                _walk_ld_job_posting(item, found)
        if not found:
            continue
        job = found[0]
        t = job.get("title")
        if isinstance(t, str) and t.strip():
            out.setdefault("title", t.strip())
        org = job.get("hiringOrganization")
        if isinstance(org, dict):
            n = org.get("name")
            if isinstance(n, str) and n.strip():
                out.setdefault("company", n.strip())
        dp = job.get("datePosted")
        if isinstance(dp, str) and dp.strip():
            ok = accept_normalized_posted_string(dp.strip())
            if ok:
                out.setdefault("posted_time", ok)
        loc = _location_from_ld(job)
        if loc:
            out.setdefault("location", loc)
        bs = job.get("baseSalary")
        if isinstance(bs, dict):
            val = bs.get("value")
            if isinstance(val, dict):
                lo = val.get("minValue")
                hi = val.get("maxValue")
                cur = val.get("unitText") or bs.get("currency")
                if lo is not None and hi is not None:
                    try:
                        lof = float(lo)
                        hif = float(hi)
                        cur_s = f" {cur}" if cur else ""
                        out.setdefault(
                            "salary",
                            f"{lof:,.0f} – {hif:,.0f}{cur_s}",
                        )
                    except (TypeError, ValueError):
                        out.setdefault("salary", f"{lo} – {hi}")
        if out:
            break
    return out


def merge_ashby_page_into_job(html: str, url: str) -> dict[str, Any]:
    """
    Build field updates from Ashby HTML. ``__appData`` wins over JSON-LD when both set a key.
    """
    merged: dict[str, Any] = {}
    app = _extract_app_data(html)
    if app:
        merged.update(_patch_from_app_data(app))
    ld = _patch_from_ld_json(html)
    for k, v in ld.items():
        if k not in merged or not str(merged.get(k) or "").strip():
            if v is not None and str(v).strip():
                merged[k] = v
    _ = url  # apply URL is already the listing URL on the job dict
    return merged


async def enrich_ashby_jobs_via_http(
    jobs: list[dict[str, Any]],
    *,
    emit: Any | None = None,
) -> int:
    """
    For each Ashby listing URL, HTTP GET once and merge ``__appData`` / JSON-LD into the job dict.
    Skips Playwright listing loads when title, company, posted_time, and description are filled.
    """
    from parsers.ats_posted_time import _fetch_job_page_sync

    n_ok = 0
    for j in jobs:
        url = str(j.get("url") or "").strip()
        if not url or "ashbyhq.com" not in urlparse(url).netloc.lower():
            continue
        try:
            html = await asyncio.to_thread(_fetch_job_page_sync, url)
        except (HTTPError, URLError, OSError) as e:
            logger.debug("Ashby HTTP fetch failed url=%s err=%s", url[:120], e)
            continue
        except Exception:
            logger.debug("Ashby HTTP fetch failed url=%s", url[:120], exc_info=True)
            continue
        patch = merge_ashby_page_into_job(html, url)
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
            f"ATS: enriched {n_ok} Ashby job page(s) via HTTP (embedded __appData / JSON-LD; "
            "no Playwright listing loads for those URLs)."
        )
    return n_ok
