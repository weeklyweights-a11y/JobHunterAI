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

from parsers.ats_posted_time import accept_normalized_posted_string, title_matches_search_roles

logger = logging.getLogger(__name__)

_JSON_DECODER = json.JSONDecoder()

_ASHBY_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.I,
)


def is_ashby_company_listing_url(url: str) -> bool:
    """
    ``jobs.ashbyhq.com/{slug}`` with exactly one path segment (company board index), not a job UUID page.
    """
    p = urlparse((url or "").strip())
    if "ashbyhq.com" not in (p.netloc or "").lower():
        return False
    parts = [x for x in (p.path or "").split("/") if x]
    if len(parts) != 1:
        return False
    return not bool(_ASHBY_UUID_RE.match(parts[0]))


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


def _company_name_from_app_data(data: dict[str, Any]) -> str:
    org = data.get("organization")
    if isinstance(org, dict):
        n = org.get("name")
        if isinstance(n, str) and n.strip():
            return n.strip()
    return ""


def _job_board_postings_from_app_data(
    data: dict[str, Any],
) -> tuple[str, list[dict[str, Any]]]:
    """Return (company_name, job posting dicts) from Ashby board ``__appData``."""
    company = _company_name_from_app_data(data)
    jb = data.get("jobBoard")
    if not isinstance(jb, dict):
        return company, []
    raw = jb.get("jobPostings")
    if raw is None:
        raw = jb.get("job_postings")
    if not isinstance(raw, list):
        return company, []
    postings = [x for x in raw if isinstance(x, dict)]
    return company, postings


def _patch_from_job_board_posting(posting: dict[str, Any]) -> dict[str, Any]:
    """Single board posting → same fields as single-job ``_patch_from_app_data`` where applicable."""
    out: dict[str, Any] = {}
    t = posting.get("title")
    if isinstance(t, str) and t.strip():
        out["title"] = t.strip()
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


def _norm_key(s: str) -> str:
    return " ".join((s or "").lower().split())


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


def job_patch_from_ld_json(html: str) -> dict[str, Any]:
    """Public wrapper for JSON-LD JobPosting fields (title, company, datePosted, location, salary)."""
    return _patch_from_ld_json(html or "")


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
    search_roles: list[str] | None = None,
) -> int:
    """
    First: Ashby **job detail** URLs — ``__appData`` single posting + JSON-LD.
    Second: Ashby **board index** URLs — ``jobBoard.jobPostings``, filtered by ``search_roles``,
    update or append rows with constructed job URLs.
    """
    from parsers.ats_posted_time import _fetch_job_page_sync

    n_ok = 0
    for j in jobs:
        url = str(j.get("url") or "").strip()
        if not url or "ashbyhq.com" not in urlparse(url).netloc.lower():
            continue
        if is_ashby_company_listing_url(url):
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
    n_board = await enrich_ashby_listing_pages_via_http(
        jobs,
        search_roles=search_roles,
        emit=emit,
    )
    return n_ok + n_board


async def enrich_ashby_listing_pages_via_http(
    jobs: list[dict[str, Any]],
    *,
    search_roles: list[str] | None = None,
    emit: Any | None = None,
) -> int:
    """
    For each distinct ``jobs.ashbyhq.com/{slug}`` board URL in ``jobs``, fetch HTML and read
    ``__appData.jobBoard.jobPostings``. Role-filter postings, then update matching rows or append
    new job dicts with constructed ``.../{slug}/{id}`` URLs.
    """
    from parsers.ats_posted_time import _fetch_job_page_sync

    listing_urls = sorted(
        {
            str(j.get("url") or "").strip().rstrip("/")
            for j in jobs
            if is_ashby_company_listing_url(str(j.get("url") or ""))
        }
    )
    if not listing_urls:
        return 0

    n_touched = 0
    for listing_url in listing_urls:
        slug = [x for x in urlparse(listing_url).path.split("/") if x][0]
        template: dict[str, Any] = {}
        for j in jobs:
            if str(j.get("url") or "").strip().rstrip("/") == listing_url:
                template = {
                    k: v
                    for k, v in j.items()
                    if k
                    in (
                        "search_role",
                        "search_location",
                        "source",
                        "platform",
                        "company",
                    )
                }
                break
        try:
            html = await asyncio.to_thread(_fetch_job_page_sync, listing_url)
        except (HTTPError, URLError, OSError) as e:
            logger.debug(
                "Ashby listing HTTP fetch failed url=%s err=%s", listing_url[:120], e
            )
            continue
        except Exception:
            logger.debug(
                "Ashby listing HTTP fetch failed url=%s", listing_url[:120], exc_info=True
            )
            continue

        data = _extract_app_data(html)
        if not data:
            logger.debug("Ashby listing: no __appData url=%s", listing_url[:120])
            continue
        company, postings = _job_board_postings_from_app_data(data)
        if not postings:
            logger.debug(
                "Ashby listing: no jobPostings in __appData url=%s", listing_url[:120]
            )
            continue
        if company and not str(template.get("company") or "").strip():
            template["company"] = company

        for posting in postings:
            title = str(posting.get("title") or "").strip()
            if not title_matches_search_roles(title, search_roles):
                continue
            pid = str(posting.get("id") or posting.get("requisitionId") or "").strip()
            if not pid:
                continue
            job_url = f"https://jobs.ashbyhq.com/{slug}/{pid}"
            patch = _patch_from_job_board_posting(posting)
            if company and "company" not in patch:
                patch["company"] = company

            match: dict[str, Any] | None = None
            for j in jobs:
                ju = str(j.get("url") or "").strip().rstrip("/")
                if ju == job_url.rstrip("/"):
                    match = j
                    break
            if match is None:
                tk = _norm_key(title)
                ck = _norm_key(company or str(template.get("company") or ""))
                for j in jobs:
                    if _norm_key(str(j.get("title") or "")) == tk and _norm_key(
                        str(j.get("company") or "")
                    ) == ck:
                        match = j
                        break

            if match is not None:
                if is_ashby_company_listing_url(str(match.get("url") or "")):
                    match["url"] = job_url
                for k, v in patch.items():
                    if v is None or (isinstance(v, str) and not v.strip()):
                        continue
                    if not str(match.get(k) or "").strip():
                        match[k] = v
                n_touched += 1
            else:
                new_j: dict[str, Any] = {
                    **template,
                    "url": job_url,
                    "title": title,
                    "company": str(patch.get("company") or template.get("company") or company or ""),
                    "source": template.get("source", "ats"),
                    "platform": "ashby",
                }
                for k, v in patch.items():
                    if k != "title" and v is not None:
                        if isinstance(v, str) and not v.strip():
                            continue
                        new_j[k] = v
                jobs.append(new_j)
                n_touched += 1

    logger.info(
        "ATS Ashby listing HTTP: processed %s board URL(s), %s posting row(s) merged/added",
        len(listing_urls),
        n_touched,
    )
    if emit is not None and listing_urls:
        await emit(
            f"ATS: Ashby board listing(s) via HTTP — {len(listing_urls)} board URL(s), "
            f"{n_touched} job row(s) updated or added from jobPostings."
        )
    return n_touched
