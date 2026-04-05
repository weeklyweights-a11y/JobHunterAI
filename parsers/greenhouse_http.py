"""Greenhouse job pages: Remix ``window.__remixContext`` plus JSON-LD / regex / board API fallbacks (HTTP)."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

from parsers.ats_posted_time import (
    _GREENHOUSE_PUBLISHED_RE,
    _USER_AGENT,
    _FETCH_MAX_BYTES,
    _FETCH_TIMEOUT_SEC,
    _generic_date_posted_in_html,
    _json_ld_date_posted,
    _normalize_date_value,
    accept_normalized_posted_string,
    title_matches_search_roles,
)
from parsers.job_description import html_fragment_to_plain_text

logger = logging.getLogger(__name__)

_JSON_DECODER = json.JSONDecoder()

_BOARD_API_TOKEN_RE = re.compile(
    r"boards-api\.greenhouse\.io/v1/boards/([a-zA-Z0-9_-]+)",
    re.I,
)
_BOARD_HOST_TOKEN_RE = re.compile(
    r"(?:job-boards|boards)\.greenhouse\.io/([a-zA-Z0-9_-]+)(?:/|\?|\"|'|\s|$)",
    re.I,
)


def is_greenhouse_board_listing_url(url: str) -> bool:
    """
    Board index: ``job-boards.greenhouse.io/{token}`` or ``.../{token}/jobs`` without ``/jobs/{id}``.
    """
    p = urlparse((url or "").strip())
    host = (p.netloc or "").lower()
    if "greenhouse.io" not in host:
        return False
    if "job-boards" not in host and "boards.greenhouse" not in host:
        return False
    path = (p.path or "").rstrip("/")
    pl = path.lower()
    if re.search(r"/jobs/\d+", pl, re.I):
        return False
    parts = [x for x in path.split("/") if x]
    if not parts:
        return False
    if len(parts) == 1:
        return True
    if len(parts) == 2 and parts[1].lower() == "jobs":
        return True
    return False


def greenhouse_board_token_from_listing_url(url: str) -> str | None:
    if not is_greenhouse_board_listing_url(url):
        return None
    parts = [x for x in urlparse(url).path.split("/") if x]
    return parts[0] if parts else None


def _fetch_greenhouse_board_jobs_list_sync(token: str) -> list[dict[str, Any]]:
    api_url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"
    req = Request(
        api_url,
        headers={"User-Agent": _USER_AGENT, "Accept": "application/json"},
        method="GET",
    )
    try:
        with urlopen(req, timeout=_FETCH_TIMEOUT_SEC) as resp:
            raw = resp.read(_FETCH_MAX_BYTES)
    except (HTTPError, URLError, OSError, ValueError) as e:
        logger.debug("Greenhouse board list API failed token=%s err=%s", token, e)
        return []
    try:
        data = json.loads(raw.decode("utf-8", errors="replace"))
    except json.JSONDecodeError:
        return []
    jobs = data.get("jobs") if isinstance(data, dict) else None
    return [x for x in jobs if isinstance(x, dict)] if isinstance(jobs, list) else []


def _norm_key(s: str) -> str:
    return " ".join((s or "").lower().split())


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
        logger.debug(
            "Greenhouse __remixContext JSON decode failed: %s (snippet near offset)",
            e,
        )
        return None
    if not isinstance(data, dict):
        return None
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


def _fallback_greenhouse_posted_time(html: str) -> str | None:
    """
    Old boards / embedded JSON without a parsable __remixContext: ``published_at`` in raw HTML,
    JSON-LD ``datePosted``, or generic ``datePosted`` key in page text.
    """
    m = _GREENHOUSE_PUBLISHED_RE.search(html)
    if m:
        v = _normalize_date_value(m.group(1))
        ok = accept_normalized_posted_string(v)
        if ok:
            return ok
    ld = _json_ld_date_posted(html)
    if ld:
        ok = accept_normalized_posted_string(_normalize_date_value(ld))
        if ok:
            return ok
    gen = _generic_date_posted_in_html(html)
    if gen:
        ok = accept_normalized_posted_string(_normalize_date_value(gen))
        if ok:
            return ok
    return None


def _merge_ld_json_patch_into_out(html: str, out: dict[str, Any]) -> None:
    from parsers.ashby_http import job_patch_from_ld_json

    ld_patch = job_patch_from_ld_json(html)
    for k, v in ld_patch.items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        if k not in out or not str(out.get(k) or "").strip():
            out[k] = v


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
    """Field updates from Greenhouse job page HTML (Remix __remixContext, then regex / JSON-LD)."""
    out: dict[str, Any] = {}
    u = (url or "")[:180]

    root = _extract_remix_context(html)
    if not root:
        logger.debug(
            "Greenhouse HTTP: no window.__remixContext (or JSON decode failed) url=%s",
            u,
        )
    else:
        jp = _find_job_post_in_remix(root)
        if not isinstance(jp, dict):
            logger.debug(
                "Greenhouse HTTP: __remixContext present but jobPost not resolved url=%s",
                u,
            )
        else:
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

    if not out.get("posted_time"):
        fb = _fallback_greenhouse_posted_time(html)
        if fb:
            out["posted_time"] = fb

    _merge_ld_json_patch_into_out(html, out)

    if not out:
        logger.debug("Greenhouse HTTP: no fields extracted after Remix + fallbacks url=%s", u)

    return out


def extract_gh_jid_from_url(url: str) -> str | None:
    """Greenhouse embed id from query ``gh_jid`` or path segments like ``/gh_jid/12345``."""
    u = (url or "").strip()
    if not u:
        return None
    p = urlparse(u)
    q = parse_qs(p.query)
    vals = q.get("gh_jid") or []
    if vals:
        s = str(vals[0]).strip()
        if s.isdigit():
            return s
    m = re.search(r"[?&]gh_jid=(\d+)", u, re.I)
    if m:
        return m.group(1)
    path = p.path or ""
    m = re.search(r"/gh_jid[/=](\d+)", path, re.I)
    if m:
        return m.group(1)
    return None


def _board_tokens_from_html(html: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for rx in (_BOARD_API_TOKEN_RE, _BOARD_HOST_TOKEN_RE):
        for m in rx.finditer(html or ""):
            t = m.group(1).strip()
            if t and t not in seen and t.lower() not in ("embed", "v1", "api"):
                seen.add(t)
                out.append(t)
    return out


def _fetch_greenhouse_board_job_json(token: str, gh_jid: str) -> dict[str, Any] | None:
    api_url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs/{gh_jid}"
    req = Request(
        api_url,
        headers={
            "User-Agent": _USER_AGENT,
            "Accept": "application/json",
        },
        method="GET",
    )
    try:
        with urlopen(req, timeout=_FETCH_TIMEOUT_SEC) as resp:
            raw = resp.read(_FETCH_MAX_BYTES)
    except (HTTPError, URLError, OSError, ValueError) as e:
        logger.debug(
            "Greenhouse board API fetch failed token=%s jid=%s err=%s",
            token,
            gh_jid,
            e,
        )
        return None
    try:
        data = json.loads(raw.decode("utf-8", errors="replace"))
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def _patch_from_greenhouse_board_api_job(data: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key in ("updated_at", "first_published", "created_at", "published_at"):
        v = data.get(key)
        if isinstance(v, str) and v.strip():
            ok = accept_normalized_posted_string(v.strip())
            if ok:
                out["posted_time"] = ok
                break
    t = data.get("title")
    if isinstance(t, str) and t.strip():
        out["title"] = t.strip()
    loc = data.get("location")
    if isinstance(loc, dict):
        n = loc.get("name")
        if isinstance(n, str) and n.strip():
            out["location"] = n.strip()
    content = data.get("content")
    if isinstance(content, str) and content.strip():
        plain = html_fragment_to_plain_text(content, max_len=14000)
        if plain:
            out["job_description"] = plain
    return out


def _apply_patch_to_job(j: dict[str, Any], patch: dict[str, Any]) -> None:
    for k, v in patch.items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        j[k] = v


async def enrich_greenhouse_jobs_via_http(
    jobs: list[dict[str, Any]],
    *,
    emit: Any | None = None,
) -> int:
    """
    For each ``greenhouse.io`` listing URL, HTTP GET and merge Remix jobPost + regex/JSON-LD fallbacks.
    Skips Playwright listing loads when date + description are both filled afterward.
    """
    from parsers.ats_posted_time import _fetch_job_page_sync

    n_gh_urls = 0
    n_fetched_ok = 0
    n_fetch_fail = 0
    n_enriched = 0

    for j in jobs:
        url = str(j.get("url") or "").strip()
        host = urlparse(url).netloc.lower()
        if not url or "greenhouse.io" not in host:
            continue
        if is_greenhouse_board_listing_url(url):
            continue
        n_gh_urls += 1
        try:
            html = await asyncio.to_thread(_fetch_job_page_sync, url)
        except (HTTPError, URLError, OSError) as e:
            n_fetch_fail += 1
            logger.debug("Greenhouse HTTP fetch failed url=%s err=%s", url[:120], e)
            continue
        except Exception:
            n_fetch_fail += 1
            logger.debug("Greenhouse HTTP fetch failed url=%s", url[:120], exc_info=True)
            continue

        n_fetched_ok += 1
        if _extract_remix_context(html) is None:
            logger.debug(
                "Greenhouse HTTP: no __remixContext found in page url=%s",
                url[:500],
            )

        patch = merge_greenhouse_remix_page_into_job(html, url)
        if not patch:
            from parsers.ashby_http import job_patch_from_ld_json

            patch = job_patch_from_ld_json(html)

        if not patch:
            continue
        _apply_patch_to_job(j, patch)
        n_enriched += 1

    n_attempted = n_gh_urls
    n_ok = n_enriched
    logger.info(
        "ATS Greenhouse HTTP: attempted %s Greenhouse page(s), enriched %s job row(s) "
        "(%s fetched OK, %s fetch error(s))",
        n_attempted,
        n_ok,
        n_fetched_ok,
        n_fetch_fail,
    )
    if emit is not None:
        await emit(
            f"ATS: attempted {n_attempted} Greenhouse pages via HTTP, enriched {n_ok}"
        )

    return n_enriched


async def enrich_greenhouse_listing_pages_via_http(
    jobs: list[dict[str, Any]],
    *,
    search_roles: list[str] | None = None,
    emit: Any | None = None,
) -> int:
    """
    For each distinct Greenhouse board index URL in ``jobs``, call the public jobs list API and
    merge role-filtered postings into existing rows (by ``absolute_url`` or title+company) or append new rows.
    """
    listing_urls = sorted(
        {
            str(j.get("url") or "").strip().rstrip("/")
            for j in jobs
            if is_greenhouse_board_listing_url(str(j.get("url") or ""))
        }
    )
    if not listing_urls:
        return 0

    n_touched = 0
    for listing_url in listing_urls:
        token = greenhouse_board_token_from_listing_url(listing_url)
        if not token:
            continue
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
        company_guess = token.replace("-", " ").title()
        if not str(template.get("company") or "").strip():
            template["company"] = company_guess

        board_jobs = await asyncio.to_thread(_fetch_greenhouse_board_jobs_list_sync, token)
        if not board_jobs:
            logger.debug(
                "Greenhouse listing: empty API response token=%s url=%s",
                token,
                listing_url[:120],
            )
            continue

        for api_job in board_jobs:
            title = str(api_job.get("title") or "").strip()
            if not title_matches_search_roles(title, search_roles):
                continue
            patch = _patch_from_greenhouse_board_api_job(api_job)
            if not patch.get("title"):
                patch["title"] = title
            abs_url = str(api_job.get("absolute_url") or "").strip()
            if not abs_url:
                jid = api_job.get("id")
                if jid is not None:
                    abs_url = (
                        f"https://job-boards.greenhouse.io/{token}/jobs/{jid}"
                    )

            match: dict[str, Any] | None = None
            if abs_url:
                for j in jobs:
                    if str(j.get("url") or "").strip().rstrip("/") == abs_url.rstrip("/"):
                        match = j
                        break
            if match is None:
                ck = _norm_key(str(template.get("company") or company_guess))
                tk = _norm_key(title)
                for j in jobs:
                    if _norm_key(str(j.get("title") or "")) == tk and _norm_key(
                        str(j.get("company") or "")
                    ) == ck:
                        match = j
                        break

            if match is not None:
                if is_greenhouse_board_listing_url(str(match.get("url") or "")) and abs_url:
                    match["url"] = abs_url
                for k, v in patch.items():
                    if v is None or (isinstance(v, str) and not v.strip()):
                        continue
                    if not str(match.get(k) or "").strip():
                        match[k] = v
                n_touched += 1
            elif abs_url:
                new_j: dict[str, Any] = {
                    **template,
                    "url": abs_url,
                    "title": title,
                    "company": str(template.get("company") or company_guess),
                    "source": template.get("source", "ats"),
                    "platform": "greenhouse",
                }
                for k, v in patch.items():
                    if k == "title":
                        continue
                    if v is None or (isinstance(v, str) and not v.strip()):
                        continue
                    new_j[k] = v
                jobs.append(new_j)
                n_touched += 1

    logger.info(
        "ATS Greenhouse listing HTTP: %s board URL(s), %s job row(s) merged/added",
        len(listing_urls),
        n_touched,
    )
    if emit is not None and listing_urls:
        await emit(
            f"ATS: Greenhouse board listing(s) via API — {len(listing_urls)} board URL(s), "
            f"{n_touched} job row(s) updated or added."
        )
    return n_touched


async def enrich_greenhouse_embedded_jobs_via_http(
    jobs: list[dict[str, Any]],
    *,
    emit: Any | None = None,
) -> int:
    """
    Career sites on custom domains with ``gh_jid``: fetch page JSON-LD / ``published_at``, or
    Greenhouse board API when a board token appears in the HTML.
    """
    from parsers.ats_posted_time import _fetch_job_page_sync

    n_attempted = 0
    n_enriched = 0

    for j in jobs:
        url = str(j.get("url") or "").strip()
        host = urlparse(url).netloc.lower()
        if not url or "greenhouse.io" in host:
            continue
        gh_jid = extract_gh_jid_from_url(url)
        if not gh_jid:
            continue
        if str(j.get("posted_time") or "").strip():
            continue
        n_attempted += 1
        patch: dict[str, Any] = {}
        try:
            html = await asyncio.to_thread(_fetch_job_page_sync, url)
        except (HTTPError, URLError, OSError) as e:
            logger.debug("Greenhouse embedded HTTP fetch failed url=%s err=%s", url[:120], e)
            html = ""
        except Exception:
            logger.debug(
                "Greenhouse embedded HTTP fetch failed url=%s",
                url[:120],
                exc_info=True,
            )
            html = ""

        if html:
            fb = _fallback_greenhouse_posted_time(html)
            if fb:
                patch["posted_time"] = fb
            _merge_ld_json_patch_into_out(html, patch)

        if not patch.get("posted_time"):
            for token in _board_tokens_from_html(html or ""):
                api_data = await asyncio.to_thread(
                    _fetch_greenhouse_board_job_json, token, gh_jid
                )
                if not api_data:
                    continue
                api_patch = _patch_from_greenhouse_board_api_job(api_data)
                if api_patch:
                    for k, v in api_patch.items():
                        if k not in patch or not str(patch.get(k) or "").strip():
                            patch[k] = v
                    if patch.get("posted_time") or patch.get("title"):
                        break

        if patch:
            _apply_patch_to_job(j, patch)
            n_enriched += 1
        else:
            logger.debug(
                "Greenhouse embedded HTTP: no date/title from career page or API url=%s",
                url[:200],
            )

    logger.info(
        "ATS Greenhouse embedded (gh_jid): attempted %s, enriched %s",
        n_attempted,
        n_enriched,
    )
    if emit is not None and n_attempted:
        await emit(
            f"ATS: embedded Greenhouse (gh_jid) via HTTP — attempted {n_attempted}, enriched {n_enriched}"
        )

    return n_enriched
