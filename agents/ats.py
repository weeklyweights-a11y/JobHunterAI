"""ATS discovery via Google domain queries + Greenhouse API enrichment."""

from __future__ import annotations

import asyncio
import logging
import os
import random
import re
from urllib.parse import quote_plus, urlparse

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from agents.base import EmitFn
from agents.browser_mgr import playwright_browser_session
from agents.ashby_api import enrich_ashby
from agents.greenhouse_api import enrich_greenhouse
from agents.lever_api import enrich_lever
from parsers.ats_posted_time import (
    enrich_ats_jobs_posted_times,
    filter_ats_jobs_by_posted_within_days,
    title_matches_search_roles,
)
from parsers.google_parser import discover_google_selectors, parse_google_results_html

logger = logging.getLogger(__name__)

# Google SERP: up to this many pages (10 results each) per role×location×platform.
# Stops earlier when a page adds zero new URLs. Override: JOBHUNTER_ATS_GOOGLE_MAX_SERP_PAGES.
ATS_GOOGLE_MAX_SERP_PAGES_DEFAULT = 5
ATS_GOOGLE_MAX_SERP_PAGES_CAP = 50

# Google site: uses root domain so all subdomains (and many paths) match.
ATS_SITE_QUERY: dict[str, str] = {
    "greenhouse": "site:greenhouse.io",
    "lever": "site:lever.co",
    "ashby": "site:ashbyhq.com",
    "smartrecruiters": "site:smartrecruiters.com",
    "workable": "site:workable.com",
    "workable_apply": "site:workable.com",
    "workday": "site:myworkdayjobs.com",
    "jobvite": "site:jobvite.com",
    "bamboohr": "site:bamboohr.com",
    "icims": "site:icims.com",
}

# Locations that should stay unquoted (broad / not exact-phrase matches).
_GENERIC_LOCATIONS = frozenset(
    {
        "remote",
        "united states",
        "usa",
        "us",
        "anywhere",
        "worldwide",
        "hybrid",
        "on-site",
        "onsite",
        "on site",
    }
)


def _ats_google_role_segment(role: str) -> str:
    """
    Short 2-word titles → quoted exact phrase. Longer → space-separated (no quotes).
    AI/ML Engineer → "AI Engineer" OR "ML Engineer"
    """
    r = (role or "").strip()
    if not r:
        return ""
    if "/" in r:
        left, right = r.split("/", 1)
        left, right = left.strip(), right.strip()
        if left and right and " " not in left:
            rparts = right.split(None, 1)
            if len(rparts) == 2:
                first_tok, suffix = rparts[0], rparts[1]
                return f'"{left} {suffix}" OR "{first_tok} {suffix}"'
    words = re.split(r"\s+", r)
    if len(words) == 2:
        return f'"{r}"'
    return r


def _ats_google_location_segment(location: str) -> str:
    """Quote city/region phrases; leave generic broad terms unquoted."""
    loc = (location or "").strip()
    if not loc:
        return ""
    if loc.lower() in _GENERIC_LOCATIONS:
        return loc
    return f'"{loc}"'


def _enabled_platform_keys(enabled: dict[str, bool] | None) -> list[str]:
    keys = [k for k in ATS_SITE_QUERY if (enabled or {}).get(k, True)]
    if not keys:
        keys = list(ATS_SITE_QUERY.keys())
    return keys


def _google_url(role: str, location: str, platform_key: str, start: int = 0) -> str:
    site = ATS_SITE_QUERY.get(platform_key, "").strip()
    role_q = _ats_google_role_segment(role)
    loc_q = _ats_google_location_segment(location)
    q = " ".join(p for p in (site, role_q, loc_q) if p)
    base = f"https://www.google.com/search?q={quote_plus(q)}"
    return f"{base}&start={max(0, int(start))}" if start else base


def _google_captcha(html: str, page_url: str) -> bool:
    b = (html or "").lower()
    u = (page_url or "").lower()
    return (
        "unusual traffic" in b
        or "our systems have detected unusual traffic" in b
        or "/sorry/" in u
        or "detected unusual traffic" in b
    )


async def _wait_google_captcha_clear(
    page,
    emit: EmitFn | None,
    *,
    max_wait_sec: float,
    poll_sec: float = 2.0,
) -> bool:
    max_wait_sec = max(10.0, float(max_wait_sec))
    poll_sec = max(0.5, min(float(poll_sec), 10.0))
    if emit:
        await emit(
            f"Google CAPTCHA detected — solve it in the browser. "
            f"Waiting up to {int(max_wait_sec)}s (then ATS Google search stops; jobs found so far are still saved and merged into Excel)."
        )
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max_wait_sec
    next_status = loop.time() + 30.0
    while True:
        now = loop.time()
        if now >= deadline:
            return False
        sleep_for = min(poll_sec, max(0.1, deadline - now))
        await asyncio.sleep(sleep_for)
        try:
            html = await page.content()
        except PlaywrightError as e:
            logger.debug("ATS captcha wait: transient page.content error: %s", e)
            await asyncio.sleep(1.0)
            continue
        if not _google_captcha(html, page.url):
            if emit:
                await emit("Google CAPTCHA cleared.")
            return True
        if emit and loop.time() >= next_status:
            left = max(0, int(deadline - loop.time()))
            await emit(f"ATS: still waiting for CAPTCHA (~{left}s left)…")
            next_status = loop.time() + 30.0


def _ats_captcha_wait_seconds(app_cfg: dict | None) -> int:
    env = (os.getenv("JOBHUNTER_ATS_CAPTCHA_WAIT_SECONDS") or "").strip()
    if env:
        try:
            return max(30, min(int(env), 900))
        except ValueError:
            pass
    raw = (app_cfg or {}).get("ats_captcha_wait_seconds")
    if raw is not None:
        try:
            return max(30, min(int(raw), 900))
        except (TypeError, ValueError):
            pass
    return 180


def _ats_google_serp_page_count(app_cfg: dict | None) -> int:
    env = (os.getenv("JOBHUNTER_ATS_GOOGLE_MAX_SERP_PAGES") or "").strip()
    if env:
        try:
            return max(1, min(int(env), ATS_GOOGLE_MAX_SERP_PAGES_CAP))
        except ValueError:
            pass
    raw = (app_cfg or {}).get("ats_google_max_serp_pages")
    if raw is not None:
        try:
            return max(1, min(int(raw), ATS_GOOGLE_MAX_SERP_PAGES_CAP))
        except (TypeError, ValueError):
            pass
    return ATS_GOOGLE_MAX_SERP_PAGES_DEFAULT


def _greenhouse_token(url: str) -> str:
    u = urlparse(url)
    host = (u.netloc or "").lower()
    if "greenhouse.io" not in host:
        return ""
    parts = [p for p in (u.path or "").split("/") if p]
    return parts[0].strip().lower() if parts else ""


def _lever_company_slug(url: str) -> str:
    u = urlparse((url or "").strip())
    if "lever.co" not in (u.netloc or "").lower():
        return ""
    parts = [p for p in (u.path or "").split("/") if p]
    return parts[0].strip().lower() if parts else ""


def _ashby_board_slug(url: str) -> str:
    u = urlparse((url or "").strip())
    if "ashbyhq.com" not in (u.netloc or "").lower():
        return ""
    parts = [p for p in (u.path or "").split("/") if p]
    return parts[0].strip().lower() if parts else ""


def _merge_ats_locations(existing: str, extra: str) -> str:
    """Pipe-separate unique posting locations (case-insensitive), same idea as LinkedIn."""
    parts = [p.strip() for p in (existing or "").split("|") if p.strip()]
    seen = {p.lower() for p in parts}
    x = (extra or "").strip()
    if x and x.lower() not in seen:
        parts.append(x)
    return " | ".join(parts)


def _ats_title_company_key(job: dict) -> tuple[str, str] | None:
    t = str(job.get("title") or "").strip().lower()
    c = str(job.get("company") or "").strip().lower()
    if not t or not c:
        return None
    return (t, c)


def _dedupe_ats_jobs_by_title_company(jobs: list[dict]) -> tuple[list[dict], int]:
    """
    One row per title×company: many ATS boards use a different listing URL per office.
    Keeps the first row's URL; merges locations with ' | '.
    Rows without both title and company are left unchanged and appended after keyed rows.
    """
    registry: dict[tuple[str, str], dict] = {}
    order: list[tuple[str, str]] = []
    unkeyed: list[dict] = []
    for j in jobs:
        key = _ats_title_company_key(j)
        if key is None:
            unkeyed.append(dict(j))
            continue
        if key not in registry:
            registry[key] = dict(j)
            order.append(key)
            continue
        first = registry[key]
        first["location"] = _merge_ats_locations(
            str(first.get("location") or "").strip(),
            str(j.get("location") or "").strip(),
        )
        d1 = str(first.get("job_description") or "")
        d2 = str(j.get("job_description") or "")
        if len(d2) > len(d1):
            first["job_description"] = str(j.get("job_description") or "")
    out = [registry[k] for k in order] + unkeyed
    return out, len(jobs) - len(out)


async def search_ats(
    roles: list[str],
    locations: list[str],
    *,
    emit: EmitFn | None = None,
    app_cfg: dict | None = None,
) -> list[dict]:
    async def _emit(msg: str) -> None:
        if emit:
            await emit(msg)

    enabled_platforms = (app_cfg or {}).get("ats_platforms") or {}
    platform_keys = _enabled_platform_keys(enabled_platforms)
    results: dict[str, dict] = {}
    greenhouse_tokens: set[str] = set()
    lever_slugs: set[str] = set()
    ashby_slugs: set[str] = set()

    combos = [(r, l) for r in roles for l in locations]
    serp_pages = _ats_google_serp_page_count(app_cfg)
    captcha_wait = float(_ats_captcha_wait_seconds(app_cfg))
    greenhouse_attempted = False
    lever_attempted = False
    ashby_attempted = False

    def _merge_api_rows(rows: list[dict]) -> None:
        for j in rows:
            u = str(j.get("url") or "").strip()
            if not u:
                continue
            if u in results:
                prev = results[u]
                prev.update({k: v for k, v in j.items() if v})
                results[u] = prev
            else:
                results[u] = dict(j)

    async def merge_greenhouse_into_results() -> int:
        nonlocal greenhouse_attempted
        if greenhouse_attempted or not greenhouse_tokens:
            return 0
        greenhouse_attempted = True
        try:
            gh_rows = await enrich_greenhouse(
                list(greenhouse_tokens), roles=roles, locations=locations
            )
            _merge_api_rows(gh_rows)
            return len(gh_rows)
        except Exception:
            logger.exception("ATS: Greenhouse API enrichment failed")
            await _emit(
                "ATS: Greenhouse API step failed — keeping Google-discovered rows only where applicable."
            )
            return 0

    async def merge_lever_into_results() -> int:
        nonlocal lever_attempted
        if lever_attempted or not lever_slugs:
            return 0
        lever_attempted = True
        try:
            rows = await enrich_lever(
                list(lever_slugs), roles=roles, locations=locations
            )
            _merge_api_rows(rows)
            return len(rows)
        except Exception:
            logger.exception("ATS: Lever API enrichment failed")
            await _emit(
                "ATS: Lever API step failed — keeping Google and other ATS rows only."
            )
            return 0

    async def merge_ashby_into_results() -> int:
        nonlocal ashby_attempted
        if ashby_attempted or not ashby_slugs:
            return 0
        ashby_attempted = True
        try:
            rows = await enrich_ashby(
                list(ashby_slugs), roles=roles, locations=locations
            )
            _merge_api_rows(rows)
            return len(rows)
        except Exception:
            logger.exception("ATS: Ashby API enrichment failed")
            await _emit(
                "ATS: Ashby API step failed — HTML/board enrichment may still run later."
            )
            return 0

    async def apply_age_filter(out: list[dict]) -> list[dict]:
        _age_raw = (app_cfg or {}).get("ats_posted_within_days")
        if _age_raw is None:
            max_age = 7
        else:
            try:
                max_age = int(_age_raw)
            except (TypeError, ValueError):
                max_age = 7
        max_age = max(0, min(max_age, 365))
        if max_age > 0:
            out, n_old, n_bad = filter_ats_jobs_by_posted_within_days(
                out, max_days=max_age
            )
            if n_old or n_bad:
                bits: list[str] = []
                if n_old:
                    bits.append(f"{n_old} older than {max_age} days")
                if n_bad:
                    bits.append(
                        f"{n_bad} with missing or unparseable posted date "
                        f"(not kept when freshness window is on)"
                    )
                await _emit("ATS: dropped " + "; ".join(bits) + ".")
        return out

    async def enrich_then_age(
        out: list[dict],
        *,
        playwright_context,
        reuse_page,
        pipeline_counts: dict[str, int],
    ) -> list[dict]:
        html_cache: dict[str, str] = {}
        stats: dict = {}
        try:
            out, stats = await enrich_ats_jobs_posted_times(
                out,
                emit=emit,
                max_concurrent=6,
                playwright_context=playwright_context,
                reuse_page=reuse_page,
                roles=roles,
                search_roles=roles,
                html_cache=html_cache,
                pipeline_counts=pipeline_counts,
            )
        except asyncio.CancelledError:
            await _emit(
                "ATS: interrupted while opening job listing pages — fetching posted dates via HTTP "
                "for any rows still missing a date, then applying the age filter."
            )
            try:
                out, stats = await enrich_ats_jobs_posted_times(
                    out,
                    emit=emit,
                    max_concurrent=6,
                    playwright_context=None,
                    reuse_page=None,
                    roles=roles,
                    search_roles=roles,
                    html_cache=html_cache,
                    pipeline_counts=pipeline_counts,
                )
            except asyncio.CancelledError:
                await _emit(
                    "ATS: stop during HTTP date fetch — applying age filter to jobs collected so far."
                )
        except Exception:
            logger.exception(
                "ATS: posted-date enrichment failed; returning rows with partial dates"
            )
            await _emit(
                "ATS: warning — could not finish loading all listing pages for posted dates; "
                "keeping jobs we already found (some may lack a date or fail age filter)."
            )
            stats = {}
        if stats:
            logger.info(
                "ATS pipeline (post-HTTP): Ashby HTTP=%s GH HTTP=%s Lever HTTP=%s "
                "GH listing HTTP=%s GH embed=%s generic HTTP dates=%s browser dates=%s "
                "browser_skip_spa=%s still_missing_posted_time=%s",
                stats.get("n_ashby_http"),
                stats.get("n_greenhouse_http"),
                stats.get("n_lever_http"),
                stats.get("n_greenhouse_listing_http"),
                stats.get("n_greenhouse_embedded_http"),
                stats.get("n_generic_http_dates"),
                stats.get("n_browser_dates"),
                stats.get("n_browser_skipped_spa"),
                stats.get("n_missing_posted_time"),
            )
            if emit:
                await emit(
                    f"ATS pipeline summary: Google URLs {pipeline_counts.get('n_google', 0)}; "
                    f"API rows — Greenhouse {pipeline_counts.get('n_greenhouse_api', 0)}, "
                    f"Lever {pipeline_counts.get('n_lever_api', 0)}, "
                    f"Ashby {pipeline_counts.get('n_ashby_api', 0)}; "
                    f"after dedup {pipeline_counts.get('n_after_dedup', len(out))} rows. "
                    f"HTTP enrich dates — Ashby {stats.get('n_ashby_http', 0)}, "
                    f"GH job pages {stats.get('n_greenhouse_http', 0)}, "
                    f"Lever {stats.get('n_lever_http', 0)}, "
                    f"GH board listing {stats.get('n_greenhouse_listing_http', 0)}, "
                    f"GH embed {stats.get('n_greenhouse_embedded_http', 0)}; "
                    f"generic HTTP {stats.get('n_generic_http_dates', 0)}, "
                    f"browser {stats.get('n_browser_dates', 0)}; "
                    f"skipped SPA browser {stats.get('n_browser_skipped_spa', 0)}; "
                    f"still no posted_time {stats.get('n_missing_posted_time', 0)}."
                )
        return await apply_age_filter(out)

    async def finalize_pipeline(
        *,
        playwright_context=None,
        reuse_page=None,
    ) -> list[dict]:
        n_google = len(results)
        n_gh_api = await merge_greenhouse_into_results()
        n_lv_api = await merge_lever_into_results()
        n_ab_api = await merge_ashby_into_results()
        out = list(results.values())
        out, n_tc_dup = _dedupe_ats_jobs_by_title_company(out)
        if n_tc_dup:
            await _emit(
                f"ATS: merged {n_tc_dup} extra listing(s) with the same title+company "
                f"(different apply URLs per location) into {len(out)} row(s) — "
                "one link per job, locations combined with ' | '."
            )
        pipeline_counts = {
            "n_google": n_google,
            "n_greenhouse_api": n_gh_api,
            "n_lever_api": n_lv_api,
            "n_ashby_api": n_ab_api,
            "n_after_dedup": len(out),
        }
        return await enrich_then_age(
            out,
            playwright_context=playwright_context,
            reuse_page=reuse_page,
            pipeline_counts=pipeline_counts,
        )

    try:
        async with playwright_browser_session(app_cfg) as session:
            page = await session.new_page()
            abort_google = False
            try:
                for role, loc in combos:
                    if abort_google:
                        break
                    for pkey in platform_keys:
                        if abort_google:
                            break
                        site_q = ATS_SITE_QUERY.get(pkey, pkey)
                        await _emit(
                            f"ATS: Google search {role} / {loc} ({site_q}) — "
                            "reading job links from the results page; each listing URL is loaded later in the "
                            "same browser tab (sequential goto) for posted dates."
                        )
                        for p in range(serp_pages):
                            if abort_google:
                                break
                            start = p * 10
                            url = _google_url(role, loc, pkey, start=start)
                            await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                            try:
                                await page.wait_for_load_state("networkidle", timeout=20_000)
                            except PlaywrightTimeoutError:
                                pass
                            try:
                                html = await page.content()
                            except PlaywrightError as e:
                                logger.debug(
                                    "ATS: transient page.content error (retrying): %s", e
                                )
                                await asyncio.sleep(1.0)
                                try:
                                    html = await page.content()
                                except PlaywrightError as e2:
                                    logger.warning(
                                        "ATS: page.content failed after retry: %s", e2
                                    )
                                    break
                            if _google_captcha(html, page.url):
                                ok = await _wait_google_captcha_clear(
                                    page, emit, max_wait_sec=captcha_wait
                                )
                                if not ok:
                                    await _emit(
                                        f"ATS: CAPTCHA not solved within {int(captcha_wait)}s — "
                                        "stopping all further Google ATS searches. "
                                        f"Keeping {len(results)} job URL(s); they still merge into this run and Excel with other sources."
                                    )
                                    abort_google = True
                                    break
                                try:
                                    html = await page.content()
                                except PlaywrightError:
                                    await asyncio.sleep(1.0)
                                    continue
                            batch = parse_google_results_html(html)
                            if not batch:
                                dbg = discover_google_selectors(html)
                                logger.warning(
                                    "ATS Google parse 0 results url=%s selectors=%s counts=%s body_prefix=%s",
                                    url,
                                    dbg.get("attempted_selectors"),
                                    dbg.get("counts"),
                                    dbg.get("body_prefix"),
                                )
                            before = len(results)
                            new_title_ok = 0
                            for j in batch:
                                u = str(j.get("url") or "").strip()
                                if not u:
                                    continue
                                title_txt = str(j.get("title") or "")
                                if not title_matches_search_roles(title_txt, [role]):
                                    continue
                                if u not in results:
                                    new_title_ok += 1
                                    row = dict(j)
                                    row["search_role"] = role
                                    row["search_location"] = loc
                                    results[u] = row
                                    tok = _greenhouse_token(u)
                                    if tok:
                                        greenhouse_tokens.add(tok)
                                    lv = _lever_company_slug(u)
                                    if lv:
                                        lever_slugs.add(lv)
                                    ab = _ashby_board_slug(u)
                                    if ab:
                                        ashby_slugs.add(ab)
                            added = len(results) - before
                            await _emit(
                                f"ATS: page {p + 1}/{serp_pages} +{added} new "
                                f"(title-match new URLs this page: {new_title_ok}, total {len(results)})"
                            )
                            if p > 0 and new_title_ok < 3:
                                break
                            await asyncio.sleep(random.uniform(5.0, 8.0))
                        await asyncio.sleep(random.uniform(5.0, 8.0))
            except asyncio.CancelledError:
                await _emit(
                    "ATS: search stopped — finishing Greenhouse merge and posted-date pass "
                    "(HTTP) before returning jobs."
                )
                return await asyncio.shield(
                    finalize_pipeline(playwright_context=None, reuse_page=None)
                )
            except Exception as e:
                logger.exception("ATS: Google discovery failed: %s", e)
                await _emit(
                    f"ATS: Google discovery stopped ({e!s:.200}). "
                    f"Keeping {len(results)} job URL(s) for this run."
                )

            return await finalize_pipeline(
                playwright_context=session.context,
                reuse_page=page,
            )

    except asyncio.CancelledError:
        await _emit(
            "ATS: browser/session interrupted — merging and resolving posted dates via HTTP, "
            "then age filter."
        )
        return await asyncio.shield(
            finalize_pipeline(playwright_context=None, reuse_page=None)
        )
    except Exception as e:
        logger.exception(
            "ATS: browser/session failed; returning partial results if any: %s", e
        )
        await _emit(
            f"ATS: browser/session error ({e!s:.200}). "
            f"Merging {len(results)} job URL(s) if any (posted dates via HTTP where possible)."
        )
        return await finalize_pipeline(playwright_context=None, reuse_page=None)
