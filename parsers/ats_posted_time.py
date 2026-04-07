"""Extract job posted/publish dates from public ATS job HTML (JSON-LD, meta, embedded JSON)."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from datetime import datetime, timedelta, timezone
from html import unescape
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urljoin, urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup
from playwright.async_api import BrowserContext
from playwright.async_api import Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from parsers.google_parser import _platform_from_url
from parsers.job_description import extract_job_description_from_html

logger = logging.getLogger(__name__)

_LISTING_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.I,
)


def is_probably_ats_listing_page(url: str) -> bool:
    """
    True when the URL is likely a company board index (many jobs), not a single posting.
    Used to skip naive date extraction and instead expand job links in the browser.
    """
    u = (url or "").strip()
    if not u:
        return False
    p = urlparse(u)
    pl = (p.path or "").rstrip("/").lower()
    path = p.path or ""
    host = (p.netloc or "").lower()
    qs = parse_qs(p.query)

    if qs.get("gh_jid"):
        return False
    if re.search(r"[?&]gh_jid=\d+", u, re.I):
        return False

    if _LISTING_UUID_RE.search(path):
        return False
    if re.search(r"/jobs/\d+(?:/|$)", pl, re.I):
        return False

    if "greenhouse.io" in host:
        from parsers.greenhouse_http import is_greenhouse_board_listing_url

        return is_greenhouse_board_listing_url(u)

    if "lever.co" in host:
        parts = [x for x in path.split("/") if x]
        return len(parts) < 2

    if "ashbyhq.com" in host:
        from parsers.ashby_http import is_ashby_company_listing_url

        parts = [x for x in path.split("/") if x]
        if not parts:
            return True
        return is_ashby_company_listing_url(u)

    for suf in ("/careers", "/jobs", "/open-positions", "/openings", "/job-openings"):
        if pl.endswith(suf):
            return True
    return False


def _normalize_title_match_string(s: str) -> str:
    """Lowercase job title or phrase with runs of whitespace collapsed (substring matching)."""
    return " ".join((s or "").lower().strip().split())


def _expand_slash_in_role_branch(branch: str) -> list[str]:
    """
    Turn one role branch into one or more phrases (same rule as ``_ats_google_role_segment`` in
    ``agents/ats``): ``AI/ML Engineer`` → ``ai engineer``, ``ml engineer``; otherwise ``/`` → space.
    """
    b = (branch or "").strip()
    if not b:
        return []
    if "/" not in b:
        p = _normalize_title_match_string(b)
        return [p] if p else []
    left, right = b.split("/", 1)
    left, right = left.strip(), right.strip()
    if left and right and " " not in left:
        rparts = right.split(None, 1)
        if len(rparts) == 2:
            first_tok, suffix = rparts[0], rparts[1]
            return [
                p
                for p in (
                    _normalize_title_match_string(f"{left} {suffix}"),
                    _normalize_title_match_string(f"{first_tok} {suffix}"),
                )
                if p
            ]
    merged = _normalize_title_match_string(b.replace("/", " "))
    return [merged] if merged else []


def role_match_phrases(role: str) -> list[str]:
    """
    Phrases for case-insensitive substring checks against a job title.
    Outer ``|`` or ``,`` splits OR alternatives (each branch may expand ``/`` like AI/ML).
    """
    r = (role or "").strip()
    if not r:
        return []
    chunks = re.split(r"\s*[|,]\s*", r)
    seen: set[str] = set()
    out: list[str] = []
    for ch in chunks:
        ch = ch.strip()
        if not ch:
            continue
        for p in _expand_slash_in_role_branch(ch):
            if p and p not in seen:
                seen.add(p)
                out.append(p)
    return out


def job_title_matches_search_role(title: str, role: str) -> bool:
    """
    True if the normalized title contains at least one **whole phrase** from the role
    (e.g. ``data analyst`` matches ``Senior Data Analyst`` but not ``Data Entry Officer`` or
    ``Privacy Operations Analyst``).
    """
    t = _normalize_title_match_string(title)
    if not t:
        return False
    phrases = role_match_phrases(role)
    if not phrases:
        return False
    return any(p in t for p in phrases)


def title_matches_search_roles(title: str, search_roles: list[str] | None) -> bool:
    """
    True if the title matches any configured role via phrase substring rules
    (see ``job_title_matches_search_role``). Empty ``search_roles`` matches all titles.
    """
    roles = search_roles or []
    if not roles:
        return True
    if not (title or "").strip():
        return False
    return any(job_title_matches_search_role(title, r) for r in roles if (r or "").strip())


def _role_keywords_for_link_filter(roles: list[str] | None) -> list[str]:
    """Same phrases as title matching so listing-page link text must contain a full phrase."""
    seen: set[str] = set()
    out: list[str] = []
    for r in roles or []:
        for p in role_match_phrases(r):
            if len(p) >= 3 and p not in seen:
                seen.add(p)
                out.append(p)
    if not out:
        out = [
            "software engineer",
            "data scientist",
            "data analyst",
            "product manager",
        ]
    return out


async def _discover_job_links_from_listing_page(
    page: Page,
    listing_url: str,
    roles: list[str] | None,
) -> list[tuple[str, str]]:
    kws = _role_keywords_for_link_filter(roles)
    try:
        raw_links = await page.evaluate(
            """() => [...document.querySelectorAll('a[href]')].map(a => ({
                href: a.href || '',
                text: (a.innerText || '').trim()
            }))"""
        )
    except Exception:
        logger.debug("listing link discovery evaluate failed url=%s", listing_url[:120], exc_info=True)
        return []

    base = listing_url.split("#")[0]
    seen: set[str] = set()
    out: list[tuple[str, str]] = []
    for item in raw_links or []:
        href = str(item.get("href") or "").strip()
        text = str(item.get("text") or "")
        if not href or href.startswith(("mailto:", "javascript:", "tel:")):
            continue
        low = f"{href.lower()} {text.lower()}"
        if not any(kw in low for kw in kws):
            continue
        absu = urljoin(listing_url, href).split("#")[0]
        if not absu or absu in seen:
            continue
        if absu.rstrip("/") == base.rstrip("/"):
            continue
        seen.add(absu)
        out.append((absu, text))
        if len(out) >= 80:
            break
    return out


def parse_posted_time_to_utc_datetime(raw: str) -> datetime | None:
    """Parse ATS posted_time strings (ISO with Z, offset, or date-only) to UTC-aware datetime."""
    s = (raw or "").strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if "T" in s or re.match(r"^\d{4}-\d{2}-\d{2}[+-]", s):
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
    if not m:
        return None
    try:
        d = datetime.strptime(m.group(1), "%Y-%m-%d").date()
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    except ValueError:
        return None


def filter_ats_jobs_by_posted_within_days(
    jobs: list[dict[str, Any]],
    *,
    max_days: int,
) -> tuple[list[dict[str, Any]], int, int]:
    """
    When max_days > 0: keep only jobs whose posted_time parses and is within the window.
    Drops stale listings and rows with missing/unparseable posted_time (so unknown dates
    cannot bypass the freshness rule).
    Returns (filtered_list, dropped_too_old, dropped_unparseable).
    """
    if max_days <= 0 or not jobs:
        return jobs, 0, 0
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=max_days)
    kept: list[dict[str, Any]] = []
    dropped_old = 0
    dropped_bad = 0
    for j in jobs:
        dt = parse_posted_time_to_utc_datetime(str(j.get("posted_time") or ""))
        if dt is None:
            dropped_bad += 1
            continue
        if dt < cutoff:
            dropped_old += 1
            continue
        kept.append(j)
    return kept, dropped_old, dropped_bad


_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_FETCH_MAX_BYTES = 2_500_000
_FETCH_TIMEOUT_SEC = 28

# Prefer specific keys over generic updated/created when scanning embedded JSON.
_JSON_DATE_KEY_PRIORITY: tuple[str, ...] = (
    "dateposted",
    "datePosted",
    "publisheddate",
    "published_at",
    "publishedat",
    "firstpublishedat",
    "first_published_at",
    "postedat",
    "posted_at",
    "publicationstartdate",
    "jobposteddate",
)

_JSON_DATE_KEY_FALLBACK: frozenset[str] = frozenset(
    {
        "updated_at",
        "updatedat",
        "created_at",
        "createdat",
    }
)

# Ashby public job pages: window.__appData → posting.updatedAt (best) or publishedDate
_ASHBY_POSTING_PREFIX_RE = re.compile(r'"posting"\s*:\s*\{', re.I)
_ASHBY_UPDATED_AT_RE = re.compile(r'"updatedAt"\s*:\s*"([^"]+)"', re.I)
_ASHBY_PUBLISHED_DATE_RE = re.compile(r'"publishedDate"\s*:\s*"([^"]+)"', re.I)

_GREENHOUSE_PUBLISHED_RE = re.compile(
    r'"published_at"\s*:\s*"([^"]+)"',
    re.IGNORECASE,
)
_GENERIC_DATEPOSTED_RE = re.compile(
    r'"datePosted"\s*:\s*"([^"]+)"',
    re.IGNORECASE,
)
_NEXT_DATA_RE = re.compile(
    r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>([\s\S]*?)</script>',
    re.IGNORECASE,
)


def _normalize_date_value(raw: str) -> str:
    s = unescape((raw or "").strip())
    if not s:
        return ""
    # Strip timezone names sometimes appended
    return s[:80] if len(s) > 80 else s


def accept_normalized_posted_string(raw: str) -> str | None:
    """
    Return normalized string only if it parses to a real instant/date.
    Rejects bogus fragments (wrong JSON matches, partial offsets, etc.).
    """
    s = _normalize_date_value(raw)
    if not s:
        return None
    if parse_posted_time_to_utc_datetime(s) is None:
        return None
    return s


def _looks_isoish(s: str) -> bool:
    """Require calendar prefix YYYY-MM-DD so random ISO-like fragments are not accepted."""
    s = s.strip()
    if len(s) < 10:
        return False
    if s.startswith("http"):
        return False
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}", s))


def _walk_jsonld_job_posting(obj: Any, found: list[str]) -> None:
    if isinstance(obj, dict):
        types = obj.get("@type")
        tlist: list[str] = []
        if isinstance(types, str):
            tlist = [types]
        elif isinstance(types, list):
            tlist = [str(x) for x in types]
        is_job = any("JobPosting" in str(t) for t in tlist)
        if is_job:
            for key in ("datePosted", "dateposted"):
                if key in obj and obj[key]:
                    v = _normalize_date_value(str(obj[key]))
                    if v:
                        found.append(v)
                        return
        for v in obj.values():
            _walk_jsonld_job_posting(v, found)
    elif isinstance(obj, list):
        for x in obj:
            _walk_jsonld_job_posting(x, found)


def _json_ld_date_posted(html: str) -> str | None:
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    found: list[str] = []
    for tag in soup.find_all("script", type=lambda x: x and "ld+json" in x.lower()):
        raw = tag.string or tag.get_text() or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            _walk_jsonld_job_posting(data, found)
        elif isinstance(data, list):
            for item in data:
                _walk_jsonld_job_posting(item, found)
        if found:
            return found[0]
    return None


def _meta_date(html: str) -> str | None:
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    for prop in (
        "article:published_time",
        "og:published_time",
        "og:updated_time",
    ):
        el = soup.find("meta", property=prop) or soup.find("meta", attrs={"property": prop})
        if el and el.get("content"):
            v = _normalize_date_value(str(el["content"]))
            if v and _looks_isoish(v):
                return v
    el = soup.find("meta", attrs={"name": re.compile(r"^date$", re.I)})
    if el and el.get("content"):
        v = _normalize_date_value(str(el["content"]))
        if v and _looks_isoish(v):
            return v
    return None


def _greenhouse_remix_date(html: str) -> str | None:
    m = _GREENHOUSE_PUBLISHED_RE.search(html)
    if m:
        v = _normalize_date_value(m.group(1))
        if v:
            return v
    return None


def _greenhouse_posted_time_from_embedded(html: str) -> str | None:
    """
    New boards (job-boards.greenhouse.io): ``window.__remixContext`` → loaderData → jobPost.published_at.
    Falls back to scanning raw HTML for ``published_at`` (legacy / escaped JSON).
    """
    try:
        from parsers.greenhouse_http import merge_greenhouse_remix_page_into_job

        patch = merge_greenhouse_remix_page_into_job(html, "")
        pt = patch.get("posted_time")
        if isinstance(pt, str) and pt.strip():
            return pt.strip()
    except Exception:
        logger.debug("greenhouse remix posted_time extract", exc_info=True)
    return _greenhouse_remix_date(html)


def _generic_date_posted_in_html(html: str) -> str | None:
    m = _GENERIC_DATEPOSTED_RE.search(html)
    if m:
        v = _normalize_date_value(m.group(1))
        if v and _looks_isoish(v):
            return v
    return None


def _collect_json_date_fields(obj: Any, acc: dict[str, str]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = str(k).lower()
            if isinstance(v, str) and v.strip() and _looks_isoish(v):
                if kl in _JSON_DATE_KEY_PRIORITY or kl in _JSON_DATE_KEY_FALLBACK:
                    acc.setdefault(kl, v.strip())
            else:
                _collect_json_date_fields(v, acc)
    elif isinstance(obj, list):
        for x in obj:
            _collect_json_date_fields(x, acc)


def _next_data_date(html: str) -> str | None:
    m = _NEXT_DATA_RE.search(html)
    if not m:
        return None
    try:
        data = json.loads(m.group(1))
    except json.JSONDecodeError:
        return None
    acc: dict[str, str] = {}
    _collect_json_date_fields(data, acc)
    for key in _JSON_DATE_KEY_PRIORITY:
        if key in acc:
            return _normalize_date_value(acc[key])
    for key in _JSON_DATE_KEY_FALLBACK:
        if key in acc:
            return _normalize_date_value(acc[key])
    return None


def _ashby_app_data_posted_time(html: str) -> str | None:
    """
    Ashby embeds job state in window.__appData: posting.updatedAt (ISO) and publishedDate (date).
    Prefer updatedAt when present — matches visible JSON-LD date but adds time for freshness.
    """
    m = _ASHBY_POSTING_PREFIX_RE.search(html)
    if m:
        window = html[m.start() : m.start() + 280_000]
        um = _ASHBY_UPDATED_AT_RE.search(window)
        if um:
            v = _normalize_date_value(um.group(1))
            if v and _looks_isoish(v):
                return v
    pm = _ASHBY_PUBLISHED_DATE_RE.search(html)
    if pm:
        v = _normalize_date_value(pm.group(1))
        if v and _looks_isoish(v):
            return v
    return None


def _lever_embedded_date(html: str) -> str | None:
    # Lever boards often embed ISO timestamps in the page JSON
    for pat in (
        r'"listCreatedAt"\s*:\s*"([^"]+)"',
        r'"postedAt"\s*:\s*"([^"]+)"',
        r'"createdAt"\s*:\s*"([^"]+)"',
        r'"updatedAt"\s*:\s*"([^"]+)"',
    ):
        m = re.search(pat, html, re.I)
        if m:
            v = _normalize_date_value(m.group(1))
            if v and _looks_isoish(v):
                return v
    return None


def _relative_posted_phrase_to_iso_date(blob: str) -> str | None:
    """
    Map visible phrases like 'Posted 5 days ago' to a calendar date (UTC).
    Conservative caps so absurd numbers do not skew dates.
    """
    low = blob.lower()
    m = re.search(
        r"(?:posted|published|reposted)\s*[:\s,.-]*(\d+)\s+hours?\s+ago",
        low,
    )
    if m:
        h = min(int(m.group(1)), 24 * 180)
        d = datetime.now(timezone.utc) - timedelta(hours=h)
        return d.date().isoformat()
    m = re.search(
        r"(?:posted|published|reposted)\s*[:\s,.-]*(\d+)\s+days?\s+ago",
        low,
    )
    if m:
        days = min(int(m.group(1)), 730)
        d = datetime.now(timezone.utc) - timedelta(days=days)
        return d.date().isoformat()
    m = re.search(
        r"(?:posted|published)\s*[:\s,.-]*(\d+)\s+weeks?\s+ago",
        low,
    )
    if m:
        w = min(int(m.group(1)), 104)
        d = datetime.now(timezone.utc) - timedelta(weeks=w)
        return d.date().isoformat()
    m = re.search(
        r"(?:posted|published)\s*[:\s,.-]*(\d+)\s+months?\s+ago",
        low,
    )
    if m:
        mo = min(int(m.group(1)), 24)
        d = datetime.now(timezone.utc) - timedelta(days=mo * 30)
        return d.date().isoformat()
    return None


def _dom_visible_posted_heuristic(html: str) -> str | None:
    """
    DOM / visible-text fallback for Lever, Ashby, and other ATS pages where JSON-LD
    is missing but the UI shows 'Posted … ago' or a date near 'posted' / 'published'.
    """
    if not html or not html.strip():
        return None
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text("\n", strip=True)
    if not text:
        return None
    blob = " ".join(text.split())
    if len(blob) > 150_000:
        blob = blob[:150_000]

    for m in re.finditer(
        r"(?:posted|published|posting\s+date|date\s+posted|job\s+posted)\s*"
        r"[:#\s,.-–—]*(\d{4}-\d{2}-\d{2})",
        blob,
        re.I,
    ):
        ok = accept_normalized_posted_string(m.group(1))
        if ok:
            return ok

    rel = _relative_posted_phrase_to_iso_date(blob)
    if rel:
        ok = accept_normalized_posted_string(rel)
        if ok:
            return ok

    head = blob[:40_000]
    for m in re.finditer(r"\b(20[12]\d-\d{2}-\d{2})\b", head):
        start = max(0, m.start() - 100)
        window = head[start : m.end() + 60].lower()
        if any(
            k in window
            for k in (
                "posted",
                "published",
                "publish",
                "date",
                "listing",
                "opening",
                "job",
            )
        ):
            ok = accept_normalized_posted_string(m.group(1))
            if ok:
                return ok
    return None


def extract_posted_time_from_html(html: str, url: str) -> str | None:
    """
    Best-effort posted date from raw job page HTML.
    Ashby: __appData posting.updatedAt / publishedDate, then JSON-LD.
    Others: JSON-LD JobPosting → Greenhouse __remixContext jobPost / published_at regex → __NEXT_DATA__ →
    Lever-style fields → generic "datePosted" in page → meta tags.
    """
    if not html or not html.strip():
        return None
    host = (urlparse(url).netloc or "").lower()

    extractors: list[Any] = []
    if "ashbyhq.com" in host:
        extractors.append(_ashby_app_data_posted_time)
    extractors.append(_json_ld_date_posted)
    if "greenhouse.io" in host:
        extractors.append(_greenhouse_posted_time_from_embedded)
    extractors.append(_next_data_date)
    if "lever.co" in host:
        extractors.append(_lever_embedded_date)
    extractors.extend((_generic_date_posted_in_html, _meta_date))

    for fn in extractors:
        try:
            got = fn(html)
        except Exception:
            logger.debug("ats_posted_time extractor error url=%s", url, exc_info=True)
            got = None
        if got:
            ok = accept_normalized_posted_string(got)
            if ok:
                return ok

    try:
        dom_got = _dom_visible_posted_heuristic(html)
    except Exception:
        logger.debug("ats_posted_time DOM heuristic error url=%s", url, exc_info=True)
        dom_got = None
    if dom_got:
        ok = accept_normalized_posted_string(dom_got)
        if ok:
            return ok

    if "workable.com" in host:
        m = re.search(r'"published_at"\s*:\s*"([^"]+)"', html, re.I)
        if m:
            ok = accept_normalized_posted_string(m.group(1))
            if ok:
                return ok

    return None


def _fetch_job_page_sync(url: str) -> str:
    req = Request(
        url,
        headers={"User-Agent": _USER_AGENT, "Accept": "text/html,application/xhtml+xml"},
        method="GET",
    )
    with urlopen(req, timeout=_FETCH_TIMEOUT_SEC) as resp:
        raw = resp.read(_FETCH_MAX_BYTES)
    return raw.decode("utf-8", errors="replace")


def fetch_job_page_html_cached(url: str, cache: dict[str, str] | None) -> str:
    """Single-flight HTML for one enrichment run (same URL may be hit by several passes)."""
    if cache is not None and url in cache:
        return cache[url]
    html = _fetch_job_page_sync(url)
    if cache is not None:
        cache[url] = html
    return html


def location_matches_search_locations(loc: str, locations: list[str]) -> bool:
    """Same rules as Greenhouse API: substring match on location text vs search locations."""
    l = (loc or "").lower()
    if not l:
        return False
    for q in locations:
        qq = (q or "").lower().strip()
        if not qq:
            continue
        if qq in l:
            return True
        if qq in ("us", "usa", "united states") and "united states" in l:
            return True
    return False


_BROWSER_SKIP_PLATFORMS = frozenset({"workday", "icims"})
_BROWSER_PLATFORM_PRIORITY: dict[str, int] = {
    "greenhouse": 0,
    "lever": 1,
    "ashby": 2,
    "smartrecruiters": 3,
    "workable": 4,
}


async def _listing_html_via_playwright_page(
    page: Page,
    url: str,
    *,
    html_cache: dict[str, str] | None = None,
) -> str:
    from agents.browser_mgr import dismiss_cookie_popup

    key = url.split("#")[0]
    await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
    await dismiss_cookie_popup(page)
    try:
        await page.wait_for_load_state("load", timeout=30_000)
    except PlaywrightTimeoutError:
        pass
    try:
        await page.wait_for_load_state("networkidle", timeout=40_000)
    except PlaywrightTimeoutError:
        pass
    await asyncio.sleep(1.0)
    content = await page.content()
    if html_cache is not None and key:
        html_cache[key] = content
    return content


async def enrich_ats_jobs_posted_times(
    jobs: list[dict[str, Any]],
    *,
    emit: Any | None = None,
    max_concurrent: int = 6,
    playwright_context: BrowserContext | None = None,
    reuse_page: Page | None = None,
    roles: list[str] | None = None,
    search_roles: list[str] | None = None,
    html_cache: dict[str, str] | None = None,
    pipeline_counts: dict[str, int] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    HTTP enrichment order: Ashby (job pages then board ``jobPostings``) → Greenhouse job pages →
    Lever job pages → Greenhouse board listings (public API) → Greenhouse ``gh_jid`` embeds;
    then parallel HTTP or Playwright for whatever still needs dates or descriptions.

    ``search_roles`` (if provided) overrides ``roles`` for board listing filters and link discovery.
    Returns ``(jobs, stats)`` for pipeline summary logging.
    """
    stats: dict[str, Any] = {
        "n_ashby_http": 0,
        "n_greenhouse_http": 0,
        "n_lever_http": 0,
        "n_greenhouse_listing_http": 0,
        "n_greenhouse_embedded_http": 0,
        "n_generic_http_dates": 0,
        "n_browser_dates": 0,
        "n_browser_skipped_spa": 0,
        "n_missing_posted_time": 0,
    }
    if not jobs:
        if pipeline_counts:
            stats.update({f"pipeline_{k}": v for k, v in pipeline_counts.items()})
        return jobs, stats

    run_cache = html_cache if html_cache is not None else {}
    sr = list(search_roles) if search_roles is not None else list(roles or [])

    from parsers.ashby_http import enrich_ashby_jobs_via_http
    from parsers.greenhouse_http import (
        enrich_greenhouse_embedded_jobs_via_http,
        enrich_greenhouse_jobs_via_http,
        enrich_greenhouse_listing_pages_via_http,
    )
    from parsers.lever_http import enrich_lever_jobs_via_http

    stats["n_ashby_http"] = await enrich_ashby_jobs_via_http(
        jobs, emit=emit, search_roles=sr, html_cache=run_cache
    )
    stats["n_greenhouse_http"] = await enrich_greenhouse_jobs_via_http(
        jobs, emit=emit, html_cache=run_cache
    )
    stats["n_lever_http"] = await enrich_lever_jobs_via_http(
        jobs, emit=emit, html_cache=run_cache
    )
    stats["n_greenhouse_listing_http"] = await enrich_greenhouse_listing_pages_via_http(
        jobs, search_roles=sr, emit=emit
    )
    stats["n_greenhouse_embedded_http"] = await enrich_greenhouse_embedded_jobs_via_http(
        jobs, emit=emit, html_cache=run_cache
    )

    targets: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for j in jobs:
        url = str(j.get("url") or "").strip()
        if not url or not _platform_from_url(url):
            continue
        need_date = not str(j.get("posted_time") or "").strip()
        need_desc = not str(j.get("job_description") or "").strip()
        if not (need_date or need_desc):
            continue
        if url in seen_urls:
            continue
        seen_urls.add(url)
        targets.append(j)

    if not targets:
        stats["n_missing_posted_time"] = sum(
            1
            for j in jobs
            if str(j.get("url") or "").strip()
            and _platform_from_url(str(j.get("url") or ""))
            and not str(j.get("posted_time") or "").strip()
        )
        if pipeline_counts:
            stats.update({f"pipeline_{k}": v for k, v in pipeline_counts.items()})
        return jobs, stats

    def _browser_skip(url: str) -> bool:
        return _platform_from_url(url) in _BROWSER_SKIP_PLATFORMS

    listing_batch = [
        j
        for j in targets
        if is_probably_ats_listing_page(str(j.get("url") or ""))
        and not _browser_skip(str(j.get("url") or ""))
    ]
    job_batch = [
        j
        for j in targets
        if not is_probably_ats_listing_page(str(j.get("url") or ""))
    ]
    work_queue: list[dict[str, Any]] = list(job_batch)

    n = len(targets)
    use_browser = playwright_context is not None
    if use_browser:
        logger.info(
            "ATS: loading %s job listing URL(s) in one browser tab (sequential goto)",
            n,
        )
    if emit is not None:
        lb = len(listing_batch)
        jb = len(job_batch)
        extra = f" ({jb} job page(s), {lb} listing index page(s))." if lb else "."
        if use_browser:
            await emit(
                f"ATS: Google phase done — loading {n} URL(s){extra} "
                f"**one at a time** in the same browser tab (goto each URL) to read posted dates…"
            )
        else:
            await emit(
                f"ATS: HTTP-fetching {jb} job URL(s) to read posted dates "
                f"(skipping {lb} listing index page(s) without Playwright; static HTML only)…"
            )

    sem = asyncio.Semaphore(max(1, min(max_concurrent, 16)))

    async def _one_http(job: dict[str, Any]) -> int:
        url = str(job.get("url") or "").strip()
        async with sem:
            try:
                html = await asyncio.to_thread(fetch_job_page_html_cached, url, run_cache)
            except (HTTPError, URLError, OSError) as e:
                logger.debug("ATS posted_time fetch failed url=%s err=%s", url, e)
                return 0
            except Exception:
                logger.debug("ATS posted_time fetch failed url=%s", url, exc_info=True)
                return 0
        dt = extract_posted_time_from_html(html, url)
        got_date = 0
        if dt and accept_normalized_posted_string(dt):
            job["posted_time"] = dt
            got_date = 1
        if not str(job.get("job_description") or "").strip():
            desc = extract_job_description_from_html(html, url)
            if desc:
                job["job_description"] = desc
        return got_date

    if use_browser:
        assert playwright_context is not None
        page = reuse_page
        own_page = False
        if page is None:
            page = await playwright_context.new_page()
            own_page = True
        filled = 0
        n_browser_skipped = 0
        discovered: list[dict[str, Any]] = []
        all_job_urls = {
            str(x.get("url") or "").strip()
            for x in jobs
            if str(x.get("url") or "").strip()
        }
        try:
            for idx, lt in enumerate(listing_batch, start=1):
                if idx > 1:
                    await asyncio.sleep(random.uniform(2.0, 4.0))
                u = str(lt.get("url") or "").strip()
                try:
                    await _listing_html_via_playwright_page(
                        page, u, html_cache=run_cache
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.debug(
                        "ATS listing page Playwright failed url=%s err=%s",
                        u[:120],
                        e,
                    )
                    continue
                try:
                    pairs = await _discover_job_links_from_listing_page(
                        page, u, sr
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.debug(
                        "ATS listing link discovery failed url=%s",
                        u[:120],
                        exc_info=True,
                    )
                    pairs = []
                for href, title in pairs:
                    if href in all_job_urls:
                        continue
                    if not _platform_from_url(href):
                        continue
                    all_job_urls.add(href)
                    new_j: dict[str, Any] = {
                        "url": href,
                        "title": (title or "").strip()
                        or str(lt.get("title") or "Job posting"),
                        "company": lt.get("company", ""),
                        "source": lt.get("source", "ats"),
                        "search_role": lt.get("search_role"),
                        "search_location": lt.get("search_location"),
                    }
                    plat = _platform_from_url(href)
                    if plat:
                        new_j["platform"] = plat
                    jobs.append(new_j)
                    discovered.append(new_j)

            raw_wq = job_batch + discovered
            browser_jobs = [j for j in raw_wq if not _browser_skip(str(j.get("url") or ""))]
            n_browser_skipped = len(raw_wq) - len(browser_jobs)
            stats["n_browser_skipped_spa"] = n_browser_skipped
            if n_browser_skipped and emit is not None:
                await emit(
                    f"ATS: browser pass skipping {n_browser_skipped} Workday/iCIMS URL(s) "
                    "(SPA — DOM date extraction not reliable)."
                )

            def _prio(j: dict[str, Any]) -> int:
                pl = _platform_from_url(str(j.get("url") or ""))
                return _BROWSER_PLATFORM_PRIORITY.get(pl, 6)

            work_queue = sorted(browser_jobs, key=_prio)
            for idx, job in enumerate(work_queue, start=1):
                if idx > 1:
                    await asyncio.sleep(random.uniform(2.0, 4.0))
                url = str(job.get("url") or "").strip()
                try:
                    html = await _listing_html_via_playwright_page(
                        page, url, html_cache=run_cache
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.debug(
                        "ATS posted_time Playwright failed url=%s err=%s",
                        url[:120],
                        e,
                    )
                    continue
                if not (html or "").strip():
                    continue
                dt = extract_posted_time_from_html(html, url)
                if dt and accept_normalized_posted_string(dt):
                    job["posted_time"] = dt
                    filled += 1
                if not str(job.get("job_description") or "").strip():
                    desc = extract_job_description_from_html(html, url)
                    if desc:
                        job["job_description"] = desc
                nwq = len(work_queue)
                if emit is not None and nwq >= 40 and idx % 40 == 0:
                    await emit(
                        f"ATS: posted-date progress {idx}/{nwq} job page(s) "
                        f"({filled} dates resolved so far)…"
                    )
        finally:
            if own_page and page is not None:
                try:
                    await page.close()
                except Exception:
                    pass
    else:
        counts = await asyncio.gather(*(_one_http(j) for j in job_batch))
        filled = sum(counts)

    stats["n_generic_http_dates"] = filled if not use_browser else 0
    stats["n_browser_dates"] = filled if use_browser else 0

    mode = "browser" if use_browser else "http"
    n_report = len(work_queue)
    logger.info(
        "ATS posted_time (%s): filled %s / %s job page target(s)",
        mode,
        filled,
        n_report,
    )
    if emit is not None:
        await emit(
            f"ATS: got a parseable posted date from {filled} of {n_report} job page target(s) "
            f"({n_report - filled} still unknown if HTML had no date or load failed)."
        )

    stats["n_missing_posted_time"] = sum(
        1
        for j in jobs
        if str(j.get("url") or "").strip()
        and _platform_from_url(str(j.get("url") or ""))
        and not str(j.get("posted_time") or "").strip()
    )
    if pipeline_counts:
        stats.update({f"pipeline_{k}": v for k, v in pipeline_counts.items()})

    logger.info(
        "ATS enrichment summary: ashby_http=%s gh_http=%s lever_http=%s gh_list=%s gh_embed=%s "
        "generic_http_dates=%s browser_dates=%s browser_skip_spa=%s missing_posted_time=%s",
        stats["n_ashby_http"],
        stats["n_greenhouse_http"],
        stats["n_lever_http"],
        stats["n_greenhouse_listing_http"],
        stats["n_greenhouse_embedded_http"],
        stats["n_generic_http_dates"],
        stats["n_browser_dates"],
        stats["n_browser_skipped_spa"],
        stats["n_missing_posted_time"],
    )

    return jobs, stats
