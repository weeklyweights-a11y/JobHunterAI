"""LinkedIn Jobs — Tier 1 Playwright + BeautifulSoup (no LLM)."""

from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from collections.abc import Callable
from typing import Any

from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from agents.browser_mgr import playwright_browser_session
from agents.base import EmitFn
from db_jobs import linkedin_url_for_title_company, url_exists
from db_paths import normalize_linkedin_employment_types
from parsers.job_description import clip_plain_description
from parsers.linkedin_parser import discover_selectors, parse_linkedin_jobs

logger = logging.getLogger(__name__)


def _linkedin_playwright_disconnect(exc: BaseException) -> bool:
    """True when the browser/CDP session died (shutdown, Chrome closed, etc.)."""
    if type(exc).__name__ == "TargetClosedError":
        return True
    msg = str(exc).lower()
    if "target page" in msg and "closed" in msg:
        return True
    if "connection closed" in msg:
        return True
    if "browser has been closed" in msg:
        return True
    return False


LINKEDIN_TARGET_JOBS = 2000
LINKEDIN_MAX_PAGES = 50
CONSECUTIVE_STALE_PAGES = 3
# Virtualized list: need many scrolls; stop only after several no-growth passes.
_SCROLL_CYCLES_PER_PAGE = 24
_SCROLL_STALL_BEFORE_STOP = 4
_SCROLL_CYCLES_GUEST = 24
_POST_PAGE_NAV_SLEEP_SEC = 1.2

JOB_CARD_SELECTOR = (
    "li.jobs-search-results__list-item, "
    "ul.jobs-search__results-list li, "
    ".scaffold-layout__list-container li"
)

_SCROLL_JS = """
() => {
  const sels = [
    '.jobs-search-results-list',
    '.scaffold-layout__list',
    '.jobs-search-results__list',
    '[class*="jobs-search-results"]',
    '.scaffold-layout__list-container'
  ];
  for (const sel of sels) {
    const el = document.querySelector(sel);
    if (el && el.scrollHeight > el.clientHeight + 40) {
      el.scrollTo(0, el.scrollHeight);
      return sel;
    }
  }
  window.scrollTo(0, document.body.scrollHeight);
  return 'window';
}
"""

# Incremental scroll — virtualized lists often need many small steps, not one jump to bottom.
_SCROLL_JS_STEP = """
() => {
  const step = 520;
  const sels = [
    '.jobs-search-results-list',
    '.scaffold-layout__list',
    '.jobs-search-results__list',
    '[class*="jobs-search-results"]',
    '.scaffold-layout__list-container',
    '.scaffold-layout__list-inner'
  ];
  for (const sel of sels) {
    const el = document.querySelector(sel);
    if (el && el.scrollHeight > el.clientHeight + 30) {
      el.scrollBy({ top: step, behavior: 'instant' });
      return 'step:' + sel;
    }
  }
  window.scrollBy({ top: step, behavior: 'instant' });
  return 'window-step';
}
"""

_PARSE_DEBUG_HTML_PREFIX = 2000

# Role text suggests internship — align LinkedIn filters even if employment type stayed default (F).
_INTERNSHIP_ROLE_HINT = re.compile(r"\bintern(ship|s)?\b", re.IGNORECASE)

_LOGIN_MARKERS = (
    "sign in to linkedin",
    "join linkedin",
    "authwall",
    "login?fromSignIn",
)
_NO_RESULTS_MARKERS = (
    "no results found",
    "couldn’t find",
    "couldn't find",
    "no jobs found",
    "try expanding your search",
)


def _effective_linkedin_jt_codes(
    role: str, employment_types: list[str] | None
) -> list[str]:
    """
    Dashboard job-type codes plus internship inference from the role string.
    If the role clearly references an internship but settings are still default
    full-time only, search internships (I). If non-default types are set but I is
    missing, prepend I so we omit f_E / experience keywords per LinkedIn rules.
    """
    codes = list(normalize_linkedin_employment_types(employment_types))
    if not _INTERNSHIP_ROLE_HINT.search(role or ""):
        return codes
    if codes == ["F"]:
        return ["I"]
    if "I" not in codes:
        return ["I"] + codes
    return codes


def _experience_prefix(experience: str) -> str:
    e = (experience or "any").strip().lower()
    if e == "entry":
        return "entry-level "
    if e == "mid":
        return "mid-level "
    if e == "senior":
        return "senior "
    if e == "lead":
        return "lead "
    return ""


def combined_linkedin_search_query(
    role: str,
    location: str,
    experience: str,
    employment_types: list[str] | None = None,
    app_cfg: dict[str, Any] | None = None,
) -> str:
    role = (role or "").strip()
    location = (location or "").strip()
    jt = _effective_linkedin_jt_codes(role, employment_types)
    use_prefix = "I" not in jt
    prefix = _experience_prefix(experience) if use_prefix else ""
    window = "past week" if bool((app_cfg or {}).get("linkedin_posted_past_week")) else "past 24 hours"
    return f"{prefix}{role} posted in the {window} in {location}".strip()


def _experience_f_e_param(experience: str) -> str | None:
    """LinkedIn f_E: 2=Entry, 4=Mid-Senior, 5=Director; omit for 'any'."""
    e = (experience or "any").strip().lower()
    if e == "any":
        return None
    if e == "entry":
        return "2"
    if e in ("mid", "senior"):
        return "4"
    if e == "lead":
        return "5"
    return None


def _search_url(
    role: str,
    location: str,
    experience: str,
    employment_types: list[str] | None = None,
    app_cfg: dict[str, Any] | None = None,
) -> str:
    from urllib.parse import quote_plus

    jt_codes = _effective_linkedin_jt_codes(role, employment_types)
    exp = (experience or "any").strip().lower()
    use_level_in_keywords = "I" not in jt_codes
    kw = (
        f"{_experience_prefix(exp) if use_level_in_keywords else ''}{(role or '').strip()}"
    ).strip()
    k = quote_plus(kw)
    loc = quote_plus((location or "").strip())
    # f_TPR: r86400 = last 24 hours, r604800 = last week (LinkedIn filter)
    tpr = "r604800" if bool((app_cfg or {}).get("linkedin_posted_past_week")) else "r86400"
    parts = [
        f"keywords={k}",
        f"location={loc}",
        f"f_TPR={tpr}",
        f"f_JT={quote_plus(','.join(jt_codes))}",
    ]
    if "I" not in jt_codes:
        fe = _experience_f_e_param(exp)
        if fe:
            parts.append(f"f_E={fe}")
    return "https://www.linkedin.com/jobs/search/?" + "&".join(parts)


def _filter_linkedin_apply_preference(
    jobs: list[dict[str, Any]], app_cfg: dict[str, Any] | None
) -> list[dict[str, Any]]:
    """
    If linkedin_include_easy_apply is false, drop jobs whose apply_type is easy_apply.
    Used twice: (1) before clicking — from list-card badge/footer only (parser);
    (2) after the right panel — from the Apply button.
    """
    if not app_cfg:
        return jobs
    if bool(app_cfg.get("linkedin_include_easy_apply")):
        return jobs
    return [
        j
        for j in jobs
        if str(j.get("apply_type") or "").strip().lower() != "easy_apply"
    ]


def _linkedin_title_company_key(title: str, company: str) -> tuple[str, str] | None:
    t = (title or "").strip().lower()
    c = (company or "").strip().lower()
    if not t or not c:
        return None
    return (t, c)


def _merge_posting_locations(existing: str, extra: str) -> str:
    """Pipe-separate unique posting locations (case-insensitive)."""
    parts = [p.strip() for p in (existing or "").split("|") if p.strip()]
    seen = {p.lower() for p in parts}
    x = (extra or "").strip()
    if x and x.lower() not in seen:
        parts.append(x)
    return " | ".join(parts)


async def _linkedin_try_title_company_dedup(
    out: dict[str, Any],
    registry: dict[tuple[str, str], dict[str, Any]],
    status_callback: Any,
) -> bool:
    """
    If title+company already in this LinkedIn run, merge location into first row and return True.
    Otherwise register out and return False (caller appends).
    """
    key = _linkedin_title_company_key(
        str(out.get("title") or ""),
        str(out.get("company") or ""),
    )
    if key is None:
        return False
    if key not in registry:
        registry[key] = out
        return False
    first = registry[key]
    new_loc = str(out.get("location") or "").strip()
    old_loc = str(first.get("location") or "").strip()
    merged = _merge_posting_locations(old_loc, new_loc)
    title_d = str(out.get("title") or "").strip()
    company_d = str(out.get("company") or "").strip()
    if merged != old_loc:
        first["location"] = merged
        have = old_loc or "first listing"
        add = new_loc or "—"
        msg = (
            f"Skipping duplicate: {title_d} at {company_d} "
            f"(already have {have}; adding {add})"
        )
        logger.info(msg)
        await _notify_status(status_callback, msg)
    else:
        msg = f"Skipping duplicate: {title_d} at {company_d} (same title/company, location already merged)"
        logger.info(msg)
        await _notify_status(status_callback, msg)
    return True


def _parse_hours_ago(low: str) -> int | None:
    m = re.search(r"(\d+)\s*hours?\s*ago", low)
    if m:
        return int(m.group(1))
    if re.search(r"\b1\s*hour\s*ago\b", low) or "an hour ago" in low:
        return 1
    return None


def _parse_days_ago(low: str) -> int | None:
    m = re.search(r"(\d+)\s*days?\s*ago", low)
    if m:
        return int(m.group(1))
    if re.search(r"\b1\s*day\s*ago\b", low):
        return 1
    return None


_LINKEDIN_REPOST_FRESHNESS_PREFIX = "Reposted —"


def _linkedin_freshness_is_repost_label(freshness: str) -> bool:
    """True when we surface LinkedIn's repost as the dashboard freshness label."""
    return str(freshness or "").strip().startswith(_LINKEDIN_REPOST_FRESHNESS_PREFIX)


def _classify_linkedin_freshness(
    posted_time: str, *, window: str = "24h"
) -> tuple[bool, str]:
    """
    Returns (keep, freshness_label). posted_time is raw text from the right panel.

    window: "24h" (default) = keep only listings within last ~24 hours by text;
    "week" = allow up to 7 days (matches LinkedIn past-week filter).
    """
    t = (posted_time or "").strip()
    if not t:
        return False, ""
    low = t.lower()
    week = str(window).lower() in ("week", "1w", "7d", "168h")
    line = re.sub(r"\s+", " ", t.split("\n")[0].strip())[:120]

    if "month" in low:
        return False, ""

    mw = re.search(r"(\d+)\s+weeks?", low)
    if mw and int(mw.group(1)) >= 1:
        return False, ""
    if "one week" in low or re.search(r"\b1\s*week\s*ago\b", low):
        return False, ""

    has_reposted = "reposted" in low

    if "minute" in low:
        return True, f"{_LINKEDIN_REPOST_FRESHNESS_PREFIX} {line}" if has_reposted else "Fresh"

    ho = _parse_hours_ago(low)
    if ho is not None:
        if not week and ho >= 24:
            return False, ""
        if has_reposted:
            return True, f"{_LINKEDIN_REPOST_FRESHNESS_PREFIX} {line}"
        return True, "Fresh" if ho < 3 else line

    if "hour" in low and ho is None:
        if "an hour" in low or re.search(r"\b1\s*hour\b", low):
            if has_reposted:
                return True, f"{_LINKEDIN_REPOST_FRESHNESS_PREFIX} {line}"
            return True, "Fresh"

    do = _parse_days_ago(low)
    if do is not None:
        if not week:
            return False, ""
        if do > 7:
            return False, ""
        if has_reposted:
            return True, f"{_LINKEDIN_REPOST_FRESHNESS_PREFIX} {line}"
        return True, line

    if "yesterday" in low:
        if not week:
            return False, ""
        if has_reposted:
            return True, f"{_LINKEDIN_REPOST_FRESHNESS_PREFIX} {line}"
        return True, line

    if "week" in low:
        return False, ""

    return False, ""


async def _linkedin_keep_and_freshness(
    posted: str,
    *,
    window: str,
    job_url: str,
    title: str,
    company: str,
    dedup_days: int,
) -> tuple[bool, str]:
    """
    Repost branding: use a visible 'Reposted — …' label only when LinkedIn says reposted
    and (no prior row for title+company in the dedup window, or the stored URL differs).
    Otherwise classify as if the post were not a repost (same URL already deduped earlier).
    """
    low = (posted or "").lower()
    if "reposted" not in low:
        return _classify_linkedin_freshness(posted, window=window)

    prev = await linkedin_url_for_title_company(
        title, company, dedup_days=max(1, int(dedup_days))
    )
    cur = (job_url or "").strip()
    qualify_repost_label = prev is None or prev.strip() != cur
    if qualify_repost_label:
        return _classify_linkedin_freshness(posted, window=window)

    plain = re.sub(r"\breposted\b", "Posted", posted, flags=re.I)
    return _classify_linkedin_freshness(plain, window=window)


# LinkedIn applicant insight lines only (not "thank all applicants" / legal text).
_LINKEDIN_APPLICANT_RE = re.compile(
    r"(?:"
    r"over\s+\d+\s+applicants?"
    r"|be\s+among\s+the\s+first\s+[\d,]+\s+applicants?"
    r"|\d+\s*-\s*\d+\s+applicants?"
    r"|\d+\+?\s+applicants?"
    r")",
    re.I,
)


def _extract_linkedin_applicant_count(text: str) -> str:
    """Return only the applicant-stat phrase (e.g. 'Over 100 applicants'), or ''."""
    if not text:
        return ""
    m = _LINKEDIN_APPLICANT_RE.search(text)
    return m.group(0).strip() if m else ""


async def _extract_right_panel_detail(page: Any) -> dict[str, str]:
    """Read posted time, description, applicant line, and apply button from the job detail panel."""
    out: dict[str, str] = {
        "posted_time": "",
        "applicant_count": "",
        "apply_type": "unknown",
        "job_description": "",
    }
    try:
        data = await page.evaluate(
            """
            () => {
              function extractApplicantCount(s) {
                if (!s) return '';
                const patterns = [
                  /over\\s+\\d+\\s+applicants?/gi,
                  /be\\s+among\\s+the\\s+first\\s+[\\d,]+\\s+applicants?/gi,
                  /\\d+\\s*-\\s*\\d+\\s+applicants?/gi,
                  /\\d+\\+?\\s+applicants?/gi
                ];
                for (const re of patterns) {
                  const m = s.match(re);
                  if (m && m[0]) return m[0].trim();
                }
                return '';
              }
              function extractJobDescription() {
                const sels = [
                  '.jobs-description-content__text',
                  '.jobs-description__text',
                  '.jobs-box__html-content',
                  'div[class*="jobs-description-content"]',
                  'div[class*="jobs-description__container"]',
                  'article.jobs-description',
                  '#job-details'
                ];
                for (const sel of sels) {
                  const el = document.querySelector(sel);
                  if (el) {
                    const t = (el.innerText || '').trim();
                    if (t.length > 80) return t.substring(0, 14000);
                  }
                }
                return '';
              }
              const top = document.querySelector(
                '.jobs-unified-top-card, [class*="jobs-details-top-card"], ' +
                'section[class*="job-details-jobs-unified-top-card"]'
              );
              const root = top || document.querySelector('.jobs-details__main') ||
                document.querySelector('.jobs-search__job-details') || document.body;
              const blob = (root && root.innerText) ? root.innerText : '';
              const topOnly = (top && top.innerText) ? top.innerText : blob;
              const lines = blob.split(/\\r?\\n/).map(s => s.trim()).filter(Boolean);
              let posted = '';
              for (const line of lines) {
                const l = line.toLowerCase();
                if (l.includes('applicant')) continue;
                if (l.includes('ago') || (l.includes('reposted') &&
                    (l.includes('day') || l.includes('hour') || l.includes('minute')))) {
                  posted = line;
                  break;
                }
              }
              const applicants = extractApplicantCount(topOnly);
              let applyType = 'unknown';
              const btn = document.querySelector(
                'button.jobs-apply-button, button[data-test-id="job-apply-button"], ' +
                'button.jobs-apply-button--top-card'
              );
              if (btn) {
                const bt = (btn.innerText || '').toLowerCase();
                if (bt.includes('easy apply')) applyType = 'easy_apply';
                else if (bt.includes('apply')) applyType = 'external';
              }
              const jobDescription = extractJobDescription();
              return {
                posted_time: posted,
                applicant_count: applicants,
                apply_type: applyType,
                job_description: jobDescription
              };
            }
            """
        )
        if isinstance(data, dict):
            out["posted_time"] = str(data.get("posted_time") or "").strip()
            raw_app = str(data.get("applicant_count") or "").strip()
            out["applicant_count"] = _extract_linkedin_applicant_count(raw_app)
            at = str(data.get("apply_type") or "unknown")
            if at in ("easy_apply", "external"):
                out["apply_type"] = at
            raw_desc = str(data.get("job_description") or "").strip()
            if raw_desc:
                out["job_description"] = clip_plain_description(
                    raw_desc.replace("\r\n", "\n"), 8000
                )
    except Exception:
        logger.debug("LinkedIn right panel extract failed", exc_info=True)
    return out


async def _click_job_in_results_list(page: Any, job_id: str, job_url: str) -> bool:
    """Click the left-panel card link for this job so the right panel updates."""
    jid = (job_id or "").strip()
    if jid:
        sel = f'a[href*="/jobs/view/{jid}"]'
    else:
        m = re.search(r"/jobs/view/(\d+)", job_url or "", re.I)
        if not m:
            return False
        sel = f'a[href*="/jobs/view/{m.group(1)}"]'
    try:
        loc = page.locator(sel).first
        if await loc.count() == 0:
            return False
        await loc.click(timeout=15_000)
        return True
    except Exception:
        logger.debug("LinkedIn click job card failed sel=%s", sel, exc_info=True)
        return False


async def _wait_right_panel_after_click(page: Any, prev_title: str) -> None:
    try:
        ttl = page.locator(
            ".jobs-unified-top-card h1, .jobs-details-top-card h1, "
            "h1[class*='job-details-jobs-unified-top-card__title'], "
            ".jobs-unified-top-card__job-title"
        ).first
        if await ttl.count() > 0:
            for _ in range(25):
                await page.wait_for_timeout(120)
                try:
                    t = (await ttl.inner_text()).strip()
                    if t and t != prev_title:
                        break
                except Exception:
                    break
    except Exception:
        pass
    await page.wait_for_timeout(1500)


async def _read_detail_panel_title(page: Any) -> str:
    try:
        ttl = page.locator(
            ".jobs-unified-top-card h1, .jobs-details-top-card h1, "
            "h1[class*='job-details-jobs-unified-top-card__title']"
        ).first
        if await ttl.count() > 0:
            return (await ttl.inner_text()).strip()
    except Exception:
        pass
    return ""


def _lower_blob(html: str) -> str:
    return html.lower() if html else ""


def _url_suggests_login(url: str) -> bool:
    u = (url or "").lower()
    return any(
        x in u
        for x in (
            "/login",
            "/checkpoint/",
            "/uas/login",
            "authwall",
        )
    )


def _is_login_wall(html: str, url: str) -> bool:
    if _url_suggests_login(url):
        return True
    b = _lower_blob(html)
    return any(m in b for m in _LOGIN_MARKERS)


def _is_no_results(html: str) -> bool:
    b = _lower_blob(html)
    return any(m in b for m in _NO_RESULTS_MARKERS)


def _is_challenge_or_captcha_url(page_url: str) -> bool:
    """True only on LinkedIn security challenge URLs — avoids false positives from page HTML."""
    u = (page_url or "").lower()
    return "/checkpoint" in u or "/challenge" in u


def _log_parse_zero_jobs(html: str, page_url: str, where: str) -> None:
    """When no cards matched: discover_selectors report + HTML prefix for debugging DOM vs login wall."""
    try:
        report = discover_selectors(html)
    except Exception:
        logger.warning(
            "LinkedIn parse 0 jobs [%s] page_url=%s — discover_selectors failed",
            where,
            page_url,
            exc_info=True,
        )
        return
    prefix = (html or "")[:_PARSE_DEBUG_HTML_PREFIX].replace("\n", " ")
    logger.warning(
        "LinkedIn parse 0 jobs [%s] page_url=%s discover_selectors=%s html_prefix=%s",
        where,
        page_url,
        report,
        prefix,
    )


def _linkedin_credentials(app_cfg: dict[str, Any] | None) -> tuple[str, str] | None:
    if not app_cfg:
        return None
    email = str(app_cfg.get("linkedin_email") or "").strip()
    password = str(app_cfg.get("linkedin_password") or "").strip()
    if not email or not password:
        return None
    return email, password


async def _fill_first_matching(page: Any, selectors: list[str], value: str) -> bool:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                await loc.fill(value)
                return True
        except Exception:
            continue
    return False


async def _click_first_matching(page: Any, selectors: list[str]) -> bool:
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                await loc.click()
                return True
        except Exception:
            continue
    return False


async def _wait_post_login(page: Any, status_callback: Any, *, phase: str) -> bool:
    """After submit: handle checkpoint/2FA wait (user may need to complete in browser)."""
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=90_000)
    except PlaywrightTimeoutError:
        pass
    await asyncio.sleep(1.5)
    url = (page.url or "").lower()
    if "/checkpoint" in url or "challenge" in url:
        await _notify_status(
            status_callback,
            "LinkedIn: complete verification (2FA, email link, or CAPTCHA) in the browser.",
        )
        for _ in range(90):
            await asyncio.sleep(2.0)
            u = (page.url or "").lower()
            html = await page.content()
            if "/checkpoint" not in u and "/login" not in u and not _is_login_wall(html, page.url):
                await _notify_status(status_callback, "LinkedIn: verification step cleared.")
                return True
        logger.warning("LinkedIn %s: checkpoint wait timed out", phase)
        return False
    return True


async def _submit_linkedin_login(
    page: Any,
    email: str,
    password: str,
    status_callback: Any,
) -> bool:
    await _notify_status(
        status_callback,
        "Signing in to LinkedIn with your saved credentials from settings…",
    )
    await page.goto(
        "https://www.linkedin.com/login",
        wait_until="domcontentloaded",
        timeout=90_000,
    )
    await asyncio.sleep(0.6)

    user_ok = await _fill_first_matching(
        page,
        ["#username", 'input[name="session_key"]', 'input[id="username"]'],
        email,
    )
    pass_ok = await _fill_first_matching(
        page,
        ["#password", 'input[name="session_password"]', 'input[id="password"]'],
        password,
    )
    if not user_ok or not pass_ok:
        logger.warning("LinkedIn login: username/password fields not found")
        await _notify_status(
            status_callback,
            "LinkedIn login form not found — complete sign-in manually in the browser.",
        )
        return False

    clicked = await _click_first_matching(
        page,
        [
            'button[type="submit"]',
            "button.btn__primary--large",
            'button[data-litms-control-urn="login-submit"]',
        ],
    )
    if not clicked:
        logger.warning("LinkedIn login: submit button not found")
        return False

    ok = await _wait_post_login(page, status_callback, phase="login")
    if not ok:
        return False
    html = await page.content()
    pu = page.url
    if _is_login_wall(html, pu):
        await _notify_status(
            status_callback,
            "LinkedIn sign-in did not complete — check email/password or finish verification in the browser.",
        )
        return False
    await _notify_status(status_callback, "LinkedIn: signed in.")
    return True


async def _ensure_linkedin_session(
    page: Any,
    app_cfg: dict[str, Any] | None,
    status_callback: Any,
) -> None:
    creds = _linkedin_credentials(app_cfg)
    if not creds:
        return
    email, password = creds
    await page.goto(
        "https://www.linkedin.com/feed/",
        wait_until="domcontentloaded",
        timeout=90_000,
    )
    await asyncio.sleep(0.8)
    html = await page.content()
    url = page.url
    if not _is_login_wall(html, url):
        await _notify_status(status_callback, "LinkedIn: session already active.")
        return
    await _submit_linkedin_login(page, email, password, status_callback)


async def _notify_status(
    status_callback: Callable[..., Any] | None, msg: str
) -> None:
    if not status_callback:
        return
    try:
        r = status_callback(msg)
        if asyncio.iscoroutine(r):
            await r
    except Exception:
        logger.debug("status_callback failed", exc_info=True)


async def _wait_after_navigation(page: Any) -> None:
    try:
        await page.wait_for_load_state("networkidle", timeout=60_000)
    except PlaywrightTimeoutError:
        logger.debug("networkidle timeout; continuing with DOM state")
    try:
        await page.wait_for_selector(JOB_CARD_SELECTOR, timeout=25_000)
    except PlaywrightTimeoutError:
        logger.debug("job card selector wait timeout")


async def _wait_captcha_resolve(page: Any, status_callback: Any) -> bool:
    """Poll up to ~120s while on /checkpoint or /challenge; until jobs UI or clear page."""
    await _notify_status(
        status_callback,
        "LinkedIn security check — complete any challenge in the browser if shown",
    )
    for _ in range(60):
        await asyncio.sleep(2.0)
        html = await page.content()
        url = page.url
        if _is_challenge_or_captcha_url(url):
            continue
        if _url_suggests_login(url):
            continue
        if _is_login_wall(html, url):
            continue
        if _is_no_results(html):
            return True
        try:
            await page.wait_for_selector(JOB_CARD_SELECTOR, timeout=5_000)
            return True
        except PlaywrightTimeoutError:
            continue
    return False


async def _scroll_list_once(page: Any) -> None:
    """
    LinkedIn job lists are virtualized: scroll last card into view + wheel + step + bottom.
    """
    try:
        n = await page.locator(JOB_CARD_SELECTOR).count()
        if n > 0:
            await page.locator(JOB_CARD_SELECTOR).nth(n - 1).scroll_into_view_if_needed(
                timeout=10_000
            )
    except Exception as e:
        logger.debug("scroll_into_view last job card: %s", e)
    await asyncio.sleep(0.4)
    try:
        list_sel = (
            ".jobs-search-results-list, .scaffold-layout__list, "
            "[class*='jobs-search-results__list']"
        )
        box = page.locator(list_sel).first
        if await box.count() > 0:
            b = await box.bounding_box()
            if b and b.get("width", 0) > 0:
                await page.mouse.move(
                    b["x"] + b["width"] / 2,
                    b["y"] + b["height"] / 2,
                )
                await page.mouse.wheel(0, 700)
    except Exception as e:
        logger.debug("mouse wheel on list: %s", e)
    try:
        await page.evaluate(_SCROLL_JS_STEP)
    except Exception as e:
        logger.debug("scroll step evaluate: %s", e)
    try:
        await page.evaluate(_SCROLL_JS_STEP)
    except Exception as e:
        logger.debug("scroll 2nd step: %s", e)
    try:
        await page.evaluate(_SCROLL_JS)
    except Exception as e:
        logger.debug("scroll evaluate: %s", e)


async def _pagination_present(page: Any) -> bool:
    """Logged-in jobs search often uses numbered pages instead of infinite scroll."""
    sels = (
        ".jobs-search-pagination__indicator-button",
        "button.jobs-search-pagination__button--next",
        "button.artdeco-pagination__button--next",
        "nav.jobs-search-pagination",
        "ul.artdeco-pagination",
    )
    for sel in sels:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                return True
        except Exception:
            continue
    return False


async def _linkedin_active_page_number(page: Any) -> int | None:
    """Read current page from the active pagination indicator (aria-label like 'Page 2')."""
    try:
        loc = page.locator(
            "button.jobs-search-pagination__indicator-button--active"
        ).first
        if await loc.count() > 0:
            aria = (await loc.get_attribute("aria-label")) or ""
            m = re.search(r"Page\s*(\d+)", aria, re.I)
            if m:
                return int(m.group(1))
        loc2 = page.locator(
            "[aria-current='true'].jobs-search-pagination__indicator-button"
        ).first
        if await loc2.count() > 0:
            aria = (await loc2.get_attribute("aria-label")) or ""
            m = re.search(r"Page\s*(\d+)", aria, re.I)
            if m:
                return int(m.group(1))
    except Exception:
        logger.debug("linkedin active page parse failed", exc_info=True)
    return None


async def _wait_for_pagination_page(
    page: Any, expected_page: int, *, timeout_ms: float = 8_000.0
) -> None:
    """Wait until the active indicator shows expected_page, then stabilize; else sleep fallback."""
    deadline = time.monotonic() + (timeout_ms / 1000.0)
    while time.monotonic() < deadline:
        cur = await _linkedin_active_page_number(page)
        if cur == expected_page:
            await asyncio.sleep(0.4)
            return
        await asyncio.sleep(0.12)
    # Fallback: LinkedIn may update list without changing aria immediately
    await asyncio.sleep(3.0)
    try:
        await page.wait_for_selector(JOB_CARD_SELECTOR, timeout=15_000)
    except PlaywrightTimeoutError:
        logger.debug("job cards after pagination wait timeout")


async def _click_next_jobs_page(page: Any, target_page: int) -> bool:
    """
    Go to the given results page (1-based). Prefer:
    button[aria-label='Page N'] / .jobs-search-pagination__indicator-button, then Next chevron.
    """
    if target_page < 1:
        return False
    before = await _linkedin_active_page_number(page)
    if before == target_page:
        return True

    # 1) Accessible name "Page N" (matches <button aria-label="Page 2" class="...indicator-button">)
    try:
        role_btn = page.get_by_role(
            "button", name=re.compile(rf"^Page\s+{target_page}\s*$", re.I)
        )
        if await role_btn.count() > 0:
            first = role_btn.first
            if not await first.is_disabled():
                logger.info(
                    "LinkedIn pagination: get_by_role Page %s (active=%s)",
                    target_page,
                    before,
                )
                await first.click(timeout=15_000)
                await _wait_for_pagination_page(page, target_page)
                after = await _linkedin_active_page_number(page)
                await _wait_after_navigation(page)
                if after == target_page or after is None:
                    return True
                logger.warning(
                    "LinkedIn pagination: role click active=%s expected=%s",
                    after,
                    target_page,
                )
    except Exception as e:
        logger.debug("get_by_role Page %s failed: %s", target_page, e)

    # 2) Exact Page N via CSS (double/single quotes for aria-label)
    page_selectors = (
        f'button.jobs-search-pagination__indicator-button[aria-label="Page {target_page}"]',
        f'button[aria-label="Page {target_page}"]',
        f"button.jobs-search-pagination__indicator-button[aria-label='Page {target_page}']",
    )
    for sel in page_selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() == 0:
                continue
            if await loc.is_disabled():
                continue
            logger.info(
                "LinkedIn pagination: page.click Page %s (selector=%s, active=%s)",
                target_page,
                sel[:80],
                before,
            )
            await page.click(sel, timeout=15_000)
            await _wait_for_pagination_page(page, target_page)
            after = await _linkedin_active_page_number(page)
            await _wait_after_navigation(page)
            if after == target_page or after is None:
                return True
            logger.warning(
                "LinkedIn pagination: after Page %s click, active=%s (expected %s)",
                target_page,
                after,
                target_page,
            )
        except Exception as e:
            logger.debug("click Page %s failed: %s", target_page, e)

    # 3) Non-active indicator whose visible text is the page number (ellipsis layout)
    try:
        loc = page.locator(
            "button.jobs-search-pagination__indicator-button:not("
            ".jobs-search-pagination__indicator-button--active)"
        ).filter(has_text=re.compile(rf"^\s*{target_page}\s*$"))
        if await loc.count() > 0:
            first = loc.first
            if not await first.is_disabled():
                logger.info(
                    "LinkedIn pagination: clicking non-active indicator %s (active=%s)",
                    target_page,
                    before,
                )
                await first.click(timeout=15_000)
                await _wait_for_pagination_page(page, target_page)
                after = await _linkedin_active_page_number(page)
                await _wait_after_navigation(page)
                if after == target_page or after is None:
                    return True
    except Exception as e:
        logger.debug("indicator text click failed: %s", e)

    # 4) Next chevron (single-step only)
    can_use_next = (before is not None and target_page == before + 1) or (
        before is None and target_page == 2
    )
    if can_use_next:
        next_selectors = (
            "button.jobs-search-pagination__button--next",
            "button.artdeco-pagination__button--next",
            'button[aria-label="Next"]',
            'button[aria-label="Next page"]',
        )
        for sel in next_selectors:
            try:
                loc = page.locator(sel).first
                if await loc.count() == 0:
                    continue
                if await loc.is_disabled():
                    continue
                logger.info(
                    "LinkedIn pagination: clicking Next (want page %s, was %s)",
                    target_page,
                    before,
                )
                await page.click(sel, timeout=15_000)
                await _wait_for_pagination_page(page, target_page)
                after = await _linkedin_active_page_number(page)
                await _wait_after_navigation(page)
                if after == target_page or after is None:
                    return True
            except Exception as e:
                logger.debug("Next click failed: %s", e)
    else:
        logger.debug(
            "LinkedIn pagination: skip Next (want page %s, active=%s)",
            target_page,
            before,
        )

    logger.warning(
        "LinkedIn pagination: could not navigate to page %s (active=%s)",
        target_page,
        before,
    )
    return False


async def _collect_jobs_for_combo(
    page: Any,
    url: str,
    role: str,
    loc: str,
    status_callback: Any,
    app_cfg: dict[str, Any] | None,
    title_company_registry: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    dedup_days = max(1, int((app_cfg or {}).get("dedup_days") or 7))
    li_window = "week" if bool((app_cfg or {}).get("linkedin_posted_past_week")) else "24h"
    tc_reg: dict[tuple[str, str], dict[str, Any]] = (
        title_company_registry if title_company_registry is not None else {}
    )

    await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
    await _wait_after_navigation(page)

    html = await page.content()
    purl = page.url

    if _is_login_wall(html, purl):
        creds = _linkedin_credentials(app_cfg)
        if creds:
            email, password = creds
            await _notify_status(
                status_callback,
                "LinkedIn session required — signing in with saved credentials…",
            )
            if await _submit_linkedin_login(page, email, password, status_callback):
                await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                await _wait_after_navigation(page)
                html = await page.content()
                purl = page.url
        if _is_login_wall(html, purl):
            await _notify_status(
                status_callback,
                "LinkedIn requires login. Add credentials in settings or sign in in the browser window.",
            )
            return []

    if _is_challenge_or_captcha_url(purl):
        ok = await _wait_captcha_resolve(page, status_callback)
        if not ok:
            return []
        html = await page.content()
        purl = page.url
        if _is_login_wall(html, purl):
            creds = _linkedin_credentials(app_cfg)
            if creds:
                email, password = creds
                if await _submit_linkedin_login(page, email, password, status_callback):
                    await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                    await _wait_after_navigation(page)
                    html = await page.content()
                    purl = page.url
            if _is_login_wall(html, purl):
                await _notify_status(
                    status_callback,
                    "LinkedIn requires login. Add credentials in settings or sign in in the browser.",
                )
                return []

    if _is_no_results(html):
        await _notify_status(
            status_callback,
            f"No LinkedIn results for {role} in {loc}.",
        )
        return []

    collected: list[dict[str, Any]] = []
    logged_empty_parse = False
    use_pagination = await _pagination_present(page)
    consecutive_stale_pages = 0

    async def scroll_and_parse_page(
        page_idx: int,
    ) -> dict[str, dict[str, Any]] | None:
        nonlocal logged_empty_parse
        merged: dict[str, dict[str, Any]] = {}
        max_cycles = _SCROLL_CYCLES_PER_PAGE if use_pagination else _SCROLL_CYCLES_GUEST
        stall = 0
        for cycle in range(max_cycles):
            html = await page.content()
            purl = page.url
            if _is_challenge_or_captcha_url(purl):
                ok = await _wait_captcha_resolve(page, status_callback)
                if not ok:
                    return None
                html = await page.content()
                purl = page.url

            batch = parse_linkedin_jobs(html)
            if not batch and not logged_empty_parse:
                logged_empty_parse = True
                _log_parse_zero_jobs(html, purl, f"{role} / {loc} p{page_idx} b{cycle + 1}")

            before = len(merged)
            for job in batch:
                u = str(job.get("url") or "").strip()
                if u and u not in merged:
                    merged[u] = dict(job)
            new_cards = len(merged) - before
            await _notify_status(
                status_callback,
                f"LinkedIn: page {page_idx} scroll {cycle + 1}: +{new_cards} new on this page "
                f"(page total {len(merged)}), run total {len(collected)} jobs.",
            )
            if cycle > 0:
                if new_cards == 0:
                    stall += 1
                    if stall >= _SCROLL_STALL_BEFORE_STOP:
                        break
                else:
                    stall = 0
            await _scroll_list_once(page)
            await asyncio.sleep(2.5)
        return merged

    async def process_jobs_on_current_page(page_idx: int) -> tuple[bool, int]:
        nonlocal consecutive_stale_pages
        before_run_total = len(collected)
        page_cards = await scroll_and_parse_page(page_idx)
        if page_cards is None:
            return True, 0
        if not page_cards:
            return False, 0

        urls_new: list[dict[str, Any]] = []
        seen_skip = 0
        for u, job in page_cards.items():
            if await url_exists(u, dedup_days=dedup_days):
                seen_skip += 1
            else:
                urls_new.append(job)

        before_left_filter = len(urls_new)
        urls_new = _filter_linkedin_apply_preference(urls_new, app_cfg)
        skipped_left_easy_apply = before_left_filter - len(urls_new)
        include_ea = bool((app_cfg or {}).get("linkedin_include_easy_apply"))

        # Clear order: total on page → DB dedup → Easy Apply skip (if off) → remainder to click.
        parts: list[str] = [
            f"LinkedIn page {page_idx}: {len(page_cards)} cards on page",
            f"{seen_skip} already in DB",
        ]
        if not include_ea:
            parts.append(
                f"{skipped_left_easy_apply} Easy Apply on list card (skipped before click)"
            )
        else:
            parts.append("Include Easy Apply ON (left card not used to skip)")
        parts.append(f"{len(urls_new)} to click → right panel (freshness / apply type)")
        parts.append(f"run total {before_run_total} jobs kept so far")
        await _notify_status(status_callback, " — ".join(parts) + ".")

        include_reposts = bool((app_cfg or {}).get("linkedin_include_reposts"))
        freshness_pass_count = 0
        prev_title = await _read_detail_panel_title(page)

        for job in urls_new:
            if len(collected) >= LINKEDIN_TARGET_JOBS:
                break
            jid = str(job.get("job_id") or "").strip()
            jurl = str(job.get("url") or "").strip()
            clicked = await _click_job_in_results_list(page, jid, jurl)
            if not clicked:
                logger.warning("LinkedIn: could not click job card %s", jurl[:80])
                continue

            await _wait_right_panel_after_click(page, prev_title)
            await asyncio.sleep(random.uniform(0.5, 1.5))
            prev_title = await _read_detail_panel_title(page)
            try:
                desc_loc = page.locator(
                    ".jobs-description-content__text, .jobs-box__html-content, "
                    "[class*='jobs-description-content']"
                ).first
                if await desc_loc.count() > 0:
                    await desc_loc.scroll_into_view_if_needed(timeout=5_000)
                    await page.wait_for_timeout(400)
            except Exception:
                pass

            detail = await _extract_right_panel_detail(page)
            posted = str(detail.get("posted_time") or "").strip()
            applicant_c = str(detail.get("applicant_count") or "").strip()
            apply_t = str(detail.get("apply_type") or "unknown")

            keep, freshness = await _linkedin_keep_and_freshness(
                posted,
                window=li_window,
                job_url=jurl,
                title=str(job.get("title") or ""),
                company=str(job.get("company") or ""),
                dedup_days=dedup_days,
            )
            row = {
                **job,
                "posted_time": posted,
                "applicant_count": applicant_c,
                "apply_type": apply_t,
                "freshness": freshness,
                "source": "linkedin",
                "job_description": str(detail.get("job_description") or "").strip(),
            }
            if not keep:
                continue
            if not include_reposts and _linkedin_freshness_is_repost_label(freshness):
                continue

            freshness_pass_count += 1
            filtered = _filter_linkedin_apply_preference([row], app_cfg)
            if not filtered:
                continue
            out = filtered[0]
            out["freshness"] = freshness
            if await _linkedin_try_title_company_dedup(out, tc_reg, status_callback):
                continue
            collected.append(out)

        if len(urls_new) > 0 and freshness_pass_count == 0:
            consecutive_stale_pages += 1
        else:
            consecutive_stale_pages = 0

        added_this_page = len(collected) - before_run_total
        await _notify_status(
            status_callback,
            f"LinkedIn page {page_idx} done: +{added_this_page} added this page "
            f"(run total {len(collected)} jobs).",
        )

        return False, freshness_pass_count

    stop_all = False
    try:
        if use_pagination:
            for page_idx in range(1, LINKEDIN_MAX_PAGES + 1):
                if stop_all or len(collected) >= LINKEDIN_TARGET_JOBS:
                    break
                abort, _ = await process_jobs_on_current_page(page_idx)
                if abort:
                    stop_all = True
                    break
                if consecutive_stale_pages >= CONSECUTIVE_STALE_PAGES:
                    await _notify_status(
                        status_callback,
                        f"LinkedIn: stopping — {CONSECUTIVE_STALE_PAGES} consecutive pages "
                        "with no jobs passing freshness.",
                    )
                    break
                if page_idx < LINKEDIN_MAX_PAGES:
                    if not await _click_next_jobs_page(page, page_idx + 1):
                        await _notify_status(
                            status_callback,
                            "LinkedIn: no further pages (or next disabled).",
                        )
                        break
                    await asyncio.sleep(_POST_PAGE_NAV_SLEEP_SEC)
                    try:
                        await page.wait_for_selector(JOB_CARD_SELECTOR, timeout=20_000)
                    except PlaywrightTimeoutError:
                        logger.debug("job cards after pagination wait timeout")
        else:
            abort, _ = await process_jobs_on_current_page(1)
            if abort:
                stop_all = True

        await _notify_status(
            status_callback,
            f"LinkedIn: {len(collected)} jobs for {role} / {loc}.",
        )
        return collected
    except Exception as e:
        if collected and _linkedin_playwright_disconnect(e):
            logger.warning(
                "LinkedIn: %s mid-scrape for %s / %s; returning %s jobs already collected",
                type(e).__name__,
                role,
                loc,
                len(collected),
            )
            await _notify_status(
                status_callback,
                f"LinkedIn: browser session ended mid-run — saving {len(collected)} jobs "
                f"collected so far for {role} / {loc}.",
            )
            return collected
        raise


async def scrape_linkedin(
    page: Any,
    roles: list[str],
    locations: list[str],
    status_callback: Callable[..., Any] | None,
    *,
    experience: str = "any",
    employment_types: list[str] | None = None,
    app_cfg: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Navigate each role×location: parse the left job list, dedup URLs, click each new job
    to read the right panel (posted time, applicants, apply type), then filter by freshness.
    Uses linkedin_email / linkedin_password from app_cfg when set (dashboard) to sign in.
    """
    await _ensure_linkedin_session(page, app_cfg, status_callback)
    exp = (experience or "any").strip().lower()
    jt_cfg = (
        employment_types
        if employment_types is not None
        else (app_cfg.get("linkedin_employment_types") if app_cfg else None)
    )
    combos = [(r, l) for r in roles for l in locations]
    collected: list[dict[str, Any]] = []
    linkedin_title_company_registry: dict[tuple[str, str], dict[str, Any]] = {}
    if combos:
        li_age = (
            "past week (7 days)"
            if bool((app_cfg or {}).get("linkedin_posted_past_week"))
            else "last 24 hours only"
        )
        await _notify_status(
            status_callback,
            f"LinkedIn time filter: {li_age} (change under “Job age on LinkedIn” in settings).",
        )

    for i, (role, loc) in enumerate(combos):
        await _notify_status(
            status_callback,
            f"Searching LinkedIn for {role} in {loc}...",
        )
        url = _search_url(role, loc, exp, jt_cfg, app_cfg)
        try:
            jobs = await _collect_jobs_for_combo(
                page,
                url,
                role,
                loc,
                status_callback,
                app_cfg,
                linkedin_title_company_registry,
            )
            for item in jobs:
                item["search_role"] = role
                item["search_location"] = loc
            collected.extend(jobs)
            logger.info("LinkedIn %s / %s: %s jobs", role, loc, len(jobs))
        except Exception:
            logger.exception("LinkedIn scrape failed for %s / %s", role, loc)
            await _notify_status(
                status_callback,
                f"LinkedIn error for {role} / {loc} — check Chrome CDP and login.",
            )

        if i < len(combos) - 1:
            await asyncio.sleep(random.uniform(3.0, 8.0))

    return collected


async def search_linkedin(
    roles: list[str],
    locations: list[str],
    *,
    experience: str = "any",
    employment_types: list[str] | None = None,
    emit: EmitFn | None = None,
    app_cfg: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    async with playwright_browser_session(app_cfg) as session:
        page = await session.new_page()
        try:
            return await scrape_linkedin(
                page,
                roles,
                locations,
                emit,
                experience=experience,
                employment_types=employment_types,
                app_cfg=app_cfg,
            )
        finally:
            await session.close_own_pages()
