"""Extract job cards from LinkedIn job search HTML (Pattern A → B → C)."""

from __future__ import annotations

import logging
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from bs4.element import Tag

logger = logging.getLogger(__name__)

_BASE = "https://www.linkedin.com"
_SNIPPET_LEN = 2000

# Pattern A: logged-in — organic + promoted (LinkedIn adds extra classes for promoted slots)
_PATTERN_A_LI = (
    "li.jobs-search-results__list-item, "
    "li[class*='jobs-search-results__list-item'], "
    "ul[class*='jobs-search-results'] > li, "
    "li[class*='jobs-search-job-card'], "
    "li[class*='job-card-list__entity'], "
    "li.occludable-update, "
    "[data-occludable-update] li, "
    "li[class*='job-card-container']"
)
# Pattern B: guest — ul.jobs-search__results-list
_PATTERN_B_LI = "ul.jobs-search__results-list > li"
# Pattern C: scaffold list container
_PATTERN_C_LI = ".scaffold-layout__list-container li, " "ul.scaffold-layout__list > li"


def _detect_apply_type(card_root: Tag | None) -> str:
    """
    Easy Apply only when the list card shows the real badge/footer pill.
    Scanning the entire card text caused false positives (job copy, promos, etc.),
    which skipped external jobs before click. Unsure → external; right panel confirms.
    """
    if not isinstance(card_root, Tag):
        return "unknown"

    for sel in (
        "[class*='easy-apply']",
        "[class*='EasyApply']",
        "[data-test-job-card-easy-apply]",
        ".job-card-container__apply-method",
        ".job-card-list__easy-apply-badge",
        "li.job-card-container__footer-item",
        "span.job-card-container__footer-item",
    ):
        for el in card_root.select(sel):
            if not isinstance(el, Tag):
                continue
            t = el.get_text(" ", strip=True).lower()
            if not t:
                continue
            if "easy apply" not in t:
                continue
            if len(t) > 120:
                continue
            return "easy_apply"

    for footer_sel in (
        ".job-card-container__footer-container",
        "ul.job-card-container__footer-items",
        "[class*='job-card-container__footer']",
    ):
        foot = card_root.select_one(footer_sel)
        if foot and isinstance(foot, Tag):
            ft = foot.get_text(" ", strip=True).lower()
            if re.search(r"\beasy\s+apply\b", ft):
                return "easy_apply"
            break

    return "external"


def _normalize_job_url(href: str) -> str:
    h = (href or "").strip()
    if not h or "/jobs/view/" not in h:
        return ""
    full = urljoin(_BASE, h.split("?")[0])
    return full


def _linkedin_job_id_from_url(url: str) -> str:
    m = re.search(r"/jobs/view/(\d+)", url or "", re.I)
    return m.group(1) if m else ""


def _extract_location_from_card(card: Tag | None) -> str:
    """Workplace location from job card (e.g. 'Austin, TX (On-site)', 'United States (Remote)')."""
    if not isinstance(card, Tag):
        return ""
    for sel in (
        "li.job-card-container__metadata-item",
        "span.job-card-container__metadata-item",
        ".job-card-container__metadata-wrapper li",
        ".job-card-container__metadata-wrapper span",
        "div.job-card-container__metadata-wrapper span",
        "[class*='job-card-list__metadata']",
        "span.job-card-container__bullet",
    ):
        for el in card.select(sel):
            if not isinstance(el, Tag):
                continue
            txt = el.get_text(" ", strip=True)
            if not txt or len(txt) < 2:
                continue
            tl = txt.lower()
            if tl in ("promoted", "easy apply"):
                continue
            if any(
                x in tl
                for x in (
                    ",",
                    "remote",
                    "on-site",
                    "onsite",
                    "hybrid",
                    "(remote)",
                    "(on-site)",
                    "(hybrid)",
                )
            ):
                return txt
    for sel in (
        "li.job-card-container__metadata-item",
        "span.job-card-container__metadata-item",
    ):
        el = card.select_one(sel)
        if el and isinstance(el, Tag):
            txt = el.get_text(" ", strip=True)
            if txt and len(txt) > 1 and txt.lower() not in ("promoted", "easy apply"):
                return txt
    return ""


def _job_card_extras(url: str, card: Tag | None) -> dict[str, str]:
    return {
        "job_id": _linkedin_job_id_from_url(url),
        "location": _extract_location_from_card(card) if isinstance(card, Tag) else "",
    }


def _dedupe_repeated_title(title: str) -> str:
    """
    LinkedIn often nests the same title twice in one link; get_text() concatenates.
    Also strip trailing 'with verification' from accessibility labels.
    """
    t = (title or "").strip()
    if len(t) < 4:
        return t
    t = re.sub(r"\s+with verification\s*$", "", t, flags=re.IGNORECASE).strip()
    if len(t) < 4:
        return t
    half = len(t) // 2
    if half >= 2 and t[:half] == t[half:]:
        return t[:half].strip()
    # "TitleTitle" without space — scan for repeated prefix
    for i in range(len(t) // 2, 1, -1):
        chunk = t[:i]
        if t.startswith(chunk + chunk):
            return chunk.strip()
    return t


def _extract_title_from_card(card: Tag, link: Tag | None) -> str:
    """Prefer innermost title nodes; avoid whole <a.job-card-container__link> get_text."""
    for sel in (
        "[data-test-job-card-title]",
        "span.job-card-list__title",
        "h3.job-card-list__title",
        "h3.base-search-card__title",
        ".job-card-list__title--link",
        "h3 a span",
    ):
        el = card.select_one(sel)
        if el and isinstance(el, Tag):
            txt = el.get_text(strip=True)
            if txt and len(txt) > 1:
                return _dedupe_repeated_title(txt)
    if link and isinstance(link, Tag):
        inner = link.select_one(
            "span.job-card-list__title, [data-test-job-card-title], "
            "span[class*='job-card-list__title']"
        )
        if inner:
            txt = inner.get_text(strip=True)
            if txt:
                return _dedupe_repeated_title(txt)
        txt = link.get_text(strip=True)
        if txt:
            return _dedupe_repeated_title(txt)
    return ""


def _extract_company_from_card(card: Tag) -> str:
    """Logged-in cards use entity-lockup + job-card-container subtitles."""
    for sel in (
        "span.job-card-container__primary-description",
        "h4.base-search-card__subtitle",
        ".artdeco-entity-lockup__subtitle",
        "span.artdeco-entity-lockup__subtitle",
        "div.artdeco-entity-lockup__subtitle",
        "[class*='entity-lockup__subtitle']",
        "span.job-card-container__company-name",
        "a.job-card-container__subtitle",
        "span[class*='job-card-container__primary-description']",
        "h4[class*='subtitle']",
        ".job-card-container__primary-description",
    ):
        el = card.select_one(sel)
        if el and isinstance(el, Tag):
            txt = el.get_text(" ", strip=True)
            if txt and len(txt) > 1 and txt.lower() not in ("promoted", "easy apply"):
                return txt
    return ""


def _title_company_from_aria(aria: str) -> tuple[str, str]:
    """Parse 'Role with verification at Company' / 'Role at Company' from link aria-label."""
    if not aria:
        return "", ""
    a = aria.strip()
    if " at " not in a:
        t = re.sub(r"\s+with verification\s*$", "", a, flags=re.IGNORECASE).strip()
        return (t, "")
    idx = a.rfind(" at ")
    if idx <= 0:
        return a, ""
    left = a[:idx].strip()
    company = a[idx + 4 :].strip()
    left = re.sub(r"\s+with verification\s*$", "", left, flags=re.IGNORECASE).strip()
    return left, company


def _parse_cards_pattern_a(soup: BeautifulSoup) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for li in soup.select(_PATTERN_A_LI):
        if not isinstance(li, Tag):
            continue
        link = li.select_one("a.base-card__full-link[href*='jobs/view']")
        if not link or not link.get("href"):
            link = li.select_one("a[href*='/jobs/view/']")
        if not link or not link.get("href"):
            continue
        url = _normalize_job_url(str(link.get("href", "")))
        if not url or url in seen:
            continue
        aria = (link.get("aria-label") or "").strip()
        at, ac = _title_company_from_aria(aria)
        title = _extract_title_from_card(li, link)
        if not title and at:
            title = _dedupe_repeated_title(at)
        company = _extract_company_from_card(li)
        if not company and ac:
            company = ac
        if not title or not url:
            continue
        seen.add(url)
        row = {
            "title": title,
            "company": company,
            "url": url,
            "apply_type": _detect_apply_type(li),
        }
        row.update(_job_card_extras(url, li))
        out.append(row)
    return out


def _parse_cards_pattern_b(soup: BeautifulSoup) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for li in soup.select(_PATTERN_B_LI):
        if not isinstance(li, Tag):
            continue
        link = li.select_one("a.base-card__full-link[href*='jobs/view']")
        if not link or not link.get("href"):
            link = li.select_one("a[href*='/jobs/view/']")
        if not link or not link.get("href"):
            continue
        url = _normalize_job_url(str(link.get("href", "")))
        if not url or url in seen:
            continue
        aria = (link.get("aria-label") or "").strip()
        at, ac = _title_company_from_aria(aria)
        title = _extract_title_from_card(li, link)
        if not title and at:
            title = _dedupe_repeated_title(at)
        company = _extract_company_from_card(li)
        if not company and ac:
            company = ac
        if not title:
            title = _dedupe_repeated_title(link.get_text(strip=True) or "")
        if not title or not url:
            continue
        seen.add(url)
        row = {
            "title": title,
            "company": company,
            "url": url,
            "apply_type": _detect_apply_type(li),
        }
        row.update(_job_card_extras(url, li))
        out.append(row)
    return out


def _parse_cards_pattern_c(soup: BeautifulSoup) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    container = soup.select_one(".scaffold-layout__list-container")
    search_root = container or soup
    for li in search_root.select(_PATTERN_C_LI):
        if not isinstance(li, Tag):
            continue
        link = li.select_one("a[href*='/jobs/view/']")
        if not link or not link.get("href"):
            continue
        url = _normalize_job_url(str(link.get("href", "")))
        if not url or url in seen:
            continue
        aria = (link.get("aria-label") or "").strip()
        at, ac = _title_company_from_aria(aria)
        title = _extract_title_from_card(li, link)
        if not title and at:
            title = _dedupe_repeated_title(at)
        company = _extract_company_from_card(li)
        if not company and ac:
            company = ac
        if not title:
            title = _dedupe_repeated_title(link.get_text(strip=True) or "")
        if not title or not url:
            continue
        seen.add(url)
        row = {
            "title": title,
            "company": company,
            "url": url,
            "apply_type": _detect_apply_type(li),
        }
        row.update(_job_card_extras(url, li))
        out.append(row)
    return out


def _card_from_job_anchor(a: Tag) -> Tag | None:
    """Nearest list/card ancestor for a job link (promoted wrappers differ from organic <li>)."""
    for tag_name in ("li", "article"):
        p = a.find_parent(tag_name)
        if isinstance(p, Tag):
            return p
    p2 = a.find_parent(
        "div",
        class_=lambda c: bool(c) and any(
            x in str(c).lower()
            for x in ("job-card", "jobs-search", "entity-lockup", "base-card")
        ),
    )
    return p2 if isinstance(p2, Tag) else a.parent if isinstance(a.parent, Tag) else None


def _parse_cards_scoped_list(soup: BeautifulSoup) -> list[dict[str, str]]:
    """
    Every /jobs/view/ link under the main results list (organic + promoted/sponsored).
    Fills gaps when Pattern A <li> selectors miss promoted markup.
    """
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    roots = soup.select(
        ".scaffold-layout__list-container, "
        "ul.scaffold-layout__list, "
        "div.jobs-search-results-list, "
        "ul[class*='jobs-search-results__list'], "
        "div[class*='jobs-search-results-list'], "
        "ol[class*='jobs-search-results'], "
        "div[class*='scaffold-layout__list-container'] "
        "ul[class*='jobs-search-results']"
    )
    seen_roots: set[int] = set()
    for root in roots:
        if not isinstance(root, Tag):
            continue
        rid = id(root)
        if rid in seen_roots:
            continue
        seen_roots.add(rid)
        for a in root.select('a[href*="/jobs/view/"]'):
            if not isinstance(a, Tag):
                continue
            url = _normalize_job_url(str(a.get("href", "")))
            if not url or url in seen:
                continue
            card = _card_from_job_anchor(a)
            aria = (a.get("aria-label") or "").strip()
            at, ac = _title_company_from_aria(aria)
            title = ""
            company = ac or ""
            if card and isinstance(card, Tag):
                title = _extract_title_from_card(card, a)
                if not company:
                    company = _extract_company_from_card(card)
            if not title and at:
                title = _dedupe_repeated_title(at)
            if not title:
                title = _dedupe_repeated_title(a.get_text(" ", strip=True))
            if not title or len(title) < 2:
                continue
            seen.add(url)
            root_for_apply = card if isinstance(card, Tag) else a
            row = {
                "title": title,
                "company": company,
                "url": url,
                "apply_type": _detect_apply_type(root_for_apply),
            }
            row.update(_job_card_extras(url, card if isinstance(card, Tag) else None))
            out.append(row)
    return out


def _parse_cards_pattern_d(soup: BeautifulSoup) -> list[dict[str, str]]:
    """
    DOM-churn fallback: any anchor to /jobs/view/, walk ancestors for title/company.
    Matches logged-in layouts where list <li> classes changed but job links remain.
    """
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for a in soup.select('a[href*="/jobs/view/"]'):
        if not isinstance(a, Tag):
            continue
        url = _normalize_job_url(str(a.get("href", "")))
        if not url or url in seen:
            continue

        aria = (a.get("aria-label") or "").strip()
        at, ac = _title_company_from_aria(aria)
        title = ""
        company = ac or ""
        if at:
            title = _dedupe_repeated_title(at)
        if not title:
            title = _dedupe_repeated_title(a.get_text(" ", strip=True))

        p: Tag | None = a.parent
        for _ in range(18):
            if not isinstance(p, Tag):
                break
            if not title or len(title) < 2:
                h = p.select_one(
                    "[data-test-job-card-title], span.job-card-list__title, "
                    "h3.base-search-card__title, h3.job-card-list__title, "
                    ".job-card-list__title--link span, "
                    "h3, .job-card-list__title"
                )
                if h:
                    t = _dedupe_repeated_title(h.get_text(" ", strip=True))
                    if t:
                        title = t
            if not company:
                co = p.select_one(
                    "span.job-card-container__primary-description, "
                    ".artdeco-entity-lockup__subtitle, "
                    "span.artdeco-entity-lockup__subtitle, "
                    "h4.base-search-card__subtitle, "
                    ".job-card-container__primary-description, "
                    "[class*='entity-lockup__subtitle'], "
                    "[class*='company-name'], "
                    "span.job-card-container__primary-description"
                )
                if co:
                    ct = co.get_text(" ", strip=True)
                    if ct and ct.lower() not in ("promoted", "easy apply"):
                        company = ct
            p = p.parent

        title = _dedupe_repeated_title(title)
        if not title or len(title) < 2:
            continue
        seen.add(url)
        card_for_apply = a.find_parent("li")
        if not isinstance(card_for_apply, Tag):
            card_for_apply = a.find_parent("div")
        apply_type = _detect_apply_type(card_for_apply if isinstance(card_for_apply, Tag) else a)
        row = {
            "title": title,
            "company": company,
            "url": url,
            "apply_type": apply_type,
        }
        row.update(
            _job_card_extras(
                url,
                card_for_apply if isinstance(card_for_apply, Tag) else None,
            )
        )
        out.append(row)
    return out


def _merge_job_dicts(soup: BeautifulSoup) -> list[dict[str, str]]:
    """Union A→B→C→scoped→D; later patterns add missing URLs and can fill empty company."""
    by_url: dict[str, dict[str, str]] = {}
    for fn in (
        _parse_cards_pattern_a,
        _parse_cards_pattern_b,
        _parse_cards_pattern_c,
        _parse_cards_scoped_list,
        _parse_cards_pattern_d,
    ):
        for job in fn(soup):
            u = str(job.get("url") or "").strip()
            if not u:
                continue
            if u not in by_url:
                by_url[u] = job
                continue
            prev = by_url[u]
            if not (prev.get("company") or "").strip() and (job.get("company") or "").strip():
                prev["company"] = job["company"]
            if not (prev.get("location") or "").strip() and (job.get("location") or "").strip():
                prev["location"] = job["location"]
            if not (prev.get("job_id") or "").strip() and (job.get("job_id") or "").strip():
                prev["job_id"] = job["job_id"]
            if (job.get("apply_type") == "easy_apply") or prev.get("apply_type") == "easy_apply":
                prev["apply_type"] = "easy_apply"
            by_url[u] = prev
    return list(by_url.values())


def parse_linkedin_jobs(html: str) -> list[dict[str, str]]:
    """Merge patterns so promoted + organic cards count; dicts have title, company, url, apply_type."""
    soup = BeautifulSoup(html, "lxml")
    jobs = _merge_job_dicts(soup)
    for j in jobs:
        j["title"] = _dedupe_repeated_title(str(j.get("title") or ""))
        jid = str(j.get("job_id") or "").strip()
        if not jid:
            j["job_id"] = _linkedin_job_id_from_url(str(j.get("url") or ""))
        j.setdefault("location", "")
    if jobs:
        logger.debug("LinkedIn parser merged %s unique job URLs", len(jobs))
    else:
        logger.warning("LinkedIn parser: no cards from any pattern; returning empty list")
    return jobs


def parse_linkedin_jobs_html(html: str) -> list[dict[str, str]]:
    """Alias for parse_linkedin_jobs (backward compatibility)."""
    return parse_linkedin_jobs(html)


def _list_area_snippet(soup: BeautifulSoup) -> str:
    for sel in (
        ".scaffold-layout__list-container",
        "ul[class*='jobs-search']",
        "ul.jobs-search__results-list",
        "[class*='jobs-search-results']",
    ):
        el = soup.select_one(sel)
        if el and isinstance(el, Tag):
            raw = el.prettify()
            return raw[:_SNIPPET_LEN] if len(raw) > _SNIPPET_LEN else raw
    body = soup.body
    if body:
        raw = body.get_text(" ", strip=True)
        return raw[:_SNIPPET_LEN] if len(raw) > _SNIPPET_LEN else raw
    return ""


def discover_selectors(html: str) -> dict[str, object]:
    """Debug report: per-pattern counts, merged total, sample, or HTML snippet."""
    soup = BeautifulSoup(html, "lxml")
    breakdown: dict[str, int] = {
        "A": len(_parse_cards_pattern_a(soup)),
        "B": len(_parse_cards_pattern_b(soup)),
        "C": len(_parse_cards_pattern_c(soup)),
        "scoped": len(_parse_cards_scoped_list(soup)),
        "D": len(_parse_cards_pattern_d(soup)),
    }
    merged = _merge_job_dicts(soup)
    if merged:
        sample = {
            "title": merged[0].get("title", ""),
            "company": merged[0].get("company", ""),
            "url": merged[0].get("url", ""),
        }
        return {
            "pattern": "merged",
            "card_count": len(merged),
            "pattern_breakdown": breakdown,
            "sample": sample,
            "html_snippet": None,
        }
    return {
        "pattern": "none",
        "card_count": 0,
        "pattern_breakdown": breakdown,
        "sample": None,
        "html_snippet": _list_area_snippet(soup),
    }
