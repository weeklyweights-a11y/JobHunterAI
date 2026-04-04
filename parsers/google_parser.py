"""Parse Google search result HTML for ATS job links."""

from __future__ import annotations

import logging
import re
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup
from bs4.element import Tag

logger = logging.getLogger(__name__)

# Match any host under these roots (boards.*, jobs.*, custom subdomains, etc.).
_ATS_HOST_SUFFIXES: tuple[tuple[str, str], ...] = (
    ("greenhouse.io", "greenhouse"),
    ("lever.co", "lever"),
    ("ashbyhq.com", "ashby"),
    ("smartrecruiters.com", "smartrecruiters"),
    ("workable.com", "workable"),
    ("myworkdayjobs.com", "workday"),
    ("jobvite.com", "jobvite"),
    ("bamboohr.com", "bamboohr"),
    ("icims.com", "icims"),
)

# Google changes wrappers frequently — try all; collect unique ATS URLs.
_RESULT_BLOCK_SELECTORS = (
    "div.g",
    "div.MjjYud",
    "div[data-hveid]",
    "div.tF2Cxc",
    "div.yuRUbf",
)

_SNIPPET_SELECTORS = (
    "div.VwiC3b",
    "span.aCOpRe",
    "div[data-sncf]",
    "div.IsZvec",
    "span.st",
    "div.lEBKkf",
)

_H3_AREA_SELECTORS = (
    "#rso h3",
    "#center_col h3",
    "#search h3",
    "div#rso h3",
)


def _clean_google_href(href: str) -> str:
    h = (href or "").strip()
    if not h:
        return ""
    if h.startswith("/url?"):
        q = parse_qs(urlparse(h).query)
        return str((q.get("q") or [""])[0]).strip()
    return h


def _platform_from_url(url: str) -> str:
    host = (urlparse(url).netloc or "").lower()
    if not host:
        return ""
    for suffix, name in _ATS_HOST_SUFFIXES:
        if host == suffix or host.endswith("." + suffix):
            return name
    return ""


def _company_from_url(url: str) -> str:
    u = urlparse(url)
    host = (u.netloc or "").lower()
    parts = [p for p in (u.path or "").split("/") if p]
    slug = ""
    if "greenhouse.io" in host and parts:
        slug = parts[0]
    elif "lever.co" in host and parts:
        slug = parts[0]
    elif "ashbyhq.com" in host and parts:
        slug = parts[0]
    elif "smartrecruiters.com" in host and parts:
        slug = parts[0]
    elif "workable.com" in host and parts:
        slug = parts[0]
    elif "myworkdayjobs.com" in host and parts:
        slug = parts[0]
    elif "jobvite.com" in host and parts:
        slug = parts[0]
    elif "bamboohr.com" in host and parts:
        slug = parts[0]
    elif "icims.com" in host and parts:
        slug = parts[0]
    if not slug:
        return ""
    return slug.replace("-", " ").replace("_", " ").title()


def _title_company_from_title(txt: str) -> tuple[str, str]:
    t = (txt or "").strip()
    if not t:
        return "", ""
    for sep in (" @ ", " - ", " at "):
        if sep in t:
            left, right = t.split(sep, 1)
            return left.strip(), right.strip()
    return t, ""


def _location_from_snippet(snippet: str) -> str:
    s = (snippet or "").strip()
    if not s:
        return "Unknown"
    low = s.lower()
    for k in ("remote", "hybrid", "on-site", "onsite"):
        if k in low:
            return "Remote" if k == "remote" else k.title()
    m = re.search(r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z]{2})", s)
    if m:
        return m.group(1).strip()
    m2 = re.search(r"\b(United States|USA|US)\b", s, re.I)
    if m2:
        return "United States"
    return "Unknown"


def _url_from_h3(h3: Tag) -> str:
    """
    Google uses both <a><h3/></a> and <h3><a/></h3>. Walk up/down for a real href.
    """
    p = h3.find_parent("a", href=True)
    if isinstance(p, Tag):
        u = _clean_google_href(str(p.get("href") or ""))
        if u:
            return u
    inner = h3.find("a", href=True)
    if isinstance(inner, Tag):
        u = _clean_google_href(str(inner.get("href") or ""))
        if u:
            return u
    cur: Tag | None = h3
    for _ in range(14):
        if cur is None:
            break
        if cur.name == "a" and cur.get("href"):
            u = _clean_google_href(str(cur.get("href") or ""))
            if u and not u.startswith("#"):
                return u
        cur = cur.parent if isinstance(cur.parent, Tag) else None
    return ""


def _snippet_near_h3(h3: Tag) -> str:
    container: Tag | None = h3
    for _ in range(10):
        if not isinstance(container, Tag):
            break
        for sn_sel in _SNIPPET_SELECTORS:
            sn = container.select_one(sn_sel)
            if isinstance(sn, Tag):
                t = sn.get_text(" ", strip=True)
                if t and len(t) > 8:
                    return t
        container = container.parent if isinstance(container.parent, Tag) else None
    return ""


def _snippet_in_block(block: Tag) -> str:
    for sn_sel in _SNIPPET_SELECTORS:
        sn = block.select_one(sn_sel)
        if isinstance(sn, Tag):
            t = sn.get_text(" ", strip=True)
            if t:
                return t
    return ""


def _collect_h3_candidates(soup: BeautifulSoup) -> list[Tag]:
    seen: set[int] = set()
    out: list[Tag] = []
    for area_sel in _H3_AREA_SELECTORS:
        for h3 in soup.select(area_sel):
            if not isinstance(h3, Tag):
                continue
            i = id(h3)
            if i in seen:
                continue
            seen.add(i)
            out.append(h3)
    if out:
        return out
    for h3 in soup.select("h3"):
        if isinstance(h3, Tag):
            out.append(h3)
    return out


def _row_from_h3(h3: Tag, url: str, seen: set[str]) -> dict[str, str] | None:
    if not url or url in seen:
        return None
    src = _platform_from_url(url)
    if not src:
        return None
    seen.add(url)
    raw_title = h3.get_text(" ", strip=True)
    title, co_from_title = _title_company_from_title(raw_title)
    snippet = _snippet_near_h3(h3)
    company = co_from_title or _company_from_url(url) or ""
    return {
        "title": title or raw_title,
        "company": company,
        "location": _location_from_snippet(snippet),
        "url": url,
        "source": src,
        "apply_type": "external",
        "job_id": "",
    }


def _row_from_block(block: Tag, seen: set[str]) -> dict[str, str] | None:
    h3 = block.select_one("h3")
    if not isinstance(h3, Tag):
        return None
    url = _url_from_h3(h3)
    if not url:
        a = block.select_one("a[href]")
        if isinstance(a, Tag):
            url = _clean_google_href(str(a.get("href") or ""))
    if not url or url in seen:
        return None
    src = _platform_from_url(url)
    if not src:
        return None
    seen.add(url)
    raw_title = h3.get_text(" ", strip=True)
    title, co_from_title = _title_company_from_title(raw_title)
    snippet = _snippet_in_block(block)
    company = co_from_title or _company_from_url(url) or ""
    return {
        "title": title or raw_title,
        "company": company,
        "location": _location_from_snippet(snippet),
        "url": url,
        "source": src,
        "apply_type": "external",
        "job_id": "",
    }


def parse_google_results_html(html: str) -> list[dict[str, str]]:
    """Return ATS-like rows: title/company/location/url/source. Only ATS platform URLs."""
    soup = BeautifulSoup(html, "lxml")
    out: list[dict[str, str]] = []
    seen: set[str] = set()

    for block_sel in _RESULT_BLOCK_SELECTORS:
        for block in soup.select(block_sel):
            if not isinstance(block, Tag):
                continue
            row = _row_from_block(block, seen)
            if row:
                out.append(row)

    h3_list = _collect_h3_candidates(soup)
    h3_considered = len(h3_list)
    for h3 in h3_list:
        url = _url_from_h3(h3)
        row = _row_from_h3(h3, url, seen)
        if row:
            out.append(row)

    ats_matched = len(out)
    logger.info(
        "Google parser: found %s h3 candidates, matched %s ATS URLs (after blocks+h3 pass)",
        h3_considered,
        ats_matched,
    )
    return out


def discover_google_selectors(html: str) -> dict[str, object]:
    """
    Debug helper for ATS Google parsing.
    Returns attempted selectors, match counts, and a body HTML prefix.
    """
    soup = BeautifulSoup(html, "lxml")
    attempted = {
        "result_blocks": ", ".join(_RESULT_BLOCK_SELECTORS),
        "title": "h3",
        "link": "a[href] via h3 parent/child/walk-up",
        "snippet": ", ".join(_SNIPPET_SELECTORS),
        "h3_areas": ", ".join(_H3_AREA_SELECTORS),
        "fallback_links": "a.jcs-JobTitle, a.jobTitle, a[data-jk]",
    }
    counts: dict[str, int] = {
        "div.g": len(soup.select("div.g")),
        "h3": len(soup.select("h3")),
        "a[href]": len(soup.select("a[href]")),
        "snippet_candidates": len(
            soup.select(
                "div.VwiC3b, span.aCOpRe, div[data-sncf], div.IsZvec, span.st, div.lEBKkf"
            )
        ),
    }
    for sel in _RESULT_BLOCK_SELECTORS:
        key = sel.replace(" ", "_")[:40]
        counts[f"block:{key}"] = len(soup.select(sel))
    counts["h3_rso"] = len(soup.select("#rso h3"))
    counts["h3_center_col"] = len(soup.select("#center_col h3"))

    body = soup.body
    body_prefix = ""
    if isinstance(body, Tag):
        raw = str(body)
        body_prefix = raw[:3000]
    else:
        raw = soup.get_text(" ", strip=True)
        body_prefix = raw[:3000]
    return {
        "attempted_selectors": attempted,
        "counts": counts,
        "body_prefix": body_prefix,
    }
