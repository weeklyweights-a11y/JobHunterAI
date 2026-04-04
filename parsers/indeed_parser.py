"""Extract job rows from Indeed SERP HTML."""

from __future__ import annotations

from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup
from bs4.element import Tag

_BASE = "https://www.indeed.com"


def _job_id_from_href(href: str) -> str:
    """Prefer jk= query param, else empty."""
    try:
        u = urlparse(href or "")
        q = parse_qs(u.query)
        v = q.get("jk", [""])[0]
        return str(v) if v else ""
    except Exception:
        return ""


def _closest_job_card(a: Tag) -> Tag | None:
    p = a.find_parent("div", class_="job_seen_beacon")
    if isinstance(p, Tag):
        return p
    p = a.find_parent("div", class_="cardOutline")
    if isinstance(p, Tag):
        return p
    # Common fallback: container div around the title link
    p2 = a.find_parent("div")
    return p2 if isinstance(p2, Tag) else None


def _append_job_from_anchor(
    a: Tag,
    seen: set[str],
    out: list[dict[str, str]],
) -> None:
    href = str(a.get("href") or "").strip()
    if not href:
        return
    if "clk?" in href and "/rc/clk" not in href and "/pagead" not in href:
        return
    if href.startswith("/"):
        full = urljoin(_BASE, href.split("&")[0])
    elif href.startswith("http"):
        full = href.split("&")[0]
    else:
        return
    if "/viewjob" not in full and "indeed.com" not in full:
        return
    if full in seen:
        return
    seen.add(full)
    title = a.get_text(strip=True)
    if not title:
        return
    card = _closest_job_card(a) or a.find_parent("td")
    company = ""
    location = ""
    if card:
        co = card.select_one(
            "span[data-testid='company-name'], .companyName, [data-testid='company-name']"
        )
        if co:
            company = co.get_text(strip=True)
        loc_el = card.select_one(
            "div[data-testid='text-location'], span[data-testid='text-location'], "
            ".companyLocation, [class*='companyLocation']"
        )
        if loc_el:
            location = loc_el.get_text(" ", strip=True)
    job_id = str(a.get("data-jk") or "").strip() or _job_id_from_href(full)
    out.append(
        {
            "title": title,
            "company": company,
            "url": full,
            "apply_type": "unknown",
            "location": location,
            "job_id": job_id,
        }
    )


def parse_indeed_jobs_html(html: str) -> list[dict[str, str]]:
    """Return dicts with title, company, url, apply_type, location, job_id."""
    soup = BeautifulSoup(html, "lxml")
    out: list[dict[str, str]] = []
    seen: set[str] = set()

    primary = (
        "a.jcs-JobTitle, a.jobTitle, h2.jobTitle a, "
        "a[href*='/viewjob'], a[href*='/rc/clk'], a[href*='/pagead/clk']"
    )
    for a in soup.select(primary):
        if isinstance(a, Tag):
            _append_job_from_anchor(a, seen, out)

    if not out:
        for a in soup.select("a[data-jk]"):
            if not isinstance(a, Tag):
                continue
            href = str(a.get("href") or "").strip()
            if not href:
                continue
            if (
                "jk=" not in href
                and "/viewjob" not in href.lower()
                and "/clk" not in href
            ):
                continue
            _append_job_from_anchor(a, seen, out)

    return out
