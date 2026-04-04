"""Plain-text job description snippets from listing HTML (ATS, career pages, etc.)."""

from __future__ import annotations

import json
import re
from typing import Any

from bs4 import BeautifulSoup


def _clip(text: str, max_len: int) -> str:
    t = re.sub(r"\s+", " ", (text or "").strip())
    if len(t) <= max_len:
        return t
    return t[: max_len - 1].rsplit(" ", 1)[0] + "…"


def clip_plain_description(text: str, max_len: int = 8000) -> str:
    """Normalize whitespace and cap length for storage."""
    return _clip(text, max_len)


def html_fragment_to_plain_text(html: str, max_len: int = 8000) -> str:
    """Strip tags from an HTML fragment (e.g. Greenhouse API ``content`` field)."""
    if not (html or "").strip():
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text("\n", strip=True)
    return _clip(text, max_len)


def extract_job_description_from_html(html: str, url: str = "", *, max_len: int = 8000) -> str:
    """
    Best-effort description text for storage (and downstream use).
    Tries JSON-LD JobPosting, common ATS containers, then meta description.
    """
    if not (html or "").strip():
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    for s in soup.find_all("script", attrs={"type": lambda x: x and "ld+json" in str(x).lower()}):
        raw = s.string or s.get_text() or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        items: list[Any] = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            typ = item.get("@type")
            types = typ if isinstance(typ, list) else [typ]
            flat = {
                str(t or "").replace("https://schema.org/", "").split("/")[-1] for t in types
            }
            if "JobPosting" not in flat:
                continue
            desc = item.get("description")
            if isinstance(desc, str) and len(desc.strip()) > 40:
                return _clip(BeautifulSoup(desc, "html.parser").get_text(" ", strip=True), max_len)
            if isinstance(desc, dict) and isinstance(desc.get("value"), str):
                return _clip(desc["value"], max_len)

    selectors = [
        "div.jobs-description-content__text",
        "div.jobs-description__text",
        "div.jobs-box__html-content",
        "div[class*='job-description']",
        "div[class*='JobDescription']",
        "#job-description",
        "div[data-job-description]",
        "article.job-description",
        "div#content",
        "main",
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            t = el.get_text("\n", strip=True)
            if len(t) > 120:
                return _clip(t, max_len)

    meta = soup.find("meta", attrs={"name": re.compile(r"^description$", re.I)})
    if meta and meta.get("content"):
        c = str(meta["content"]).strip()
        if len(c) > 60:
            return _clip(c, max_len)

    return ""
