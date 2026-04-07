"""Parse Jobright job detail HTML: embedded JSON in ``#jobright-helper-job-detail-info`` or ``__NEXT_DATA__``."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from bs4 import BeautifulSoup

from parsers.ats_posted_time import accept_normalized_posted_string

logger = logging.getLogger(__name__)


def _first_str(d: dict[str, Any], *keys: str) -> str:
    for k in keys:
        v = d.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def _pick_real_job_url(job: dict[str, Any]) -> str:
    return _first_str(
        job,
        "originalUrl",
        "original_url",
        "applyLink",
        "apply_link",
        "applyUrl",
        "apply_url",
        "externalUrl",
        "external_url",
    )


def _normalize_posted_time(publish_time: str) -> str:
    s = (publish_time or "").strip()
    if not s:
        return ""
    if re.match(r"^\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2}:\d{2}", s):
        s = s.replace(" ", "T", 1)
    norm = accept_normalized_posted_string(s)
    return norm or ""


def _payload_from_parsed_json(data: Any) -> dict[str, Any] | None:
    if not isinstance(data, dict):
        return None
    jr = data.get("jobResult") or data.get("job_result")
    cr = data.get("companyResult") or data.get("company_result")
    if isinstance(jr, dict) and isinstance(cr, dict):
        out = dict(data)
        out["jobResult"] = jr
        out["companyResult"] = cr
        return out
    return None


def _walk_find_job_payload(obj: Any, depth: int = 0) -> dict[str, Any] | None:
    if depth > 24:
        return None
    p = _payload_from_parsed_json(obj)
    if p is not None:
        return p
    if isinstance(obj, dict):
        for v in obj.values():
            p = _walk_find_job_payload(v, depth + 1)
            if p is not None:
                return p
    elif isinstance(obj, list):
        for x in obj:
            p = _walk_find_job_payload(x, depth + 1)
            if p is not None:
                return p
    return None


def _parse_jobright_from_next_data(
    soup: BeautifulSoup,
    *,
    search_role: str,
    search_location: str,
) -> dict[str, str] | None:
    nd = soup.find("script", id="__NEXT_DATA__")
    if not nd:
        return None
    txt = (nd.string or nd.get_text() or "").strip()
    if not txt:
        return None
    try:
        tree = json.loads(txt)
    except (json.JSONDecodeError, TypeError):
        logger.debug("Jobright __NEXT_DATA__ JSON failed", exc_info=True)
        return None
    if isinstance(tree, dict):
        props = tree.get("props")
        if isinstance(props, dict):
            page_props = props.get("pageProps")
            if isinstance(page_props, dict):
                ds = page_props.get("dataSource")
                if isinstance(ds, dict):
                    payload = _payload_from_parsed_json(ds) or _walk_find_job_payload(ds)
                    if payload is not None:
                        return _jobright_payload_to_row(
                            payload,
                            search_role=search_role,
                            search_location=search_location,
                        )
    payload = _walk_find_job_payload(tree)
    if payload is None:
        return None
    return _jobright_payload_to_row(
        payload,
        search_role=search_role,
        search_location=search_location,
    )


def parse_jobright_detail_html(
    html: str,
    *,
    search_role: str = "",
    search_location: str = "",
) -> dict[str, str] | None:
    """
    Extract one job row from Jobright detail HTML.
    ``url`` in the returned dict is the real ATS apply URL when present.
    """
    soup = BeautifulSoup(html or "", "lxml")
    tag = soup.find("script", id="jobright-helper-job-detail-info")
    raw_json = (tag.string or tag.get_text() or "").strip() if tag else ""
    if raw_json:
        try:
            data = json.loads(raw_json)
        except json.JSONDecodeError:
            logger.debug("Jobright helper script JSON decode failed", exc_info=True)
        else:
            payload = _payload_from_parsed_json(data) or _walk_find_job_payload(data)
            if payload is not None:
                row = _jobright_payload_to_row(
                    payload,
                    search_role=search_role,
                    search_location=search_location,
                )
                if row:
                    return row
    return _parse_jobright_from_next_data(
        soup,
        search_role=search_role,
        search_location=search_location,
    )


def _jobright_payload_to_row(
    data: dict[str, Any],
    *,
    search_role: str,
    search_location: str,
) -> dict[str, str] | None:
    jr = data.get("jobResult") or data.get("job_result") or {}
    cr = data.get("companyResult") or data.get("company_result") or {}
    if not isinstance(jr, dict) or not isinstance(cr, dict):
        return None

    title = _first_str(jr, "jobTitle", "job_title", "title")
    company = _first_str(cr, "companyName", "company_name", "name")
    real_url = _pick_real_job_url(jr)
    if not title or not real_url:
        return None

    loc = _first_str(jr, "jobLocation", "job_location", "location")
    posted = _normalize_posted_time(_first_str(jr, "publishTime", "publish_time"))
    fresh = _first_str(jr, "publishTimeDesc", "publish_time_desc")
    salary = _first_str(jr, "salaryDesc", "salary_desc", "salary")
    emp = _first_str(jr, "employmentType", "employment_type")
    seniority = _first_str(jr, "jobSeniority", "job_seniority", "seniority")
    work_model = _first_str(jr, "workModel", "work_model")
    summary = _first_str(jr, "jobSummary", "job_summary", "summary")
    jid = _first_str(jr, "jobId", "job_id", "id")
    applicants = _first_str(jr, "applicantsCount", "applicants_count")
    is_remote = jr.get("isRemote")
    remote_s = "true" if is_remote is True else ("false" if is_remote is False else "")

    score = data.get("displayScore")
    score_s = str(score).strip() if score is not None else ""

    desc_parts = [summary] if summary else []
    extras: list[str] = []
    if work_model:
        extras.append(f"Work model: {work_model}")
    if remote_s:
        extras.append(f"Remote: {remote_s}")
    if score_s:
        extras.append(f"Jobright match score: {score_s}")
    if extras:
        desc_parts.append("\n".join(extras))
    job_description = "\n\n".join(p for p in desc_parts if p).strip()

    return {
        "title": title,
        "company": company or "Unknown",
        "url": real_url,
        "location": loc or "Unknown",
        "source": "jobright",
        "apply_type": "external",
        "job_id": jid,
        "posted_time": posted,
        "freshness": fresh,
        "applicant_count": applicants,
        "job_description": job_description,
        "seniority": seniority,
        "salary": salary,
        "employment_type": emp,
        "search_role": search_role,
        "search_location": search_location,
    }
