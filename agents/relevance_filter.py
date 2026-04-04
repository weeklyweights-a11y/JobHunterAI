"""LLM batch filter: job relevance from title + company only."""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from typing import Any

from langchain_core.messages import HumanMessage

from agents.llm_setup import build_chat_model, is_llm_configured_for_filter

logger = logging.getLogger(__name__)

_BATCH_SIZE = 24
_BATCH_TIMEOUT_SEC = 120.0
_REMOVED_LOG_MAX = 100

_LINE_DECISION = re.compile(
    r"^\s*(\d+)\s*[:.)-]?\s*(YES|NO)\b",
    re.IGNORECASE,
)


def _message_text(msg: Any) -> str:
    c = getattr(msg, "content", "")
    if isinstance(c, str):
        return c
    if isinstance(c, list):
        parts: list[str] = []
        for block in c:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get("type") == "text":
                parts.append(str(block.get("text") or ""))
            elif isinstance(block, dict) and "text" in block:
                parts.append(str(block["text"]))
        return "\n".join(parts)
    return str(c or "")


def _parse_relevance_response(text: str) -> dict[int, bool]:
    """Map 1-based line index -> keep."""
    out: dict[int, bool] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _LINE_DECISION.match(line)
        if not m:
            continue
        idx = int(m.group(1))
        keep = m.group(2).upper() == "YES"
        out[idx] = keep
    return out


def _experience_hint(exp: str) -> str:
    e = (exp or "any").strip().lower()
    if e == "any":
        return "No fixed title-level target — judge each job on its own from the title."
    return (
        f"The user selected experience level **{e}** in search settings. "
        f"If the job title clearly targets a very different level (e.g. user wants senior but the title is Intern or Entry-level only), lean toward NO. "
        f"If level is unclear from the title alone, still allow YES when the title matches the user's role keywords."
    )


def _build_prompt(
    roles: list[str],
    jobs_batch: list[dict[str, Any]],
    experience: str,
) -> str:
    role_line = ", ".join(r for r in roles if isinstance(r, str) and r.strip()) or "your search roles"
    lines = [
        f"I am looking for these types of roles: {role_line}",
        "",
        _experience_hint(experience),
        "",
        "For each numbered job below you have only TITLE and COMPANY (no full description).",
        "Decide if the job is **relevant** to my role intent from title and company name alone.",
        "Be generous for AI/ML/data science/LLM/GenAI/research engineering / applied ML roles.",
        "Say NO for clearly unrelated work suggested by the title (generic CRUD backend-only, pure frontend, "
        "DevOps-only, QA, recruiting, sales, etc.) unless the title clearly suggests AI/ML or aligned work.",
        "",
        "Reply with EXACTLY one line per job, format:",
        "N YES",
        "N NO",
        "(Examples: `1 YES` then `2 NO`)",
        "",
    ]
    for i, j in enumerate(jobs_batch, start=1):
        title = str(j.get("title") or "").replace("\r", " ").replace("\n", " ").strip() or "(no title)"
        company = str(j.get("company") or "").replace("\r", " ").replace("\n", " ").strip() or "(unknown company)"
        lines.append(f"{i}. TITLE: {title}")
        lines.append(f"   COMPANY: {company}")
        lines.append("")
    return "\n".join(lines)


async def _filter_one_batch(
    llm: Any,
    batch: list[dict[str, Any]],
    roles: list[str],
    batch_index: int,
    experience: str,
) -> list[dict[str, Any]]:
    prompt = _build_prompt(roles, batch, experience)
    try:
        msg = await asyncio.wait_for(
            llm.ainvoke([HumanMessage(content=prompt)]),
            timeout=_BATCH_TIMEOUT_SEC,
        )
    except Exception as exc:
        logger.warning(
            "LLM relevance filter failed for batch %s — keeping all jobs",
            batch_index,
        )
        logger.debug("Batch %s relevance error: %s", batch_index, exc)
        return list(batch)
    text = _message_text(msg)
    decisions = _parse_relevance_response(text)
    kept: list[dict[str, Any]] = []
    for i, job in enumerate(batch):
        n = i + 1
        if n in decisions:
            if not decisions[n]:
                continue
            kept.append(dict(job))
        else:
            kept.append(dict(job))
    return kept


async def filter_relevant_jobs(
    jobs: list[dict[str, Any]],
    roles: list[str],
    llm_provider: str,
    llm_api_key: str,
    *,
    experience: str = "any",
    emit: Callable[[str], Awaitable[None]] | None = None,
    enabled: bool = True,
) -> list[dict[str, Any]]:
    """
    Batch LLM filter using title + company only.
    Does not read job_description or assign seniority. On batch failure, keeps batch unchanged.
    """
    if not jobs:
        return jobs
    if not enabled:
        return jobs
    if not is_llm_configured_for_filter(llm_provider, llm_api_key):
        logger.info("No LLM configured — skipping relevance filter")
        if emit is not None:
            await emit("No LLM configured — skipping relevance filter")
        return jobs

    try:
        llm = build_chat_model(llm_provider, llm_api_key)
    except Exception as exc:
        logger.warning("Relevance filter: could not build LLM (%s) — keeping all jobs", exc)
        if emit is not None:
            await emit("Relevance filter: LLM unavailable — keeping all scraped jobs")
        return jobs

    total = len(jobs)
    n_batches = (total + _BATCH_SIZE - 1) // _BATCH_SIZE
    if emit is not None:
        await emit(
            f"Filtering {total} jobs for relevance (title + company)… batch 1/{n_batches}"
        )

    out: list[dict[str, Any]] = []
    for bi in range(n_batches):
        start = bi * _BATCH_SIZE
        chunk = jobs[start : start + _BATCH_SIZE]
        if emit is not None and bi > 0:
            await emit(
                f"Filtering {total} jobs for relevance (title + company)… "
                f"batch {bi + 1}/{n_batches}"
            )
        out.extend(await _filter_one_batch(llm, chunk, roles, bi + 1, experience))

    removed = total - len(out)
    kept_ids = {id(x) for x in out}
    removed_titles = [
        str(j.get("title") or "").strip() or "(no title)"
        for j in jobs
        if id(j) not in kept_ids
    ]

    logger.info(
        "Relevance filter: %s jobs → %s relevant, %s removed",
        total,
        len(out),
        removed,
    )
    if removed_titles:
        preview = ", ".join(removed_titles[:_REMOVED_LOG_MAX])
        if len(removed_titles) > _REMOVED_LOG_MAX:
            preview += f", … (+{len(removed_titles) - _REMOVED_LOG_MAX} more)"
        logger.info("Removed: %s", preview)
    if emit is not None:
        await emit(
            f"Relevance filter: {total} jobs → {len(out)} kept, {removed} removed (LLM, title+company)"
        )

    return out
