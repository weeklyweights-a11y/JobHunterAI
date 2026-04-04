"""Pydantic models for API request and response bodies."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, HttpUrl, TypeAdapter, field_validator

from db_paths import LINKEDIN_EMPLOYMENT_TYPE_CODES, normalize_linkedin_employment_types

ExperienceLevel = Literal["any", "entry", "mid", "senior", "lead"]
LlmProvider = Literal["gemini", "openai", "anthropic", "ollama"]
ScheduleHours = Literal[2, 4, 6, 8, 12, 24]

_url_adapter = TypeAdapter(HttpUrl)


class ConfigIn(BaseModel):
    model_config = {"extra": "forbid"}

    roles: list[str] | None = None
    locations: list[str] | None = None
    experience: ExperienceLevel | None = None
    email_address: str | None = None
    email_app_password: str | None = None
    schedule_hours: ScheduleHours | None = None
    sources: dict[str, bool] | None = None
    career_pages: list[str] | None = None
    custom_sites: list[str] | None = None
    llm_provider: LlmProvider | None = None
    llm_api_key: str | None = None
    resume_path: str | None = None
    browser_cdp_url: str | None = None
    auto_run_enabled: bool | None = None
    linkedin_email: str | None = None
    linkedin_password: str | None = None
    linkedin_include_easy_apply: bool | None = None
    linkedin_posted_past_week: bool | None = None
    linkedin_include_reposts: bool | None = None
    linkedin_employment_types: list[str] | None = None
    filter_jobs_by_relevance_llm: bool | None = None
    ats_platforms: dict[str, bool] | None = None
    dedup_days: int | None = None
    ats_posted_within_days: int | None = None
    ats_google_max_serp_pages: int | None = None
    ats_captcha_wait_seconds: int | None = None

    @field_validator("linkedin_employment_types", mode="before")
    @classmethod
    def validate_linkedin_employment_types(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return None
        if not isinstance(v, list):
            return ["F"]
        seen: set[str] = set()
        out: list[str] = []
        for x in v:
            if not isinstance(x, str):
                continue
            u = x.strip().upper()
            if u in LINKEDIN_EMPLOYMENT_TYPE_CODES and u not in seen:
                seen.add(u)
                out.append(u)
        return out if out else ["F"]

    @field_validator("dedup_days", mode="before")
    @classmethod
    def clamp_dedup_days(cls, v: int | None) -> int | None:
        if v is None:
            return None
        try:
            n = int(v)
        except (TypeError, ValueError):
            return None
        return max(1, min(n, 365))

    @field_validator("ats_posted_within_days", mode="before")
    @classmethod
    def clamp_ats_posted_within_days(cls, v: int | None) -> int | None:
        if v is None:
            return None
        try:
            n = int(v)
        except (TypeError, ValueError):
            return None
        return max(0, min(n, 365))

    @field_validator("ats_google_max_serp_pages", mode="before")
    @classmethod
    def clamp_ats_google_max_serp_pages(cls, v: int | None) -> int | None:
        if v is None:
            return None
        try:
            n = int(v)
        except (TypeError, ValueError):
            return None
        return max(1, min(n, 50))

    @field_validator("ats_captcha_wait_seconds", mode="before")
    @classmethod
    def clamp_ats_captcha_wait_seconds(cls, v: int | None) -> int | None:
        if v is None:
            return None
        try:
            n = int(v)
        except (TypeError, ValueError):
            return None
        return max(30, min(n, 900))

    @field_validator("roles", "locations", mode="before")
    @classmethod
    def trim_string_lists(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return None
        return [s.strip() for s in v if isinstance(s, str) and s.strip()]

    @field_validator("career_pages", "custom_sites", mode="after")
    @classmethod
    def validate_url_lists(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return None
        out: list[str] = []
        for item in v:
            s = str(item).strip()
            if not s:
                continue
            try:
                out.append(str(_url_adapter.validate_python(s)))
            except ValueError:
                out.append(s)
        return out


class ConfigOut(BaseModel):
    id: int = 1
    roles: list[str]
    locations: list[str]
    experience: str
    email_address: str
    email_app_password: str
    schedule_hours: int
    sources: dict[str, bool]
    career_pages: list[str]
    custom_sites: list[str]
    llm_provider: str
    llm_api_key: str
    resume_path: str
    browser_cdp_url: str = ""
    auto_run_enabled: bool = True
    linkedin_email: str = ""
    linkedin_password: str = ""
    linkedin_include_easy_apply: bool = False
    linkedin_posted_past_week: bool = False
    linkedin_include_reposts: bool = False
    linkedin_employment_types: list[str] = ["F"]
    filter_jobs_by_relevance_llm: bool = True
    ats_platforms: dict[str, bool] = {}
    dedup_days: int = 7
    ats_posted_within_days: int = 7
    ats_google_max_serp_pages: int = 20
    ats_captcha_wait_seconds: int = 180

    @field_validator("linkedin_employment_types", mode="before")
    @classmethod
    def out_jt(cls, v: object) -> list[str]:
        return normalize_linkedin_employment_types(v)


class LlmKeyValidateIn(BaseModel):
    """Optional api_key: omit or empty to test the key already saved in config (Gemini only)."""

    model_config = {"extra": "forbid"}

    provider: LlmProvider = "gemini"
    api_key: str | None = None


class UrlPayload(BaseModel):
    url: str

    @field_validator("url", mode="after")
    @classmethod
    def normalize_url(cls, v: str) -> str:
        return str(_url_adapter.validate_python(v))


class AgentControlResponse(BaseModel):
    message: str


class StartAgentData(BaseModel):
    run_id: int
    status: str


class StopAgentData(BaseModel):
    status: str


class AgentStatusResponse(BaseModel):
    state: str
    progress: str
    run_id: int | None = None
    summary: dict | None = None
    duration_seconds: float | None = None


class JobStatsResponse(BaseModel):
    total: int
    today: int
    by_source: dict[str, int]
    by_role: dict[str, int]


class SchedulerStatusResponse(BaseModel):
    """GET /api/scheduler inner `data` object."""

    active: bool
    next_run: str | None = None
    interval_hours: int
    reason: str | None = None
