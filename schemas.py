"""Pydantic models for API request and response bodies."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, HttpUrl, TypeAdapter, field_validator

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
