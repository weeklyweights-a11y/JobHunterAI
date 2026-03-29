"""Build LangChain chat models from dashboard config (Phase 3)."""

from __future__ import annotations

import logging
import os

from langchain_core.language_models.chat_models import BaseChatModel

logger = logging.getLogger(__name__)

_MISSING_KEY = (
    "No API key configured for {provider}. Set it in the dashboard under LLM Provider."
)


def _require_api_key(provider: str, api_key: str | None) -> str:
    key = (api_key or "").strip()
    if not key:
        raise ValueError(_MISSING_KEY.format(provider=provider))
    return key


def build_chat_model(provider: str, api_key: str | None) -> BaseChatModel:
    """Return a LangChain chat model for browser-use (spec Step 1)."""
    p = (provider or "gemini").strip().lower()
    logger.debug("Building chat model for provider=%s", p)

    if p == "gemini":
        from langchain_google_genai import ChatGoogleGenerativeAI

        key = _require_api_key(p, api_key)
        return ChatGoogleGenerativeAI(
            model="gemini-2.0-flash",
            google_api_key=key,
        )

    if p == "openai":
        from langchain_openai import ChatOpenAI

        key = _require_api_key(p, api_key)
        return ChatOpenAI(model="gpt-4o-mini", api_key=key)

    if p == "anthropic":
        from langchain_anthropic import ChatAnthropic

        key = _require_api_key(p, api_key)
        return ChatAnthropic(
            model="claude-sonnet-4-20250514",
            api_key=key,
        )

    if p == "ollama":
        from langchain_ollama import ChatOllama

        _require_api_key(p, api_key)
        base = os.getenv("OLLAMA_BASE_URL")
        model = os.getenv("OLLAMA_MODEL", "llama3.2-vision")
        kwargs: dict = {"model": model}
        if base:
            kwargs["base_url"] = base
        return ChatOllama(**kwargs)

    raise ValueError(f"Unknown LLM provider: {provider}")
