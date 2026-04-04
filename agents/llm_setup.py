"""Build LangChain chat models from dashboard config (Phase 3)."""

from __future__ import annotations

import asyncio
import logging
import os

from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import HumanMessage

logger = logging.getLogger(__name__)

_MISSING_KEY = (
    "No API key configured for {provider}. Set it in the dashboard, or set the provider "
    "env var (e.g. GOOGLE_API_KEY for Gemini in .env)."
)

_VERTEX_PROJECT_MISSING = (
    "JOBHUNTER_GEMINI_BACKEND=vertex requires GOOGLE_CLOUD_PROJECT (or GCP_PROJECT) in .env "
    "and Application Default Credentials (gcloud auth application-default login or "
    "GOOGLE_APPLICATION_CREDENTIALS)."
)


def _env_fallback_key(provider: str) -> str | None:
    if provider == "gemini":
        return os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if provider == "openai":
        return os.getenv("OPENAI_API_KEY")
    if provider == "anthropic":
        return os.getenv("ANTHROPIC_API_KEY")
    return None


def _require_api_key(provider: str, api_key: str | None) -> str:
    """Env vars win over the dashboard DB value so .env can override a stale saved key."""
    if provider == "ollama":
        dash = (api_key or "").strip()
        return dash if dash else "not-used"

    env_k = (_env_fallback_key(provider) or "").strip()
    dash_k = (api_key or "").strip()
    key = env_k or dash_k
    if not key:
        raise ValueError(_MISSING_KEY.format(provider=provider))
    return key


def build_chat_model(provider: str, api_key: str | None) -> BaseChatModel:
    """Return a LangChain chat model for browser-use (spec Step 1)."""
    p = (provider or "gemini").strip().lower()
    logger.debug("Building chat model for provider=%s", p)

    if p == "gemini":
        backend = _gemini_backend()
        if backend in ("vertex", "vertexai", "gcp"):
            return build_gemini_vertex()

        from langchain_google_genai import ChatGoogleGenerativeAI

        env_k = (_env_fallback_key(p) or "").strip()
        if env_k:
            logger.debug("Gemini: using API key from environment (GOOGLE_API_KEY / GEMINI_API_KEY)")
        elif (api_key or "").strip():
            logger.debug("Gemini: using API key from saved dashboard settings")
        key = _require_api_key(p, api_key)
        model_name = (os.getenv("JOBHUNTER_GEMINI_MODEL") or "gemini-2.0-flash").strip()
        return ChatGoogleGenerativeAI(
            model=model_name,
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


def is_llm_configured_for_filter(provider: str, api_key: str | None) -> bool:
    """True if we can build a chat model for post-scrape relevance filtering (title+company)."""
    p = (provider or "gemini").strip().lower()
    if p == "ollama":
        return True
    if p == "gemini":
        if _gemini_backend() in ("vertex", "vertexai", "gcp"):
            return bool(
                (os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or "").strip()
            )
        return bool((_env_fallback_key(p) or "").strip() or (api_key or "").strip())
    if p in ("openai", "anthropic"):
        return bool((_env_fallback_key(p) or "").strip() or (api_key or "").strip())
    return False


def _gemini_backend() -> str:
    return (os.getenv("JOBHUNTER_GEMINI_BACKEND") or "studio").strip().lower()


def _vertex_model_name() -> str:
    """Vertex model id: explicit JOBHUNTER_VERTEX_MODEL, else JOBHUNTER_GEMINI_MODEL, else stable default."""
    explicit = (os.getenv("JOBHUNTER_VERTEX_MODEL") or "").strip()
    if explicit:
        return explicit
    shared = (os.getenv("JOBHUNTER_GEMINI_MODEL") or "").strip()
    if shared:
        return shared
    # 1.5-* is absent from many newer Model Garden catalogs; 2.5 Flash is listed and works with LangChain.
    return "gemini-2.5-flash"


def _vertex_max_retries() -> int:
    """Attempts per LangChain ChatVertexAI / tenacity (includes the first try)."""
    raw = (os.getenv("JOBHUNTER_VERTEX_MAX_RETRIES") or "").strip()
    if not raw:
        return 12
    try:
        n = int(raw)
    except ValueError:
        return 12
    return max(1, min(n, 50))


def _vertex_wait_exponential_kwargs() -> dict[str, float]:
    """Backoff between retries; wider max than LangChain default (10s) helps 429 / quota recovery."""
    defaults = {"min": 5.0, "max": 120.0}
    out: dict[str, float] = {}
    min_raw = (os.getenv("JOBHUNTER_VERTEX_RETRY_BACKOFF_MIN") or "").strip()
    max_raw = (os.getenv("JOBHUNTER_VERTEX_RETRY_BACKOFF_MAX") or "").strip()
    if min_raw:
        try:
            out["min"] = max(0.5, float(min_raw))
        except ValueError:
            out["min"] = defaults["min"]
    else:
        out["min"] = defaults["min"]
    if max_raw:
        try:
            out["max"] = max(out["min"], float(max_raw))
        except ValueError:
            out["max"] = defaults["max"]
    else:
        out["max"] = max(out["min"], defaults["max"])
    return out


def _vertex_request_parallelism() -> int | None:
    """Optional cap on Vertex client parallelism (lower may reduce burst 429). Unset = library default."""
    raw = (os.getenv("JOBHUNTER_VERTEX_REQUEST_PARALLELISM") or "").strip()
    if not raw:
        return None
    try:
        n = int(raw)
    except ValueError:
        return None
    return max(1, min(n, 32))


def build_gemini_vertex() -> BaseChatModel:
    """Gemini via Vertex AI (GCP billing / project quotas, not AI Studio API key)."""
    from langchain_google_vertexai import ChatVertexAI

    project = (
        os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or ""
    ).strip()
    if not project:
        raise ValueError(_VERTEX_PROJECT_MISSING)
    location = (
        os.getenv("GOOGLE_CLOUD_LOCATION")
        or os.getenv("VERTEX_LOCATION")
        or "us-central1"
    ).strip()
    model = _vertex_model_name()
    transport = (os.getenv("JOBHUNTER_VERTEX_API_TRANSPORT") or "grpc").strip().lower()
    if transport not in ("grpc", "rest"):
        transport = "grpc"
    max_retries = _vertex_max_retries()
    wait_exp = _vertex_wait_exponential_kwargs()
    req_par = _vertex_request_parallelism()
    log_extra = ""
    if req_par is not None:
        log_extra = f", request_parallelism={req_par}"
    logger.info(
        "Gemini: Vertex AI (project=%s, location=%s, model=%s, transport=%s, "
        "max_retries=%s, retry_backoff_sec=[%s,%s]%s)",
        project,
        location,
        model,
        transport,
        max_retries,
        wait_exp["min"],
        wait_exp["max"],
        log_extra,
    )
    kwargs: dict = {
        "model": model,
        "project": project,
        "location": location,
        "max_retries": max_retries,
        "wait_exponential_kwargs": wait_exp,
        "api_transport": transport,
    }
    if req_par is not None:
        kwargs["request_parallelism"] = req_par
    return ChatVertexAI(**kwargs)


async def maybe_preflight_vertex_llm(llm: BaseChatModel) -> None:
    """One cheap Vertex call before Playwright opens; avoids about:blank when API/billing is off."""
    flag = (os.getenv("JOBHUNTER_VERTEX_PREFLIGHT") or "true").strip().lower()
    if flag in ("0", "false", "no", "off"):
        return
    if _gemini_backend() not in ("vertex", "vertexai", "gcp"):
        return
    timeout = 120.0
    try:
        timeout = float(os.getenv("JOBHUNTER_VERTEX_PREFLIGHT_TIMEOUT", "120"))
    except ValueError:
        pass
    logger.info("Vertex: preflight LLM (before browser opens)...")
    await asyncio.wait_for(
        llm.ainvoke([HumanMessage(content="Reply with exactly: ok")]),
        timeout=timeout,
    )
    logger.info("Vertex: preflight OK")
