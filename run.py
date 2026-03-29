"""Run the JobHunter AI FastAPI server."""

from __future__ import annotations

import asyncio
import os
import sys


def _configure_runtime() -> None:
    """Windows: Proactor event loop for Playwright subprocesses; UTF-8 stdio for console logs."""
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except (OSError, ValueError):
                pass


_configure_runtime()

import uvicorn
from dotenv import load_dotenv

load_dotenv()


def main() -> None:
    # Default off: uvicorn reload spawns a child process where Playwright/asyncio on Windows breaks.
    reload_on = os.getenv("JOBHUNTER_RELOAD", "false").lower() in ("1", "true", "yes")
    try:
        port = int(os.getenv("JOBHUNTER_PORT", "8000"))
    except ValueError:
        port = 8000
    print(f"JobHunter AI — open http://localhost:{port} in your browser")
    if not (
        os.getenv("JOBHUNTER_CDP_URL", "").strip()
        or os.getenv("CHROME_CDP_URL", "").strip()
    ):
        print(
            "Browser tip: Without JOBHUNTER_CDP_URL, hunts open a separate Chromium window. "
            "For the same Chrome (new tabs only): start Chrome with "
            "--remote-debugging-port=9222, use that window for the dashboard, set "
            "Chrome CDP URL to http://127.0.0.1:9222 in settings, Save."
        )
    if (os.getenv("JOBHUNTER_GEMINI_BACKEND") or "").strip().lower() in (
        "vertex",
        "vertexai",
        "gcp",
    ):
        print(
            "Vertex tip: Uses Application Default Credentials (not GOOGLE_API_KEY). "
            "If you see invalid_grant / blank browser: run "
            "`gcloud auth application-default login` or set GOOGLE_APPLICATION_CREDENTIALS "
            "to a service-account JSON. Or comment JOBHUNTER_GEMINI_BACKEND to use AI Studio key."
        )
        print(
            "Vertex API: If logs show 403 SERVICE_DISABLED / Vertex AI API disabled, enable it: "
            "`gcloud services enable aiplatform.googleapis.com --project=YOUR_PROJECT_ID` "
            "(same id as GOOGLE_CLOUD_PROJECT), or open the activationUrl from the error. "
            "Wait 1–2 minutes after enabling."
        )
        print(
            "Vertex billing: Gemini on Vertex needs a billing account on that GCP project. "
            "If you see BILLING_DISABLED, open: "
            "https://console.developers.google.com/billing/enable?project=YOUR_PROJECT_ID "
            "— or skip GCP and use AI Studio only (comment JOBHUNTER_GEMINI_BACKEND, keep GOOGLE_API_KEY)."
        )
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=port,
        reload=reload_on,
    )


if __name__ == "__main__":
    main()
