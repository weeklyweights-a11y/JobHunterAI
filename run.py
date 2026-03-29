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
    print("JobHunter AI — open http://localhost:8000 in your browser")
    # Default off: uvicorn reload spawns a child process where Playwright/asyncio on Windows breaks.
    reload_on = os.getenv("JOBHUNTER_RELOAD", "false").lower() in ("1", "true", "yes")
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=reload_on,
    )


if __name__ == "__main__":
    main()
