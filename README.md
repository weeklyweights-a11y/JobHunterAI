# JobHunter AI

Self-hosted job search assistant. Run `python run.py` and open http://localhost:8000.

On **Windows**, use `python run.py` (not bare `uvicorn app:app`) so the asyncio policy matches Playwright. **Reload is off by default** (`JOBHUNTER_RELOAD=true` only when you need dev auto-reload). For **LinkedIn while logged in**, start Chrome with remote debugging and set `JOBHUNTER_CDP_URL=http://127.0.0.1:9222` — see `.env.example`.

See [PROJECT.md](PROJECT.md) for the full product spec and [PHASE_1_SPEC.md](PHASE_1_SPEC.md) for the current build phase.
