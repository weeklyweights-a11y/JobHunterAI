# JobHunter AI

Self-hosted job search assistant. Run `python run.py` and open http://localhost:8000.

On **Windows**, use `python run.py` (not bare `uvicorn app:app`) so the asyncio policy matches Playwright. **Reload is off by default** (`JOBHUNTER_RELOAD=true` only when you need dev auto-reload). For **LinkedIn while logged in**, start Chrome with remote debugging and set `JOBHUNTER_CDP_URL=http://127.0.0.1:9222` — see `.env.example`.

**Gemini via Vertex AI (GCP billing):** set `JOBHUNTER_GEMINI_BACKEND=vertex`, `GOOGLE_CLOUD_PROJECT`, and Application Default Credentials (`gcloud auth application-default login` or `GOOGLE_APPLICATION_CREDENTIALS`). Then `pip install -r requirements.txt` and restart. Dashboard can stay on provider “Gemini”.

See [PROJECT.md](PROJECT.md) for the full product spec and [PHASE_1_SPEC.md](PHASE_1_SPEC.md) for the current build phase.
