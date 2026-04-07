# JobHunter AI

🇬🇧 English | 🇪🇸 Español (coming soon)

AI-powered job search pipeline that discovers jobs across 6 sources, filters aggressively, and delivers fresh matches to your inbox every morning.

Built with Python, FastAPI, Playwright, BeautifulSoup, SQLite, and Gemini Flash.

---

## What Is This

JobHunter AI turns job hunting from a manual 4-hour daily loop into an automated pipeline.

Instead of opening LinkedIn, Indeed, Greenhouse, Lever, Ashby, and YC one by one, you configure your search once (roles + locations + freshness window), click **Start**, and let the system run end-to-end:

- Discover jobs across multiple sources in parallel
- Enrich listings with structured ATS APIs
- Extract posted dates using a multi-stage pipeline
- Deduplicate across sources
- Filter by title relevance + optional LLM relevance
- Export to Excel + send email digest

This is **not** a spray-and-pray auto-apply bot. It is a discovery and filtering engine designed to surface the few jobs worth your time.

---

## Why It Exists

Most job platforms are incomplete and noisy:

- LinkedIn has coverage but repeated/reposted noise
- Indeed has breadth but inconsistent freshness
- Startup jobs live on scattered ATS boards (Greenhouse/Lever/Ashby)
- No single source gives high recall + high precision

JobHunter AI combines all of them in one local pipeline and applies strict filtering so the final list is actually usable.

---

## What It Does

### Sources (parallel)

- **LinkedIn** (Playwright)
- **Indeed** (Playwright)
- **ATS pipeline** (Google `site:` discovery + ATS APIs + HTTP/browser fallback)
- **YC Work at a Startup**
- **Custom Career Pages**
- **Jobright AI**

### ATS Pipeline highlights

- Uses Google `site:` discovery to find company boards you may not already track
- Enriches with free public APIs where available:
  - Greenhouse boards API
  - Lever postings API
  - Ashby job-board API
- Handles posted date extraction via:
  1. API timestamps
  2. JSON-LD (`datePosted`)
  3. HTML date parsing
  4. Browser fallback for JS-heavy pages

### Output

- Unified job table
- Cross-source deduplication
- Optional LLM relevance pass (Gemini Flash)
- Excel report with links + metadata
- Email digest when new jobs are found

---

## Features

| Feature | Description |
|---|---|
| Multi-source scrape | 6 sources in one run |
| ATS discovery | Google `site:` + board/API enrichment |
| Freshness filtering | Drop jobs older than configured window |
| Title phrase matching | Keep role-relevant titles, drop junk early |
| Cross-source dedup | URL + title/company consolidation |
| Optional LLM relevance | Description-aware filtering with Gemini Flash |
| Dashboard | FastAPI + Jinja2 + live status updates (SSE) |
| Excel + email | Daily digest with direct apply links |
| Local-first | Runs on your laptop, no Docker required |

---

## Quick Start

```bash
# 1) Clone
git clone https://github.com/weeklyweights-a11y/JobHunterAI.git
cd JobHunterAI

# 2) Install dependencies
pip install -r requirements.txt
playwright install chromium

# 3) Run app
python run.py
```

Open: [http://localhost:8000](http://localhost:8000)

---

## Configuration

In the dashboard, configure:

- Roles (e.g., ML Engineer, AI Engineer)
- Locations (e.g., NYC, Remote)
- Source toggles
- ATS freshness window
- Email credentials (optional)
- LinkedIn / Jobright login credentials (optional but recommended)

Optional: set Gemini key for LLM relevance filtering.

---

## Architecture

```text
JobHunter AI
├── agents/
│   ├── orchestrator.py      # runs enabled sources, merge + dedup
│   ├── linkedin.py          # Playwright + DOM parsing
│   ├── indeed.py            # Playwright + DOM parsing
│   ├── ats.py               # Google discovery + ATS API merge + date enrichment
│   ├── greenhouse_api.py
│   ├── lever_api.py
│   ├── ashby_api.py
│   ├── yc.py
│   ├── career_pages.py
│   └── jobright.py
├── parsers/
│   ├── ats_posted_time.py   # multi-stage posted-date extraction
│   ├── greenhouse_http.py
│   ├── lever_http.py
│   ├── ashby_http.py
│   └── jobright_parser.py
├── templates/               # dashboard UI
├── static/                  # client JS/CSS
├── db*.py                   # config/jobs persistence + migrations
├── excel_report.py
├── emailer.py
└── run.py
```

---

## Pipeline (High-Level)

1. **Collect** from enabled sources (parallel where possible)
2. **Merge** all rows into one stream
3. **Dedup** by URL and title/company
4. **Enrich** posted dates and metadata
5. **Filter** by title + freshness (+ optional LLM relevance)
6. **Persist** to SQLite with rolling dedup window
7. **Export** Excel and send email digest

---

## Tech Stack

- **Backend:** FastAPI, asyncio, aiosqlite
- **Scraping:** Playwright, BeautifulSoup, HTTP API fetchers
- **UI:** Jinja2 + vanilla JS/CSS
- **Storage:** SQLite
- **Reporting:** openpyxl + SMTP
- **Optional AI:** Gemini Flash relevance filtering

---

## Roadmap

- Smarter ATS early-stop and ranking heuristics
- Stronger Jobright extraction coverage
- Better relevance calibration by profile seniority
- ApplyForMe integration (human-in-the-loop form filling)

---

## Philosophy

- Local-first
- Human-in-the-loop
- Quality over volume
- Transparent, inspectable pipeline

---

## Acknowledgements

This README style and product-story inspiration are influenced by the structure and clarity of Career-Ops:

- [Career-Ops repository](https://github.com/santifer/career-ops)

---

## License

MIT
