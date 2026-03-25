# full_scraper

Self-contained Docker service that scrapes the **full Swedish population register** from biluppgifter.se and exposes a REST API for monitoring and data export.

Covers all people — including those without vehicles (~60% of the population).

**Dashboard:** The same UI ships as [`dashboard.html`](dashboard.html) from this API (e.g. open `/` on Render). A **Vercel-hosted** copy lives in the sibling repo [**full-scraper-dashboard**](https://github.com/Jakeminator123/full-scraper-dashboard) (`full-scraper-dashboard.vercel.app`) and proxies to Render so the browser never holds your `API_KEY`. The HTML auto-detects `*.vercel.app` vs same-origin.

**Alerts / webhooks:** See [`docs/ALERTS.md`](docs/ALERTS.md) (Render notifications, deploy hooks, optional uptime checks on `/health` or `/diag`).

## Quick start

```bash
# Start locally
export API_KEY=test123
export DATA_DIR=./local_data
mkdir -p local_data
pip install -r requirements.txt
npm install && npx playwright install chromium
uvicorn main:app --port 8080

# Start scraping 100 people
curl -X POST -H "Authorization: Bearer test123" \
  "http://localhost:8080/job/start?target=100&start_year=1985&end_year=1986"

# Check progress
curl -H "Authorization: Bearer test123" http://localhost:8080/status
```

## Deploy to Render

1. Copy `render.yaml` to the root of your full_scraper git repo
2. `git push`
3. Render dashboard: New -> Blueprint -> select repo
4. Find auto-generated `API_KEY` in Render Environment tab
5. Keep `DATA_DIR=/var/data` aligned with the disk `mountPath` (see `render.yaml`)

**Manual environment variables (no Blueprint):** If you create the service by hand and set variables only in the Render **Environment** tab, those values override both `render.yaml` (when not using Blueprint) and Docker `ENV` in the image. Set at least `API_KEY`, `DATA_DIR=/var/data`, and `PARALLEL_WORKERS=4` (for ~4 GB RAM) to match safe defaults.

## Goals: what to run (personer vs fordon vs berikning)

Phases are complementary (see `docs/ARCHITECTURE.md`). Pick a **preset** via `POST /job/start` query flags:

| Primary goal | Suggested flags | Effect |
|--------------|-----------------|--------|
| **Max personregister** (default mindset) | *(none)* or `people_first=true` | Fas 0 + 1 + 2E in stage 1, then Fas 2; Fas 3 after |
| **Snabbast möjliga Fas 2-only** | `skip_phase0=true&skip_phase1=true&skip_phase2e=true` | Bara PNR → biluppgifter; längre totalt HTTP mot ogiltiga PNR |
| **Ingen fordonsdatabas** | `skip_phase1=true` | Spar CPU/RAM och tid om du bara vill ha personer |
| **Ingen Ratsit-harvest** | `skip_phase0=true` | Allt via Fas 2 brute-force; mer belastning på biluppgifter |
| **Berikning sist** | `skip_phase3=true` tills Fas 2 är tillräcklig | Undvik onödig Ratsit-trafik innan du har personbas |

Examples:

```bash
# Full pipeline (typical)
curl -X POST -H "Authorization: Bearer $API_KEY" \
  "http://localhost:8080/job/start?target=10400000"

# Lean: only people via PNR enumeration
curl -X POST -H "Authorization: Bearer $API_KEY" \
  "http://localhost:8080/job/start?target=10400000&skip_phase0=true&skip_phase1=true&skip_phase2e=true"
```

**Bottleneck (expectation):** Fas 2 (PNR → biluppgifter) usually dominates wall-clock for people coverage; Fas 0 is slow per calendar day but saves Fas 2 work. The dashboard and `GET /status` expose `bottleneck_hint_sv` and a rough `eta_phase0_seconds_rough` for orientation.

## API

All endpoints except `/health` require `Authorization: Bearer <API_KEY>`.

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Liveness (no auth) |
| GET | `/status` | Progress, ETA, hit rate, DB size |
| GET | `/people?page=1&limit=100` | Browse with filters |
| GET | `/export/json` | Download full DB as JSON |
| GET | `/export/csv` | Download full DB as CSV |
| GET | `/export/sqlite` | Download raw SQLite file |
| POST | `/job/start?target=500000` | Start/resume scraping |
| POST | `/job/stop` | Pause after current batch |

`POST /job/start` optional query flags: `skip_phase0`, `skip_phase1`, `skip_phase2e`, `skip_phase3`, `people_first`, `people_plus_phone` (see `main.py`).

### Filters on `/people`

- `har_fordon=0` — only people without vehicles
- `har_fordon=1` — only vehicle owners
- `stad=Stockholm` — partial city match
- `search=Karlsson` — partial name match

## Docker

```bash
docker build -t full-scraper .
docker run -p 8080:8080 -e API_KEY=test123 -v $(pwd)/data:/var/data full-scraper
```

## How it works

1. Generates every valid PNR in a birth-year range using the Luhn checksum
2. Fetches `/brukare/base64(PNR)/` via headless Chromium (Playwright)
3. Parses name, age, city, address, vehicles from server-rendered HTML
4. Saves to SQLite on persistent disk with checkpointing for resume

**Scale:** 1940-2005 = ~22M PNRs -> ~8M people at 37% hit rate.
Single instance at 0.5s/req takes ~128 days for full coverage.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `API_KEY` | *(required)* | Bearer token |
| `START_YEAR` | 1940 | Birth year range start |
| `END_YEAR` | 2005 | Birth year range end |
| `PAGE_PAUSE` | 0.5 | Seconds between requests |
| `BATCH_SIZE` | 25 | URLs per Playwright batch |
| `PARALLEL_WORKERS` | 4 | Playwright workers (keep ≤4 on ~4 GB RAM) |
| `PHASE0_ETA_SEC_PER_DAY` | 600 | Rough seconds per remaining calendar day for Fas 0 ETA hint |
| `AUTO_RESUME` | true | Resume on container restart |
| `DATA_DIR` | /var/data | SQLite storage directory |

## Files

```
full_scraper/
├── Dockerfile          Docker image definition
├── .dockerignore       Excludes node_modules, __pycache__ etc.
├── render.yaml         Render Blueprint config
├── package.json        Node.js deps (playwright)
├── requirements.txt    Python deps (fastapi, bs4, lxml)
├── main.py             FastAPI REST API
├── scraper.py          Background scraper thread + Luhn PNR generation
├── db.py               SQLite helper (WAL mode, thread-safe)
├── fetch_helper.js     Playwright HTTP bridge (new context per URL)
└── README.md           This file
```
