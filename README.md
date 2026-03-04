# full_scraper

Self-contained Docker service that scrapes the **full Swedish population register** from biluppgifter.se and exposes a REST API for monitoring and data export.

Covers all people — including those without vehicles (~60% of the population).

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

1. Copy `render.yaml` to the root of your git repo
2. `git push`
3. Render dashboard: New -> Blueprint -> select repo
4. Find auto-generated `API_KEY` in Render Environment tab

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
| `BATCH_SIZE` | 20 | URLs per Playwright batch |
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
