"""
main.py — FastAPI REST API for the full population scraper.

Endpoints:
  GET  /health            — liveness check (no auth)
  GET  /status            — job progress, ETA, stats
  GET  /people            — browse scraped people (paginated)
  GET  /export/json       — stream full database as JSON
  GET  /export/csv        — stream full database as CSV
  GET  /export/sqlite     — download raw SQLite database file
  POST /job/start         — start or resume scraping
  POST /job/stop          — pause scraping

Authentication:
  All endpoints except /health require header:
    Authorization: Bearer <API_KEY>
"""

import csv
import io
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

import db
import scraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger("main")

API_KEY = os.environ.get("API_KEY", "")


@asynccontextmanager
async def lifespan(application):
    db.init_db()
    log.info("Database initialised at %s", db.DB_PATH)
    scraper.maybe_auto_resume()
    yield

app = FastAPI(
    title="Full Population Scraper API",
    description=(
        "Systematically scrapes biluppgifter.se via Luhn-generated PNRs. "
        "Covers the full Swedish population register — including people without vehicles."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# ── Auth ──────────────────────────────────────────────────────────────────────

_security = HTTPBearer()


def require_auth(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(_security)],
) -> None:
    if not API_KEY:
        raise HTTPException(
            status_code=500,
            detail="API_KEY environment variable not set on the server.",
        )
    if credentials.credentials != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key.")


Auth = Annotated[None, Depends(require_auth)]


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["meta"])
def health() -> dict:
    return {"ok": True, "service": "full-scraper"}


# ── Status ────────────────────────────────────────────────────────────────────

@app.get("/status", tags=["job"])
def status(_: Auth) -> dict:
    s = scraper.get_status()
    s["total_people_in_db"]      = db.count_people()
    s["people_with_vehicles"]    = db.count_people(har_fordon=1)
    s["people_without_vehicles"] = db.count_people(har_fordon=0)
    s["total_vehicles_in_db"]    = db.count_vehicles()
    return s


# ── Browse ────────────────────────────────────────────────────────────────────

@app.get("/people", tags=["data"])
def people(
    _: Auth,
    page:       int        = Query(1,    ge=1,          description="Page number"),
    limit:      int        = Query(100,  ge=1, le=1000, description="Rows per page (max 1000)"),
    har_fordon: int | None = Query(None, ge=0, le=1,    description="1=has vehicles, 0=no vehicles"),
    stad:       str | None = Query(None,                description="Filter by city (partial match)"),
    search:     str | None = Query(None,                description="Search by name (partial match)"),
) -> dict:
    rows  = db.get_people(page=page, limit=limit, har_fordon=har_fordon, stad=stad, search=search)
    total = db.count_people(har_fordon=har_fordon)
    return {
        "page":     page,
        "limit":    limit,
        "total":    total,
        "has_more": page * limit < total,
        "results":  rows,
    }


# ── Exports ───────────────────────────────────────────────────────────────────

@app.get("/export/json", tags=["export"])
def export_json(_: Auth) -> StreamingResponse:
    def generate():
        yield "[\n"
        first = True
        for row in db.iter_all_people():
            if not first:
                yield ",\n"
            yield json.dumps(row, ensure_ascii=False)
            first = False
        yield "\n]\n"

    return StreamingResponse(
        generate(),
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=people.json"},
    )


@app.get("/export/csv", tags=["export"])
def export_csv(_: Auth) -> StreamingResponse:
    COLS = [
        "pnr", "namn", "alder", "stad", "gata",
        "har_fordon", "antal_fordon_egna", "fordon_egna_regnr", "fordon_egna_modell",
        "antal_fordon_adress", "fordon_adress_regnr", "hamtad",
    ]

    def generate():
        buf    = io.StringIO()
        writer = csv.writer(buf)
        yield "\ufeff"
        writer.writerow(COLS)
        yield buf.getvalue()

        for row in db.iter_all_people():
            buf.seek(0); buf.truncate()
            writer.writerow([row.get(c, "") for c in COLS])
            yield buf.getvalue()

    return StreamingResponse(
        generate(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=people.csv"},
    )


@app.get("/export/sqlite", tags=["export"])
def export_sqlite(_: Auth) -> StreamingResponse:
    if not os.path.exists(db.DB_PATH):
        raise HTTPException(status_code=404, detail="Database file not found.")

    # Flush WAL to main file so the download is self-contained
    db.wal_checkpoint()

    def generate():
        with open(db.DB_PATH, "rb") as f:
            while chunk := f.read(65536):
                yield chunk

    size = os.path.getsize(db.DB_PATH)
    return StreamingResponse(
        generate(),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": "attachment; filename=people.db",
            "Content-Length": str(size),
        },
    )


# ── Job control ───────────────────────────────────────────────────────────────

@app.post("/job/start", tags=["job"])
def job_start(
    _: Auth,
    target:      int  = Query(0,     ge=0,             description="Stop after N people found (0 = unlimited)"),
    start_year:  int  = Query(None,  ge=1900, le=2010, description="Override START_YEAR"),
    end_year:    int  = Query(None,  ge=1900, le=2010, description="Override END_YEAR"),
    skip_phase1: bool = Query(False,                   description="Skip vehicle prefix enumeration (Phase 1)"),
) -> dict:
    if scraper.is_running():
        raise HTTPException(status_code=409, detail="Scraper is already running.")

    ok = scraper.start(target=target, start_year=start_year,
                       end_year=end_year, skip_phase1=skip_phase1)
    if not ok:
        raise HTTPException(status_code=409, detail="Could not start scraper.")

    return {"started": True, "target": target, "skip_phase1": skip_phase1}


@app.post("/job/reset", tags=["job"])
def job_reset(_: Auth) -> dict:
    """Drop and recreate all tables. Loses all data."""
    if scraper.is_running():
        raise HTTPException(status_code=409, detail="Stop the scraper first.")
    db.reset_db()
    return {"reset": True, "detail": "All tables dropped and recreated."}


@app.post("/job/stop", tags=["job"])
def job_stop(_: Auth) -> dict:
    if not scraper.is_running():
        return {"stopped": False, "detail": "Scraper was not running."}
    scraper.stop()
    return {"stopped": True, "detail": "Stop signal sent. Job will pause after current batch."}
