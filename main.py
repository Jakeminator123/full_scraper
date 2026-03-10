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
import signal
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

import db
import scraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger("main")

API_KEY = os.environ.get("API_KEY", "")


def _handle_sigterm(signum, frame):
    """Docker sends SIGTERM on stop/redeploy. Gracefully stop the scraper so
    the current batch finishes and checkpoint is saved before the process dies."""
    log.info("SIGTERM received — stopping scraper gracefully")
    drained = scraper.stop(wait=True, timeout=25.0)
    if not drained:
        log.warning("Scraper did not stop before timeout; process may be terminated by platform.")

signal.signal(signal.SIGTERM, _handle_sigterm)


@asynccontextmanager
async def lifespan(application):
    db.init_db()
    log.info("Database initialised at %s", db.DB_PATH)
    scraper.maybe_auto_resume()
    yield
    log.info("Shutting down — stopping scraper")
    scraper.stop(wait=True, timeout=20.0)

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


DASHBOARD_URL = os.environ.get(
    "DASHBOARD_URL", ""
)


# ── Dashboard (served directly from the container) ───────────────────────────

_DASHBOARD_HTML: str | None = None


def _load_dashboard() -> str:
    global _DASHBOARD_HTML
    if _DASHBOARD_HTML is not None:
        return _DASHBOARD_HTML
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
    if os.path.exists(html_path):
        with open(html_path, encoding="utf-8") as f:
            _DASHBOARD_HTML = f.read()
    else:
        _DASHBOARD_HTML = "<h1>dashboard.html not found</h1>"
    return _DASHBOARD_HTML


@app.get("/", include_in_schema=False)
def root():
    if DASHBOARD_URL:
        return RedirectResponse(DASHBOARD_URL)
    return HTMLResponse(_load_dashboard())


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["meta"])
def health() -> dict:
    return {"ok": True, "service": "full-scraper"}


@app.get("/diag", tags=["meta"])
def diagnostics_public() -> dict:
    """Quick diagnostics — no auth required. Safe to bookmark/webhook."""
    state = db.get_job_state()
    total = db.count_people()
    enrichment = db.count_enrichment()
    phase = state.get("phase", "idle")
    status = state.get("status", "idle")

    p0_found = state.get("phase0_found", 0)
    p0_phones = state.get("phase0_phones", 0)
    p0_date = state.get("phase0_date", "")

    tested = state.get("total_tested", 0)
    found = state.get("total_found", 0)
    errors = state.get("total_errors", 0)

    heartbeat = state.get("last_progress_at", "") or state.get("updated_at", "")
    heartbeat_age = None
    if heartbeat:
        try:
            heartbeat_age = max(0, int((datetime.now() - datetime.fromisoformat(heartbeat)).total_seconds()))
        except ValueError:
            pass

    return {
        "ok": status not in ("error",),
        "status": status,
        "phase": phase,
        "is_running": scraper.is_running(),
        "total_people": total,
        "total_vehicles": db.count_vehicles(),
        "phase0_found": p0_found,
        "phase0_phones": p0_phones,
        "phase0_date": p0_date,
        "phase2_tested": tested,
        "phase2_found": found,
        "phase2_errors": errors,
        "phase2_hit_rate": round(found / tested * 100, 1) if tested else 0,
        "phase2e_resolved": state.get("phase2e_resolved", 0),
        "phase2e_searched": state.get("phase2e_searched", 0),
        "phase3_enriched": state.get("phase3_enriched", 0),
        "phase3_phones": state.get("phase3_phones", 0),
        "phase3_unenriched": db.count_unenriched(),
        "enrichment_telefon": enrichment.get("med_telefon", 0),
        "enrichment_kon": enrichment.get("med_kon", 0),
        "enrichment_gps": enrichment.get("med_gps", 0),
        "enrichment_fran_ratsit": enrichment.get("fran_ratsit", 0),
        "heartbeat_age_seconds": heartbeat_age,
        "heartbeat_stale": heartbeat_age is not None and heartbeat_age > scraper.HEARTBEAT_STALE_SECONDS,
        "db_size_mb": round(db.db_file_size_mb(), 1),
        "parallel_workers": scraper.PARALLEL_WORKERS,
        "checked_at": datetime.now().isoformat(timespec="seconds"),
    }


# ── Status ────────────────────────────────────────────────────────────────────

@app.get("/status", tags=["job"])
def status(_: Auth) -> dict:
    s = scraper.get_status()
    total_people = db.count_people()
    s["total_people_in_db"]      = total_people
    s["people_with_vehicles"]    = db.count_people(har_fordon=1)
    s["people_without_vehicles"] = db.count_people(har_fordon=0)
    s["total_vehicles_in_db"]    = db.count_vehicles()

    enrichment = db.count_enrichment()
    s["enrichment"] = {
        "med_telefon":      enrichment.get("med_telefon", 0),
        "med_kon":          enrichment.get("med_kon", 0),
        "med_gift":         enrichment.get("med_gift", 0),
        "med_gps":          enrichment.get("med_gps", 0),
        "med_bolag":        enrichment.get("med_bolag", 0),
        "med_tilltalsnamn": enrichment.get("med_tilltalsnamn", 0),
        "med_grannar":      enrichment.get("med_grannar", 0),
        "fran_ratsit":      enrichment.get("fran_ratsit", 0),
        "fran_biluppgifter": enrichment.get("fran_biluppgifter", 0),
    }

    # Rolling speed metrics are persisted so ETA survives container redeploys.
    snap = db.update_status_snapshot(total_people, int(s.get("total_tested", 0)))
    s["speed_people_per_hour"] = float(snap.get("speed_people_per_hour", s.get("speed_people_per_hour", 0)) or 0)
    s["speed_tested_per_hour"] = float(snap.get("speed_tested_per_hour", s.get("speed_tested_per_hour", 0)) or 0)

    target_goal = int(s.get("target_people_goal", 0) or 0)
    if target_goal > 0:
        now = datetime.now()
        remaining = max(0, target_goal - total_people)
        s["people_remaining_to_goal"] = remaining
        s["goal_progress_pct"] = round(min(100.0, (total_people / target_goal) * 100), 2)

        # If rolling speed has not stabilised yet, use average speed since started_at.
        people_speed = float(s.get("speed_people_per_hour", 0) or 0)
        if people_speed <= 0 and s.get("started_at"):
            try:
                elapsed_h = max((now - datetime.fromisoformat(str(s["started_at"]))).total_seconds() / 3600.0, 1e-6)
                people_speed = total_people / elapsed_h
            except (ValueError, TypeError):
                people_speed = 0.0
        s["speed_people_per_hour"] = round(people_speed, 2)

        if remaining == 0:
            s["eta_seconds_to_goal"] = 0
            s["eta_at"] = now.isoformat(timespec="seconds")
        elif people_speed > 0:
            eta_seconds = int((remaining / people_speed) * 3600)
            s["eta_seconds_to_goal"] = eta_seconds
            s["eta_at"] = (now + timedelta(seconds=eta_seconds)).isoformat(timespec="seconds")
        else:
            s["eta_seconds_to_goal"] = None
            s["eta_at"] = ""
    else:
        s["people_remaining_to_goal"] = None
        s["goal_progress_pct"] = None
        s["eta_seconds_to_goal"] = None
        s["eta_at"] = ""

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
        "kon", "gift", "tilltalsnamn", "postnummer", "lat", "lng",
        "bolag", "telefon", "grannar", "kalla",
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
    target:      int  = Query(0,     ge=0,             description="Stop after N people found (0 = unlimited; dashboard still tracks TARGET_PEOPLE_DEFAULT)"),
    start_year:  int  = Query(None,  ge=1900, le=2010, description="Override START_YEAR"),
    end_year:    int  = Query(None,  ge=1900, le=2010, description="Override END_YEAR"),
    skip_phase0: bool = Query(False,                   description="Skip Ratsit date-harvesting (Phase 0)"),
    skip_phase1: bool = Query(False,                   description="Skip vehicle prefix enumeration (Phase 1)"),
) -> dict:
    if scraper.is_running():
        raise HTTPException(status_code=409, detail="Scraper is already running.")

    ok = scraper.start(target=target, start_year=start_year,
                       end_year=end_year, skip_phase1=skip_phase1,
                       skip_phase0=skip_phase0)
    if not ok:
        raise HTTPException(status_code=409, detail="Could not start scraper.")

    return {
        "started": True,
        "target": target,
        "target_goal": target if target > 0 else scraper.TARGET_PEOPLE_DEFAULT,
        "skip_phase0": skip_phase0,
        "skip_phase1": skip_phase1,
    }


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
