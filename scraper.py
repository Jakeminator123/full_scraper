"""
scraper.py — Systematic PNR enumeration scraper for biluppgifter.se

Strategy:
  Enumerate ALL valid Swedish PNRs in the range [START_YEAR, END_YEAR] in order:
    year -> month -> day (1-28) -> individ (1-999)
  For each position, compute the Luhn check digit to get a valid PNR, then
  fetch /brukare/base64(PNR)/ to check if the person exists in biluppgifter's DB.

  The position is checkpointed to SQLite after each batch, so the job can be
  paused and resumed at any time — even after a container restart.

  Total enumeration space (1940-2005): 66 x 12 x 28 x 999 ~ 22.2M PNRs
  Expected hit rate: ~37%  ->  ~8.2M people found
"""

import os
import re
import base64
import json
import subprocess
import threading
import logging
from datetime import date, datetime
from typing import Generator

from bs4 import BeautifulSoup

import db

log = logging.getLogger("scraper")

BASE        = "https://biluppgifter.se"
APP_DIR     = os.path.dirname(os.path.abspath(__file__))
FETCH_JS    = os.path.join(APP_DIR, "fetch_helper.js")
PAGE_PAUSE  = float(os.environ.get("PAGE_PAUSE",  "0.5"))
BATCH_SIZE  = int(os.environ.get("BATCH_SIZE",    "20"))
START_YEAR  = int(os.environ.get("START_YEAR",    "1940"))
END_YEAR    = int(os.environ.get("END_YEAR",      "2005"))

_thread:     threading.Thread | None = None
_stop_event: threading.Event          = threading.Event()
_state_lock: threading.Lock           = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────────
#  PNR generation (Luhn algorithm)
# ─────────────────────────────────────────────────────────────────────────────

def _luhn_check(nine_digits: str) -> int:
    total = 0
    for i, ch in enumerate(nine_digits):
        n = int(ch)
        if i % 2 == 0:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return (10 - (total % 10)) % 10


def make_pnr(year: int, month: int, day: int, individ: int) -> str | None:
    try:
        date(year, month, day)
    except ValueError:
        return None
    yy  = str(year)[2:]
    mm  = f"{month:02d}"
    dd  = f"{day:02d}"
    nnn = f"{individ:03d}"
    chk = _luhn_check(yy + mm + dd + nnn)
    return f"{year}{mm}{dd}-{nnn}{chk}"


def _pnr_to_url(pnr: str) -> str:
    digits = re.sub(r"\D", "", pnr)
    b64    = base64.b64encode(digits.encode()).decode()
    return f"{BASE}/brukare/{b64}/"


# ─────────────────────────────────────────────────────────────────────────────
#  PNR enumeration — systematic, resumable
# ─────────────────────────────────────────────────────────────────────────────

def _total_size(start_year: int, end_year: int) -> int:
    return (end_year - start_year + 1) * 12 * 28 * 999


def _position_index(year: int, month: int, day: int, individ: int, start_year: int) -> int:
    return (
        (year  - start_year) * 12 * 28 * 999
        + (month - 1)         * 28 * 999
        + (day   - 1)         *      999
        + (individ - 1)
    )


def _iter_pnrs(
    start_year: int,
    end_year: int,
    resume: tuple[int, int, int, int] | None = None,
) -> Generator[tuple[int, int, int, int, str], None, None]:
    """
    Yield (year, month, day, individ, pnr) for every valid PNR in range.
    If resume is given, skip to the NEXT position after the checkpoint
    (the checkpointed position itself was already processed).
    """
    if resume:
        ry, rm, rd, ri = resume
        # Advance one step past the checkpoint (it was already processed)
        ri += 1
        if ri > 999:
            ri = 1; rd += 1
        if rd > 28:
            rd = 1; rm += 1
        if rm > 12:
            rm = 1; ry += 1
    else:
        ry, rm, rd, ri = start_year, 1, 1, 1

    for year in range(ry, end_year + 1):
        m_start = rm if year == ry else 1
        for month in range(m_start, 13):
            d_start = rd if (year == ry and month == rm) else 1
            for day in range(d_start, 29):
                i_start = ri if (year == ry and month == rm and day == rd) else 1
                for individ in range(i_start, 1000):
                    pnr = make_pnr(year, month, day, individ)
                    if pnr:
                        yield (year, month, day, individ, pnr)


# ─────────────────────────────────────────────────────────────────────────────
#  HTTP fetch via Playwright (Node.js subprocess)
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_batch(pnr_list: list[str]) -> dict[str, str | None]:
    url_map = {}
    for pnr in pnr_list:
        url = _pnr_to_url(pnr)
        url_map[url] = pnr

    try:
        r = subprocess.run(
            ["node", FETCH_JS, "--stdin"],
            input=json.dumps(list(url_map.keys())),
            capture_output=True,
            text=True,
            timeout=int(PAGE_PAUSE * len(pnr_list) * 3 + 120),
            cwd=APP_DIR,
        )
        if r.returncode != 0:
            log.warning("fetch_helper error: %s", r.stderr.strip()[:200])
            return {p: None for p in pnr_list}
        html_by_url = json.loads(r.stdout)
        return {url_map[u]: h for u, h in html_by_url.items()}
    except subprocess.TimeoutExpired:
        log.warning("fetch_helper timeout for batch of %d", len(pnr_list))
        return {p: None for p in pnr_list}
    except Exception as e:
        log.error("fetch_helper exception: %s", e)
        return {p: None for p in pnr_list}


# ─────────────────────────────────────────────────────────────────────────────
#  HTML parsing
# ─────────────────────────────────────────────────────────────────────────────

_NOT_FOUND_PHRASES = [
    "Kunde inte hitta brukaren",
    "hittar inte den sida",
    "Ooups",
]


def _parse_brukare(html: str | None, pnr: str) -> dict | None:
    if not html:
        return None

    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(separator=" ", strip=True)

    for phrase in _NOT_FOUND_PHRASES:
        if phrase in text:
            return None

    name_m = re.search(r"Visa\s+(.+?)\s+på\s+Ratsit", text)
    if name_m:
        namn = name_m.group(1).strip()
    else:
        name_m2 = re.search(
            r"([A-ZÅÄÖ][a-zåäöA-ZÅÄÖ][a-zåäöA-ZÅÄÖ\-\s]{1,50}),\s+en\s+privatperson",
            text,
        )
        namn = name_m2.group(1).strip() if name_m2 else ""

    if not namn:
        return None

    age_m  = re.search(r"(\d{1,3})\s+år", text)
    alder  = int(age_m.group(1)) if age_m else 0

    city_m = re.search(r"bor\s+i\s+([A-ZÅÄÖ][a-zåäö]+(?:[\s-][A-ZÅÄÖ][a-zåäö]+)?)", text)
    stad   = city_m.group(1).strip() if city_m else ""

    addr_m = re.search(r"\bAdress\s+([^\n\r,]{3,80})", text)
    if addr_m:
        gata = addr_m.group(1).strip()
        gata = re.split(r"\s{2,}|Kontakt|Telefon|Fordon|Visa\s", gata)[0].strip()
    else:
        gata = ""

    fordon_egna_regnr  = []
    fordon_egna_modell = []

    owned_hdr = soup.find(string=re.compile(r"s\s+fordon", re.I))
    if owned_hdr:
        parent = owned_hdr.find_parent()
        if parent:
            table = parent.find_next("table")
            if table:
                for row in table.find_all("tr")[1:]:
                    cols = [td.get_text(strip=True) for td in row.find_all("td")]
                    if len(cols) >= 2:
                        fordon_egna_regnr.append(cols[1])
                        fordon_egna_modell.append(cols[0][:30])

    fordon_adress_regnr = []
    addr_hdr = soup.find(string=re.compile(r"Andra fordon på adressen", re.I))
    if addr_hdr:
        parent = addr_hdr.find_parent()
        if parent:
            table = parent.find_next("table")
            if table:
                for row in table.find_all("tr")[1:]:
                    cols = [td.get_text(strip=True) for td in row.find_all("td")]
                    if len(cols) >= 2:
                        fordon_adress_regnr.append(cols[1])

    return {
        "pnr":                   pnr,
        "namn":                  namn,
        "alder":                 alder,
        "stad":                  stad,
        "gata":                  gata,
        "har_fordon":            len(fordon_egna_regnr) > 0,
        "antal_fordon_egna":     len(fordon_egna_regnr),
        "fordon_egna_regnr":     "|".join(fordon_egna_regnr),
        "fordon_egna_modell":    "|".join(fordon_egna_modell),
        "antal_fordon_adress":   len(fordon_adress_regnr),
        "fordon_adress_regnr":   "|".join(fordon_adress_regnr),
        "hamtad":                datetime.now().isoformat(timespec="seconds"),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Main scraper loop
# ─────────────────────────────────────────────────────────────────────────────

def _run(start_year: int, end_year: int, target: int) -> None:
    log.info("Scraper started: years=%d-%d target=%d", start_year, end_year, target)

    state = db.get_job_state()
    resume = None
    tested    = state.get("total_tested",    0)
    found     = state.get("total_found",     0)
    not_found = state.get("total_not_found", 0)
    errors    = state.get("total_errors",    0)

    if state.get("current_year", 0) > 0:
        resume = (
            state["current_year"],
            state["current_month"],
            state["current_day"],
            state["current_individ"],
        )
        log.info("Resuming from %d-%02d-%02d individ %d",
                 resume[0], resume[1], resume[2], resume[3])

    db.update_job_state(
        status="running",
        start_year=start_year,
        end_year=end_year,
        target_people=target,
        started_at=state.get("started_at") or datetime.now().isoformat(timespec="seconds"),
    )

    batch_pnrs: list[str]   = []
    batch_pos:  list[tuple]  = []

    def flush_batch() -> None:
        nonlocal tested, found, not_found, errors

        if not batch_pnrs:
            return

        html_map = _fetch_batch(batch_pnrs)

        last_year = last_month = last_day = last_individ = 0

        for (y, mo, d, ind), pnr in zip(batch_pos, batch_pnrs):
            html = html_map.get(pnr)
            tested += 1
            if html is None:
                errors += 1
            else:
                person = _parse_brukare(html, pnr)
                if person:
                    db.insert_person(person)
                    found += 1
                else:
                    not_found += 1
            last_year, last_month, last_day, last_individ = y, mo, d, ind

        db.save_checkpoint(
            last_year, last_month, last_day, last_individ,
            tested, found, not_found, errors,
        )
        log.info("checkpoint year=%d tested=%d found=%d",
                 last_year, tested, found)

        batch_pnrs.clear()
        batch_pos.clear()

    try:
        for y, mo, d, ind, pnr in _iter_pnrs(start_year, end_year, resume):
            if _stop_event.is_set():
                log.info("Stop signal received — pausing.")
                flush_batch()
                db.update_job_state(status="paused")
                return

            if target > 0 and found >= target:
                log.info("Target %d reached.", target)
                flush_batch()
                db.update_job_state(status="done")
                return

            batch_pnrs.append(pnr)
            batch_pos.append((y, mo, d, ind))

            if len(batch_pnrs) >= BATCH_SIZE:
                flush_batch()

        flush_batch()

        log.info("Enumeration complete. total_tested=%d total_found=%d", tested, found)
        db.update_job_state(status="done")

    except Exception as e:
        log.exception("Scraper crashed: %s", e)
        db.update_job_state(status="error")
    finally:
        with _state_lock:
            global _thread
            _thread = None


# ─────────────────────────────────────────────────────────────────────────────
#  Public control interface (called by main.py)
# ─────────────────────────────────────────────────────────────────────────────

def start(target: int = 0, start_year: int | None = None, end_year: int | None = None) -> bool:
    global _thread

    with _state_lock:
        if _thread is not None and _thread.is_alive():
            return False

        _stop_event.clear()
        sy = start_year or START_YEAR
        ey = end_year   or END_YEAR
        _thread = threading.Thread(
            target=_run,
            args=(sy, ey, target),
            daemon=True,
            name="scraper",
        )
        _thread.start()
    return True


def stop() -> None:
    _stop_event.set()


def is_running() -> bool:
    return _thread is not None and _thread.is_alive()


def get_status() -> dict:
    state  = db.get_job_state()
    sy     = state.get("start_year",  START_YEAR)
    ey     = state.get("end_year",    END_YEAR)
    cy     = state.get("current_year",  sy)
    cm     = state.get("current_month",  1)
    cd     = state.get("current_day",    1)
    ci     = state.get("current_individ", 1)
    tested = state.get("total_tested",   0)
    found  = state.get("total_found",    0)
    n_f    = state.get("total_not_found",0)
    errs   = state.get("total_errors",   0)
    target = state.get("target_people",  0)

    total_space = _total_size(sy, ey)
    current_pos = _position_index(max(cy, sy), cm, cd, ci, sy) if cy >= sy else 0
    pct_done    = round(current_pos / total_space * 100, 2) if total_space else 0
    remaining   = max(0, total_space - current_pos)
    hit_rate    = round(found / tested * 100, 1) if tested else 0
    started     = state.get("started_at", "")

    eta_seconds = None
    if tested > 0 and started:
        try:
            elapsed = (datetime.now() - datetime.fromisoformat(started)).total_seconds()
            rate    = tested / elapsed if elapsed > 0 else 0
            eta_seconds = int(remaining / rate) if rate > 0 else None
        except Exception:
            pass

    return {
        "status":           state.get("status", "idle"),
        "is_running":       is_running(),
        "start_year":       sy,
        "end_year":         ey,
        "current_position": f"{cy}-{cm:02d}-{cd:02d} individ {ci}",
        "pct_done":         pct_done,
        "total_space":      total_space,
        "total_tested":     tested,
        "total_found":      found,
        "total_not_found":  n_f,
        "total_errors":     errs,
        "hit_rate_pct":     hit_rate,
        "target_people":    target,
        "db_size_mb":       round(db.db_file_size_mb(), 1),
        "eta_seconds":      eta_seconds,
        "started_at":       started,
        "updated_at":       state.get("updated_at", ""),
    }


def maybe_auto_resume() -> None:
    if os.environ.get("AUTO_RESUME", "true").lower() not in ("true", "1", "yes"):
        return
    state = db.get_job_state()
    if state.get("status") == "running" and state.get("total_tested", 0) > 0:
        log.info("AUTO_RESUME: resuming interrupted job from checkpoint.")
        target = state.get("target_people", 0)
        sy     = state.get("start_year", START_YEAR)
        ey     = state.get("end_year",   END_YEAR)
        start(target=target, start_year=sy, end_year=ey)
