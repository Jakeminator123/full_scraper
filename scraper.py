"""
scraper.py — Three-phase population scraper for biluppgifter.se

Phase 0 (SMART — ~3-5 days, 100% hit rate):
  Search Ratsit by birth date (YYYYMMDD) + gender filter.
  Each search returns up to 30 people (3 pages × 10).
  Intercept /person/biluppgifter/ on each profile → extract PNR via base64.
  Then fetch biluppgifter.se/brukare/{b64}/ for full person data.
  → ~1.4M people from 24,000 dates × 2 genders × 30 hits/search.

Phase 1 (FAST — ~1-2 days):
  Enumerate all 17,576 three-letter vehicle prefixes (AAA-ZZZ).
  Each prefix page returns 100 vehicles. Paginate until exhausted.
  For each vehicle: extract regnr, model, year, status, vehicleId.
  → Covers ~11.4M vehicles.

Phase 2 (SLOWER — parallel PNR enumeration):
  Generate valid PNRs via Luhn algorithm, fetch /brukare/base64(PNR)/.
  Runs N parallel Playwright workers for speed.
  Phase 0 PNRs are already in DB → existing_pnrs() skips them automatically.
  → Fills in remaining ~8-9M people not covered by Phase 0.

All phases checkpoint to SQLite for resume after restart.
"""

import os
import re
import base64
import json
import subprocess
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from typing import Generator
from urllib.parse import urljoin

from bs4 import BeautifulSoup

import db

log = logging.getLogger("scraper")

BASE             = "https://biluppgifter.se"
APP_DIR          = os.path.dirname(os.path.abspath(__file__))
FETCH_JS         = os.path.join(APP_DIR, "fetch_helper.js")
RATSIT_JS        = os.path.join(APP_DIR, "ratsit_helper.js")
PAGE_PAUSE       = float(os.environ.get("PAGE_PAUSE",       "0.3"))
PHASE0_PAUSE     = float(os.environ.get("PHASE0_PAUSE",     "1.0"))
BATCH_SIZE       = int(os.environ.get("BATCH_SIZE",         "30"))
PARALLEL_WORKERS = int(os.environ.get("PARALLEL_WORKERS",   "3"))
START_YEAR       = int(os.environ.get("START_YEAR",         "1940"))
END_YEAR         = int(os.environ.get("END_YEAR",           "2005"))

_thread:     threading.Thread | None = None
_stop_event: threading.Event          = threading.Event()
_state_lock: threading.Lock           = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────────
#  Luhn PNR generation
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

def _total_pnr_space(start_year: int, end_year: int) -> int:
    return (end_year - start_year + 1) * 12 * 31 * 999


def _pnr_position_index(year: int, month: int, day: int, individ: int, start_year: int) -> int:
    return (
        (year - start_year) * 12 * 31 * 999
        + (month - 1) * 31 * 999
        + (day - 1) * 999
        + (individ - 1)
    )


def _iter_pnrs(
    start_year: int,
    end_year: int,
    resume: tuple[int, int, int, int] | None = None,
) -> Generator[tuple[int, int, int, int, str], None, None]:
    if resume:
        ry, rm, rd, ri = resume
        ri += 1
        if ri > 999:
            ri = 1; rd += 1
        if rd > 31:
            rd = 1; rm += 1
        if rm > 12:
            rm = 1; ry += 1
    else:
        ry, rm, rd, ri = start_year, 1, 1, 1

    for year in range(ry, end_year + 1):
        m_start = rm if year == ry else 1
        for month in range(m_start, 13):
            d_start = rd if (year == ry and month == rm) else 1
            for day in range(d_start, 32):
                i_start = ri if (year == ry and month == rm and day == rd) else 1
                for individ in range(i_start, 1000):
                    pnr = make_pnr(year, month, day, individ)
                    if pnr:
                        yield (year, month, day, individ, pnr)


# ─────────────────────────────────────────────────────────────────────────────
#  Prefix enumeration (AAA-ZZZ) for Phase 1
# ─────────────────────────────────────────────────────────────────────────────

TOTAL_PREFIXES = 26 ** 3  # 17,576


def _iter_prefixes(resume_prefix: str = "") -> Generator[str, None, None]:
    """Yield all 3-letter prefixes AAA..ZZZ, optionally resuming."""
    started = not bool(resume_prefix)
    for a in range(26):
        for b in range(26):
            for c in range(26):
                prefix = chr(65+a) + chr(65+b) + chr(65+c)
                if not started:
                    if prefix == resume_prefix:
                        started = True
                    continue
                yield prefix


def _prefix_page_url(prefix: str, page: int) -> str:
    if page == 1:
        return f"{BASE}/fordon/{prefix}/"
    return f"{BASE}/fordon/{prefix}/{page}/"


# ─────────────────────────────────────────────────────────────────────────────
#  HTTP fetch — single worker (one Playwright/Chromium process)
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_batch(url_list: list[str]) -> dict[str, str | None]:
    """Run fetch_helper.js with a batch of URLs. Returns {url: html|None}."""
    if not url_list:
        return {}
    try:
        r = subprocess.run(
            ["node", FETCH_JS, "--stdin"],
            input=json.dumps(url_list),
            capture_output=True,
            text=True,
            timeout=int(PAGE_PAUSE * len(url_list) * 3 + 90),
            cwd=APP_DIR,
        )
        if r.returncode != 0:
            log.warning("fetch_helper error: %s", r.stderr.strip()[:200])
            return {u: None for u in url_list}
        return json.loads(r.stdout)
    except subprocess.TimeoutExpired:
        log.warning("fetch_helper timeout for %d urls", len(url_list))
        return {u: None for u in url_list}
    except Exception as e:
        log.error("fetch_helper exception: %s", e)
        return {u: None for u in url_list}


def _fetch_parallel(url_list: list[str], workers: int = None) -> dict[str, str | None]:
    """
    Split url_list across N parallel Playwright processes.
    Each process launches its own Chromium — independent TLS fingerprints.
    """
    workers = workers or PARALLEL_WORKERS
    if len(url_list) <= BATCH_SIZE or workers <= 1:
        return _fetch_batch(url_list)

    chunk_size = max(1, len(url_list) // workers)
    chunks = [url_list[i:i+chunk_size] for i in range(0, len(url_list), chunk_size)]

    results = {}
    with ThreadPoolExecutor(max_workers=len(chunks)) as pool:
        futures = {pool.submit(_fetch_batch, chunk): chunk for chunk in chunks}
        for future in as_completed(futures):
            try:
                results.update(future.result())
            except Exception as e:
                log.warning("parallel worker error: %s", e)
                for u in futures[future]:
                    results[u] = None
    return results


# ─────────────────────────────────────────────────────────────────────────────
#  HTML parsing — brukare page (person)
# ─────────────────────────────────────────────────────────────────────────────

_NOT_FOUND = ["Kunde inte hitta brukaren", "hittar inte den sida", "Ooups"]


def _parse_brukare(html: str | None, pnr: str) -> dict | None:
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(separator=" ", strip=True)

    for phrase in _NOT_FOUND:
        if phrase in text:
            return None

    name_m = re.search(r"Visa\s+(.+?)\s+på\s+Ratsit", text)
    if name_m:
        namn = name_m.group(1).strip()
    else:
        name_m2 = re.search(
            r"([A-ZÅÄÖ][a-zåäöA-ZÅÄÖ][a-zåäöA-ZÅÄÖ\-\s]{1,50}),\s+en\s+privatperson", text)
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

    fordon_egna_regnr, fordon_egna_modell = [], []
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
        "pnr": pnr, "namn": namn, "alder": alder, "stad": stad, "gata": gata,
        "har_fordon": len(fordon_egna_regnr) > 0,
        "antal_fordon_egna": len(fordon_egna_regnr),
        "fordon_egna_regnr": "|".join(fordon_egna_regnr),
        "fordon_egna_modell": "|".join(fordon_egna_modell),
        "antal_fordon_adress": len(fordon_adress_regnr),
        "fordon_adress_regnr": "|".join(fordon_adress_regnr),
        "hamtad": datetime.now().isoformat(timespec="seconds"),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  HTML parsing — prefix search page (vehicles)
# ─────────────────────────────────────────────────────────────────────────────

def _parse_prefix_page(html: str | None) -> tuple[list[dict], int]:
    """Parse a /fordon/{PREFIX}/{PAGE}/ page. Returns (vehicles, total_hits)."""
    if not html:
        return [], 0
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(separator=" ", strip=True)

    count_m = re.search(r"Visar\s+\d+\s+till\s+\d+\s+av\s+([\d\s]+)\s+träffar", text)
    total = int(count_m.group(1).replace(" ", "")) if count_m else 0

    table = soup.find("table")
    if not table:
        return [], total

    vehicles = []
    for row in table.find_all("tr")[1:]:
        cols = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cols) < 4:
            continue
        link = row.find("a", href=re.compile(r"/fordon/"))
        href = link["href"] if link else ""
        uuid_m = re.search(
            r"/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/?$", href)
        status = row.get("class", [""])[0] if row.get("class") else ""

        vehicles.append({
            "regnr":      cols[1].strip() if len(cols) > 1 else "",
            "modell":     cols[0].strip(),
            "farg":       cols[2].strip() if len(cols) > 2 else "",
            "fordonstyp": cols[3].strip() if len(cols) > 3 else "",
            "modellar":   cols[4].strip() if len(cols) > 4 else "",
            "status":     status,
            "vehicle_id": uuid_m.group(1) if uuid_m else None,
            "url":        urljoin(BASE, href) if href else None,
        })
    return vehicles, total


# ─────────────────────────────────────────────────────────────────────────────
#  Phase 0: Ratsit date-harvesting (smart — high hit rate, ~1.4M people)
# ─────────────────────────────────────────────────────────────────────────────

def _iter_dates(start_year: int, end_year: int, resume_date: str = "") -> Generator[date, None, None]:
    """Yield dates from end_year-12-31 DOWN TO start_year-01-01 (reverse order).

    Phase 0 works backwards so it covers different dates than Phase 2
    (which iterates forwards from start_year). They meet in the middle.
    """
    current = date(end_year, 12, 31)
    stop    = date(start_year, 1, 1)

    if resume_date:
        try:
            resumed = date.fromisoformat(resume_date)
            current = resumed - timedelta(days=1)
        except ValueError:
            pass

    while current >= stop:
        yield current
        current -= timedelta(days=1)


def _ratsit_harvest(date_str: str, gender: str) -> list[dict]:
    """
    Call ratsit_helper.js for one date+gender combination.
    Returns list of {pnr, name, age, city, gender}.
    """
    payload = json.dumps({"date": date_str, "gender": gender})
    try:
        r = subprocess.run(
            ["node", RATSIT_JS, "--stdin"],
            input=payload,
            capture_output=True,
            text=True,
            timeout=int(PHASE0_PAUSE * 60 + 120),
            cwd=APP_DIR,
            env={**os.environ, "PHASE0_PAUSE": str(PHASE0_PAUSE)},
        )
        if r.returncode != 0:
            log.warning("ratsit_helper error [%s %s]: %s", date_str, gender, r.stderr.strip()[:200])
            return []
        hits = json.loads(r.stdout)
        return hits if isinstance(hits, list) else []
    except subprocess.TimeoutExpired:
        log.warning("ratsit_helper timeout [%s %s]", date_str, gender)
        return []
    except Exception as e:
        log.error("ratsit_helper exception [%s %s]: %s", date_str, gender, e)
        return []


def _run_phase0(start_year: int, end_year: int) -> None:
    """
    Phase 0: Harvest PNRs from Ratsit by searching each birth date + gender.
    For each resolved PNR, fetch biluppgifter.se/brukare/{b64}/ and store person.
    ~1.4M people expected from 24,000 dates × 2 genders × 30 max hits/search.
    """
    state       = db.get_job_state()
    resume_date = state.get("phase0_date", "")
    found       = state.get("phase0_found", 0)

    log.info("Phase 0: Ratsit date-harvesting. Resume from '%s', found so far: %d",
             resume_date or "start", found)
    db.update_job_state(status="running", phase="phase0")

    dates_processed = 0
    for d in _iter_dates(start_year, end_year, resume_date):
        if _stop_event.is_set():
            db.update_job_state(status="paused", phase="phase0",
                                phase0_date=d.isoformat())
            log.info("Phase 0 paused at %s", d.isoformat())
            return

        date_str = d.isoformat()

        date_new = 0
        for gender in ("m", "f"):
            if _stop_event.is_set():
                db.update_job_state(status="paused", phase="phase0",
                                    phase0_date=date_str)
                return

            hits = _ratsit_harvest(date_str, gender)
            g_label = "M" if gender == "m" else "F"
            if not hits:
                log.info("Phase 0: %s %s → 0 PNR from Ratsit", date_str, g_label)
                continue

            pnr_list = [h["pnr"] for h in hits if h.get("pnr")]
            known    = db.existing_pnrs(pnr_list)
            to_fetch = [h for h in hits if h.get("pnr") and h["pnr"] not in known]

            log.info("Phase 0: %s %s → %d PNR, %d new, %d already in DB",
                     date_str, g_label, len(pnr_list), len(to_fetch), len(known))

            if to_fetch:
                urls     = [_pnr_to_url(h["pnr"]) for h in to_fetch]
                html_map = _fetch_batch(urls)

                new_people = []
                for h in to_fetch:
                    url  = _pnr_to_url(h["pnr"])
                    html = html_map.get(url)
                    if not html:
                        continue
                    person = _parse_brukare(html, h["pnr"])
                    if person:
                        new_people.append(person)

                batch_inserted = db.insert_people_batch(new_people)
                found += batch_inserted
                date_new += batch_inserted

        dates_processed += 1
        db.update_job_state(
            phase0_date=date_str,
            phase0_found=found,
            updated_at=datetime.now().isoformat(timespec="seconds"),
        )
        log.info("Phase 0: %s done — +%d new, %d total found, %d dates done",
                 date_str, date_new, found, dates_processed)

    log.info("Phase 0 complete: %d dates, %d people found", dates_processed, found)
    db.update_job_state(phase="phase0_done", phase0_found=found)


# ─────────────────────────────────────────────────────────────────────────────
#  Phase 1: Prefix enumeration (fast — all vehicles)
# ─────────────────────────────────────────────────────────────────────────────

def _run_phase1() -> None:
    """
    Enumerate all 3-letter prefixes AAA..ZZZ.
    Each prefix: fetch pages until exhausted (100 vehicles/page).
    Insert each vehicle owner as a person (har_fordon=1).
    ~123,000 page loads → ~11.4M vehicles → estimated 1-2 days.
    """
    state = db.get_job_state()
    resume_prefix = state.get("phase1_prefix", "")
    prefixes_done = state.get("phase1_prefixes_done", 0)
    vehicles_found = state.get("phase1_vehicles", 0)

    log.info("Phase 1: prefix enumeration. Resume from '%s', %d prefixes done",
             resume_prefix, prefixes_done)
    db.update_job_state(status="running", phase="phase1")

    for prefix in _iter_prefixes(resume_prefix):
        if _stop_event.is_set():
            db.update_job_state(status="paused", phase1_prefix=prefix)
            log.info("Phase 1 paused at prefix %s", prefix)
            return

        page_num = 1
        while True:
            if _stop_event.is_set():
                db.update_job_state(status="paused", phase1_prefix=prefix)
                return

            urls = []
            for p in range(page_num, page_num + PARALLEL_WORKERS):
                urls.append(_prefix_page_url(prefix, p))

            html_map = _fetch_parallel(urls, workers=len(urls))

            any_results = False
            batch_vehicles = []
            for p_idx, url in enumerate(urls):
                html = html_map.get(url)
                vehicles, total = _parse_prefix_page(html)
                if not vehicles:
                    continue
                any_results = True
                ts = datetime.now().isoformat(timespec="seconds")
                for v in vehicles:
                    regnr = v.get("regnr", "")
                    if regnr:
                        batch_vehicles.append({
                            "regnr": regnr,
                            "modell": v.get("modell", ""),
                            "farg": v.get("farg", ""),
                            "fordonstyp": v.get("fordonstyp", ""),
                            "modellar": v.get("modellar", ""),
                            "status": v.get("status", ""),
                            "vehicle_id": v.get("vehicle_id", ""),
                            "hamtad": ts,
                        })
            vehicles_found += db.insert_vehicles_batch(batch_vehicles)

            if not any_results:
                break
            page_num += PARALLEL_WORKERS

        prefixes_done += 1
        db.update_job_state(
            phase1_prefix=prefix,
            phase1_prefixes_done=prefixes_done,
            phase1_vehicles=vehicles_found,
            updated_at=datetime.now().isoformat(timespec="seconds"),
        )
        if prefixes_done % 50 == 0:
            log.info("Phase 1: %d/%d prefixes, %d vehicles",
                     prefixes_done, TOTAL_PREFIXES, vehicles_found)

    log.info("Phase 1 complete: %d prefixes, %d vehicles", prefixes_done, vehicles_found)
    db.update_job_state(status="phase1_done", phase="phase1_done",
                        phase1_prefixes_done=prefixes_done, phase1_vehicles=vehicles_found)


# ─────────────────────────────────────────────────────────────────────────────
#  Phase 2: PNR enumeration with parallel workers
# ─────────────────────────────────────────────────────────────────────────────

def _run_phase2(start_year: int, end_year: int, target: int) -> None:
    state = db.get_job_state()
    resume = None
    tested    = state.get("total_tested",    0)
    found     = state.get("total_found",     0)
    not_found = state.get("total_not_found", 0)
    errors    = state.get("total_errors",    0)

    if state.get("current_year", 0) > 0:
        resume = (
            state["current_year"], state["current_month"],
            state["current_day"],  state["current_individ"],
        )
        log.info("Phase 2: resuming from %d-%02d-%02d individ %d",
                 resume[0], resume[1], resume[2], resume[3])

    db.update_job_state(
        status="running", phase="phase2",
        start_year=start_year, end_year=end_year, target_people=target,
        started_at=state.get("started_at") or datetime.now().isoformat(timespec="seconds"),
    )

    big_batch_size = BATCH_SIZE * PARALLEL_WORKERS
    batch_pnrs: list[str]  = []
    batch_pos:  list[tuple] = []

    def flush() -> None:
        nonlocal tested, found, not_found, errors
        if not batch_pnrs:
            return

        known = db.existing_pnrs(list(batch_pnrs))
        to_fetch_pnrs = []
        to_fetch_pos = []
        for (y, mo, d, ind), pnr in zip(batch_pos, batch_pnrs):
            if pnr in known:
                tested += 1
                found += 1
            else:
                to_fetch_pnrs.append(pnr)
                to_fetch_pos.append((y, mo, d, ind))

        if known:
            log.debug("Skipped %d already-known PNRs", len(known))

        if to_fetch_pnrs:
            url_map = {_pnr_to_url(p): p for p in to_fetch_pnrs}
            html_map = _fetch_parallel(list(url_map.keys()))

            new_people = []
            for (y, mo, d, ind), pnr in zip(to_fetch_pos, to_fetch_pnrs):
                url = _pnr_to_url(pnr)
                html = html_map.get(url)
                tested += 1
                if html is None:
                    errors += 1
                else:
                    person = _parse_brukare(html, pnr)
                    if person:
                        new_people.append(person)
                    else:
                        not_found += 1

            found += db.insert_people_batch(new_people)

        ly, lm, ld, li = batch_pos[-1]
        db.save_checkpoint(ly, lm, ld, li, tested, found, not_found, errors)
        log.info("Phase 2 checkpoint: year=%d tested=%d found=%d", ly, tested, found)
        batch_pnrs.clear()
        batch_pos.clear()

    try:
        for y, mo, d, ind, pnr in _iter_pnrs(start_year, end_year, resume):
            if _stop_event.is_set():
                flush()
                db.update_job_state(status="paused")
                return
            if target > 0 and found >= target:
                flush()
                db.update_job_state(status="done")
                return

            batch_pnrs.append(pnr)
            batch_pos.append((y, mo, d, ind))

            if len(batch_pnrs) >= big_batch_size:
                flush()

        flush()
        log.info("Phase 2 complete: tested=%d found=%d", tested, found)
        db.update_job_state(status="done")

    except Exception as e:
        log.exception("Phase 2 crashed: %s", e)
        db.update_job_state(status="error")


# ─────────────────────────────────────────────────────────────────────────────
#  Main run loop (both phases)
# ─────────────────────────────────────────────────────────────────────────────

def _run(start_year: int, end_year: int, target: int,
         skip_phase1: bool = False, skip_phase0: bool = False) -> None:
    try:
        state = db.get_job_state()
        phase = state.get("phase", "")

        bg_threads: list[threading.Thread] = []

        # Phase 0: Ratsit harvesting in background (parallel, conservative rate)
        phase0_needed = (
            not skip_phase0
            and phase not in ("phase0_done",)
        )
        if phase0_needed and not _stop_event.is_set():
            phase0_thread = threading.Thread(
                target=_run_phase0, args=(start_year, end_year),
                daemon=True, name="phase0")
            phase0_thread.start()
            bg_threads.append(phase0_thread)
            log.info("Phase 0 (Ratsit harvesting) started in background thread")

        # Phase 1: vehicle prefix enumeration in background
        phase1_needed = (not skip_phase1 and phase not in ("phase1_done",))
        if phase1_needed and not _stop_event.is_set():
            phase1_thread = threading.Thread(
                target=_run_phase1, daemon=True, name="phase1")
            phase1_thread.start()
            bg_threads.append(phase1_thread)
            log.info("Phase 1 started in background thread")

        # Phase 2 runs in main thread — skips PNRs found by Phase 0
        if not _stop_event.is_set():
            _run_phase2(start_year, end_year, target)

        # Wait for background threads
        for t in bg_threads:
            if t.is_alive():
                log.info("Waiting for %s to finish...", t.name)
                t.join()

    except Exception as e:
        log.exception("Scraper crashed: %s", e)
        db.update_job_state(status="error")
    finally:
        with _state_lock:
            global _thread
            _thread = None


# ─────────────────────────────────────────────────────────────────────────────
#  Public control interface
# ─────────────────────────────────────────────────────────────────────────────

def start(target: int = 0, start_year: int | None = None,
          end_year: int | None = None, skip_phase1: bool = False,
          skip_phase0: bool = False) -> bool:
    global _thread
    with _state_lock:
        if _thread is not None and _thread.is_alive():
            return False
        _stop_event.clear()
        sy = start_year or START_YEAR
        ey = end_year   or END_YEAR
        _thread = threading.Thread(
            target=_run, args=(sy, ey, target, skip_phase1, skip_phase0),
            daemon=True, name="scraper",
        )
        _thread.start()
    return True


def stop() -> None:
    _stop_event.set()


def is_running() -> bool:
    return _thread is not None and _thread.is_alive()


def get_status() -> dict:
    state  = db.get_job_state()
    phase  = state.get("phase", "idle")
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

    total_space = _total_pnr_space(sy, ey)
    current_pos = _pnr_position_index(max(cy, sy), cm, cd, ci, sy) if cy >= sy else 0
    pct_done    = round(current_pos / total_space * 100, 2) if total_space else 0

    # Phase 0 stats
    p0_date  = state.get("phase0_date",  "")
    p0_found = state.get("phase0_found", 0)
    total_days = (END_YEAR - START_YEAR + 1) * 365
    p0_pct = 0.0
    if p0_date:
        try:
            p0_d   = date.fromisoformat(p0_date)
            p0_day = (p0_d - date(START_YEAR, 1, 1)).days
            p0_pct = round(p0_day / total_days * 100, 1) if total_days else 0
        except ValueError:
            pass

    # Phase 1 stats
    p1_prefixes = state.get("phase1_prefixes_done", 0)
    p1_vehicles = state.get("phase1_vehicles", 0)
    p1_pct      = round(p1_prefixes / TOTAL_PREFIXES * 100, 1) if p1_prefixes else 0

    eta_seconds = None
    started = state.get("started_at", "")
    if tested > 0 and started:
        try:
            elapsed = (datetime.now() - datetime.fromisoformat(started)).total_seconds()
            rate = tested / elapsed if elapsed > 0 else 0
            remaining = max(0, total_space - current_pos)
            eta_seconds = int(remaining / rate) if rate > 0 else None
        except Exception:
            pass

    return {
        "status":               state.get("status", "idle"),
        "phase":                phase,
        "is_running":           is_running(),
        "parallel_workers":     PARALLEL_WORKERS,
        # Phase 0
        "phase0_date":          p0_date,
        "phase0_found":         p0_found,
        "phase0_pct_done":      p0_pct,
        # Phase 1
        "phase1_prefixes_done": p1_prefixes,
        "phase1_prefixes_total": TOTAL_PREFIXES,
        "phase1_pct":           p1_pct,
        "phase1_vehicles":      p1_vehicles,
        # Phase 2
        "start_year":           sy,
        "end_year":             ey,
        "current_position":     f"{cy}-{cm:02d}-{cd:02d} individ {ci}",
        "phase2_pct_done":      pct_done,
        "total_pnr_space":      total_space,
        "total_tested":         tested,
        "total_found":          found,
        "total_not_found":      n_f,
        "total_errors":         errs,
        "hit_rate_pct":         round(found / tested * 100, 1) if tested else 0,
        "target_people":        target,
        "db_size_mb":           round(db.db_file_size_mb(), 1),
        "eta_seconds":          eta_seconds,
        "started_at":           started,
        "updated_at":           state.get("updated_at", ""),
    }


def maybe_auto_resume() -> None:
    if os.environ.get("AUTO_RESUME", "true").lower() not in ("true", "1", "yes"):
        return
    state = db.get_job_state()
    status = state.get("status", "idle")
    has_progress = (
        state.get("total_tested", 0) > 0
        or state.get("phase1_prefixes_done", 0) > 0
        or state.get("phase0_found", 0) > 0
    )
    if status in ("running", "paused") and has_progress:
        log.info("AUTO_RESUME: status=%s, resuming from checkpoint.", status)
        target = state.get("target_people", 0)
        sy     = state.get("start_year", START_YEAR)
        ey     = state.get("end_year",   END_YEAR)
        start(target=target, start_year=sy, end_year=ey)
