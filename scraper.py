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
PAGE_PAUSE       = float(os.environ.get("PAGE_PAUSE",       "0.5"))
PHASE0_PAUSE     = float(os.environ.get("PHASE0_PAUSE",     "1.5"))
BATCH_SIZE       = int(os.environ.get("BATCH_SIZE",         "25"))
PARALLEL_WORKERS = int(os.environ.get("PARALLEL_WORKERS",   "6"))
START_YEAR       = int(os.environ.get("START_YEAR",         "1940"))
END_YEAR         = int(os.environ.get("END_YEAR",           "2005"))
TARGET_PEOPLE_DEFAULT = int(os.environ.get("TARGET_PEOPLE_DEFAULT", "10400000"))
HEARTBEAT_STALE_SECONDS = int(os.environ.get("HEARTBEAT_STALE_SECONDS", "300"))

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
    proc = None
    try:
        proc = subprocess.Popen(
            ["node", FETCH_JS, "--stdin"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, cwd=APP_DIR,
        )
        timeout_sec = int(PAGE_PAUSE * len(url_list) * 3 + 90)
        stdout, stderr = proc.communicate(input=json.dumps(url_list), timeout=timeout_sec)
        if proc.returncode != 0:
            log.warning("fetch_helper error: %s", (stderr or "").strip()[:200])
            return {u: None for u in url_list}
        return json.loads(stdout)
    except subprocess.TimeoutExpired:
        log.warning("fetch_helper timeout for %d urls — killing process", len(url_list))
        if proc:
            proc.kill()
            proc.wait()
        return {u: None for u in url_list}
    except Exception as e:
        log.error("fetch_helper exception: %s", e)
        if proc and proc.poll() is None:
            proc.kill()
            proc.wait()
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
        "kalla": "biluppgifter",
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
    proc = None
    try:
        proc = subprocess.Popen(
            ["node", RATSIT_JS, "--stdin"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, cwd=APP_DIR,
            env={**os.environ, "PHASE0_PAUSE": str(PHASE0_PAUSE)},
        )
        timeout_sec = int(PHASE0_PAUSE * 60 + 120)
        stdout, stderr = proc.communicate(input=payload, timeout=timeout_sec)
        if proc.returncode != 0:
            log.warning("ratsit_helper error [%s %s]: %s", date_str, gender, (stderr or "").strip()[:200])
            return []
        hits = json.loads(stdout)
        return hits if isinstance(hits, list) else []
    except subprocess.TimeoutExpired:
        log.warning("ratsit_helper timeout [%s %s] — killing process", date_str, gender)
        if proc:
            proc.kill()
            proc.wait()
        return []
    except Exception as e:
        log.error("ratsit_helper exception [%s %s]: %s", date_str, gender, e)
        if proc and proc.poll() is None:
            proc.kill()
            proc.wait()
        return []


def _run_phase0(start_year: int, end_year: int) -> None:
    """
    Phase 0: Harvest people from Ratsit by searching each birth date + gender.
    For each resolved person the ratsit_helper returns all search fields PLUS
    PNR, phone, and grannar from the profile visit.  We then fetch the
    biluppgifter brukare page for fordon data and merge everything into a
    single rich person dict before inserting into the DB.
    """
    state       = db.get_job_state()
    resume_date = state.get("phase0_date", "")
    found       = state.get("phase0_found", 0)
    phones      = state.get("phase0_phones", 0)

    if resume_date and found == 0:
        try:
            resumed = date.fromisoformat(resume_date)
            if resumed <= date(start_year, 1, 1):
                log.warning(
                    "Phase 0 checkpoint looks stale (%s, found=0). "
                    "Resetting phase0 checkpoint to restart from end_year.",
                    resume_date,
                )
                resume_date = ""
                db.update_job_state(phase0_date="", phase0_found=0, phase0_phones=0)
        except ValueError:
            pass

    log.info("Phase 0: Ratsit date-harvesting. Resume from '%s', found so far: %d",
             resume_date or "start", found)
    now_iso = datetime.now().isoformat(timespec="seconds")
    db.update_job_state(status="running", phase="phase0", phase0_status="running",
                        updated_at=now_iso, last_progress_at=now_iso)

    dates_processed = 0
    for d in _iter_dates(start_year, end_year, resume_date):
        if _stop_event.is_set():
            db.update_job_state(status="paused", phase="phase0",
                                phase0_status="paused",
                                phase0_date=d.isoformat(),
                                updated_at=datetime.now().isoformat(timespec="seconds"))
            log.info("Phase 0 paused at %s", d.isoformat())
            return

        date_str = d.isoformat()

        date_new = 0
        date_phones = 0
        for gender in ("m", "f"):
            if _stop_event.is_set():
                db.update_job_state(status="paused", phase="phase0",
                                    phase0_status="paused",
                                    phase0_date=date_str,
                                    updated_at=datetime.now().isoformat(timespec="seconds"))
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
                    if not person:
                        continue

                    person["kon"] = h.get("gender", "")
                    person["tilltalsnamn"] = h.get("givenName", "")
                    person["lat"] = str(h.get("lat", ""))
                    person["lng"] = str(h.get("lng", ""))
                    person["telefon"] = h.get("phone", "")
                    person["grannar"] = h.get("neighbours", -1)
                    person["kalla"] = "ratsit+biluppgifter"

                    married = h.get("married")
                    person["gift"] = int(married) if isinstance(married, bool) else -1

                    bolag = h.get("hasCorporateEngagements")
                    person["bolag"] = int(bolag) if isinstance(bolag, bool) else -1

                    new_people.append(person)
                    if h.get("phone"):
                        date_phones += 1

                batch_inserted = db.insert_people_batch(new_people)
                found += batch_inserted
                date_new += batch_inserted

        phones += date_phones
        dates_processed += 1
        now_iso = datetime.now().isoformat(timespec="seconds")
        db.update_job_state(
            phase0_date=date_str,
            phase0_found=found,
            phase0_phones=phones,
            updated_at=now_iso,
            last_progress_at=now_iso,
        )
        log.info("Phase 0: %s done — +%d new (+%d phones), %d total found, %d dates done",
                 date_str, date_new, date_phones, found, dates_processed)

    log.info("Phase 0 complete: %d dates, %d people found, %d phones", dates_processed, found, phones)
    now_iso = datetime.now().isoformat(timespec="seconds")
    db.update_job_state(phase="phase0_done", phase0_status="done",
                        phase0_found=found, phase0_phones=phones,
                        updated_at=now_iso, last_progress_at=now_iso)


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
    now_iso = datetime.now().isoformat(timespec="seconds")
    db.update_job_state(status="running", phase="phase1", phase1_status="running",
                        updated_at=now_iso, last_progress_at=now_iso)

    for prefix in _iter_prefixes(resume_prefix):
        if _stop_event.is_set():
            db.update_job_state(
                status="paused",
                phase1_status="paused",
                phase1_prefix=prefix,
                updated_at=datetime.now().isoformat(timespec="seconds"),
            )
            log.info("Phase 1 paused at prefix %s", prefix)
            return

        page_num = 1
        while True:
            if _stop_event.is_set():
                db.update_job_state(
                    status="paused",
                    phase1_status="paused",
                    phase1_prefix=prefix,
                    updated_at=datetime.now().isoformat(timespec="seconds"),
                )
                return

            urls = []
            for p in range(page_num, page_num + PARALLEL_WORKERS):
                urls.append(_prefix_page_url(prefix, p))

            html_map = _fetch_parallel(urls, workers=min(len(urls), 2))

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
        now_iso = datetime.now().isoformat(timespec="seconds")
        db.update_job_state(
            phase1_prefix=prefix,
            phase1_prefixes_done=prefixes_done,
            phase1_vehicles=vehicles_found,
            updated_at=now_iso,
            last_progress_at=now_iso,
        )
        if prefixes_done % 50 == 0:
            log.info("Phase 1: %d/%d prefixes, %d vehicles",
                     prefixes_done, TOTAL_PREFIXES, vehicles_found)

    log.info("Phase 1 complete: %d prefixes, %d vehicles", prefixes_done, vehicles_found)
    now_iso = datetime.now().isoformat(timespec="seconds")
    db.update_job_state(
        phase="phase1_done",
        phase1_status="done",
        phase1_prefixes_done=prefixes_done,
        phase1_vehicles=vehicles_found,
        updated_at=now_iso,
        last_progress_at=now_iso,
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Phase 2E: Eniro-guided PNR resolution (fast — ~10 biluppgifter req/person)
# ─────────────────────────────────────────────────────────────────────────────

PHASE2E_BATCH = int(os.environ.get("PHASE2E_BATCH", "5"))


def _birthdate_to_candidates(birthdate_str: str) -> list[str]:
    """Given YYYY-MM-DD, generate all Luhn-valid PNR candidates."""
    parts = birthdate_str.split("-")
    if len(parts) != 3:
        return []
    try:
        year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
    except ValueError:
        return []
    candidates = []
    for individ in range(1, 1000):
        pnr = make_pnr(year, month, day, individ)
        if pnr:
            candidates.append(pnr)
    return candidates


def _run_phase2e(start_year: int, end_year: int) -> None:
    """Eniro-guided PNR resolution: use birthDate to narrow brute-force from 24M to ~100 per person."""
    import asyncio
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        log.warning("Phase 2E: playwright not installed, skipping")
        return

    import eniro_helper

    state = db.get_job_state()
    resolved = state.get("phase2e_resolved", 0)
    searched = state.get("phase2e_searched", 0)

    eniro_pending = db.count_eniro_pending()
    log.info("Phase 2E: Eniro-guided PNR resolution. %d pending, %d already resolved",
             eniro_pending, resolved)

    if eniro_pending == 0:
        log.info("Phase 2E: nothing to resolve")
        db.update_job_state(phase2e_status="done")
        return

    now_iso = datetime.now().isoformat(timespec="seconds")
    db.update_job_state(phase="phase2e", phase2e_status="running",
                        updated_at=now_iso, last_progress_at=now_iso)

    PERSON_TIMEOUT = 180  # max seconds per person (Eniro + biluppgifter)
    CONSECUTIVE_ERRORS_RESTART = 3  # restart browser after this many consecutive errors

    async def _eniro_loop():
        nonlocal resolved, searched
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            consecutive_errors = 0

            try:
                while not _stop_event.is_set():
                    batch = db.get_eniro_pending_pnrs(PHASE2E_BATCH)
                    if not batch:
                        break

                    for person in batch:
                        if _stop_event.is_set():
                            break

                        namn = person.get("namn", "")
                        stad = person.get("stad", "")
                        pnr = person.get("pnr", "")
                        if not namn:
                            db.enrich_person(pnr, {"kalla": "biluppgifter+eniro_skip"})
                            continue

                        searched += 1
                        log.info("Phase 2E: searching Eniro for '%s' (%s)", namn, stad)

                        try:
                            eniro_data = await asyncio.wait_for(
                                eniro_helper.resolve_birthdate(browser, namn, stad),
                                timeout=PERSON_TIMEOUT,
                            )
                            consecutive_errors = 0
                        except asyncio.TimeoutError:
                            log.warning("Phase 2E: HARD TIMEOUT (%ds) for '%s' — skipping", PERSON_TIMEOUT, namn)
                            db.enrich_person(pnr, {"kalla": "biluppgifter+eniro_timeout"})
                            consecutive_errors += 1
                            if consecutive_errors >= CONSECUTIVE_ERRORS_RESTART:
                                log.warning("Phase 2E: %d consecutive errors — restarting browser", consecutive_errors)
                                try:
                                    await browser.close()
                                except Exception:
                                    pass
                                browser = await pw.chromium.launch(headless=True)
                                consecutive_errors = 0
                            now_iso = datetime.now().isoformat(timespec="seconds")
                            db.update_job_state(
                                phase2e_resolved=resolved, phase2e_searched=searched,
                                updated_at=now_iso, last_progress_at=now_iso,
                            )
                            continue
                        except Exception as e:
                            log.warning("Phase 2E: Eniro error for '%s': %s", namn, e)
                            db.enrich_person(pnr, {"kalla": "biluppgifter+eniro_error"})
                            consecutive_errors += 1
                            if consecutive_errors >= CONSECUTIVE_ERRORS_RESTART:
                                log.warning("Phase 2E: %d consecutive errors — restarting browser", consecutive_errors)
                                try:
                                    await browser.close()
                                except Exception:
                                    pass
                                browser = await pw.chromium.launch(headless=True)
                                consecutive_errors = 0
                            continue

                        if not eniro_data or not eniro_data.get("birthDate"):
                            log.info("Phase 2E: no birthDate for '%s'", namn)
                            db.enrich_person(pnr, {"kalla": "biluppgifter+eniro_miss"})
                        else:
                            birth = eniro_data["birthDate"]
                            telefon_eniro = eniro_data.get("telefon", "")
                            postnr = eniro_data.get("postnummer", "")

                            enrich_fields = {"kalla": "biluppgifter+eniro"}
                            if telefon_eniro:
                                enrich_fields["telefon"] = telefon_eniro
                            if postnr:
                                enrich_fields["postnummer"] = postnr

                            candidates = _birthdate_to_candidates(birth)
                            log.info("Phase 2E: '%s' born %s → %d PNR candidates, testing...",
                                     namn, birth, len(candidates))

                            target_parts = set(namn.lower().split())
                            matched = False

                            for i in range(0, len(candidates), BATCH_SIZE):
                                if _stop_event.is_set():
                                    break
                                chunk = candidates[i:i + BATCH_SIZE]
                                urls = [_pnr_to_url(c) for c in chunk]
                                html_map = _fetch_batch(urls)

                                for c_pnr in chunk:
                                    url = _pnr_to_url(c_pnr)
                                    html = html_map.get(url)
                                    if not html:
                                        continue
                                    person_data = _parse_brukare(html, c_pnr)
                                    if not person_data:
                                        continue
                                    found_parts = set(person_data["namn"].lower().split())
                                    if target_parts & found_parts:
                                        person_data.update(enrich_fields)
                                        db.insert_people_batch([person_data])
                                        resolved += 1
                                        matched = True
                                        log.info("Phase 2E: MATCHED '%s' → %s (attempt %d)",
                                                 namn, c_pnr, i + chunk.index(c_pnr) + 1)
                                        break

                                if matched:
                                    break

                            if not matched:
                                db.enrich_person(pnr, enrich_fields)
                                log.info("Phase 2E: no biluppgifter match for '%s' (tested %d candidates)",
                                         namn, len(candidates))

                        now_iso = datetime.now().isoformat(timespec="seconds")
                        db.update_job_state(
                            phase2e_resolved=resolved,
                            phase2e_searched=searched,
                            updated_at=now_iso,
                            last_progress_at=now_iso,
                        )

                        await asyncio.sleep(eniro_helper._jitter(1.0))

            finally:
                try:
                    await browser.close()
                except Exception:
                    pass

    try:
        asyncio.run(_eniro_loop())
    except Exception as e:
        log.exception("Phase 2E crashed: %s", e)
        db.update_job_state(phase2e_status="error")

    now_iso = datetime.now().isoformat(timespec="seconds")
    if _stop_event.is_set():
        db.update_job_state(phase2e_status="paused", updated_at=now_iso, last_progress_at=now_iso)
    else:
        db.update_job_state(phase2e_status="done", updated_at=now_iso, last_progress_at=now_iso)

    log.info("Phase 2E done/paused: %d searched, %d resolved", searched, resolved)


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

    now_iso = datetime.now().isoformat(timespec="seconds")
    db.update_job_state(
        status="running", phase="phase2",
        start_year=start_year, end_year=end_year, target_people=target,
        started_at=state.get("started_at") or now_iso,
        updated_at=now_iso,
        last_progress_at=state.get("last_progress_at") or now_iso,
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
                now_iso = datetime.now().isoformat(timespec="seconds")
                db.update_job_state(status="paused", updated_at=now_iso)
                return
            if target > 0 and found >= target:
                flush()
                now_iso = datetime.now().isoformat(timespec="seconds")
                db.update_job_state(status="done", updated_at=now_iso, last_progress_at=now_iso)
                return

            batch_pnrs.append(pnr)
            batch_pos.append((y, mo, d, ind))

            if len(batch_pnrs) >= big_batch_size:
                flush()

        flush()
        log.info("Phase 2 complete: tested=%d found=%d", tested, found)
        now_iso = datetime.now().isoformat(timespec="seconds")
        db.update_job_state(status="done", updated_at=now_iso, last_progress_at=now_iso)

    except Exception as e:
        log.exception("Phase 2 crashed: %s", e)
        now_iso = datetime.now().isoformat(timespec="seconds")
        db.update_job_state(status="error", updated_at=now_iso)


# ─────────────────────────────────────────────────────────────────────────────
#  Phase 3: Ratsit enrichment of existing people (fast — ~2s per person)
# ─────────────────────────────────────────────────────────────────────────────

ENRICH_BATCH_SIZE = int(os.environ.get("ENRICH_BATCH_SIZE", "10"))


def _ratsit_enrich_batch(people: list[dict]) -> list[dict]:
    """Call ratsit_helper.js in enrich mode. Returns list of enriched dicts."""
    payload = json.dumps({"mode": "enrich", "people": people})
    proc = None
    try:
        proc = subprocess.Popen(
            ["node", RATSIT_JS, "--stdin"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, cwd=APP_DIR,
            env={**os.environ, "PHASE0_PAUSE": str(PHASE0_PAUSE)},
        )
        timeout_sec = int(PHASE0_PAUSE * len(people) * 10 + 120)
        stdout, stderr = proc.communicate(input=payload, timeout=timeout_sec)
        if stderr:
            log.info("Phase 3 ratsit_helper: %s", stderr.strip()[:300])
        if proc.returncode != 0:
            log.warning("Phase 3 ratsit_helper error (exit %d)", proc.returncode)
            return []
        result = json.loads(stdout)
        return result if isinstance(result, list) else []
    except subprocess.TimeoutExpired:
        log.warning("Phase 3 ratsit_helper timeout — killing")
        if proc:
            proc.kill()
            proc.wait()
        return []
    except Exception as e:
        log.error("Phase 3 ratsit_helper exception: %s", e)
        if proc and proc.poll() is None:
            proc.kill()
            proc.wait()
        return []


def _run_phase3() -> None:
    """Enrich existing people that lack Ratsit data (kön, GPS, telefon, etc.)."""
    state = db.get_job_state()
    enriched = state.get("phase3_enriched", 0)
    phones = state.get("phase3_phones", 0)

    remaining = db.count_unenriched()
    log.info("Phase 3: Ratsit enrichment. %d unenriched, %d already enriched",
             remaining, enriched)

    if remaining == 0:
        log.info("Phase 3: nothing to enrich")
        return

    now_iso = datetime.now().isoformat(timespec="seconds")
    db.update_job_state(status="running", phase="phase3", updated_at=now_iso, last_progress_at=now_iso)

    while not _stop_event.is_set():
        batch = db.get_unenriched_pnrs(ENRICH_BATCH_SIZE)
        if not batch:
            break

        results = _ratsit_enrich_batch(batch)
        if not results:
            log.info("Phase 3: batch returned 0 results, continuing")
            continue

        enrich_pairs = []
        batch_phones = 0
        for r in results:
            pnr = r.get("pnr")
            if not pnr:
                continue
            fields = {
                "kon": r.get("gender", ""),
                "tilltalsnamn": r.get("givenName", ""),
                "lat": str(r.get("lat", "")),
                "lng": str(r.get("lng", "")),
                "telefon": r.get("phone", ""),
                "grannar": r.get("neighbours", -1),
                "kalla": "ratsit+biluppgifter",
            }
            married = r.get("married")
            fields["gift"] = int(married) if isinstance(married, bool) else -1
            bolag = r.get("hasCorporateEngagements")
            fields["bolag"] = int(bolag) if isinstance(bolag, bool) else -1

            enrich_pairs.append((pnr, fields))
            if r.get("phone"):
                batch_phones += 1

        updated = db.enrich_people_batch(enrich_pairs)
        enriched += updated
        phones += batch_phones

        now_iso = datetime.now().isoformat(timespec="seconds")
        db.update_job_state(
            phase3_enriched=enriched,
            phase3_phones=phones,
            updated_at=now_iso,
            last_progress_at=now_iso,
        )
        log.info("Phase 3: enriched +%d (%d phones), total %d enriched",
                 updated, batch_phones, enriched)

    now_iso = datetime.now().isoformat(timespec="seconds")
    if _stop_event.is_set():
        db.update_job_state(status="paused", phase="phase3", updated_at=now_iso, last_progress_at=now_iso)
    else:
        db.update_job_state(status="done", phase="phase3_done", updated_at=now_iso, last_progress_at=now_iso)

    log.info("Phase 3 done/paused: %d enriched, %d phones", enriched, phones)


# ─────────────────────────────────────────────────────────────────────────────
#  Main run loop
# ─────────────────────────────────────────────────────────────────────────────

def _run(start_year: int, end_year: int, target: int,
         skip_phase1: bool = False, skip_phase0: bool = False) -> None:
    """Run scraping phases with smart parallelism within 4 GB RAM.

    Stage 1 — parallel harvest (~1.5 GB total):
      Phase 0  (Ratsit date-harvest, 1 Chromium, ~400 MB)
      Phase 1  (vehicle prefixes, 2 Chromium, ~400 MB)
      Phase 2E (Eniro PNR resolver, 1 Chromium, ~400 MB)

    Stage 2 — full-power brute-force (~2.5 GB):
      Phase 2  (PNR enumeration, 8 workers, gets all remaining RAM)

    Stage 3 — enrichment:
      Phase 3  (Ratsit enrichment, 1 Chromium, after Phase 2)
    """
    try:
        state = db.get_job_state()
        phase = state.get("phase", "")
        bg_threads: list[threading.Thread] = []

        # ── Stage 1: parallel harvest ────────────────────────────────────
        phase0_needed = (
            not skip_phase0
            and phase not in ("phase0_done",)
        )
        phase1_needed = (not skip_phase1 and phase not in ("phase1_done",))
        phase2e_needed = db.count_eniro_pending() > 0

        if phase0_needed and not _stop_event.is_set():
            phase0_thread = threading.Thread(
                target=_run_phase0, args=(start_year, end_year),
                daemon=True, name="phase0")
            phase0_thread.start()
            bg_threads.append(phase0_thread)
            log.info("Phase 0 (Ratsit) started in background")

        if phase1_needed and not _stop_event.is_set():
            phase1_thread = threading.Thread(
                target=_run_phase1, daemon=True, name="phase1")
            phase1_thread.start()
            bg_threads.append(phase1_thread)
            log.info("Phase 1 (vehicles) started in background")

        if phase2e_needed and not _stop_event.is_set():
            phase2e_thread = threading.Thread(
                target=_run_phase2e, args=(start_year, end_year),
                daemon=True, name="phase2e")
            phase2e_thread.start()
            bg_threads.append(phase2e_thread)
            log.info("Phase 2E (Eniro) started in background")

        # Wait for Stage 1 to finish (Phase 0, 1, 2E all complete)
        for t in bg_threads:
            if t.is_alive():
                log.info("Waiting for %s to finish...", t.name)
                t.join()
        bg_threads.clear()

        if _stop_event.is_set():
            return

        # ── Stage 2: full-power Phase 2 (gets all RAM) ──────────────────
        global PARALLEL_WORKERS
        original_workers = PARALLEL_WORKERS
        PARALLEL_WORKERS = max(original_workers, 8)
        log.info("Phase 2 starting with %d workers (boosted from %d)",
                 PARALLEL_WORKERS, original_workers)

        if not _stop_event.is_set():
            _run_phase2(start_year, end_year, target)

        PARALLEL_WORKERS = original_workers

        # ── Stage 3: enrichment ──────────────────────────────────────────
        remaining_unenriched = db.count_unenriched()
        if not _stop_event.is_set() and remaining_unenriched > 0:
            log.info("Phase 3 (Ratsit enrichment) starting after Phase 2")
            _run_phase3()
        elif not _stop_event.is_set():
            now_iso = datetime.now().isoformat(timespec="seconds")
            db.update_job_state(status="done", phase="phase3_done", updated_at=now_iso, last_progress_at=now_iso)

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


def stop(wait: bool = False, timeout: float = 30.0) -> bool:
    _stop_event.set()
    state = db.get_job_state()
    status = state.get("status", "idle")
    if is_running() and status not in ("paused", "done", "error"):
        db.update_job_state(status="paused", updated_at=datetime.now().isoformat(timespec="seconds"))
    if not wait:
        return False
    with _state_lock:
        t = _thread
    if t is None:
        return True
    t.join(timeout=timeout)
    return not t.is_alive()


def is_running() -> bool:
    return _thread is not None and _thread.is_alive()


def get_status() -> dict:
    state  = db.get_job_state()
    now    = datetime.now()
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
    target_goal = target if target and target > 0 else TARGET_PEOPLE_DEFAULT

    total_space = _total_pnr_space(sy, ey)
    current_pos = _pnr_position_index(max(cy, sy), cm, cd, ci, sy) if cy >= sy else 0
    pct_done    = round(current_pos / total_space * 100, 2) if total_space else 0

    # Phase 0 stats
    p0_date  = state.get("phase0_date",  "")
    p0_found = state.get("phase0_found", 0)
    total_days = (date(ey, 12, 31) - date(sy, 1, 1)).days + 1
    p0_days_done = 0
    p0_pct = 0.0
    if p0_date:
        try:
            p0_d = date.fromisoformat(p0_date)
            remaining_days = max(0, (p0_d - date(sy, 1, 1)).days)
            p0_days_done = max(0, total_days - remaining_days)
            p0_pct = round(min(100.0, (p0_days_done / total_days) * 100), 1) if total_days else 0.0
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

    heartbeat_at = state.get("last_progress_at", "") or state.get("updated_at", "")
    heartbeat_age = None
    if heartbeat_at:
        try:
            heartbeat_age = max(0, int((now - datetime.fromisoformat(heartbeat_at)).total_seconds()))
        except ValueError:
            heartbeat_age = None
    heartbeat_is_stale = (
        state.get("status", "idle") == "running"
        and heartbeat_age is not None
        and heartbeat_age > HEARTBEAT_STALE_SECONDS
    )

    running_local = is_running()
    status = state.get("status", "idle")
    has_any_progress = tested > 0 or p1_prefixes > 0 or p0_found > 0
    in_progress_status = status in ("running", "phase0_done", "phase1_done")
    if in_progress_status and not running_local and has_any_progress:
        runtime_state = "resuming"
    elif heartbeat_is_stale:
        runtime_state = "stalled"
    elif running_local:
        runtime_state = "healthy"
    else:
        runtime_state = state.get("status", "idle")

    speed_people = float(state.get("speed_people_per_hour", 0) or 0)
    speed_tested = float(state.get("speed_tested_per_hour", 0) or 0)

    return {
        "status":               state.get("status", "idle"),
        "phase":                phase,
        "is_running":           is_running(),
        "parallel_workers":     PARALLEL_WORKERS,
        # Phase 0
        "phase0_date":          p0_date,
        "phase0_found":         p0_found,
        "phase0_pct_done":      p0_pct,
        "phase0_days_done":     p0_days_done,
        "phase0_days_total":    total_days,
        "phase0_phones":        state.get("phase0_phones", 0),
        "phase0_status":        state.get("phase0_status", "idle"),
        # Phase 2E (Eniro-guided)
        "phase2e_resolved":     state.get("phase2e_resolved", 0),
        "phase2e_searched":     state.get("phase2e_searched", 0),
        "phase2e_status":       state.get("phase2e_status", "idle"),
        "phase2e_pending":      db.count_eniro_pending(),
        # Phase 3 (enrichment)
        "phase3_enriched":      state.get("phase3_enriched", 0),
        "phase3_phones":        state.get("phase3_phones", 0),
        "phase3_unenriched":    db.count_unenriched(),
        # Phase 1
        "phase1_prefixes_done": p1_prefixes,
        "phase1_prefixes_total": TOTAL_PREFIXES,
        "phase1_pct":           p1_pct,
        "phase1_vehicles":      p1_vehicles,
        "phase1_status":        state.get("phase1_status", "idle"),
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
        "target_people_goal":   target_goal,
        "target_people_default": TARGET_PEOPLE_DEFAULT,
        "db_size_mb":           round(db.db_file_size_mb(), 1),
        "eta_seconds":          eta_seconds,
        "eta_phase2_seconds":   eta_seconds,
        "speed_people_per_hour": speed_people,
        "speed_tested_per_hour": speed_tested,
        "heartbeat_at":         heartbeat_at,
        "heartbeat_age_seconds": heartbeat_age,
        "heartbeat_is_stale":   heartbeat_is_stale,
        "runtime_state":        runtime_state,
        "heartbeat_stale_after_seconds": HEARTBEAT_STALE_SECONDS,
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
    terminal_statuses = {"idle", "done", "error"}
    has_checkpoint = (
        state.get("current_year", 0) > 0
        or state.get("phase1_prefixes_done", 0) > 0
        or state.get("phase0_found", 0) > 0
    )
    if status not in terminal_statuses and has_progress and has_checkpoint:
        log.info("AUTO_RESUME: status=%s, resuming from checkpoint.", status)
        target = state.get("target_people", 0)
        sy     = state.get("start_year", START_YEAR)
        ey     = state.get("end_year",   END_YEAR)
        start(target=target, start_year=sy, end_year=ey)
