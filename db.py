"""
db.py — SQLite helper for the full population scraper.

Tables:
  people     — one row per found person (from brukare pages)
  vehicles   — one row per vehicle (from prefix enumeration, Phase 1)
  job_state  — single row tracking the scraper's position and counters
"""

import logging
import sqlite3
import os
import threading
from datetime import datetime
from typing import Any, Generator

DATA_DIR = os.environ.get("DATA_DIR", "/var/data")
DB_PATH  = os.path.join(DATA_DIR, "people.db")

_local = threading.local()
log = logging.getLogger("db")


def _conn() -> sqlite3.Connection:
    if not hasattr(_local, "conn") or _local.conn is None:
        os.makedirs(DATA_DIR, exist_ok=True)
        c = sqlite3.connect(DB_PATH, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA busy_timeout=10000")
        c.execute("PRAGMA synchronous=NORMAL")
        _local.conn = c
    return _local.conn


def init_db() -> None:
    conn = _conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS people (
            pnr                 TEXT PRIMARY KEY,
            namn                TEXT    DEFAULT '',
            alder               INTEGER DEFAULT 0,
            stad                TEXT    DEFAULT '',
            gata                TEXT    DEFAULT '',
            har_fordon          INTEGER DEFAULT 0,
            antal_fordon_egna   INTEGER DEFAULT 0,
            fordon_egna_regnr   TEXT    DEFAULT '',
            fordon_egna_modell  TEXT    DEFAULT '',
            antal_fordon_adress INTEGER DEFAULT 0,
            fordon_adress_regnr TEXT    DEFAULT '',
            hamtad              TEXT    DEFAULT '',
            kon                 TEXT    DEFAULT '',
            gift                INTEGER DEFAULT -1,
            tilltalsnamn        TEXT    DEFAULT '',
            postnummer          TEXT    DEFAULT '',
            lat                 TEXT    DEFAULT '',
            lng                 TEXT    DEFAULT '',
            bolag               INTEGER DEFAULT -1,
            telefon             TEXT    DEFAULT '',
            grannar             INTEGER DEFAULT -1,
            kalla               TEXT    DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS vehicles (
            regnr       TEXT PRIMARY KEY,
            modell      TEXT    DEFAULT '',
            farg        TEXT    DEFAULT '',
            fordonstyp  TEXT    DEFAULT '',
            modellar    TEXT    DEFAULT '',
            status      TEXT    DEFAULT '',
            vehicle_id  TEXT    DEFAULT '',
            hamtad      TEXT    DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS job_state (
            id                     INTEGER PRIMARY KEY DEFAULT 1,
            status                 TEXT    DEFAULT 'idle',
            phase                  TEXT    DEFAULT '',
            run_mode               TEXT    DEFAULT 'standard',
            skip_phase0            INTEGER DEFAULT 0,
            skip_phase1            INTEGER DEFAULT 0,
            skip_phase2e           INTEGER DEFAULT 0,
            skip_phase3            INTEGER DEFAULT 0,
            -- Phase 1: prefix enumeration
            phase1_prefix          TEXT    DEFAULT '',
            phase1_page            INTEGER DEFAULT 0,
            phase1_prefixes_done   INTEGER DEFAULT 0,
            phase1_vehicles        INTEGER DEFAULT 0,
            -- Phase 2: PNR enumeration
            current_year           INTEGER DEFAULT 0,
            current_month          INTEGER DEFAULT 0,
            current_day            INTEGER DEFAULT 0,
            current_individ        INTEGER DEFAULT 0,
            total_tested           INTEGER DEFAULT 0,
            total_found            INTEGER DEFAULT 0,
            total_not_found        INTEGER DEFAULT 0,
            total_errors           INTEGER DEFAULT 0,
            target_people          INTEGER DEFAULT 0,
            start_year             INTEGER DEFAULT 1940,
            end_year               INTEGER DEFAULT 2005,
            started_at             TEXT    DEFAULT '',
            updated_at             TEXT    DEFAULT ''
        );

        INSERT OR IGNORE INTO job_state (id) VALUES (1);
    """)
    # Migrate: add columns that may be missing from older schema versions
    migrations = [
        "ALTER TABLE job_state ADD COLUMN phase TEXT DEFAULT ''",
        "ALTER TABLE job_state ADD COLUMN phase1_prefix TEXT DEFAULT ''",
        "ALTER TABLE job_state ADD COLUMN phase1_page INTEGER DEFAULT 0",
        "ALTER TABLE job_state ADD COLUMN phase1_prefixes_done INTEGER DEFAULT 0",
        "ALTER TABLE job_state ADD COLUMN phase1_vehicles INTEGER DEFAULT 0",
        # Phase 0 columns (added in v3)
        "ALTER TABLE job_state ADD COLUMN phase0_date TEXT DEFAULT ''",
        "ALTER TABLE job_state ADD COLUMN phase0_found INTEGER DEFAULT 0",
        # Heartbeat + status snapshot fields (added in v4)
        "ALTER TABLE job_state ADD COLUMN last_progress_at TEXT DEFAULT ''",
        "ALTER TABLE job_state ADD COLUMN status_snapshot_at TEXT DEFAULT ''",
        "ALTER TABLE job_state ADD COLUMN status_snapshot_people INTEGER DEFAULT 0",
        "ALTER TABLE job_state ADD COLUMN status_snapshot_tested INTEGER DEFAULT 0",
        "ALTER TABLE job_state ADD COLUMN speed_people_per_hour REAL DEFAULT 0",
        "ALTER TABLE job_state ADD COLUMN speed_tested_per_hour REAL DEFAULT 0",
        # Enrichment counters (added in v5)
        "ALTER TABLE job_state ADD COLUMN phase0_phones INTEGER DEFAULT 0",
        # Phase 3: enrichment pass (added in v6)
        "ALTER TABLE job_state ADD COLUMN phase3_enriched INTEGER DEFAULT 0",
        "ALTER TABLE job_state ADD COLUMN phase3_phones INTEGER DEFAULT 0",
        # Phase 2E: Eniro-guided (added in v7)
        "ALTER TABLE job_state ADD COLUMN phase2e_resolved INTEGER DEFAULT 0",
        "ALTER TABLE job_state ADD COLUMN phase2e_searched INTEGER DEFAULT 0",
        # Per-phase status fields for Stage 1 parallel tracking (added in v8)
        "ALTER TABLE job_state ADD COLUMN phase0_status TEXT DEFAULT 'idle'",
        "ALTER TABLE job_state ADD COLUMN phase1_status TEXT DEFAULT 'idle'",
        "ALTER TABLE job_state ADD COLUMN phase2e_status TEXT DEFAULT 'idle'",
        # Stage barrier telemetry + Phase 2E dedicated heartbeat (added in v9)
        "ALTER TABLE job_state ADD COLUMN stage_name TEXT DEFAULT ''",
        "ALTER TABLE job_state ADD COLUMN stage_blockers TEXT DEFAULT ''",
        "ALTER TABLE job_state ADD COLUMN phase2e_last_progress_at TEXT DEFAULT ''",
        # Run-mode controls (added in v10)
        "ALTER TABLE job_state ADD COLUMN run_mode TEXT DEFAULT 'standard'",
        "ALTER TABLE job_state ADD COLUMN skip_phase0 INTEGER DEFAULT 0",
        "ALTER TABLE job_state ADD COLUMN skip_phase1 INTEGER DEFAULT 0",
        "ALTER TABLE job_state ADD COLUMN skip_phase2e INTEGER DEFAULT 0",
        "ALTER TABLE job_state ADD COLUMN skip_phase3 INTEGER DEFAULT 0",
        # People-table enrichment columns (added in v5)
        "ALTER TABLE people ADD COLUMN kon TEXT DEFAULT ''",
        "ALTER TABLE people ADD COLUMN gift INTEGER DEFAULT -1",
        "ALTER TABLE people ADD COLUMN tilltalsnamn TEXT DEFAULT ''",
        "ALTER TABLE people ADD COLUMN postnummer TEXT DEFAULT ''",
        "ALTER TABLE people ADD COLUMN lat TEXT DEFAULT ''",
        "ALTER TABLE people ADD COLUMN lng TEXT DEFAULT ''",
        "ALTER TABLE people ADD COLUMN bolag INTEGER DEFAULT -1",
        "ALTER TABLE people ADD COLUMN telefon TEXT DEFAULT ''",
        "ALTER TABLE people ADD COLUMN grannar INTEGER DEFAULT -1",
        "ALTER TABLE people ADD COLUMN kalla TEXT DEFAULT ''",
    ]
    for sql in migrations:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()
    # Force reconnect so the Row factory sees new columns
    conn.close()
    _local.conn = None


# ── job_state ──────────────────────────────────────────────────────────────────

def get_job_state() -> dict:
    row = _conn().execute("SELECT * FROM job_state WHERE id=1").fetchone()
    return dict(row) if row else {}


def update_job_state(**kwargs: Any) -> None:
    if not kwargs:
        return
    sets = ", ".join(f"{k}=?" for k in kwargs)
    vals = list(kwargs.values())
    _conn().execute(f"UPDATE job_state SET {sets} WHERE id=1", vals)
    _conn().commit()


def save_checkpoint(
    year: int, month: int, day: int, individ: int,
    tested: int, found: int, not_found: int, errors: int,
) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    _conn().execute(
        """UPDATE job_state SET
               current_year=?, current_month=?, current_day=?, current_individ=?,
               total_tested=?, total_found=?, total_not_found=?, total_errors=?,
               updated_at=?, last_progress_at=?
           WHERE id=1""",
        (year, month, day, individ, tested, found, not_found, errors, now, now),
    )
    _conn().commit()


def mark_progress() -> str:
    """Touch heartbeat fields when workers make progress."""
    now = datetime.now().isoformat(timespec="seconds")
    update_job_state(last_progress_at=now, updated_at=now)
    return now


def _parse_iso(ts: str) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        return None


def update_status_snapshot(total_people: int, total_tested: int) -> dict[str, float | str]:
    """
    Update rolling speed metrics from status polling.
    Uses EWMA for stable speed/ETA values across restarts.
    """
    state = get_job_state()
    now = datetime.now()
    now_iso = now.isoformat(timespec="seconds")

    prev_ts = _parse_iso(str(state.get("status_snapshot_at", "")))
    prev_people = int(state.get("status_snapshot_people", 0) or 0)
    prev_tested = int(state.get("status_snapshot_tested", 0) or 0)
    prev_pph = float(state.get("speed_people_per_hour", 0) or 0.0)
    prev_tph = float(state.get("speed_tested_per_hour", 0) or 0.0)

    people_speed = prev_pph
    tested_speed = prev_tph

    if prev_ts is not None:
        elapsed_hours = max((now - prev_ts).total_seconds() / 3600.0, 1e-6)
        inst_people = max(0.0, (total_people - prev_people) / elapsed_hours)
        inst_tested = max(0.0, (total_tested - prev_tested) / elapsed_hours)
        alpha = 0.35
        people_speed = inst_people if prev_pph <= 0 else (alpha * inst_people + (1 - alpha) * prev_pph)
        tested_speed = inst_tested if prev_tph <= 0 else (alpha * inst_tested + (1 - alpha) * prev_tph)

    people_speed = round(people_speed, 2)
    tested_speed = round(tested_speed, 2)

    try:
        update_job_state(
            status_snapshot_at=now_iso,
            status_snapshot_people=max(0, int(total_people)),
            status_snapshot_tested=max(0, int(total_tested)),
            speed_people_per_hour=people_speed,
            speed_tested_per_hour=tested_speed,
        )
    except sqlite3.Error as exc:
        # Dashboard polling must stay read-mostly; a locked snapshot write should
        # not take down the whole /status endpoint.
        log.warning("Could not persist status snapshot: %s", exc)

    return {
        "status_snapshot_at": now_iso,
        "speed_people_per_hour": people_speed,
        "speed_tested_per_hour": tested_speed,
    }


# ── people ─────────────────────────────────────────────────────────────────────

_ENRICHMENT_COLS = ("kon", "gift", "tilltalsnamn", "postnummer", "lat", "lng",
                    "bolag", "telefon", "grannar", "kalla")

_PEOPLE_COLS = (
    "pnr", "namn", "alder", "stad", "gata",
    "har_fordon", "antal_fordon_egna", "fordon_egna_regnr", "fordon_egna_modell",
    "antal_fordon_adress", "fordon_adress_regnr", "hamtad",
    *_ENRICHMENT_COLS,
)
_PEOPLE_PLACEHOLDERS = ",".join("?" * len(_PEOPLE_COLS))

# Upsert: insert new rows, but if PNR exists merge enrichment fields.
# Only overwrites an enrichment column when the NEW value is non-empty/non-default
# AND the existing value IS empty/default — never clobbers good data.
_UPSERT_SET_PARTS = []
for _c in _ENRICHMENT_COLS:
    if _c in ("gift", "bolag", "grannar"):
        _UPSERT_SET_PARTS.append(f"{_c}=CASE WHEN {_c}<0 AND excluded.{_c}>=0 THEN excluded.{_c} ELSE {_c} END")
    else:
        _UPSERT_SET_PARTS.append(f"{_c}=CASE WHEN {_c}='' AND excluded.{_c}!='' THEN excluded.{_c} ELSE {_c} END")
_UPSERT_TAIL = ", ".join(_UPSERT_SET_PARTS)

_PEOPLE_UPSERT_SQL = (
    f"INSERT INTO people ({','.join(_PEOPLE_COLS)}) "
    f"VALUES ({_PEOPLE_PLACEHOLDERS}) "
    f"ON CONFLICT(pnr) DO UPDATE SET {_UPSERT_TAIL}"
)


def _person_row(p: dict) -> tuple:
    return (
        p["pnr"], p.get("namn", ""), p.get("alder", 0),
        p.get("stad", ""), p.get("gata", ""),
        1 if p.get("har_fordon") else 0,
        p.get("antal_fordon_egna", 0), p.get("fordon_egna_regnr", ""),
        p.get("fordon_egna_modell", ""),
        p.get("antal_fordon_adress", 0), p.get("fordon_adress_regnr", ""),
        p.get("hamtad", ""),
        p.get("kon", ""), int(p["gift"]) if isinstance(p.get("gift"), bool) else int(p.get("gift", -1)),
        p.get("tilltalsnamn", ""), p.get("postnummer", ""),
        p.get("lat", ""), p.get("lng", ""),
        int(p["bolag"]) if isinstance(p.get("bolag"), bool) else int(p.get("bolag", -1)),
        p.get("telefon", ""), p.get("grannar", -1),
        p.get("kalla", ""),
    )


def insert_person(p: dict) -> bool:
    try:
        _conn().execute(_PEOPLE_UPSERT_SQL, _person_row(p))
        _conn().commit()
        return True
    except sqlite3.IntegrityError:
        return False


def insert_people_batch(people: list[dict]) -> int:
    if not people:
        return 0
    conn = _conn()
    inserted = 0
    try:
        conn.execute("BEGIN")
        for p in people:
            cur = conn.execute(_PEOPLE_UPSERT_SQL, _person_row(p))
            if cur.rowcount > 0:
                inserted += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return inserted


def insert_vehicles_batch(vehicles: list[dict]) -> int:
    """Insert multiple vehicles in a single transaction."""
    if not vehicles:
        return 0
    conn = _conn()
    inserted = 0
    try:
        conn.execute("BEGIN")
        for v in vehicles:
            cur = conn.execute(
                """INSERT OR IGNORE INTO vehicles
                   (regnr, modell, farg, fordonstyp, modellar, status, vehicle_id, hamtad)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (
                    v["regnr"], v.get("modell",""), v.get("farg",""),
                    v.get("fordonstyp",""), v.get("modellar",""),
                    v.get("status",""), v.get("vehicle_id",""),
                    v.get("hamtad",""),
                ),
            )
            if cur.rowcount > 0:
                inserted += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return inserted


def count_people(har_fordon: int | None = None) -> int:
    if har_fordon is None:
        row = _conn().execute("SELECT COUNT(*) FROM people").fetchone()
    else:
        row = _conn().execute(
            "SELECT COUNT(*) FROM people WHERE har_fordon=?", (har_fordon,)
        ).fetchone()
    return row[0] if row else 0


def count_enrichment() -> dict:
    """Return counts of people with enriched fields (non-default values)."""
    conn = _conn()
    rows = conn.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN telefon != '' THEN 1 ELSE 0 END) AS med_telefon,
            SUM(CASE WHEN kon != '' THEN 1 ELSE 0 END) AS med_kon,
            SUM(CASE WHEN gift >= 0 THEN 1 ELSE 0 END) AS med_gift,
            SUM(CASE WHEN lat != '' THEN 1 ELSE 0 END) AS med_gps,
            SUM(CASE WHEN bolag >= 0 THEN 1 ELSE 0 END) AS med_bolag,
            SUM(CASE WHEN tilltalsnamn != '' THEN 1 ELSE 0 END) AS med_tilltalsnamn,
            SUM(CASE WHEN grannar >= 0 THEN 1 ELSE 0 END) AS med_grannar,
            SUM(CASE WHEN kalla LIKE '%ratsit%' THEN 1 ELSE 0 END) AS fran_ratsit,
            SUM(CASE WHEN kalla LIKE '%biluppgifter%' THEN 1 ELSE 0 END) AS fran_biluppgifter
        FROM people
    """).fetchone()
    return dict(rows) if rows else {}


def get_people(
    page: int = 1, limit: int = 100,
    har_fordon: int | None = None,
    stad: str | None = None, search: str | None = None,
) -> list[dict]:
    limit  = min(limit, 1000)
    offset = (page - 1) * limit
    wheres, params = [], []
    if har_fordon is not None:
        wheres.append("har_fordon=?"); params.append(har_fordon)
    if stad:
        wheres.append("stad LIKE ?"); params.append(f"%{stad}%")
    if search:
        wheres.append("namn LIKE ?"); params.append(f"%{search}%")
    where_sql = f"WHERE {' AND '.join(wheres)}" if wheres else ""
    params += [limit, offset]
    rows = _conn().execute(
        f"SELECT * FROM people {where_sql} ORDER BY hamtad DESC LIMIT ? OFFSET ?",
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def existing_pnrs(pnr_list: list[str]) -> set[str]:
    """Return the subset of pnr_list that already exist in the people table."""
    if not pnr_list:
        return set()
    conn = _conn()
    placeholders = ",".join("?" * len(pnr_list))
    rows = conn.execute(
        f"SELECT pnr FROM people WHERE pnr IN ({placeholders})", pnr_list
    ).fetchall()
    return {r[0] for r in rows}


def enrich_person(pnr: str, fields: dict) -> bool:
    """Update enrichment columns for a person, only filling blanks."""
    sets, vals = [], []
    for col in _ENRICHMENT_COLS:
        new_val = fields.get(col)
        if new_val is None:
            continue
        if col in ("gift", "bolag", "grannar"):
            if isinstance(new_val, bool):
                new_val = int(new_val)
            if not isinstance(new_val, int) or new_val < 0:
                continue
            sets.append(f"{col}=CASE WHEN {col}<0 THEN ? ELSE {col} END")
        else:
            new_val = str(new_val)
            if not new_val:
                continue
            sets.append(f"{col}=CASE WHEN {col}='' THEN ? ELSE {col} END")
        vals.append(new_val)

    if not sets:
        return False
    vals.append(pnr)
    sql = f"UPDATE people SET {', '.join(sets)} WHERE pnr=?"
    cur = _conn().execute(sql, vals)
    _conn().commit()
    return cur.rowcount > 0


def enrich_people_batch(batch: list[tuple[str, dict]]) -> int:
    """Enrich multiple people. batch = [(pnr, fields_dict), ...]. Returns count updated."""
    if not batch:
        return 0
    updated = 0
    conn = _conn()
    try:
        conn.execute("BEGIN")
        for pnr, fields in batch:
            sets, vals = [], []
            for col in _ENRICHMENT_COLS:
                new_val = fields.get(col)
                if new_val is None:
                    continue
                if col in ("gift", "bolag", "grannar"):
                    if isinstance(new_val, bool):
                        new_val = int(new_val)
                    if not isinstance(new_val, int) or new_val < 0:
                        continue
                    sets.append(f"{col}=CASE WHEN {col}<0 THEN ? ELSE {col} END")
                else:
                    new_val = str(new_val)
                    if not new_val:
                        continue
                    sets.append(f"{col}=CASE WHEN {col}='' THEN ? ELSE {col} END")
                vals.append(new_val)
            if not sets:
                continue
            vals.append(pnr)
            cur = conn.execute(f"UPDATE people SET {', '.join(sets)} WHERE pnr=?", vals)
            if cur.rowcount > 0:
                updated += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return updated


def get_unenriched_pnrs(limit: int = 50) -> list[dict]:
    """Return people that lack Ratsit enrichment data, ordered oldest first."""
    rows = _conn().execute(
        """SELECT pnr, namn, alder, stad FROM people
           WHERE kalla NOT LIKE '%ratsit%' AND namn != ''
           ORDER BY rowid ASC LIMIT ?""",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def count_unenriched() -> int:
    row = _conn().execute(
        "SELECT COUNT(*) FROM people WHERE kalla NOT LIKE '%ratsit%' AND namn != ''"
    ).fetchone()
    return row[0] if row else 0


def get_eniro_pending_pnrs(limit: int = 50) -> list[dict]:
    """Return people not yet processed by Phase 2E (no eniro tag in kalla)."""
    rows = _conn().execute(
        """SELECT pnr, namn, alder, stad FROM people
           WHERE kalla NOT LIKE '%eniro%' AND kalla NOT LIKE '%ratsit%' AND namn != ''
           ORDER BY rowid ASC LIMIT ?""",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def count_eniro_pending() -> int:
    row = _conn().execute(
        "SELECT COUNT(*) FROM people WHERE kalla NOT LIKE '%eniro%' AND kalla NOT LIKE '%ratsit%' AND namn != ''"
    ).fetchone()
    return row[0] if row else 0


def iter_all_people() -> Generator[dict, None, None]:
    for row in _conn().execute("SELECT * FROM people ORDER BY rowid"):
        yield dict(row)


# ── vehicles ───────────────────────────────────────────────────────────────────

def insert_vehicle(v: dict) -> bool:
    try:
        _conn().execute(
            """INSERT OR IGNORE INTO vehicles
               (regnr, modell, farg, fordonstyp, modellar, status, vehicle_id, hamtad)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                v["regnr"], v.get("modell",""), v.get("farg",""),
                v.get("fordonstyp",""), v.get("modellar",""),
                v.get("status",""), v.get("vehicle_id",""),
                v.get("hamtad",""),
            ),
        )
        _conn().commit()
        return True
    except sqlite3.IntegrityError:
        return False


def count_vehicles() -> int:
    row = _conn().execute("SELECT COUNT(*) FROM vehicles").fetchone()
    return row[0] if row else 0


def get_vehicles(page: int = 1, limit: int = 100) -> list[dict]:
    limit  = min(limit, 1000)
    offset = (page - 1) * limit
    rows = _conn().execute(
        "SELECT * FROM vehicles ORDER BY rowid DESC LIMIT ? OFFSET ?",
        (limit, offset),
    ).fetchall()
    return [dict(r) for r in rows]


def iter_all_vehicles() -> Generator[dict, None, None]:
    for row in _conn().execute("SELECT * FROM vehicles ORDER BY rowid"):
        yield dict(row)


# ── utils ──────────────────────────────────────────────────────────────────────

def reset_db() -> None:
    """Drop and recreate all tables. Called by /job/reset."""
    conn = _conn()
    conn.executescript("""
        DROP TABLE IF EXISTS people;
        DROP TABLE IF EXISTS vehicles;
        DROP TABLE IF EXISTS job_state;
    """)
    conn.commit()
    init_db()


def wal_checkpoint() -> None:
    _conn().execute("PRAGMA wal_checkpoint(TRUNCATE)")


def db_file_size_mb() -> float:
    try:
        return os.path.getsize(DB_PATH) / 1_048_576
    except OSError:
        return 0.0
