"""
db.py — SQLite helper for the full population scraper.

Tables:
  people     — one row per found person (from brukare pages)
  vehicles   — one row per vehicle (from prefix enumeration, Phase 1)
  job_state  — single row tracking the scraper's position and counters
"""

import sqlite3
import os
import threading
from datetime import datetime
from typing import Any, Generator

DATA_DIR = os.environ.get("DATA_DIR", "/var/data")
DB_PATH  = os.path.join(DATA_DIR, "people.db")

_local = threading.local()


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
            -- Phase 1: prefix enumeration
            phase1_prefix          TEXT    DEFAULT '',
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

    update_job_state(
        status_snapshot_at=now_iso,
        status_snapshot_people=max(0, int(total_people)),
        status_snapshot_tested=max(0, int(total_tested)),
        speed_people_per_hour=people_speed,
        speed_tested_per_hour=tested_speed,
    )

    return {
        "status_snapshot_at": now_iso,
        "speed_people_per_hour": people_speed,
        "speed_tested_per_hour": tested_speed,
    }


# ── people ─────────────────────────────────────────────────────────────────────

_PEOPLE_COLS = (
    "pnr", "namn", "alder", "stad", "gata",
    "har_fordon", "antal_fordon_egna", "fordon_egna_regnr", "fordon_egna_modell",
    "antal_fordon_adress", "fordon_adress_regnr", "hamtad",
    "kon", "gift", "tilltalsnamn", "postnummer", "lat", "lng",
    "bolag", "telefon", "grannar", "kalla",
)
_PEOPLE_PLACEHOLDERS = ",".join("?" * len(_PEOPLE_COLS))
_PEOPLE_INSERT_SQL = (
    f"INSERT OR IGNORE INTO people ({','.join(_PEOPLE_COLS)}) "
    f"VALUES ({_PEOPLE_PLACEHOLDERS})"
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
        _conn().execute(_PEOPLE_INSERT_SQL, _person_row(p))
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
            cur = conn.execute(_PEOPLE_INSERT_SQL, _person_row(p))
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
