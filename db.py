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
            hamtad              TEXT    DEFAULT ''
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
    # Migrate: add columns that may be missing from v1 schema
    migrations = [
        "ALTER TABLE job_state ADD COLUMN phase TEXT DEFAULT ''",
        "ALTER TABLE job_state ADD COLUMN phase1_prefix TEXT DEFAULT ''",
        "ALTER TABLE job_state ADD COLUMN phase1_prefixes_done INTEGER DEFAULT 0",
        "ALTER TABLE job_state ADD COLUMN phase1_vehicles INTEGER DEFAULT 0",
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
    _conn().execute(
        """UPDATE job_state SET
               current_year=?, current_month=?, current_day=?, current_individ=?,
               total_tested=?, total_found=?, total_not_found=?, total_errors=?,
               updated_at=datetime('now','localtime')
           WHERE id=1""",
        (year, month, day, individ, tested, found, not_found, errors),
    )
    _conn().commit()


# ── people ─────────────────────────────────────────────────────────────────────

def insert_person(p: dict) -> bool:
    try:
        _conn().execute(
            """INSERT OR IGNORE INTO people
               (pnr, namn, alder, stad, gata,
                har_fordon, antal_fordon_egna, fordon_egna_regnr, fordon_egna_modell,
                antal_fordon_adress, fordon_adress_regnr, hamtad)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                p["pnr"], p.get("namn",""), p.get("alder",0),
                p.get("stad",""), p.get("gata",""),
                1 if p.get("har_fordon") else 0,
                p.get("antal_fordon_egna",0), p.get("fordon_egna_regnr",""),
                p.get("fordon_egna_modell",""),
                p.get("antal_fordon_adress",0), p.get("fordon_adress_regnr",""),
                p.get("hamtad",""),
            ),
        )
        _conn().commit()
        return True
    except sqlite3.IntegrityError:
        return False


def count_people(har_fordon: int | None = None) -> int:
    if har_fordon is None:
        row = _conn().execute("SELECT COUNT(*) FROM people").fetchone()
    else:
        row = _conn().execute(
            "SELECT COUNT(*) FROM people WHERE har_fordon=?", (har_fordon,)
        ).fetchone()
    return row[0] if row else 0


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
