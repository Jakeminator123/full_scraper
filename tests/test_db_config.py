"""Sanity checks for SQLite paths (Render persistent disk uses DATA_DIR=/var/data)."""

import importlib
import os
from pathlib import Path

import pytest


@pytest.fixture
def reload_db(monkeypatch):
    import db as db_mod

    def _go():
        importlib.reload(db_mod)
        return db_mod

    yield _go
    monkeypatch.delenv("DATA_DIR", raising=False)
    importlib.reload(db_mod)


def test_default_data_dir_points_at_var_data(reload_db, monkeypatch):
    monkeypatch.delenv("DATA_DIR", raising=False)
    db = reload_db()
    assert db.DATA_DIR == "/var/data"
    assert db.DB_PATH == os.path.join("/var/data", "people.db")


def test_data_dir_override_from_env(reload_db, monkeypatch):
    custom = str(Path(os.environ.get("TEMP", "/tmp")) / "full_scraper_test_data")
    monkeypatch.setenv("DATA_DIR", custom)
    db = reload_db()
    assert db.DATA_DIR == custom
    assert db.DB_PATH == os.path.join(custom, "people.db")
