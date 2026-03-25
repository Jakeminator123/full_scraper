"""Phase 2 worker cap must never exceed PARALLEL_WORKERS from env (OOM on ~4 GB)."""

import importlib

import pytest


@pytest.fixture
def reload_scraper(monkeypatch):
    import scraper as sc

    def _go():
        importlib.reload(sc)
        return sc

    yield _go
    for key in ("PARALLEL_WORKERS",):
        monkeypatch.delenv(key, raising=False)
    importlib.reload(sc)


def test_parallel_workers_reads_env_four(reload_scraper, monkeypatch):
    monkeypatch.setenv("PARALLEL_WORKERS", "4")
    sc = reload_scraper()
    assert sc.PARALLEL_WORKERS == 4


def test_parallel_workers_reads_env_two(reload_scraper, monkeypatch):
    monkeypatch.setenv("PARALLEL_WORKERS", "2")
    sc = reload_scraper()
    assert sc.PARALLEL_WORKERS == 2


@pytest.mark.parametrize(
    "original,p1,p2e,p0,expected",
    [
        (4, True, False, False, 4),
        (4, False, True, False, 4),
        (4, False, False, True, 4),
        (4, False, False, False, 4),
        (8, True, False, False, 4),
        (8, False, True, False, 6),
        (8, False, False, True, 8),
        (8, False, False, False, 8),
        (6, False, True, False, 6),
        (10, False, False, True, 8),
    ],
)
def test_compute_phase2_never_exceeds_cap(original, p1, p2e, p0, expected):
    import scraper as sc

    got = sc._compute_phase2_parallel_workers(
        original,
        phase1_still_alive=p1,
        phase2e_alive=p2e,
        phase0_alive=p0,
    )
    assert got == expected
    assert got <= max(2, original)


def test_compute_phase2_minimum_two():
    import scraper as sc

    assert (
        sc._compute_phase2_parallel_workers(
            1,
            phase1_still_alive=False,
            phase2e_alive=False,
            phase0_alive=False,
        )
        == 2
    )
