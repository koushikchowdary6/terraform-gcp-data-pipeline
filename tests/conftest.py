"""Shared test fixtures.

Every test here is offline and deterministic: no network, no GCP, no clock
dependency. Date validation is pinned by passing an explicit ``today``.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from pipeline.ingest import ingest_file, write_parquet
from pipeline.sources import VIX, SourceSpec
from pipeline.warehouse import LocalWarehouse

FIXTURE_DIR = Path(__file__).parent / "fixtures"

# Pinned "today" so future-date validation never depends on the real clock.
TODAY = date(2026, 10, 1)


@pytest.fixture
def vix_spec() -> SourceSpec:
    return VIX


@pytest.fixture
def fixture_dir() -> Path:
    return FIXTURE_DIR


@pytest.fixture
def warehouse() -> LocalWarehouse:
    with LocalWarehouse(":memory:") as wh:
        yield wh


@pytest.fixture
def loaded_warehouse(vix_spec: SourceSpec, tmp_path: Path) -> LocalWarehouse:
    """A warehouse with the clean fixture ingested, upserted and modelled."""
    result = ingest_file(vix_spec, FIXTURE_DIR / "vix_clean.csv", batch_id="test-batch", today=TODAY)
    write_parquet(result, tmp_path / "vix_clean.parquet")
    with LocalWarehouse(":memory:") as wh:
        wh.load_staging(str(tmp_path / "*.parquet"))
        wh.upsert()
        wh.build_analytics()
        yield wh


def ingest_fixture(spec: SourceSpec, filename: str, **kwargs):
    """Ingest a fixture file with the pinned test clock."""
    kwargs.setdefault("today", TODAY)
    kwargs.setdefault("batch_id", "test-batch")
    return ingest_file(spec, FIXTURE_DIR / filename, **kwargs)
