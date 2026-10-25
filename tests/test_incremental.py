"""Incremental processing: watermark store and the lookback window."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from pipeline.sources import VIX
from pipeline.state import WatermarkStore, effective_floor

from .conftest import ingest_fixture


class TestWatermarkStore:
    def test_missing_watermark_reads_as_none(self, tmp_path: Path):
        store = WatermarkStore(tmp_path / "wm.json")
        assert store.get("vix_daily") is None

    def test_roundtrips_a_date(self, tmp_path: Path):
        store = WatermarkStore(tmp_path / "wm.json")
        store.set("vix_daily", date(1990, 2, 2))
        assert store.get("vix_daily") == date(1990, 2, 2)

    def test_survives_a_new_store_instance(self, tmp_path: Path):
        path = tmp_path / "wm.json"
        WatermarkStore(path).set("vix_daily", date(1990, 2, 2))
        assert WatermarkStore(path).get("vix_daily") == date(1990, 2, 2)

    def test_tracks_sources_independently(self, tmp_path: Path):
        store = WatermarkStore(tmp_path / "wm.json")
        store.set("vix_daily", date(1990, 2, 2))
        store.set("sp500_monthly", date(1871, 1, 1))
        assert store.get("vix_daily") == date(1990, 2, 2)
        assert store.get("sp500_monthly") == date(1871, 1, 1)

    def test_advance_moves_forward(self, tmp_path: Path):
        store = WatermarkStore(tmp_path / "wm.json")
        assert store.advance("vix_daily", date(1990, 1, 1)) is True
        assert store.advance("vix_daily", date(1990, 2, 2)) is True
        assert store.get("vix_daily") == date(1990, 2, 2)

    def test_advance_never_moves_backward(self, tmp_path: Path):
        store = WatermarkStore(tmp_path / "wm.json")
        store.set("vix_daily", date(1990, 2, 2))
        assert store.advance("vix_daily", date(1990, 1, 1)) is False
        assert store.get("vix_daily") == date(1990, 2, 2)

    def test_corrupt_state_file_raises_an_actionable_error(self, tmp_path: Path):
        path = tmp_path / "wm.json"
        path.write_text("{not json", encoding="utf-8")
        with pytest.raises(RuntimeError, match="corrupt"):
            WatermarkStore(path).get("vix_daily")

    def test_write_is_atomic_leaving_no_temp_file(self, tmp_path: Path):
        store = WatermarkStore(tmp_path / "wm.json")
        store.set("vix_daily", date(1990, 2, 2))
        assert list(tmp_path.glob("*.tmp")) == []


class TestEffectiveFloor:
    def test_no_watermark_means_process_everything(self):
        assert effective_floor(None, 7) is None

    def test_floor_is_watermark_minus_lookback(self):
        assert effective_floor(date(1990, 2, 10), 7) == date(1990, 2, 3)

    def test_zero_lookback_means_strict_watermark(self):
        assert effective_floor(date(1990, 2, 10), 0) == date(1990, 2, 10)

    def test_negative_lookback_is_rejected(self):
        with pytest.raises(ValueError, match="lookback_days"):
            effective_floor(date(1990, 2, 10), -1)


class TestFloorFiltering:
    def test_rows_older_than_the_floor_are_skipped(self):
        result = ingest_fixture(VIX, "vix_clean.csv", floor_date=date(1990, 2, 1))
        assert result.rows_skipped_by_watermark == 3
        assert {o.observation_date for o in result.observations} == {date(1990, 2, 1), date(1990, 2, 2)}

    def test_floor_is_inclusive(self):
        result = ingest_fixture(VIX, "vix_clean.csv", floor_date=date(1990, 2, 2))
        assert {o.observation_date for o in result.observations} == {date(1990, 2, 2)}

    def test_floor_beyond_all_data_yields_an_empty_batch(self):
        result = ingest_fixture(VIX, "vix_clean.csv", floor_date=date(2000, 1, 1))
        assert result.observations == []
        assert result.rows_skipped_by_watermark == 5

    def test_lookback_rereads_a_trailing_window_so_revisions_are_caught(self):
        # Watermark at 1990-02-02 with a 2-day lookback must re-stage 02-01,
        # which is how an upstream correction to an already-loaded date is
        # picked up on the next run.
        floor = effective_floor(date(1990, 2, 2), 2)
        result = ingest_fixture(VIX, "vix_clean.csv", floor_date=floor)
        assert date(1990, 2, 1) in {o.observation_date for o in result.observations}


def test_failed_watermark_publish_preserves_previous_state(tmp_path: Path, monkeypatch):
    path = tmp_path / "wm.json"
    store = WatermarkStore(path)
    store.set("vix_daily", date(1990, 2, 2))
    previous = path.read_text(encoding="utf-8")

    def fail_replace(source, destination):
        raise OSError("simulated interrupted publish")

    monkeypatch.setattr("pipeline.state.os.replace", fail_replace)
    with pytest.raises(OSError, match="interrupted publish"):
        store.set("vix_daily", date(1990, 2, 3))

    assert path.read_text(encoding="utf-8") == previous
    assert list(tmp_path.glob("*.tmp")) == []
