"""Warehouse tests: staging load, idempotent upsert, revisions, analytics model."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from pipeline.ingest import write_parquet
from pipeline.sources import VIX
from pipeline.warehouse import LocalWarehouse

from .conftest import ingest_fixture


def _stage(warehouse: LocalWarehouse, tmp_path: Path, fixture: str, name: str, **kwargs) -> int:
    """Ingest a fixture to Parquet and load it as the current staging batch."""
    result = ingest_fixture(VIX, fixture, **kwargs)
    target = tmp_path / name
    target.mkdir(parents=True, exist_ok=True)
    write_parquet(result, target / "vix.parquet")
    return warehouse.load_staging(str(target / "*.parquet"))


class TestStagingLoad:
    def test_loads_parquet_into_staging(self, warehouse: LocalWarehouse, tmp_path: Path):
        assert _stage(warehouse, tmp_path, "vix_clean.csv", "b1") == 20

    def test_staging_is_replaced_not_appended_between_batches(self, warehouse: LocalWarehouse, tmp_path: Path):
        _stage(warehouse, tmp_path, "vix_clean.csv", "b1")
        assert _stage(warehouse, tmp_path, "vix_incremental.csv", "b2") == 12

    def test_empty_batch_loads_as_zero_rows(self, warehouse: LocalWarehouse, tmp_path: Path):
        assert _stage(warehouse, tmp_path, "vix_header_only.csv", "b1") == 0


class TestUpsert:
    def test_first_run_inserts_every_row(self, warehouse: LocalWarehouse, tmp_path: Path):
        _stage(warehouse, tmp_path, "vix_clean.csv", "b1")
        stats = warehouse.upsert()
        assert stats == {"fact_rows_before": 0, "fact_rows_after": 20, "rows_inserted": 20}

    def test_rerunning_the_same_batch_inserts_nothing(self, warehouse: LocalWarehouse, tmp_path: Path):
        _stage(warehouse, tmp_path, "vix_clean.csv", "b1")
        warehouse.upsert()
        stats = warehouse.upsert()
        assert stats["rows_inserted"] == 0
        assert stats["fact_rows_after"] == 20

    def test_rerun_leaves_updated_at_untouched(self, warehouse: LocalWarehouse, tmp_path: Path):
        _stage(warehouse, tmp_path, "vix_clean.csv", "b1")
        warehouse.upsert()
        before = warehouse.updated_at_fingerprint()
        warehouse.upsert()
        # Unchanged rows are excluded by the anti-join, so even the audit
        # column is stable. This is the strict form of idempotency.
        assert warehouse.updated_at_fingerprint() == before

    def test_new_dates_are_appended_without_duplicating_existing_rows(self, warehouse: LocalWarehouse, tmp_path: Path):
        _stage(warehouse, tmp_path, "vix_clean.csv", "b1")
        warehouse.upsert()
        _stage(warehouse, tmp_path, "vix_incremental.csv", "b2")
        stats = warehouse.upsert()
        # 2 new dates x 4 metrics = 8 inserts; the 3rd date already exists.
        assert stats["rows_inserted"] == 8
        assert stats["fact_rows_after"] == 28

    def test_revised_value_is_updated_in_place(self, warehouse: LocalWarehouse, tmp_path: Path):
        _stage(warehouse, tmp_path, "vix_clean.csv", "b1")
        warehouse.upsert()
        assert warehouse.value_for("CBOE_VIX", "close", "1990-02-02") == Decimal("21.050000")

        _stage(warehouse, tmp_path, "vix_incremental.csv", "b2")
        warehouse.upsert()
        assert warehouse.value_for("CBOE_VIX", "close", "1990-02-02") == Decimal("22.500000")

    def test_business_grain_stays_unique_after_a_revision(self, warehouse: LocalWarehouse, tmp_path: Path):
        _stage(warehouse, tmp_path, "vix_clean.csv", "b1")
        warehouse.upsert()
        _stage(warehouse, tmp_path, "vix_incremental.csv", "b2")
        warehouse.upsert()
        duplicates = warehouse.query(
            """
            SELECT COUNT(*) FROM (
                SELECT series_id, metric_name, observation_date
                FROM analytics.fct_market_observation
                GROUP BY series_id, metric_name, observation_date
                HAVING COUNT(*) > 1
            )
            """
        )
        assert duplicates[0][0] == 0

    def test_within_batch_duplicates_collapse_to_the_last_occurrence(self, warehouse: LocalWarehouse, tmp_path: Path):
        _stage(warehouse, tmp_path, "vix_duplicates.csv", "b1")
        warehouse.upsert()
        assert warehouse.value_for("CBOE_VIX", "close", "1990-01-02") == Decimal("99.990000")
        assert warehouse.fact_row_count() == 8  # 2 distinct dates x 4 metrics

    def test_empty_batch_is_a_no_op(self, warehouse: LocalWarehouse, tmp_path: Path):
        _stage(warehouse, tmp_path, "vix_clean.csv", "b1")
        warehouse.upsert()
        _stage(warehouse, tmp_path, "vix_header_only.csv", "b2")
        stats = warehouse.upsert()
        assert stats["rows_inserted"] == 0
        assert stats["fact_rows_after"] == 20


class TestAnalyticsModel:
    def test_aggregates_to_one_row_per_series_metric_month(self, loaded_warehouse: LocalWarehouse):
        rows = loaded_warehouse.query("SELECT COUNT(*) FROM analytics.mart_metric_monthly WHERE metric_name = 'close'")
        assert rows[0][0] == 2  # January and February 1990

    def test_close_value_is_the_last_observation_in_the_month(self, loaded_warehouse: LocalWarehouse):
        value = loaded_warehouse.scalar(
            """
            SELECT close_value FROM analytics.mart_metric_monthly
            WHERE metric_name = 'close' AND month_start = DATE '1990-01-01'
            """
        )
        assert value == Decimal("19.220000")  # 1990-01-04, the last January date

    def test_month_over_month_change_uses_the_previous_month(self, loaded_warehouse: LocalWarehouse):
        row = loaded_warehouse.query(
            """
            SELECT prev_month_close, mom_pct_change FROM analytics.mart_metric_monthly
            WHERE metric_name = 'close' AND month_start = DATE '1990-02-01'
            """
        )[0]
        assert row[0] == Decimal("19.220000")
        # (21.05 - 19.22) / 19.22 * 100
        assert round(float(row[1]), 2) == 9.52

    def test_first_month_has_no_previous_month(self, loaded_warehouse: LocalWarehouse):
        row = loaded_warehouse.query(
            """
            SELECT prev_month_close, mom_pct_change FROM analytics.mart_metric_monthly
            WHERE metric_name = 'close' AND month_start = DATE '1990-01-01'
            """
        )[0]
        assert row == (None, None)

    def test_observation_count_reflects_underlying_rows(self, loaded_warehouse: LocalWarehouse):
        count = loaded_warehouse.scalar(
            """
            SELECT observation_count FROM analytics.mart_metric_monthly
            WHERE metric_name = 'close' AND month_start = DATE '1990-01-01'
            """
        )
        assert count == 3

    def test_model_is_rebuilt_not_appended(self, loaded_warehouse: LocalWarehouse):
        before = loaded_warehouse.scalar("SELECT COUNT(*) FROM analytics.mart_metric_monthly")
        loaded_warehouse.build_analytics()
        assert loaded_warehouse.scalar("SELECT COUNT(*) FROM analytics.mart_metric_monthly") == before


class TestCoverage:
    def test_reports_date_span_per_series_and_metric(self, loaded_warehouse: LocalWarehouse):
        coverage = {(r[0], r[1]): (r[2], r[3], r[4]) for r in loaded_warehouse.coverage()}
        assert coverage[("CBOE_VIX", "close")] == (5, date(1990, 1, 2), date(1990, 2, 2))
