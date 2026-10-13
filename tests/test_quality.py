"""Data-quality tests: the suite passes on good data and catches each defect."""

from __future__ import annotations

import logging

import pytest

from pipeline.quality import QualityCheckFailed, enforce, failures, format_table, run_local_checks
from pipeline.warehouse import LocalWarehouse

EXPECTED_CHECKS = {
    "all_staged_rows_loaded",
    "business_grain_is_unique",
    "fact_table_not_empty",
    "no_future_observation_dates",
    "no_null_metric_values",
    "observation_key_is_unique",
    "staged_values_match_fact",
    "vix_close_within_plausible_range",
}


class TestCleanData:
    def test_every_check_runs(self, loaded_warehouse: LocalWarehouse):
        checks = run_local_checks(loaded_warehouse)
        assert {c.check_name for c in checks} == EXPECTED_CHECKS

    def test_every_check_passes(self, loaded_warehouse: LocalWarehouse):
        checks = run_local_checks(loaded_warehouse)
        assert failures(checks) == []

    def test_enforce_does_not_raise(self, loaded_warehouse: LocalWarehouse):
        enforce(run_local_checks(loaded_warehouse), fail_on_error=True)


class TestDefectDetection:
    def test_duplicate_business_grain_is_caught(self, loaded_warehouse: LocalWarehouse):
        # Same (series, metric, date) under a different surrogate key: exactly
        # what a non-deterministic key would produce on a re-run.
        loaded_warehouse.connection.execute(
            """
            INSERT INTO analytics.fct_market_observation
            SELECT 'deliberately-different-key', series_id, metric_name, observation_date,
                   metric_value, source_name, batch_id, ingested_at, updated_at
            FROM analytics.fct_market_observation LIMIT 1
            """
        )
        checks = {c.check_name: c for c in run_local_checks(loaded_warehouse)}
        assert checks["business_grain_is_unique"].status == "FAIL"
        assert checks["business_grain_is_unique"].observed == 1
        assert checks["observation_key_is_unique"].status == "PASS"

    def test_future_dated_row_is_caught(self, loaded_warehouse: LocalWarehouse):
        loaded_warehouse.connection.execute(
            """
            INSERT INTO analytics.fct_market_observation VALUES
            ('future-key', 'CBOE_VIX', 'close', DATE '2099-01-01', 20.0,
             'test', 'test-batch', now(), now())
            """
        )
        checks = {c.check_name: c for c in run_local_checks(loaded_warehouse)}
        assert checks["no_future_observation_dates"].status == "FAIL"

    def test_implausible_vix_value_is_caught(self, loaded_warehouse: LocalWarehouse):
        loaded_warehouse.connection.execute(
            """
            INSERT INTO analytics.fct_market_observation VALUES
            ('implausible-key', 'CBOE_VIX', 'close', DATE '1995-01-03', 9999.0,
             'test', 'test-batch', now(), now())
            """
        )
        checks = {c.check_name: c for c in run_local_checks(loaded_warehouse)}
        assert checks["vix_close_within_plausible_range"].status == "FAIL"

    def test_staged_row_missing_from_fact_is_caught(self, loaded_warehouse: LocalWarehouse):
        loaded_warehouse.connection.execute("DELETE FROM analytics.fct_market_observation WHERE metric_name = 'close'")
        checks = {c.check_name: c for c in run_local_checks(loaded_warehouse)}
        assert checks["all_staged_rows_loaded"].status == "FAIL"
        assert checks["all_staged_rows_loaded"].observed == 5

    def test_value_drift_between_staging_and_fact_is_caught(self, loaded_warehouse: LocalWarehouse):
        loaded_warehouse.connection.execute(
            "UPDATE analytics.fct_market_observation SET metric_value = 1.0 WHERE metric_name = 'close'"
        )
        checks = {c.check_name: c for c in run_local_checks(loaded_warehouse)}
        assert checks["staged_values_match_fact"].status == "FAIL"

    def test_empty_fact_table_is_caught(self, loaded_warehouse: LocalWarehouse):
        loaded_warehouse.connection.execute("DELETE FROM analytics.fct_market_observation")
        checks = {c.check_name: c for c in run_local_checks(loaded_warehouse)}
        assert checks["fact_table_not_empty"].status == "FAIL"


class TestEnforcement:
    def test_failure_raises_with_an_actionable_message(self, loaded_warehouse: LocalWarehouse):
        loaded_warehouse.connection.execute("DELETE FROM analytics.fct_market_observation")
        checks = run_local_checks(loaded_warehouse)
        with pytest.raises(QualityCheckFailed) as excinfo:
            enforce(checks, fail_on_error=True)
        message = str(excinfo.value)
        assert "fact_table_not_empty" in message
        assert "04_quality.sql" in message  # tells the reader where to look

    def test_failures_can_be_downgraded_to_warnings(self, loaded_warehouse: LocalWarehouse, caplog):
        loaded_warehouse.connection.execute("DELETE FROM analytics.fct_market_observation")
        checks = run_local_checks(loaded_warehouse)
        # caplog.at_level forces the warning branch to actually execute. Without
        # it, any test that raised the root log level earlier in the session
        # would make this call a no-op and hide a crash inside it - which is
        # exactly how a real bug in this branch went unnoticed once.
        with caplog.at_level(logging.WARNING):
            enforce(checks, fail_on_error=False)  # must not raise
        assert "configured as warnings" in caplog.text


class TestReporting:
    def test_table_rendering_includes_each_check(self, loaded_warehouse: LocalWarehouse):
        rendered = format_table(run_local_checks(loaded_warehouse))
        for name in EXPECTED_CHECKS:
            assert name in rendered

    def test_empty_check_list_renders_safely(self):
        assert format_table([]) == "(no checks run)"
