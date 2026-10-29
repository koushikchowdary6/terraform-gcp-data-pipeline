"""A missing or ambiguous quality suite must never be reported as green."""

import pytest

from pipeline.quality import QualityCheck, QualityCheckFailed, enforce


@pytest.mark.parametrize("fail_on_error", [True, False])
def test_empty_quality_result_fails_closed_even_in_warning_mode(fail_on_error):
    with pytest.raises(QualityCheckFailed, match="No data-quality checks returned"):
        enforce([], fail_on_error=fail_on_error)


@pytest.mark.parametrize("fail_on_error", [True, False])
def test_duplicate_passing_check_names_cannot_hide_missing_checks(fail_on_error):
    checks = [
        QualityCheck("row_count", "PASS", 0, "ok"),
        QualityCheck("row_count", "PASS", 0, "same check repeated"),
    ]
    with pytest.raises(QualityCheckFailed, match="missing or duplicate names"):
        enforce(checks, fail_on_error=fail_on_error)


@pytest.mark.parametrize("check_name", ["", "   "])
def test_unnamed_quality_check_is_not_valid_evidence(check_name):
    with pytest.raises(QualityCheckFailed, match="missing or duplicate names"):
        enforce([QualityCheck(check_name, "PASS", 0, "ok")])


def test_distinct_passing_checks_remain_valid():
    checks = [
        QualityCheck("row_count", "PASS", 0, "ok"),
        QualityCheck("freshness", "PASS", 0, "ok"),
    ]
    assert enforce(checks) is None
