"""Quality-suite evidence must not silently coerce invalid statuses or counts."""

from types import SimpleNamespace

import pytest

from pipeline.quality import (
    QualityCheck,
    QualityCheckFailed,
    enforce,
    run_local_checks,
)


@pytest.mark.parametrize("status", ["OK", "PASS ", "", None, True, 1])
@pytest.mark.parametrize("fail_on_error", [True, False])
def test_invalid_status_never_counts_as_quality_evidence(status, fail_on_error):
    with pytest.raises(QualityCheckFailed, match="invalid PASS/FAIL status"):
        enforce([QualityCheck("row_count", status, 1, "rows")], fail_on_error=fail_on_error)


@pytest.mark.parametrize("observed", [True, False, 1.5, -1, "0", None])
@pytest.mark.parametrize("fail_on_error", [True, False])
def test_invalid_observed_count_fails_even_in_warning_mode(observed, fail_on_error):
    with pytest.raises(QualityCheckFailed, match="invalid observed row count"):
        enforce([QualityCheck("row_count", "PASS", observed, "rows")], fail_on_error=fail_on_error)


@pytest.mark.parametrize("detail", ["", " ", None])
def test_missing_check_detail_is_not_valid_evidence(detail):
    with pytest.raises(QualityCheckFailed, match="missing detail"):
        enforce([QualityCheck("row_count", "PASS", 1, detail)])


@pytest.mark.parametrize("raw", [
    ("row_count", "PASS", True, "rows"),
    ("row_count", "PASS", 0.75, "rows"),
    ("row_count", "UNKNOWN", 1, "rows"),
    ("row_count", "PASS", 1, None),
])
def test_local_query_rejects_malformed_results_before_logging(raw):
    warehouse = SimpleNamespace(query=lambda sql: [raw])
    with pytest.raises(QualityCheckFailed):
        run_local_checks(warehouse)


def test_valid_integer_counts_and_explicit_failure_status_remain_supported():
    assert enforce([QualityCheck("row_count", "PASS", 0, "no defects")]) is None
    with pytest.raises(QualityCheckFailed, match="1 data-quality check"):
        enforce([QualityCheck("row_count", "FAIL", 2, "two defects")])
