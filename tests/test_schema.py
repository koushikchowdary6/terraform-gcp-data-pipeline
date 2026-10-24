"""Unit tests for validation, normalization and deterministic identifiers."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from pipeline.schema import (
    SchemaError,
    observation_key,
    parse_metric_value,
    parse_observation_date,
    validate_header,
)
from pipeline.sources import SP500, VIX

TODAY = date(2026, 10, 1)


class TestObservationKey:
    def test_is_stable_for_same_business_key(self):
        a = observation_key("CBOE_VIX", "close", date(1990, 1, 2))
        b = observation_key("CBOE_VIX", "close", date(1990, 1, 2))
        assert a == b

    def test_is_a_sha256_hex_digest(self):
        key = observation_key("CBOE_VIX", "close", date(1990, 1, 2))
        assert len(key) == 64
        assert set(key) <= set("0123456789abcdef")

    def test_does_not_change_across_releases(self):
        # Pinned so a refactor cannot silently re-key the warehouse, which
        # would turn every existing row into a duplicate on the next merge.
        assert observation_key("CBOE_VIX", "close", date(1990, 1, 2)) == (
            "edea16508931326ee25328c199580a6f4c32f8b0a674e71453a941048de87c80"
        )

    @pytest.mark.parametrize(
        ("series", "metric", "day"),
        [
            ("SP500_COMPOSITE", "close", date(1990, 1, 2)),
            ("CBOE_VIX", "open", date(1990, 1, 2)),
            ("CBOE_VIX", "close", date(1990, 1, 3)),
        ],
    )
    def test_differs_when_any_business_key_field_differs(self, series, metric, day):
        baseline = observation_key("CBOE_VIX", "close", date(1990, 1, 2))
        assert observation_key(series, metric, day) != baseline


class TestHeaderValidation:
    def test_accepts_expected_header(self):
        validate_header(VIX, ["DATE", "OPEN", "HIGH", "LOW", "CLOSE"])

    def test_tolerates_extra_columns(self):
        validate_header(VIX, ["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"])

    def test_rejects_missing_column_and_names_it(self):
        with pytest.raises(SchemaError, match="CLOSE"):
            validate_header(VIX, ["DATE", "OPEN", "HIGH", "LOW"])

    def test_rejects_absent_header(self):
        with pytest.raises(SchemaError, match="no header row"):
            validate_header(VIX, None)

    @pytest.mark.parametrize("header", [
        ["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "CLOSE"],
        ["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "VOLUME"],
    ])
    def test_rejects_duplicate_required_or_optional_columns(self, header):
        with pytest.raises(SchemaError, match="duplicate CSV column"):
            validate_header(VIX, header)

    @pytest.mark.parametrize("invalid", ["", "   ", None])
    def test_rejects_blank_or_invalid_column_names(self, invalid):
        with pytest.raises(SchemaError, match="blank or invalid"):
            validate_header(VIX, ["DATE", "OPEN", "HIGH", "LOW", "CLOSE", invalid])


class TestDateParsing:
    def test_parses_iso_date(self):
        assert parse_observation_date(VIX, "1990-01-02", today=TODAY) == date(1990, 1, 2)

    def test_normalizes_month_granularity_to_first_of_month(self):
        assert parse_observation_date(SP500, "1871-01", today=TODAY) == date(1871, 1, 1)

    def test_strips_surrounding_whitespace(self):
        assert parse_observation_date(VIX, "  1990-01-02 ", today=TODAY) == date(1990, 1, 2)

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="empty"):
            parse_observation_date(VIX, "", today=TODAY)

    def test_rejects_unparseable(self):
        with pytest.raises(ValueError, match="does not match"):
            parse_observation_date(VIX, "not-a-date", today=TODAY)

    def test_rejects_future_date(self):
        with pytest.raises(ValueError, match="future"):
            parse_observation_date(VIX, "2099-01-01", today=TODAY)

    def test_rejects_implausibly_old_date(self):
        with pytest.raises(ValueError, match="before 1800"):
            parse_observation_date(VIX, "1700-01-01", today=TODAY)


class TestMetricValueParsing:
    def test_parses_decimal_without_float_drift(self):
        assert parse_metric_value(VIX, "close", "17.240000") == Decimal("17.240000")

    def test_quantizes_to_six_decimal_places(self):
        assert parse_metric_value(VIX, "close", "17.2") == Decimal("17.200000")

    def test_strips_thousands_separators(self):
        assert parse_metric_value(SP500, "index_level", "4,512.33") == Decimal("4512.330000")

    @pytest.mark.parametrize("raw", ["", "   ", "NA", "N/A", "null", "NaN", "-"])
    def test_blank_and_sentinel_text_is_missing_not_an_error(self, raw):
        assert parse_metric_value(VIX, "close", raw) is None

    def test_sentinel_zero_is_missing_for_declared_metrics(self):
        # The upstream feed pads not-yet-reported fundamentals with 0.0.
        assert parse_metric_value(SP500, "cpi", "0.0") is None
        assert parse_metric_value(VIX, "close", "0.000000") is None

    def test_genuine_zero_is_kept_where_zero_is_meaningful(self):
        spec = SP500  # 'long_interest_rate' is declared sentinel; pick another
        assert "index_level" in spec.zero_means_missing
        # A source without the sentinel rule keeps 0 as a real observation.
        from dataclasses import replace

        plain = replace(spec, zero_means_missing=frozenset())
        assert parse_metric_value(plain, "index_level", "0.0") == Decimal("0.000000")

    def test_rejects_non_numeric(self):
        with pytest.raises(ValueError, match="not numeric"):
            parse_metric_value(VIX, "close", "not_a_number")

    def test_rejects_value_above_plausible_range(self):
        with pytest.raises(ValueError, match="outside the plausible"):
            parse_metric_value(VIX, "close", "900.0")

    def test_rejects_value_below_plausible_range(self):
        with pytest.raises(ValueError, match="outside the plausible"):
            parse_metric_value(VIX, "close", "-5.0")

    def test_rejects_non_finite(self):
        with pytest.raises(ValueError, match="not finite"):
            parse_metric_value(VIX, "close", "Infinity")
