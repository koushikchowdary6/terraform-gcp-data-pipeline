"""Ingestion tests: normal rows, malformed input, duplicates, empty files."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline.ingest import write_parquet, write_rejects
from pipeline.schema import SchemaError, observation_key
from pipeline.sources import VIX

from .conftest import ingest_fixture


class TestCleanIngestion:
    def test_unpivots_each_row_into_one_observation_per_metric(self):
        result = ingest_fixture(VIX, "vix_clean.csv")
        # 5 rows x 4 metrics
        assert result.rows_read == 5
        assert len(result.observations) == 20
        assert result.rejects == []

    def test_assigns_deterministic_keys(self):
        result = ingest_fixture(VIX, "vix_clean.csv")
        expected = observation_key("CBOE_VIX", "close", date(1990, 1, 2))
        assert any(o.observation_key == expected for o in result.observations)

    def test_preserves_decimal_values(self):
        result = ingest_fixture(VIX, "vix_clean.csv")
        close = next(
            o for o in result.observations if o.metric_name == "close" and o.observation_date == date(1990, 1, 2)
        )
        assert close.metric_value == Decimal("17.240000")

    def test_records_source_row_number_for_traceability(self):
        result = ingest_fixture(VIX, "vix_clean.csv")
        first = next(o for o in result.observations if o.observation_date == date(1990, 1, 2))
        assert first.source_row_number == 2  # header is row 1

    def test_reports_max_observation_date(self):
        result = ingest_fixture(VIX, "vix_clean.csv")
        assert result.max_observation_date == date(1990, 2, 2)

    def test_two_runs_of_the_same_file_produce_identical_keys(self):
        first = {o.observation_key for o in ingest_fixture(VIX, "vix_clean.csv").observations}
        second = {o.observation_key for o in ingest_fixture(VIX, "vix_clean.csv").observations}
        assert first == second


class TestMalformedInput:
    def test_bad_cells_are_rejected_with_reasons_not_crashing(self):
        result = ingest_fixture(VIX, "vix_malformed.csv")
        reasons = " ".join(r.reason for r in result.rejects)
        assert "not numeric" in reasons
        assert "empty" in reasons
        assert "does not match" in reasons
        assert "future" in reasons
        assert "outside the plausible" in reasons

    def test_each_reject_identifies_the_file_row_and_column(self):
        result = ingest_fixture(VIX, "vix_malformed.csv")
        bad = next(r for r in result.rejects if "not numeric" in r.reason)
        assert bad.source_row_number == 2
        assert bad.column == "CLOSE"
        assert bad.raw_value == "not_a_number"

    def test_valid_rows_still_load_alongside_rejects(self):
        result = ingest_fixture(VIX, "vix_malformed.csv")
        # Row 3 (1990-01-03) is entirely valid: 4 metrics.
        good = [o for o in result.observations if o.observation_date == date(1990, 1, 3)]
        assert len(good) == 4

    def test_blank_row_counts_as_missing_not_rejected(self):
        result = ingest_fixture(VIX, "vix_malformed.csv")
        # Row 8 has a valid date and four blank metrics.
        assert result.values_missing >= 4

    def test_sentinel_zero_is_treated_as_missing(self):
        result = ingest_fixture(VIX, "vix_malformed.csv")
        opens = [o for o in result.observations if o.observation_date == date(1990, 1, 8) and o.metric_name == "open"]
        assert opens == []

    def test_missing_column_fails_fast_with_schema_error(self):
        with pytest.raises(SchemaError, match="CLOSE"):
            ingest_fixture(VIX, "vix_missing_column.csv")

    def test_duplicate_csv_headers_do_not_silently_overwrite_metric_values(self, tmp_path: Path):
        from pipeline.ingest import ingest_file

        csv_path = tmp_path / "duplicate.csv"
        csv_path.write_text(
            "DATE,OPEN,HIGH,LOW,CLOSE,CLOSE\n"
            "1990-01-02,17,18,16,17.24,99.99\n",
            encoding="utf-8",
        )
        with pytest.raises(SchemaError, match="duplicate CSV column"):
            ingest_file(VIX, csv_path)

    @pytest.mark.parametrize("row", [
        "1990-01-02,17,18,16,17.24,unexpected",
        "1990-01-02,17,18,16",
    ])
    def test_ragged_csv_rows_fail_instead_of_loading_partial_data(self, tmp_path: Path, row):
        from pipeline.ingest import ingest_file

        csv_path = tmp_path / "ragged.csv"
        csv_path.write_text(
            "DATE,OPEN,HIGH,LOW,CLOSE\n" + row + "\n",
            encoding="utf-8",
        )
        with pytest.raises(SchemaError, match="different number of fields"):
            ingest_file(VIX, csv_path)


class TestDuplicates:
    def test_same_business_key_is_collapsed_to_one_observation(self):
        result = ingest_fixture(VIX, "vix_duplicates.csv")
        keys = [o.observation_key for o in result.observations]
        assert len(keys) == len(set(keys))

    def test_duplicates_are_counted(self):
        result = ingest_fixture(VIX, "vix_duplicates.csv")
        assert result.duplicates_dropped == 4  # 4 metrics on the repeated date

    def test_last_occurrence_wins(self):
        result = ingest_fixture(VIX, "vix_duplicates.csv")
        close = next(
            o for o in result.observations if o.metric_name == "close" and o.observation_date == date(1990, 1, 2)
        )
        assert close.metric_value == Decimal("99.990000")


class TestEmptyInput:
    def test_header_only_file_yields_no_observations_and_no_error(self):
        result = ingest_fixture(VIX, "vix_header_only.csv")
        assert result.rows_read == 0
        assert result.observations == []
        assert result.max_observation_date is None

    def test_header_only_file_is_still_schema_checked(self, tmp_path: Path):
        bad = tmp_path / "wrong_header_only.csv"
        bad.write_text("DATE,OPEN\n", encoding="utf-8")
        with pytest.raises(SchemaError):
            from pipeline.ingest import ingest_file

            ingest_file(VIX, bad)


class TestParquetOutput:
    def test_roundtrips_through_parquet_with_expected_schema(self, tmp_path: Path):
        result = ingest_fixture(VIX, "vix_clean.csv")
        path = write_parquet(result, tmp_path / "out.parquet")
        table = pq.read_table(path)
        assert table.num_rows == 20
        assert table.schema.field("metric_value").type.precision == 20
        assert table.schema.field("observation_date").type == pa.date32()

    def test_empty_result_still_writes_a_file_for_lineage(self, tmp_path: Path):
        result = ingest_fixture(VIX, "vix_header_only.csv")
        path = write_parquet(result, tmp_path / "empty.parquet")
        assert path.exists()
        assert pq.read_table(path).num_rows == 0

    def test_rejects_sidecar_is_written_only_when_there_are_rejects(self, tmp_path: Path):
        clean = ingest_fixture(VIX, "vix_clean.csv")
        assert write_rejects(clean, tmp_path / "none.parquet") is None

        dirty = ingest_fixture(VIX, "vix_malformed.csv")
        path = write_rejects(dirty, tmp_path / "rejects.parquet")
        assert path is not None
        assert pq.read_table(path).num_rows == len(dirty.rejects)
