"""End-to-end CLI tests.

These drive the real entry point against the committed fixtures, with the data
directory redirected into tmp_path. No network and no GCP involved.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from pipeline.cli import main
from pipeline.state import WatermarkStore

FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def local_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the pipeline at a temporary data dir seeded with fixture CSVs."""
    data_dir = tmp_path / "data"
    raw = data_dir / "raw"
    raw.mkdir(parents=True)
    # The CLI expects one file per source, named after the source.
    shutil.copy(FIXTURE_DIR / "vix_clean.csv", raw / "vix_daily.csv")
    shutil.copy(FIXTURE_DIR / "vix_clean.csv", raw / "sp500_monthly.csv")
    monkeypatch.setenv("PIPELINE_DATA_DIR", str(data_dir))
    monkeypatch.setenv("PIPELINE_DUCKDB_PATH", str(data_dir / "local" / "warehouse.duckdb"))
    monkeypatch.setenv("PIPELINE_STATE_PATH", str(data_dir / "local" / "watermarks.json"))
    return data_dir


class TestRunLocal:
    def test_single_source_run_succeeds(self, local_env: Path):
        assert main(["--log-level", "ERROR", "run-local", "--sources", "vix_daily", "--offline"]) == 0

    def test_writes_a_batch_of_parquet(self, local_env: Path):
        main(["--log-level", "ERROR", "run-local", "--sources", "vix_daily", "--offline"])
        batches = list((local_env / "processed").glob("batch=*/vix_daily.parquet"))
        assert len(batches) == 1

    def test_creates_the_duckdb_warehouse(self, local_env: Path):
        main(["--log-level", "ERROR", "run-local", "--sources", "vix_daily", "--offline"])
        assert (local_env / "local" / "warehouse.duckdb").exists()

    def test_advances_the_watermark(self, local_env: Path):
        main(["--log-level", "ERROR", "run-local", "--sources", "vix_daily", "--offline"])
        store = WatermarkStore(local_env / "local" / "watermarks.json")
        assert store.get("vix_daily").isoformat() == "1990-02-02"

    def test_second_run_is_idempotent(self, local_env: Path):
        argv = ["--log-level", "ERROR", "run-local", "--sources", "vix_daily", "--offline"]
        assert main(argv) == 0
        assert main(argv) == 0
        from pipeline.warehouse import LocalWarehouse

        with LocalWarehouse(local_env / "local" / "warehouse.duckdb") as wh:
            assert wh.fact_row_count() == 20

    def test_full_refresh_run_is_also_idempotent(self, local_env: Path):
        argv = ["--log-level", "ERROR", "run-local", "--sources", "vix_daily", "--offline"]
        main(argv)
        assert main([*argv, "--full-refresh"]) == 0
        from pipeline.warehouse import LocalWarehouse

        with LocalWarehouse(local_env / "local" / "warehouse.duckdb") as wh:
            assert wh.fact_row_count() == 20

    def test_offline_mode_reports_a_missing_source_file(self, local_env: Path):
        (local_env / "raw" / "vix_daily.csv").unlink()
        assert main(["--log-level", "ERROR", "run-local", "--sources", "vix_daily", "--offline"]) == 1

    def test_unknown_source_is_rejected(self, local_env: Path):
        assert main(["--log-level", "ERROR", "run-local", "--sources", "nope", "--offline"]) == 1


class TestOtherCommands:
    def test_state_reports_no_watermarks_before_a_run(self, local_env: Path, capsys):
        assert main(["state"]) == 0
        assert "No watermarks recorded yet" in capsys.readouterr().out

    def test_state_lists_watermarks_after_a_run(self, local_env: Path, capsys):
        main(["--log-level", "ERROR", "run-local", "--sources", "vix_daily", "--offline"])
        capsys.readouterr()
        assert main(["state"]) == 0
        assert "vix_daily" in capsys.readouterr().out

    def test_quality_without_a_warehouse_fails_clearly(self, local_env: Path):
        assert main(["--log-level", "ERROR", "quality"]) == 1

    def test_quality_passes_after_a_run(self, local_env: Path):
        main(["--log-level", "ERROR", "run-local", "--sources", "vix_daily", "--offline"])
        assert main(["--log-level", "ERROR", "quality"]) == 0

    def test_examples_run_against_the_local_warehouse(self, local_env: Path):
        main(["--log-level", "ERROR", "run-local", "--sources", "vix_daily", "--offline"])
        assert main(["--log-level", "ERROR", "examples"]) == 0


class TestCloudGuards:
    def test_run_cloud_without_configuration_exits_with_config_error(
        self, local_env: Path, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        monkeypatch.delenv("GCS_BUCKET", raising=False)
        # Exit code 2 is reserved for configuration problems, and the pipeline
        # must refuse before making any cloud call.
        assert main(["--log-level", "ERROR", "run-cloud"]) == 2


class TestCloudDeliveryIntegrity:
    """A cloud receipt must never suppress retries of missing or failed data."""

    def test_empty_batch_remains_pending_instead_of_being_marked_delivered(
        self, local_env: Path, monkeypatch: pytest.MonkeyPatch
    ):
        from pipeline.state import DELIVERED_MARKER, pending_batches

        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
        monkeypatch.setenv("GCS_BUCKET", "test-bucket")
        batch = local_env / "processed" / "batch=20260101T000000Z-aaaa"
        batch.mkdir(parents=True)

        class FakeWarehouse:
            def __init__(self, *args):
                pass

            def load_staging_from_gcs(self, uris):
                raise AssertionError("no warehouse load is allowed without artifacts")

        monkeypatch.setattr("pipeline.bq.BigQueryWarehouse", FakeWarehouse)
        assert main(["--log-level", "ERROR", "run-cloud", "--skip-upload"]) == 1
        assert not (batch / DELIVERED_MARKER).exists()
        assert pending_batches(local_env / "processed") == [batch]

    def test_cloud_quality_failure_stays_pending_even_in_local_warn_mode(
        self, local_env: Path, monkeypatch: pytest.MonkeyPatch
    ):
        from pipeline.quality import QualityCheck
        from pipeline.state import DELIVERED_MARKER, pending_batches

        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
        monkeypatch.setenv("GCS_BUCKET", "test-bucket")
        monkeypatch.setenv("PIPELINE_FAIL_ON_QUALITY_ERROR", "false")
        batch = local_env / "processed" / "batch=20260101T000000Z-aaaa"
        batch.mkdir(parents=True)
        (batch / "vix_daily.parquet").write_bytes(b"fake artifact for mocked cloud")

        class FakeWarehouse:
            def __init__(self, *args):
                pass

            def load_staging_from_gcs(self, uris):
                assert len(uris) == 1
                return 1

            def merge(self):
                return 1

            def build_analytics(self):
                return None

            def run_quality(self):
                return [QualityCheck("integrity", "FAIL", 1, "bad cloud row")]

        monkeypatch.setattr("pipeline.bq.BigQueryWarehouse", FakeWarehouse)
        assert main(["--log-level", "ERROR", "run-cloud", "--skip-upload"]) == 1
        assert not (batch / DELIVERED_MARKER).exists()
        assert pending_batches(local_env / "processed") == [batch]
