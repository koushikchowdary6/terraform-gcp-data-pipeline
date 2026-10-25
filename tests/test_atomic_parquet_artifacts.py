"""Parquet batch artifacts must survive partial writes and failed replacement."""

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from pipeline import ingest
from pipeline.ingest import IngestResult, write_parquet, write_rejects
from pipeline.schema import RejectedValue


def _batch():
    return IngestResult(
        source_name="vix_daily",
        batch_id="batch-test",
        rejects=[RejectedValue("vix_daily", 2, "CLOSE", "bad", "not numeric")],
    )


@pytest.mark.parametrize("artifact", ["observations", "rejects"])
def test_failed_parquet_writer_preserves_previous_artifact(tmp_path, monkeypatch, artifact):
    result = _batch()
    destination = tmp_path / "batch.parquet"
    destination.write_bytes(b"previously verified artifact")

    def partial_write(table, path, **kwargs):
        Path(path).write_bytes(b"partial Parquet data")
        raise OSError("simulated disk failure")

    monkeypatch.setattr(ingest.pq, "write_table", partial_write)
    with pytest.raises(OSError, match="simulated disk failure"):
        if artifact == "observations":
            write_parquet(result, destination)
        else:
            write_rejects(result, destination)

    assert destination.read_bytes() == b"previously verified artifact"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["batch.parquet"]
    assert result.parquet_path is None
    assert result.rejects_path is None


def test_failed_atomic_replace_preserves_previous_artifact(tmp_path, monkeypatch):
    destination = tmp_path / "batch.parquet"
    destination.write_bytes(b"old valid artifact")

    def fail_replace(source, target):
        assert Path(target) == destination
        assert Path(source).exists()
        raise OSError("simulated publish failure")

    monkeypatch.setattr(ingest.os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated publish failure"):
        write_parquet(_batch(), destination)

    assert destination.read_bytes() == b"old valid artifact"
    assert list(tmp_path.iterdir()) == [destination]


def test_successful_artifacts_roundtrip_without_temporary_files(tmp_path):
    result = _batch()
    observations = write_parquet(result, tmp_path / "observations.parquet")
    rejects = write_rejects(result, tmp_path / "rejects.parquet")

    assert pq.read_table(observations).num_rows == 0
    rejected = pq.read_table(rejects)
    assert rejected.num_rows == 1
    assert rejected.column("reason").to_pylist() == ["not numeric"]
    assert result.parquet_path == observations
    assert result.rejects_path == rejects
    assert sorted(p.name for p in tmp_path.iterdir()) == ["observations.parquet", "rejects.parquet"]
