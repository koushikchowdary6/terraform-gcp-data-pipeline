"""Regression tests for cloud delivery tracking.

These cover a real bug: the local watermark advances as soon as a batch passes
its *local* checks, so it cannot be used to decide what the cloud warehouse
already holds. The original `run-cloud` uploaded only the newest batch, which
meant a batch whose upload failed was never retried - the next run produced a
batch of only newer rows, and the failed one was skipped permanently.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.state import (
    DELIVERED_MARKER,
    batch_dirs,
    is_delivered,
    mark_delivered,
    pending_batches,
)


def make_batch(root: Path, batch_id: str, *, parquet: bool = True) -> Path:
    d = root / f"batch={batch_id}"
    d.mkdir(parents=True)
    if parquet:
        (d / "vix_daily.parquet").write_bytes(b"")
    return d


class TestBatchDiscovery:
    def test_finds_batches_oldest_first(self, tmp_path: Path):
        make_batch(tmp_path, "20260102T000000Z-bbbb")
        make_batch(tmp_path, "20260101T000000Z-aaaa")
        make_batch(tmp_path, "20260103T000000Z-cccc")
        assert [d.name for d in batch_dirs(tmp_path)] == [
            "batch=20260101T000000Z-aaaa",
            "batch=20260102T000000Z-bbbb",
            "batch=20260103T000000Z-cccc",
        ]

    def test_ignores_unrelated_entries(self, tmp_path: Path):
        make_batch(tmp_path, "20260101T000000Z-aaaa")
        (tmp_path / "notes.txt").write_text("x")
        (tmp_path / "batch=not-a-dir").write_text("x")
        assert len(batch_dirs(tmp_path)) == 1

    def test_empty_directory_yields_nothing(self, tmp_path: Path):
        assert batch_dirs(tmp_path) == []
        assert pending_batches(tmp_path) == []


class TestDeliveryMarker:
    def test_new_batch_is_pending(self, tmp_path: Path):
        d = make_batch(tmp_path, "20260101T000000Z-aaaa")
        assert not is_delivered(d)
        assert pending_batches(tmp_path) == [d]

    def test_marking_removes_it_from_pending(self, tmp_path: Path):
        d = make_batch(tmp_path, "20260101T000000Z-aaaa")
        mark_delivered(d, batch_id="20260101T000000Z-aaaa", rows_staged=5)
        assert is_delivered(d)
        assert pending_batches(tmp_path) == []

    def test_marker_records_what_was_delivered(self, tmp_path: Path):
        d = make_batch(tmp_path, "20260101T000000Z-aaaa")
        mark_delivered(d, batch_id="20260101T000000Z-aaaa", rows_staged=42)
        payload = json.loads((d / DELIVERED_MARKER).read_text())
        assert payload["rows_staged"] == 42
        assert payload["batch_id"] == "20260101T000000Z-aaaa"
        assert "delivered_at" in payload

    def test_write_is_atomic_leaving_no_temp_file(self, tmp_path: Path):
        d = make_batch(tmp_path, "20260101T000000Z-aaaa")
        mark_delivered(d, batch_id="x")
        assert list(d.glob("*.tmp")) == []

    def test_marker_is_not_mistaken_for_a_data_file(self, tmp_path: Path):
        d = make_batch(tmp_path, "20260101T000000Z-aaaa")
        mark_delivered(d, batch_id="x")
        # The loader globs *.parquet; the marker must never be picked up.
        assert [p.name for p in sorted(d.glob("*.parquet"))] == ["vix_daily.parquet"]


class TestFailedDeliveryIsRetried:
    """The regression this module exists for."""

    def test_an_undelivered_batch_stays_pending_behind_newer_ones(self, tmp_path: Path):
        failed = make_batch(tmp_path, "20260101T000000Z-aaaa")  # upload failed
        later = make_batch(tmp_path, "20260102T000000Z-bbbb")  # produced after
        # The old behaviour delivered only batches[-1], dropping `failed`.
        assert pending_batches(tmp_path) == [failed, later]

    def test_partial_delivery_leaves_the_rest_pending(self, tmp_path: Path):
        first = make_batch(tmp_path, "20260101T000000Z-aaaa")
        second = make_batch(tmp_path, "20260102T000000Z-bbbb")
        third = make_batch(tmp_path, "20260103T000000Z-cccc")
        # Delivery stopped after the first batch (e.g. a quality failure).
        mark_delivered(first, batch_id="20260101T000000Z-aaaa")
        assert pending_batches(tmp_path) == [second, third]

    def test_redelivery_is_idempotent(self, tmp_path: Path):
        d = make_batch(tmp_path, "20260101T000000Z-aaaa")
        mark_delivered(d, batch_id="x", rows_staged=1)
        mark_delivered(d, batch_id="x", rows_staged=1)
        assert pending_batches(tmp_path) == []
        assert len(list(d.glob(DELIVERED_MARKER))) == 1

    def test_every_batch_is_eventually_delivered(self, tmp_path: Path):
        made = [make_batch(tmp_path, f"2026010{i}T000000Z-{i:04d}") for i in range(1, 5)]
        # Simulate run-cloud draining the queue.
        for d in pending_batches(tmp_path):
            mark_delivered(d, batch_id=d.name)
        assert pending_batches(tmp_path) == []
        assert all(is_delivered(d) for d in made)


@pytest.mark.parametrize("receipt", [
    "",
    "{broken json",
    "[]",
    '{"delivered_at": "not-a-timestamp"}',
    '{"delivered_at": "2026-10-24T00:00:00"}',
])
def test_invalid_delivery_receipt_is_retried(tmp_path: Path, receipt: str):
    batch = make_batch(tmp_path, "20260101T000000Z-aaaa")
    (batch / DELIVERED_MARKER).write_text(receipt, encoding="utf-8")
    assert not is_delivered(batch)
    assert pending_batches(tmp_path) == [batch]


def test_failed_receipt_publish_preserves_previous_receipt(tmp_path: Path, monkeypatch):
    batch = make_batch(tmp_path, "20260101T000000Z-aaaa")
    marker = mark_delivered(batch, batch_id="original", rows_staged=1)
    previous = marker.read_text(encoding="utf-8")

    def fail_replace(source, destination):
        raise OSError("simulated interrupted publish")

    monkeypatch.setattr("pipeline.state.os.replace", fail_replace)
    with pytest.raises(OSError, match="interrupted publish"):
        mark_delivered(batch, batch_id="replacement", rows_staged=2)

    assert marker.read_text(encoding="utf-8") == previous
    assert list(batch.glob("*.tmp")) == []
