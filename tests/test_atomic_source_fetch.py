"""Downloaded CSV snapshots must survive interrupted fetches and failed publication."""

from pathlib import Path

import pytest
import requests

from pipeline import ingest
from pipeline.sources import VIX


class FakeResponse:
    def __init__(self, content=b"new verified CSV", *, failure=None):
        self.content = content
        self.failure = failure

    def raise_for_status(self):
        if self.failure is not None:
            raise self.failure


def _mock_download(monkeypatch, response):
    calls = []

    def fake_get(url, *, timeout):
        calls.append((url, timeout))
        return response

    monkeypatch.setattr(requests, "get", fake_get)
    return calls


def test_successful_fetch_atomically_replaces_snapshot(tmp_path, monkeypatch):
    destination = tmp_path / "raw" / VIX.raw_filename
    destination.parent.mkdir()
    destination.write_bytes(b"previous CSV")
    calls = _mock_download(monkeypatch, FakeResponse(b"new CSV"))

    assert ingest.fetch_source(VIX, destination, timeout=12) == destination
    assert destination.read_bytes() == b"new CSV"
    assert calls == [(VIX.url, 12)]
    assert list(destination.parent.iterdir()) == [destination]


@pytest.mark.parametrize("stage", ["sync", "publish"])
def test_failed_fetch_preserves_last_good_snapshot_and_cleans_temp(tmp_path, monkeypatch, stage):
    destination = tmp_path / VIX.raw_filename
    destination.write_bytes(b"last good snapshot")
    _mock_download(monkeypatch, FakeResponse(b"partially staged new CSV"))

    def fail(*args):
        raise OSError(f"simulated {stage} failure")

    if stage == "sync":
        monkeypatch.setattr(ingest.os, "fsync", fail)
    else:
        monkeypatch.setattr(ingest.os, "replace", fail)

    with pytest.raises(OSError, match=f"simulated {stage} failure"):
        ingest.fetch_source(VIX, destination)

    assert destination.read_bytes() == b"last good snapshot"
    assert list(tmp_path.iterdir()) == [destination]


def test_http_failure_keeps_snapshot_and_does_not_create_temp(tmp_path, monkeypatch):
    destination = tmp_path / VIX.raw_filename
    destination.write_bytes(b"previous CSV")
    _mock_download(monkeypatch, FakeResponse(failure=requests.HTTPError("503 upstream")))

    with pytest.raises(RuntimeError, match="Could not download"):
        ingest.fetch_source(VIX, destination)

    assert destination.read_bytes() == b"previous CSV"
    assert list(tmp_path.iterdir()) == [destination]


def test_new_destination_is_not_published_on_sync_failure(tmp_path, monkeypatch):
    destination = tmp_path / "raw" / VIX.raw_filename
    _mock_download(monkeypatch, FakeResponse())

    def fail_sync(fd):
        raise OSError("simulated interrupted write")

    monkeypatch.setattr(ingest.os, "fsync", fail_sync)
    with pytest.raises(OSError, match="interrupted write"):
        ingest.fetch_source(VIX, destination)

    assert not destination.exists()
    assert list(Path(destination.parent).iterdir()) == []


@pytest.mark.parametrize("existing", [True, False])
def test_empty_http_200_never_publishes_source_snapshot(tmp_path, monkeypatch, existing):
    destination = tmp_path / VIX.raw_filename
    if existing:
        destination.write_bytes(b"last known-good CSV")
    _mock_download(monkeypatch, FakeResponse(b""))

    with pytest.raises(RuntimeError, match="is empty"):
        ingest.fetch_source(VIX, destination)

    if existing:
        assert destination.read_bytes() == b"last known-good CSV"
    else:
        assert not destination.exists()
    assert list(tmp_path.glob("*.tmp")) == []
