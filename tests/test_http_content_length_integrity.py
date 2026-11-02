"""HTTP framing mismatches must not replace the last-good market CSV."""

import pytest
import requests

from pipeline.ingest import fetch_source
from pipeline.sources import VIX

CSV = b"DATE,OPEN,HIGH,LOW,CLOSE\n1990-01-02,17,18,16,17.24\n"


class Response:
    status_code = 200

    def __init__(self, headers):
        self.headers = headers
        self.closed = False
        self.streamed = False

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size):
        self.streamed = True
        yield CSV[:8]
        yield CSV[8:]

    def close(self):
        self.closed = True


@pytest.mark.parametrize("header", [
    "1",
    "999",
    "-1",
    "1, 2",
    "invalid",
    "9" * 30,
])
@pytest.mark.parametrize("had_previous", [True, False])
def test_inconsistent_or_malformed_length_never_publishes(
    tmp_path, monkeypatch, header, had_previous
):
    destination = tmp_path / VIX.raw_filename
    previous = b"last-good-market-snapshot"
    if had_previous:
        destination.write_bytes(previous)
    response = Response({"Content-Length": header})
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: response)

    with pytest.raises(RuntimeError, match="Content-Length|maximum size"):
        fetch_source(VIX, destination)

    assert response.closed
    if had_previous:
        assert destination.read_bytes() == previous
    else:
        assert not destination.exists()
    assert not list(tmp_path.glob("*.tmp"))


def test_exact_identity_length_allows_complete_snapshot(tmp_path, monkeypatch):
    destination = tmp_path / VIX.raw_filename
    response = Response({"Content-Length": str(len(CSV))})
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: response)

    assert fetch_source(VIX, destination) == destination
    assert destination.read_bytes() == CSV
    assert response.closed and response.streamed


def test_compressed_wire_length_is_not_compared_to_decoded_body(tmp_path, monkeypatch):
    destination = tmp_path / VIX.raw_filename
    response = Response({"Content-Encoding": "gzip", "Content-Length": "2"})
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: response)

    assert fetch_source(VIX, destination) == destination
    assert destination.read_bytes() == CSV
    assert response.closed
