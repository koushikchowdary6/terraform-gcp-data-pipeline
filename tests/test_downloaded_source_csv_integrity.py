"""A complete HTTP 200 response must be structurally valid before publication."""

import pytest
import requests

from pipeline.ingest import fetch_source
from pipeline.sources import VIX

VALID_CSV = b"DATE,OPEN,HIGH,LOW,CLOSE\n1990-01-02,17,18,16,17.24\n"


class FakeResponse:
    status_code = 200

    def __init__(self, payload):
        self.payload = payload
        self.closed = False

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size):
        yield self.payload

    def close(self):
        self.closed = True


@pytest.mark.parametrize(
    "payload",
    [
        b"<html>upstream error</html>",
        b"DATE,OPEN,HIGH,LOW,CLOSE\n",
        b"DATE,OPEN,HIGH,LOW,CLOSE\n1990-01-02,17,18,16\n",
        b"DATE,OPEN,HIGH,LOW,CLOSE\n1990-01-02,17,18,16,17.24,extra\n",
        b'DATE,OPEN,HIGH,LOW,CLOSE\n"1990-01-02,17,18,16,17.24\n',
        b"DATE,OPEN,HIGH,LOW,CLOSE\n1990-01-02,17,18,16,\xff\n",
        b"DATE,OPEN,HIGH,LOW,CLOSE,CLOSE\n1990-01-02,17,18,16,17.24,19\n",
    ],
    ids=[
        "html-response",
        "header-only",
        "short-row",
        "extra-field",
        "unterminated-quote",
        "invalid-utf8",
        "duplicate-header",
    ],
)
@pytest.mark.parametrize("had_previous", [True, False])
def test_invalid_http_200_never_publishes(tmp_path, monkeypatch, payload, had_previous):
    destination = tmp_path / VIX.raw_filename
    if had_previous:
        destination.write_bytes(VALID_CSV)
    response = FakeResponse(payload)
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: response)

    with pytest.raises(RuntimeError, match="invalid CSV snapshot; previous snapshot preserved"):
        fetch_source(VIX, destination)

    assert response.closed
    assert destination.read_bytes() == VALID_CSV if had_previous else not destination.exists()
    assert list(tmp_path.glob("*.tmp")) == []


def test_valid_http_200_preserves_exact_csv_bytes(tmp_path, monkeypatch):
    destination = tmp_path / VIX.raw_filename
    payload = b"DATE,OPEN,HIGH,LOW,CLOSE\r\n1990-01-02,17,18,16,17.24\r\n"
    response = FakeResponse(payload)
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: response)

    assert fetch_source(VIX, destination) == destination
    assert destination.read_bytes() == payload
    assert response.closed
