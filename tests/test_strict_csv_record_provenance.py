"""Strict CSV record framing and physical-line source provenance."""

from datetime import date
from pathlib import Path

import pytest

from pipeline.ingest import ingest_file
from pipeline.schema import SchemaError
from pipeline.sources import VIX


@pytest.mark.parametrize(
    "payload",
    [
        'DATE,OPEN,HIGH,LOW,CLOSE\n1990-01-02,17,18,16,17.24\n"unfinished',
        'DATE,OPEN,HIGH,LOW,CLOSE\n"1990-01-02,17,18,16,17.24',
        '"DATE,OPEN,HIGH,LOW,CLOSE\n1990-01-02,17,18,16,17.24\n',
    ],
    ids=["trailing-broken-record", "unterminated-data-quote", "unterminated-header"],
)
def test_truncated_quoted_csv_fails_closed(tmp_path: Path, payload: str):
    path = tmp_path / "source.csv"
    path.write_text(payload, encoding="utf-8")
    with pytest.raises(SchemaError, match="malformed CSV near physical line"):
        ingest_file(VIX, path, today=date(2026, 11, 2))


def test_multiline_quoted_optional_column_uses_physical_line_number(tmp_path: Path):
    path = tmp_path / "source.csv"
    path.write_text(
        'DATE,OPEN,HIGH,LOW,CLOSE,NOTES\n'
        '1990-01-02,17,18,16,17.24,"quoted first line\n'
        'second line"\n'
        '1990-01-03,18,19,17,18.25,"normal"\n',
        encoding="utf-8",
    )
    result = ingest_file(VIX, path, today=date(2026, 11, 2))
    assert result.rows_read == 2
    assert result.rejects == []
    first = [row for row in result.observations if row.observation_date == date(1990, 1, 2)]
    second = [row for row in result.observations if row.observation_date == date(1990, 1, 3)]
    assert len(first) == len(second) == 4
    assert {row.source_row_number for row in first} == {3}
    assert {row.source_row_number for row in second} == {4}
