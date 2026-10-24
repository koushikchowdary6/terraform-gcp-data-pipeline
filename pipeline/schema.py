"""Canonical schema, validation, normalization and deterministic identifiers.

Every upstream feed is normalized into one long-format observation row:

    (series_id, metric_name, observation_date) -> metric_value

That grain is the business key. The deterministic ``observation_key`` is a
SHA-256 over exactly those three fields, which is what makes the pipeline
idempotent: re-ingesting the same logical observation produces the same key,
so the warehouse upsert updates in place instead of appending a duplicate.
Upstream revisions to a published value are therefore applied as updates.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation

from .sources import SourceSpec

# metric_value is stored as DECIMAL(20, 6) / BigQuery NUMERIC to avoid binary
# float drift on monetary and index values.
VALUE_PRECISION = 20
VALUE_SCALE = 6
_QUANTIZER = Decimal(1).scaleb(-VALUE_SCALE)

EARLIEST_PLAUSIBLE_DATE = date(1800, 1, 1)

CANONICAL_COLUMNS: tuple[str, ...] = (
    "observation_key",
    "series_id",
    "metric_name",
    "observation_date",
    "metric_value",
    "source_name",
    "source_row_number",
    "batch_id",
    "ingested_at",
)


@dataclass(frozen=True)
class Observation:
    """One validated canonical observation."""

    observation_key: str
    series_id: str
    metric_name: str
    observation_date: date
    metric_value: Decimal
    source_name: str
    source_row_number: int
    batch_id: str
    ingested_at: datetime


@dataclass(frozen=True)
class RejectedValue:
    """One rejected input cell, carrying enough context to act on it."""

    source_name: str
    source_row_number: int
    column: str
    raw_value: str
    reason: str

    def as_dict(self) -> dict[str, object]:
        return {
            "source_name": self.source_name,
            "source_row_number": self.source_row_number,
            "column": self.column,
            "raw_value": self.raw_value,
            "reason": self.reason,
        }


class SchemaError(ValueError):
    """Raised when an input file does not match its declared schema."""


def observation_key(series_id: str, metric_name: str, observation_date: date) -> str:
    """Deterministic SHA-256 business key.

    Stable across runs, machines and Python versions: it depends only on the
    three business-key fields, never on row order, ingestion time or file name.
    """
    payload = f"{series_id}|{metric_name}|{observation_date.isoformat()}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def validate_header(spec: SourceSpec, header: Iterable[str] | None) -> None:
    """Fail fast when the feed's columns are not what the code expects."""
    if header is None:
        raise SchemaError(f"{spec.name}: input file has no header row")
    columns = list(header)
    if any(not isinstance(column, str) or not column.strip() for column in columns):
        raise SchemaError(f"{spec.name}: input header contains a blank or invalid column name")
    seen: set[str] = set()
    duplicates: set[str] = set()
    for column in columns:
        if column in seen:
            duplicates.add(column)
        seen.add(column)
    if duplicates:
        raise SchemaError(
            f"{spec.name}: duplicate CSV column name(s) {sorted(duplicates)}. "
            "Duplicate headers overwrite values in csv.DictReader."
        )
    present = set(columns)
    required = {spec.date_column, *spec.metric_columns}
    missing = sorted(required - present)
    if missing:
        raise SchemaError(
            f"{spec.name}: input is missing required column(s) {missing}. "
            f"Found columns: {sorted(present)}. The upstream schema may have changed."
        )


def parse_observation_date(spec: SourceSpec, raw: str, *, today: date | None = None) -> date:
    """Normalize a date string to a ``date``, rejecting implausible values.

    Month-granularity feeds (``YYYY-MM``) are normalized to the first of the
    month so every series shares one DATE column.
    """
    text = (raw or "").strip()
    if not text:
        raise ValueError("date is empty")
    parsed: date | None = None
    for fmt in spec.date_formats:
        try:
            parsed = datetime.strptime(text, fmt).date()
            break
        except ValueError:
            continue
    if parsed is None:
        raise ValueError(f"date {text!r} does not match any accepted format {list(spec.date_formats)}")
    horizon = today or datetime.now(tz=UTC).date()
    if parsed > horizon:
        raise ValueError(f"date {parsed.isoformat()} is in the future (today is {horizon.isoformat()})")
    if parsed < EARLIEST_PLAUSIBLE_DATE:
        raise ValueError(f"date {parsed.isoformat()} is before {EARLIEST_PLAUSIBLE_DATE.isoformat()}")
    return parsed


def parse_metric_value(spec: SourceSpec, metric_name: str, raw: str) -> Decimal | None:
    """Normalize a numeric cell.

    Returns ``None`` when the cell carries no observation (blank, or a sentinel
    zero for a metric declared in ``zero_means_missing``). Raises ``ValueError``
    for values that are present but unusable, so they land in the reject file.
    """
    text = (raw or "").strip().replace(",", "")
    if text == "" or text.upper() in {"NA", "N/A", "NULL", "NAN", "-"}:
        return None
    try:
        value = Decimal(text)
    except InvalidOperation:
        raise ValueError(f"value {raw!r} is not numeric") from None
    if not value.is_finite():
        raise ValueError(f"value {raw!r} is not finite")
    if value == 0 and metric_name in spec.zero_means_missing:
        # Upstream sentinel for "not reported yet", not a real zero.
        return None
    as_float = float(value)
    if math.isnan(as_float) or math.isinf(as_float):
        raise ValueError(f"value {raw!r} is not finite")
    if not (spec.min_value <= as_float <= spec.max_value):
        raise ValueError(
            f"value {as_float} for metric {metric_name!r} is outside the plausible "
            f"range [{spec.min_value}, {spec.max_value}]"
        )
    return value.quantize(_QUANTIZER)
