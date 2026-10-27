"""Ingestion: CSV feed -> validated canonical observations -> Parquet.

Responsibilities, in order:
  1. optionally fetch the source file over HTTPS
  2. validate the header against the declared schema (fail fast)
  3. normalize dates and numeric values, rejecting unusable cells with a reason
  4. apply the incremental floor (watermark minus lookback)
  5. assign deterministic observation keys
  6. de-duplicate within the batch, last occurrence winning
  7. write a Parquet file plus a rejects sidecar
"""

from __future__ import annotations

import csv
import os
import tempfile
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .logging_setup import get_logger
from .schema import (
    VALUE_PRECISION,
    VALUE_SCALE,
    Observation,
    RejectedValue,
    SchemaError,
    observation_key,
    parse_metric_value,
    parse_observation_date,
    validate_header,
)
from .sources import SourceSpec

logger = get_logger(__name__)

HTTP_TIMEOUT_SECONDS = 60

PARQUET_SCHEMA = pa.schema(
    [
        pa.field("observation_key", pa.string(), nullable=False),
        pa.field("series_id", pa.string(), nullable=False),
        pa.field("metric_name", pa.string(), nullable=False),
        pa.field("observation_date", pa.date32(), nullable=False),
        pa.field("metric_value", pa.decimal128(VALUE_PRECISION, VALUE_SCALE), nullable=False),
        pa.field("source_name", pa.string(), nullable=False),
        pa.field("source_row_number", pa.int64(), nullable=False),
        pa.field("batch_id", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)

REJECT_SCHEMA = pa.schema(
    [
        pa.field("source_name", pa.string(), nullable=False),
        pa.field("source_row_number", pa.int64(), nullable=False),
        pa.field("column", pa.string(), nullable=False),
        pa.field("raw_value", pa.string(), nullable=True),
        pa.field("reason", pa.string(), nullable=False),
        pa.field("batch_id", pa.string(), nullable=False),
    ]
)


@dataclass
class IngestResult:
    """Outcome of one ingestion run, suitable for logging and assertions."""

    source_name: str
    batch_id: str
    observations: list[Observation] = field(default_factory=list)
    rejects: list[RejectedValue] = field(default_factory=list)
    rows_read: int = 0
    rows_skipped_by_watermark: int = 0
    values_missing: int = 0
    duplicates_dropped: int = 0
    parquet_path: Path | None = None
    rejects_path: Path | None = None

    @property
    def max_observation_date(self) -> date | None:
        if not self.observations:
            return None
        return max(o.observation_date for o in self.observations)

    def summary(self) -> dict[str, object]:
        return {
            "source_name": self.source_name,
            "batch_id": self.batch_id,
            "rows_read": self.rows_read,
            "observations": len(self.observations),
            "rejects": len(self.rejects),
            "values_missing": self.values_missing,
            "duplicates_dropped": self.duplicates_dropped,
            "rows_skipped_by_watermark": self.rows_skipped_by_watermark,
            "max_observation_date": self.max_observation_date.isoformat() if self.max_observation_date else None,
        }


def new_batch_id() -> str:
    stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


def fetch_source(spec: SourceSpec, destination: Path, *, timeout: int = HTTP_TIMEOUT_SECONDS) -> Path:
    """Download a source feed to ``destination``.

    Imported lazily so the package stays importable (and unit-testable) in
    environments without network access or the ``requests`` dependency.
    """
    import requests  # noqa: PLC0415 - intentional lazy import

    destination.parent.mkdir(parents=True, exist_ok=True)
    logger.info("fetching source", extra={"source_name": spec.name, "url": spec.url})
    try:
        response = requests.get(spec.url, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Could not download {spec.name} from {spec.url}: {exc}. "
            f"If this environment has no outbound network access, download the file "
            f"manually and pass --input <path>. See docs/DATASETS.md."
        ) from exc
    # A 200 response with no bytes is not a valid source snapshot. Reject it
    # before staging so a transient upstream outage cannot erase the last
    # known-good offline input.
    if not response.content:
        raise RuntimeError(f"Downloaded source {spec.name} is empty; previous snapshot preserved")
    # Preserve the last verified source snapshot if a download is interrupted,
    # the disk fills, or publication fails. Stage beside the destination so
    # os.replace is atomic on the same filesystem, even under concurrent runs.
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(response.content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    logger.info(
        "fetched source",
        extra={"source_name": spec.name, "bytes": len(response.content), "path": str(destination)},
    )
    return destination


def _iter_rows(path: Path) -> Iterator[tuple[int, dict[str, str], Sequence[str] | None]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        header = reader.fieldnames
        for offset, row in enumerate(reader):
            # Row 1 is the header, so data rows start at 2 - matching what a
            # person sees when they open the file to investigate a reject.
            yield offset + 2, row, header


def ingest_file(
    spec: SourceSpec,
    path: Path,
    *,
    batch_id: str | None = None,
    floor_date: date | None = None,
    today: date | None = None,
) -> IngestResult:
    """Read one CSV file and return validated canonical observations."""
    batch_id = batch_id or new_batch_id()
    ingested_at = datetime.now(tz=UTC).replace(microsecond=0)
    result = IngestResult(source_name=spec.name, batch_id=batch_id)
    deduped: dict[str, Observation] = {}
    header_checked = False

    for row_number, row, header in _iter_rows(path):
        if not header_checked:
            validate_header(spec, header)
            header_checked = True
        # DictReader silently collects extra fields under None and fills
        # missing trailing fields with None. Either case corrupts the
        # observation without an explicit schema failure.
        if None in row or any(value is None for value in row.values()):
            raise SchemaError(f"{spec.name}: CSV row {row_number} has a different number of fields than its header")
        result.rows_read += 1

        raw_date = row.get(spec.date_column) or ""
        try:
            obs_date = parse_observation_date(spec, raw_date, today=today)
        except ValueError as exc:
            result.rejects.append(RejectedValue(spec.name, row_number, spec.date_column, raw_date, str(exc)))
            continue

        if floor_date is not None and obs_date < floor_date:
            result.rows_skipped_by_watermark += 1
            continue

        for column, metric_name in spec.metric_columns.items():
            raw_value = row.get(column) or ""
            try:
                value = parse_metric_value(spec, metric_name, raw_value)
            except ValueError as exc:
                result.rejects.append(RejectedValue(spec.name, row_number, column, raw_value, str(exc)))
                continue
            if value is None:
                result.values_missing += 1
                continue
            key = observation_key(spec.series_id, metric_name, obs_date)
            if key in deduped:
                result.duplicates_dropped += 1
            deduped[key] = Observation(
                observation_key=key,
                series_id=spec.series_id,
                metric_name=metric_name,
                observation_date=obs_date,
                metric_value=value,
                source_name=spec.name,
                source_row_number=row_number,
                batch_id=batch_id,
                ingested_at=ingested_at,
            )

    if not header_checked:
        # Empty file (header only, or completely empty): validate what we can
        # so a truncated upload surfaces as a schema error, not silent success.
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            validate_header(spec, csv.DictReader(handle).fieldnames)

    result.observations = sorted(deduped.values(), key=lambda o: (o.observation_date, o.metric_name))
    logger.info("ingest complete", extra=result.summary())
    return result


def _atomic_parquet_write(table: pa.Table, destination: Path) -> None:
    """Publish only complete Parquet artifacts, preserving a prior good batch.

    Parquet writers can fail after creating a partial file. Stage each artifact
    beside its destination, sync it, then atomically replace the old version.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
    os.close(fd)
    try:
        pq.write_table(table, temporary, compression="snappy")
        with open(temporary, "rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def write_parquet(result: IngestResult, destination: Path) -> Path:
    """Write observations to Parquet (always, even when empty, for lineage)."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pydict(
        {
            "observation_key": [o.observation_key for o in result.observations],
            "series_id": [o.series_id for o in result.observations],
            "metric_name": [o.metric_name for o in result.observations],
            "observation_date": [o.observation_date for o in result.observations],
            "metric_value": [o.metric_value for o in result.observations],
            "source_name": [o.source_name for o in result.observations],
            "source_row_number": [o.source_row_number for o in result.observations],
            "batch_id": [o.batch_id for o in result.observations],
            "ingested_at": [o.ingested_at for o in result.observations],
        },
        schema=PARQUET_SCHEMA,
    )
    _atomic_parquet_write(table, destination)
    result.parquet_path = destination
    logger.info(
        "wrote parquet",
        extra={"path": str(destination), "rows": table.num_rows, "source_name": result.source_name},
    )
    return destination


def write_rejects(result: IngestResult, destination: Path) -> Path | None:
    """Write the reject sidecar. Returns None when there was nothing to write."""
    if not result.rejects:
        return None
    destination.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pydict(
        {
            "source_name": [r.source_name for r in result.rejects],
            "source_row_number": [r.source_row_number for r in result.rejects],
            "column": [r.column for r in result.rejects],
            "raw_value": [r.raw_value for r in result.rejects],
            "reason": [r.reason for r in result.rejects],
            "batch_id": [result.batch_id for _ in result.rejects],
        },
        schema=REJECT_SCHEMA,
    )
    _atomic_parquet_write(table, destination)
    result.rejects_path = destination
    logger.warning(
        "wrote rejects",
        extra={"path": str(destination), "rows": table.num_rows, "source_name": result.source_name},
    )
    return destination


def decimal_to_float(value: Decimal) -> float:
    """Helper for assertions and reporting; storage keeps full decimal precision."""
    return float(value)
