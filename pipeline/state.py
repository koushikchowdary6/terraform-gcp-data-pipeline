"""Incremental processing state (watermarks).

The watermark is the newest ``observation_date`` successfully merged for a
given source. The next run only stages rows newer than
``watermark - lookback_days``.

The lookback is deliberate: published financial series get revised after first
publication, so a strict ``> watermark`` filter would silently miss corrections.
Re-staging a small trailing window is safe because the warehouse upsert is keyed
on the deterministic observation key.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

# Written into a batch directory once that batch has been loaded into the
# cloud warehouse AND passed its quality checks there.
DELIVERED_MARKER = "_delivered.json"


def _atomic_write_json(path: Path, payload: dict[str, object]) -> None:
    """Publish state with a unique same-directory temporary file.

    A fixed .tmp name lets overlapping runs clobber one another before
    rename; unique names and cleanup protect the previously published state
    when serialization or replacement fails.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _unique_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Do not let duplicate source keys silently change the ingestion floor."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate watermark source key: {key!r}")
        result[key] = value
    return result


class WatermarkStore:
    """JSON-file watermark store used by local runs and tests."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def _read_all(self) -> dict[str, str]:
        if not self.path.exists():
            return {}
        try:
            data = json.loads(
                self.path.read_text(encoding="utf-8") or "{}",
                object_pairs_hook=_unique_json_keys,
            )
        except (json.JSONDecodeError, ValueError) as exc:
            raise RuntimeError(
                f"Watermark file {self.path} is corrupt ({exc}). "
                "Delete it to re-process from the beginning, or restore it from a backup."
            ) from exc
        if not isinstance(data, dict):
            raise RuntimeError(f"Watermark file {self.path} must contain a JSON object, got {type(data).__name__}")
        # Validate every stored source, not just the one requested: otherwise
        # set() could preserve unrelated corrupt entries indefinitely.
        for source, raw in data.items():
            if not source or not isinstance(raw, str):
                raise RuntimeError(f"Watermark file {self.path} has an invalid source or date value")
            try:
                valid = date.fromisoformat(raw).isoformat() == raw
            except ValueError:
                valid = False
            if not valid:
                raise RuntimeError(f"Watermark file {self.path} has an invalid ISO date for {source!r}")
        return data

    def get(self, source_name: str) -> date | None:
        raw = self._read_all().get(source_name)
        if not raw:
            return None
        try:
            return date.fromisoformat(raw)
        except ValueError as exc:
            raise RuntimeError(f"Watermark for {source_name!r} is not an ISO date: {raw!r}") from exc

    def set(self, source_name: str, value: date) -> None:
        data = self._read_all()
        data[source_name] = value.isoformat()
        _atomic_write_json(self.path, data)

    def advance(self, source_name: str, value: date) -> bool:
        """Move the watermark forward only. Returns True if it moved."""
        current = self.get(source_name)
        if current is not None and value <= current:
            return False
        self.set(source_name, value)
        return True

    def all(self) -> dict[str, str]:
        return self._read_all()


def effective_floor(watermark: date | None, lookback_days: int) -> date | None:
    """Earliest observation_date to stage, given a watermark and lookback."""
    if lookback_days < 0:
        raise ValueError("lookback_days must be >= 0")
    if watermark is None:
        return None
    return watermark - timedelta(days=lookback_days)


# ---------------------------------------------------------------- delivery state
#
# The local watermark advances once a batch is verified *locally*. That alone is
# not enough to decide what the cloud warehouse already holds: a batch can be
# produced and locally verified, then fail to upload. Because the watermark has
# already moved, the next run produces a batch containing only newer rows - so
# uploading "the latest batch" would silently skip the failed one forever.
#
# Delivery is therefore tracked per batch, independently of the watermark.


def batch_dirs(processed_dir: Path) -> list[Path]:
    """Every batch directory, oldest first.

    Batch ids start with a UTC timestamp, so lexical order is chronological.
    """
    return sorted((p for p in Path(processed_dir).glob("batch=*") if p.is_dir()), key=lambda p: p.name)


def is_delivered(batch_dir: Path) -> bool:
    """Only a readable, valid delivery receipt may suppress a retry.

    A truncated or malformed marker is not evidence that cloud delivery
    succeeded. Re-queueing is safe because warehouse loads are idempotent.
    """
    marker = Path(batch_dir) / DELIVERED_MARKER
    if not marker.is_file():
        return False
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return False
    if not isinstance(payload, dict) or not isinstance(payload.get("delivered_at"), str):
        return False
    try:
        delivered_at = datetime.fromisoformat(payload["delivered_at"])
    except ValueError:
        return False
    return delivered_at.tzinfo is not None


def pending_batches(processed_dir: Path) -> list[Path]:
    """Batches not yet confirmed delivered to the cloud warehouse, oldest first."""
    return [d for d in batch_dirs(processed_dir) if not is_delivered(d)]


def mark_delivered(batch_dir: Path, **details: object) -> Path:
    """Record that a batch reached the cloud warehouse and passed its checks."""
    path = Path(batch_dir) / DELIVERED_MARKER
    payload = {"delivered_at": datetime.now(tz=UTC).isoformat(), **details}
    _atomic_write_json(path, payload)
    return path
