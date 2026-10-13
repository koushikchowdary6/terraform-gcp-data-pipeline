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
from datetime import date, timedelta
from pathlib import Path


class WatermarkStore:
    """JSON-file watermark store used by local runs and tests."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    def _read_all(self) -> dict[str, str]:
        if not self.path.exists():
            return {}
        try:
            data = json.loads(self.path.read_text(encoding="utf-8") or "{}")
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Watermark file {self.path} is corrupt ({exc}). "
                "Delete it to re-process from the beginning, or restore it from a backup."
            ) from exc
        if not isinstance(data, dict):
            raise RuntimeError(f"Watermark file {self.path} must contain a JSON object, got {type(data).__name__}")
        return {str(k): str(v) for k, v in data.items()}

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
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Write-then-rename so an interrupted run cannot leave a partial file.
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp.replace(self.path)

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
    if watermark is None:
        return None
    if lookback_days < 0:
        raise ValueError("lookback_days must be >= 0")
    return watermark - timedelta(days=lookback_days)
