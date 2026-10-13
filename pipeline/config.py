"""Runtime configuration.

Configuration is read from environment variables so the same code runs locally
and in CI without edits. Nothing here reads or stores credentials: GCP access
uses Application Default Credentials resolved by the Google client libraries.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    if value is not None:
        value = value.strip()
    return value or None


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name)
    if raw is None:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


class ConfigError(RuntimeError):
    """Raised when configuration is missing or invalid."""


@dataclass(frozen=True)
class Config:
    """Resolved pipeline configuration."""

    # Local execution
    data_dir: Path = field(default_factory=lambda: REPO_ROOT / "data")
    duckdb_path: Path = field(default_factory=lambda: REPO_ROOT / "data" / "local" / "warehouse.duckdb")
    state_path: Path = field(default_factory=lambda: REPO_ROOT / "data" / "local" / "watermarks.json")

    # Incremental behaviour
    lookback_days: int = 7
    fail_on_quality_error: bool = True

    # GCP targets (only required for cloud subcommands)
    gcp_project_id: str | None = None
    gcp_region: str = "us-central1"
    gcs_bucket: str | None = None
    bq_dataset: str = "market_analytics"
    bq_location: str = "US"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def processed_dir(self) -> Path:
        return self.data_dir / "processed"

    @classmethod
    def from_env(cls) -> Config:
        data_dir = Path(_env("PIPELINE_DATA_DIR") or (REPO_ROOT / "data"))
        return cls(
            data_dir=data_dir,
            duckdb_path=Path(_env("PIPELINE_DUCKDB_PATH") or (data_dir / "local" / "warehouse.duckdb")),
            state_path=Path(_env("PIPELINE_STATE_PATH") or (data_dir / "local" / "watermarks.json")),
            lookback_days=_env_int("PIPELINE_LOOKBACK_DAYS", 7),
            fail_on_quality_error=_env_bool("PIPELINE_FAIL_ON_QUALITY_ERROR", True),
            gcp_project_id=_env("GCP_PROJECT_ID"),
            gcp_region=_env("GCP_REGION") or "us-central1",
            gcs_bucket=_env("GCS_BUCKET"),
            bq_dataset=_env("BQ_DATASET") or "market_analytics",
            bq_location=_env("BQ_LOCATION") or "US",
        )

    def require_gcp(self) -> tuple[str, str]:
        """Validate cloud settings before any GCP call.

        Returns (project_id, bucket). Raises ConfigError with an actionable
        message rather than letting a client library fail opaquely later.
        """
        missing = [
            name
            for name, value in (("GCP_PROJECT_ID", self.gcp_project_id), ("GCS_BUCKET", self.gcs_bucket))
            if not value
        ]
        if missing:
            raise ConfigError(
                "Missing required environment variable(s): "
                + ", ".join(missing)
                + ". Copy .env.example to .env and fill it in, or export them in your shell. "
                "See README section 'Cloud execution'."
            )
        assert self.gcp_project_id and self.gcs_bucket  # narrowed by check above
        return self.gcp_project_id, self.gcs_bucket

    def ensure_local_dirs(self) -> None:
        for path in (self.raw_dir, self.processed_dir, self.duckdb_path.parent, self.state_path.parent):
            path.mkdir(parents=True, exist_ok=True)
