"""Local warehouse (DuckDB).

DuckDB runs the same modelling steps as BigQuery - staging load, idempotent
upsert, analytical model, quality checks - against the same Parquet files. It
exists so the pipeline is fully runnable and testable without a GCP project.

This is emulation, not proof of cloud behaviour: see the README's
"What is verified" section.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import duckdb

from .logging_setup import get_logger
from .sql_files import duckdb_sql, split_statements

logger = get_logger(__name__)


class LocalWarehouse:
    """Thin wrapper around a DuckDB connection holding the warehouse model."""

    def __init__(self, database_path: Path | str = ":memory:") -> None:
        self.database_path = str(database_path)
        if self.database_path != ":memory:":
            Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
        self.connection = duckdb.connect(self.database_path)

    def __enter__(self) -> LocalWarehouse:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        self.connection.close()

    # ---------------------------------------------------------------- helpers

    def execute_script(self, sql: str, params: Sequence[Any] | None = None) -> None:
        """Run a multi-statement script, binding ``params`` where placeholders appear."""
        for statement in split_statements(sql):
            if params and "?" in statement:
                self.connection.execute(statement, list(params))
            else:
                self.connection.execute(statement)

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[tuple[Any, ...]]:
        cursor = self.connection.execute(sql, list(params)) if params else self.connection.execute(sql)
        return cursor.fetchall()

    def scalar(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        rows = self.query(sql, params)
        return rows[0][0] if rows else None

    # ------------------------------------------------------------------ steps

    def load_staging(self, parquet_glob: str) -> int:
        """Rebuild the staging table from a Parquet glob. Returns row count."""
        self.execute_script(duckdb_sql("01_staging.sql"), params=[parquet_glob])
        count = int(self.scalar("SELECT COUNT(*) FROM staging.market_observation") or 0)
        logger.info("staging loaded", extra={"rows": count, "parquet_glob": parquet_glob})
        return count

    def upsert(self) -> dict[str, int]:
        """Upsert staging into the fact table. Returns before/after/changed counts."""
        before = (
            int(self.scalar("SELECT COUNT(*) FROM analytics.fct_market_observation") or 0) if self._fact_exists() else 0
        )
        self.execute_script(duckdb_sql("02_upsert.sql"))
        after = int(self.scalar("SELECT COUNT(*) FROM analytics.fct_market_observation") or 0)
        stats = {"fact_rows_before": before, "fact_rows_after": after, "rows_inserted": after - before}
        logger.info("upsert complete", extra=stats)
        return stats

    def build_analytics(self) -> int:
        self.execute_script(duckdb_sql("03_analytics.sql"))
        count = int(self.scalar("SELECT COUNT(*) FROM analytics.mart_metric_monthly") or 0)
        logger.info("analytics model built", extra={"rows": count})
        return count

    def _fact_exists(self) -> bool:
        return bool(
            self.scalar(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'analytics' AND table_name = 'fct_market_observation'"
            )
        )

    # --------------------------------------------------------------- reporting

    def fact_row_count(self) -> int:
        if not self._fact_exists():
            return 0
        return int(self.scalar("SELECT COUNT(*) FROM analytics.fct_market_observation") or 0)

    def value_for(self, series_id: str, metric_name: str, observation_date: str) -> Any:
        return self.scalar(
            "SELECT metric_value FROM analytics.fct_market_observation "
            "WHERE series_id = ? AND metric_name = ? AND observation_date = ?",
            [series_id, metric_name, observation_date],
        )

    def updated_at_fingerprint(self) -> list[tuple[Any, ...]]:
        """(key, value, updated_at) for every fact row - used to prove no-op reruns.

        ``updated_at`` is cast to VARCHAR in SQL on purpose: DuckDB needs the
        optional ``pytz`` package to convert TIMESTAMP WITH TIME ZONE into a
        Python object, and this comparison only ever needs the text form.
        """
        return self.query(
            "SELECT observation_key, metric_value, CAST(updated_at AS VARCHAR) "
            "FROM analytics.fct_market_observation ORDER BY observation_key"
        )

    def coverage(self) -> list[tuple[Any, ...]]:
        return self.query(
            """
            SELECT series_id, metric_name, COUNT(*) AS observations,
                   MIN(observation_date) AS first_date, MAX(observation_date) AS last_date
            FROM analytics.fct_market_observation
            GROUP BY series_id, metric_name
            ORDER BY series_id, metric_name
            """
        )
