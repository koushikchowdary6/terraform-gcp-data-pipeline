"""BigQuery staging, MERGE, analytics and quality checks.

Table ownership is split deliberately:

  * Terraform owns the dataset and the durable tables (fct_market_observation,
    mart_metric_monthly's dataset, pipeline_watermark). Schemas live in
    terraform/schemas/*.json, so the structure is reviewed in version control
    rather than created as a side effect of a job.
  * This module owns the ephemeral staging table, which is truncated and
    rewritten on every run by the load job.

The google-cloud-bigquery client is imported lazily, so unit tests and the
local DuckDB path run without the cloud dependency.

Credentials come from Application Default Credentials. Nothing here reads,
prints or stores a key.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from string import Template
from typing import Any

from .logging_setup import get_logger
from .quality import QualityCheck
from .schema import VALUE_PRECISION, VALUE_SCALE
from .sql_files import bigquery_sql

logger = get_logger(__name__)

STAGING_TABLE = "stg_market_observation"
FACT_TABLE = "fct_market_observation"
WATERMARK_TABLE = "pipeline_watermark"
BYTES_PER_TIB = 1024**4
# On-demand BigQuery analysis pricing at time of writing (us multi-region).
USD_PER_TIB_SCANNED = 6.25


def _render(filename: str, project_id: str, dataset: str) -> str:
    """Substitute ${project_id} / ${dataset} into a SQL template.

    Table names cannot be passed as query parameters in BigQuery, so they are
    templated. Both values come from our own configuration, never from
    row data or user input.
    """
    return Template(bigquery_sql(filename)).substitute(project_id=project_id, dataset=dataset)


class BigQueryWarehouse:
    """Cloud counterpart of pipeline.warehouse.LocalWarehouse."""

    def __init__(self, project_id: str, dataset: str, location: str = "US") -> None:
        try:
            from google.cloud import bigquery  # noqa: PLC0415
        except ImportError as exc:
            raise RuntimeError("google-cloud-bigquery is not installed. Run: pip install -r requirements.txt") from exc
        self._bigquery = bigquery
        self.project_id = project_id
        self.dataset = dataset
        self.location = location
        self.client = bigquery.Client(project=project_id, location=location)

    # ------------------------------------------------------------------ paths

    def table_ref(self, table: str) -> str:
        return f"{self.project_id}.{self.dataset}.{table}"

    # ------------------------------------------------------------------ steps

    def staging_schema(self) -> list[Any]:
        bq = self._bigquery
        return [
            bq.SchemaField("observation_key", "STRING", mode="REQUIRED"),
            bq.SchemaField("series_id", "STRING", mode="REQUIRED"),
            bq.SchemaField("metric_name", "STRING", mode="REQUIRED"),
            bq.SchemaField("observation_date", "DATE", mode="REQUIRED"),
            bq.SchemaField("metric_value", "NUMERIC", mode="REQUIRED", precision=VALUE_PRECISION, scale=VALUE_SCALE),
            bq.SchemaField("source_name", "STRING", mode="REQUIRED"),
            bq.SchemaField("source_row_number", "INT64", mode="REQUIRED"),
            bq.SchemaField("batch_id", "STRING", mode="REQUIRED"),
            bq.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        ]

    def load_staging_from_gcs(self, uris: Sequence[str]) -> int:
        """Load Parquet from GCS into the staging table, replacing its contents.

        WRITE_TRUNCATE makes the load itself idempotent: re-running the same
        batch cannot accumulate rows in staging.
        """
        bq = self._bigquery
        if not uris:
            raise ValueError("No GCS URIs supplied to load_staging_from_gcs")
        job_config = bq.LoadJobConfig(
            source_format=bq.SourceFormat.PARQUET,
            write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
            schema=self.staging_schema(),
        )
        target = self.table_ref(STAGING_TABLE)
        logger.info("loading staging from gcs", extra={"uris": list(uris), "target": target})
        job = self.client.load_table_from_uri(list(uris), target, job_config=job_config)
        job.result()  # raises on failure, with BigQuery's own error detail
        rows = int(self.client.get_table(target).num_rows)
        logger.info("staging loaded", extra={"target": target, "rows": rows, "job_id": job.job_id})
        return rows

    def merge(self) -> int:
        """Run the incremental MERGE. Returns rows affected."""
        sql = _render("02_merge.sql", self.project_id, self.dataset)
        job = self.client.query(sql)
        job.result()
        affected = int(job.num_dml_affected_rows or 0)
        logger.info(
            "merge complete",
            extra={
                "rows_affected": affected,
                "job_id": job.job_id,
                "total_bytes_processed": job.total_bytes_processed,
            },
        )
        return affected

    def build_analytics(self) -> None:
        sql = _render("03_analytics.sql", self.project_id, self.dataset)
        job = self.client.query(sql)
        job.result()
        logger.info(
            "analytics model built",
            extra={"job_id": job.job_id, "total_bytes_processed": job.total_bytes_processed},
        )

    def run_quality(self) -> list[QualityCheck]:
        sql = _render("04_quality.sql", self.project_id, self.dataset)
        job = self.client.query(sql)
        checks = [
            QualityCheck(
                check_name=str(row["check_name"]),
                status=str(row["status"]),
                observed=int(row["observed"]),
                detail=str(row["detail"]),
            )
            for row in job.result()
        ]
        for check in checks:
            log = logger.info if check.passed else logger.error
            log("quality check", extra=check.as_dict())
        return checks

    # ------------------------------------------------------------- watermarks

    def get_watermark(self, source_name: str) -> date | None:
        sql = f"SELECT max_observation_date FROM `{self.table_ref(WATERMARK_TABLE)}` WHERE source_name = @source_name"
        config = self._bigquery.QueryJobConfig(
            query_parameters=[self._bigquery.ScalarQueryParameter("source_name", "STRING", source_name)]
        )
        rows = list(self.client.query(sql, job_config=config).result())
        return rows[0]["max_observation_date"] if rows else None

    def set_watermark(self, source_name: str, value: date) -> None:
        sql = f"""
        MERGE `{self.table_ref(WATERMARK_TABLE)}` AS T
        USING (SELECT @source_name AS source_name, @value AS max_observation_date) AS S
        ON T.source_name = S.source_name
        WHEN MATCHED THEN UPDATE SET
              max_observation_date = S.max_observation_date
            , updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (source_name, max_observation_date, updated_at)
            VALUES (S.source_name, S.max_observation_date, CURRENT_TIMESTAMP())
        """
        config = self._bigquery.QueryJobConfig(
            query_parameters=[
                self._bigquery.ScalarQueryParameter("source_name", "STRING", source_name),
                self._bigquery.ScalarQueryParameter("value", "DATE", value),
            ]
        )
        self.client.query(sql, job_config=config).result()
        logger.info("watermark updated", extra={"source_name": source_name, "value": value.isoformat()})

    # -------------------------------------------------------------- cost tool

    def dry_run_bytes(self, sql: str) -> tuple[int, float]:
        """Estimate a query's scan without running it. Returns (bytes, usd)."""
        config = self._bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = self.client.query(sql, job_config=config)
        total = int(job.total_bytes_processed or 0)
        return total, total / BYTES_PER_TIB * USD_PER_TIB_SCANNED
