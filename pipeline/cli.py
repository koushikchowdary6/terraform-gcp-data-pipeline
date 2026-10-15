"""Command-line entry point.

    python -m pipeline.cli run-local          # full local pipeline (DuckDB)
    python -m pipeline.cli run-local --full-refresh
    python -m pipeline.cli state              # show incremental watermarks
    python -m pipeline.cli quality            # re-run checks on existing warehouse
    python -m pipeline.cli examples           # run the example analytical queries
    python -m pipeline.cli run-cloud          # GCS + BigQuery (needs credentials)
    python -m pipeline.cli estimate-cost      # dry-run byte/cost estimate

Exit codes: 0 success, 1 pipeline or data-quality failure, 2 configuration error.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from .config import Config, ConfigError
from .ingest import fetch_source, ingest_file, new_batch_id, write_parquet, write_rejects
from .logging_setup import configure_logging, get_logger
from .quality import QualityCheckFailed, enforce, format_table, run_local_checks
from .sources import SOURCES, get_source
from .sql_files import duckdb_sql, split_statements
from .state import WatermarkStore, effective_floor, mark_delivered, pending_batches
from .warehouse import LocalWarehouse

logger = get_logger(__name__)


def _resolve_sources(raw: str | None) -> list[str]:
    if not raw or raw == "all":
        return list(SOURCES)
    names = [n.strip() for n in raw.split(",") if n.strip()]
    for name in names:
        get_source(name)  # raises KeyError with the available names
    return names


def _batch_dir(cfg: Config, batch_id: str) -> Path:
    return cfg.processed_dir / f"batch={batch_id}"


# --------------------------------------------------------------------- commands


def cmd_fetch(args: argparse.Namespace) -> int:
    cfg = Config.from_env()
    cfg.ensure_local_dirs()
    for name in _resolve_sources(args.sources):
        spec = get_source(name)
        fetch_source(spec, cfg.raw_dir / spec.raw_filename)
    return 0


def cmd_run_local(args: argparse.Namespace) -> int:
    cfg = Config.from_env()
    cfg.ensure_local_dirs()
    store = WatermarkStore(cfg.state_path)
    batch_id = new_batch_id()
    batch_dir = _batch_dir(cfg, batch_id)
    lookback = args.lookback_days if args.lookback_days is not None else cfg.lookback_days

    results = []
    for name in _resolve_sources(args.sources):
        spec = get_source(name)
        raw_path = cfg.raw_dir / spec.raw_filename
        if not raw_path.exists() or args.refresh_source:
            if args.offline:
                raise FileNotFoundError(
                    f"{raw_path} is missing and --offline was given. Fetch it first with "
                    f"'python -m pipeline.cli fetch --sources {name}', or place the CSV at that path. "
                    f"See docs/DATASETS.md."
                )
            fetch_source(spec, raw_path)

        floor = None if args.full_refresh else effective_floor(store.get(name), lookback)
        logger.info(
            "ingesting",
            extra={
                "source_name": name,
                "watermark": store.get(name).isoformat() if store.get(name) else None,
                "floor_date": floor.isoformat() if floor else None,
                "full_refresh": bool(args.full_refresh),
            },
        )
        result = ingest_file(spec, raw_path, batch_id=batch_id, floor_date=floor)
        write_parquet(result, batch_dir / f"{name}.parquet")
        write_rejects(result, cfg.data_dir / "rejects" / f"batch={batch_id}" / f"{name}.parquet")
        results.append(result)

    with LocalWarehouse(cfg.duckdb_path) as warehouse:
        staged = warehouse.load_staging(str(batch_dir / "*.parquet"))
        stats = warehouse.upsert()
        analytics_rows = warehouse.build_analytics()
        checks = run_local_checks(warehouse)
        print("\n=== Data-quality checks ===")
        print(format_table(checks))
        # Watermarks advance only after the checks pass, so a failed run is
        # retried from the same position rather than silently skipping data.
        enforce(checks, fail_on_error=cfg.fail_on_quality_error)

        print("\n=== Run summary ===")
        print(f"batch_id              : {batch_id}")
        print(f"staged rows           : {staged}")
        print(f"fact rows before/after: {stats['fact_rows_before']} / {stats['fact_rows_after']}")
        print(f"rows inserted         : {stats['rows_inserted']}")
        print(f"monthly mart rows     : {analytics_rows}")
        for result in results:
            summary = result.summary()
            print(
                f"  {summary['source_name']:<16} read={summary['rows_read']:<6} "
                f"observations={summary['observations']:<6} rejects={summary['rejects']:<4} "
                f"missing={summary['values_missing']:<5} dup_dropped={summary['duplicates_dropped']:<4} "
                f"skipped_by_watermark={summary['rows_skipped_by_watermark']:<6} "
                f"max_date={summary['max_observation_date']}"
            )

        print("\n=== Coverage ===")
        for row in warehouse.coverage():
            print(f"  {row[0]:<16} {row[1]:<20} n={row[2]:<6} {row[3]} -> {row[4]}")

    advanced = []
    for result in results:
        high_water = result.max_observation_date
        if high_water and store.advance(result.source_name, high_water):
            advanced.append((result.source_name, high_water.isoformat()))
    if advanced:
        print("\n=== Watermarks advanced ===")
        for name, value in advanced:
            print(f"  {name:<16} -> {value}")
    return 0


def cmd_quality(_: argparse.Namespace) -> int:
    cfg = Config.from_env()
    if not cfg.duckdb_path.exists():
        raise FileNotFoundError(
            f"No local warehouse at {cfg.duckdb_path}. Run 'python -m pipeline.cli run-local' first."
        )
    with LocalWarehouse(cfg.duckdb_path) as warehouse:
        checks = run_local_checks(warehouse)
        print(format_table(checks))
        enforce(checks, fail_on_error=cfg.fail_on_quality_error)
    return 0


def cmd_state(_: argparse.Namespace) -> int:
    cfg = Config.from_env()
    store = WatermarkStore(cfg.state_path)
    current = store.all()
    if not current:
        print(f"No watermarks recorded yet (state file: {cfg.state_path})")
        return 0
    print(f"Watermarks ({cfg.state_path}):")
    for name, value in sorted(current.items()):
        print(f"  {name:<16} {value}")
    return 0


def cmd_examples(_: argparse.Namespace) -> int:
    cfg = Config.from_env()
    if not cfg.duckdb_path.exists():
        raise FileNotFoundError(
            f"No local warehouse at {cfg.duckdb_path}. Run 'python -m pipeline.cli run-local' first."
        )
    with LocalWarehouse(cfg.duckdb_path) as warehouse:
        for index, statement in enumerate(split_statements(duckdb_sql("05_example_queries.sql")), start=1):
            print(f"\n--- example query {index} ---")
            cursor = warehouse.connection.execute(statement)
            columns = [d[0] for d in cursor.description or []]
            rows = cursor.fetchall()
            print(" | ".join(columns))
            for row in rows:
                print(" | ".join(str(value) for value in row))
    return 0


def cmd_run_cloud(args: argparse.Namespace) -> int:
    """Deliver every not-yet-delivered local batch to GCS and BigQuery.

    Delivery is tracked per batch rather than inferred from the watermark.
    The local watermark advances as soon as a batch passes its *local* checks,
    so using it to decide what the cloud already holds would silently skip any
    batch whose upload failed: the next run would produce a batch of only newer
    rows, and the failed one would never be retried. Processing every pending
    batch in order makes a failed delivery self-healing on the next run.
    """
    from .bq import BigQueryWarehouse  # noqa: PLC0415 - lazy: cloud deps optional
    from .gcs import gcs_uri, raw_object_name, upload_file  # noqa: PLC0415

    cfg = Config.from_env()
    project_id, bucket = cfg.require_gcp()

    pending = pending_batches(cfg.processed_dir)
    if not pending:
        print(f"Nothing to deliver: every batch under {cfg.processed_dir} is already marked delivered.")
        return 0

    print(f"{len(pending)} batch(es) pending delivery:")
    for batch_dir in pending:
        print(f"  {batch_dir.name}")

    warehouse = BigQueryWarehouse(project_id, cfg.bq_dataset, cfg.bq_location)
    delivered = 0

    # Oldest first, so the warehouse receives batches in the order they were
    # produced and a mid-sequence failure leaves the rest pending.
    for batch_dir in pending:
        batch_id = batch_dir.name.removeprefix("batch=")
        print(f"\n=== delivering {batch_id} ===")

        uris = []
        for parquet_path in sorted(batch_dir.glob("*.parquet")):
            source_name = parquet_path.stem
            object_name = raw_object_name(source_name, batch_id)
            if args.skip_upload:
                uris.append(gcs_uri(bucket, object_name))
            else:
                uris.append(upload_file(bucket, parquet_path, object_name, project_id=project_id))

        if not uris:
            logger.warning("batch has no parquet files; marking delivered", extra={"batch_id": batch_id})
            mark_delivered(batch_dir, batch_id=batch_id, rows_staged=0, note="no parquet files")
            delivered += 1
            continue

        staged = warehouse.load_staging_from_gcs(uris)
        affected = warehouse.merge()
        warehouse.build_analytics()
        checks = warehouse.run_quality()
        print(format_table(checks))
        # Raises on failure, so the marker below is never written for a batch
        # that did not verify - it stays pending and is retried next run.
        enforce(checks, fail_on_error=cfg.fail_on_quality_error)

        mark_delivered(batch_dir, batch_id=batch_id, rows_staged=staged, merge_rows_affected=affected)
        delivered += 1
        print(f"  staged {staged}, merge affected {affected} -> marked delivered")

    print(f"\n=== delivered {delivered}/{len(pending)} pending batch(es) ===")

    # The BigQuery watermark reflects what the cloud warehouse actually holds,
    # and is only written once every pending batch has been delivered.
    store = WatermarkStore(cfg.state_path)
    for name, value in store.all().items():
        warehouse.set_watermark(name, date.fromisoformat(value))
    return 0


def cmd_estimate_cost(_: argparse.Namespace) -> int:
    """Dry-run the MERGE and analytics statements to report bytes and cost."""
    from .bq import BigQueryWarehouse, _render  # noqa: PLC0415

    cfg = Config.from_env()
    project_id, _bucket = cfg.require_gcp()
    warehouse = BigQueryWarehouse(project_id, cfg.bq_dataset, cfg.bq_location)
    total_bytes = 0
    for filename in ("02_merge.sql", "03_analytics.sql", "04_quality.sql"):
        sql = _render(filename, project_id, cfg.bq_dataset)
        try:
            size, usd = warehouse.dry_run_bytes(sql)
        except Exception as exc:  # noqa: BLE001 - report and continue
            print(f"{filename:<20} dry run failed: {exc}")
            continue
        total_bytes += size
        print(f"{filename:<20} {size / 1e6:>10.2f} MB   ~${usd:.6f}")
    print(f"{'TOTAL':<20} {total_bytes / 1e6:>10.2f} MB")
    return 0


# ----------------------------------------------------------------------- parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline", description=__doc__.splitlines()[0])
    parser.add_argument("--log-level", default=None, help="DEBUG, INFO (default), WARNING, ERROR")
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch = subparsers.add_parser("fetch", help="download source CSV files to data/raw")
    fetch.add_argument("--sources", default="all", help="comma-separated source names, or 'all'")
    fetch.set_defaults(func=cmd_fetch)

    run_local = subparsers.add_parser("run-local", help="run the full local pipeline with DuckDB")
    run_local.add_argument("--sources", default="all", help="comma-separated source names, or 'all'")
    run_local.add_argument("--full-refresh", action="store_true", help="ignore watermarks and re-process every row")
    run_local.add_argument("--refresh-source", action="store_true", help="re-download source files even if present")
    run_local.add_argument("--offline", action="store_true", help="never download; require local files")
    run_local.add_argument("--lookback-days", type=int, default=None, help="override PIPELINE_LOOKBACK_DAYS")
    run_local.set_defaults(func=cmd_run_local)

    quality = subparsers.add_parser("quality", help="re-run data-quality checks on the local warehouse")
    quality.set_defaults(func=cmd_quality)

    state = subparsers.add_parser("state", help="show incremental watermarks")
    state.set_defaults(func=cmd_state)

    examples = subparsers.add_parser("examples", help="run the example analytical queries")
    examples.set_defaults(func=cmd_examples)

    run_cloud = subparsers.add_parser("run-cloud", help="upload to GCS and run BigQuery staging/MERGE")
    run_cloud.add_argument("--skip-upload", action="store_true", help="assume objects are already in GCS")
    run_cloud.set_defaults(func=cmd_run_cloud)

    estimate = subparsers.add_parser("estimate-cost", help="dry-run BigQuery statements for a cost estimate")
    estimate.set_defaults(func=cmd_estimate_cost)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    try:
        return int(args.func(args))
    except ConfigError as exc:
        logger.error("configuration error", extra={"error": str(exc)})
        print(f"\nConfiguration error: {exc}", file=sys.stderr)
        return 2
    except QualityCheckFailed as exc:
        logger.error("data quality failure", extra={"error": str(exc)})
        print(f"\nData-quality failure: {exc}", file=sys.stderr)
        return 1
    except (FileNotFoundError, KeyError, RuntimeError, ValueError) as exc:
        logger.error("pipeline failure", extra={"error": str(exc), "error_type": type(exc).__name__})
        print(f"\n{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
