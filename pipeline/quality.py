"""Data-quality checks.

The checks themselves are SQL (sql/duckdb/04_quality.sql and the BigQuery
equivalent) so the same assertions can be run by hand against either engine.
This module executes them, reports the result and decides whether the run
should fail.
"""

from __future__ import annotations

from dataclasses import dataclass

from .logging_setup import get_logger
from .sql_files import duckdb_sql
from .warehouse import LocalWarehouse

logger = get_logger(__name__)


class QualityCheckFailed(RuntimeError):
    """Raised when one or more data-quality checks fail."""


@dataclass(frozen=True)
class QualityCheck:
    check_name: str
    status: str
    observed: int
    detail: str

    @property
    def passed(self) -> bool:
        return self.status.upper() == "PASS"

    def as_dict(self) -> dict[str, object]:
        return {
            "check_name": self.check_name,
            "status": self.status,
            "observed": self.observed,
            "detail": self.detail,
        }


def run_local_checks(warehouse: LocalWarehouse) -> list[QualityCheck]:
    """Execute the DuckDB quality suite and return one result per check."""
    rows = warehouse.query(duckdb_sql("04_quality.sql"))
    checks = [QualityCheck(check_name=str(r[0]), status=str(r[1]), observed=int(r[2]), detail=str(r[3])) for r in rows]
    for check in checks:
        log = logger.info if check.passed else logger.error
        log("quality check", extra=check.as_dict())
    return checks


def failures(checks: list[QualityCheck]) -> list[QualityCheck]:
    return [c for c in checks if not c.passed]


def enforce(checks: list[QualityCheck], *, fail_on_error: bool = True) -> None:
    """Fail closed on missing/ambiguous checks; optionally warn on measured failures."""
    # An empty query result is not evidence of a clean warehouse. This can
    # happen if a quality SQL file is accidentally changed to return no rows.
    # Even warning-only deployments must not publish a fabricated PASS.
    if not checks:
        raise QualityCheckFailed("No data-quality checks returned; warehouse was not verified")
    names = [check.check_name for check in checks]
    if any(not name.strip() for name in names) or len(names) != len(set(names)):
        raise QualityCheckFailed("Data-quality checks have missing or duplicate names")
    bad = failures(checks)
    if not bad:
        return
    detail = "; ".join(f"{c.check_name} (observed {c.observed}: {c.detail})" for c in bad)
    message = (
        f"{len(bad)} data-quality check(s) failed: {detail}. "
        "Re-run the offending predicate from sql/duckdb/04_quality.sql as a SELECT "
        "to list the rows responsible."
    )
    if fail_on_error:
        raise QualityCheckFailed(message)
    # NOTE: the key here must not collide with a LogRecord attribute.
    # logging.Logger.makeRecord raises KeyError if `extra` contains a reserved
    # name such as "message", "module" or "filename", which turns a log line
    # into a crash. Using "message" here was a real bug caught by the tests.
    logger.warning(
        "quality checks failed but failures are configured as warnings",
        extra={"failure_count": len(bad), "failed_checks": detail},
    )


def format_table(checks: list[QualityCheck]) -> str:
    """Render checks as a fixed-width table for terminal output and the README."""
    if not checks:
        return "(no checks run)"
    name_width = max(len(c.check_name) for c in checks + [QualityCheck("check", "", 0, "")])
    lines = [f"{'CHECK'.ljust(name_width)}  STATUS  OBSERVED  DETAIL", "-" * (name_width + 40)]
    for c in checks:
        lines.append(f"{c.check_name.ljust(name_width)}  {c.status:<6}  {c.observed:>8}  {c.detail}")
    return "\n".join(lines)
