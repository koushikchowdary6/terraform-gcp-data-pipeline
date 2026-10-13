"""Loading and splitting the .sql files that hold the warehouse logic.

SQL lives in files rather than in Python string literals so the same statements
can be reviewed, diffed and run by hand in a DuckDB or BigQuery console.
"""

from __future__ import annotations

from pathlib import Path

from .config import REPO_ROOT

DUCKDB_SQL_DIR = REPO_ROOT / "sql" / "duckdb"
BIGQUERY_SQL_DIR = REPO_ROOT / "sql" / "bigquery"


def split_statements(sql: str) -> list[str]:  # noqa: PLR0912 - one branch per lexer state
    """Split a SQL script into statements on top-level semicolons.

    Semicolons inside single-quoted literals, double-quoted identifiers and
    ``--`` line comments are ignored.

    This is a small character-by-character lexer rather than a regex split,
    because a regex on ``;`` would break any statement containing a semicolon
    inside a string literal or a comment. The branch count is inherent to the
    state machine; splitting it into helpers would obscure rather than clarify.
    """
    statements: list[str] = []
    buffer: list[str] = []
    in_single = in_double = in_line_comment = False
    index = 0
    while index < len(sql):
        char = sql[index]
        nxt = sql[index + 1] if index + 1 < len(sql) else ""
        if in_line_comment:
            buffer.append(char)
            if char == "\n":
                in_line_comment = False
        elif in_single:
            buffer.append(char)
            if char == "'":
                if nxt == "'":  # escaped quote
                    buffer.append(nxt)
                    index += 1
                else:
                    in_single = False
        elif in_double:
            buffer.append(char)
            if char == '"':
                in_double = False
        elif char == "-" and nxt == "-":
            in_line_comment = True
            buffer.append(char)
        elif char == "'":
            in_single = True
            buffer.append(char)
        elif char == '"':
            in_double = True
            buffer.append(char)
        elif char == ";":
            statements.append("".join(buffer))
            buffer = []
        else:
            buffer.append(char)
        index += 1
    statements.append("".join(buffer))
    return [s.strip() for s in statements if _is_executable(s)]


def _is_executable(statement: str) -> bool:
    """True when the fragment contains SQL, not only whitespace and comments."""
    for line in statement.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("--"):
            return True
    return False


def read_sql(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise FileNotFoundError(
            f"SQL file not found: {path}. Run commands from the repository root "
            f"so the sql/ directory resolves correctly."
        ) from None


def duckdb_sql(filename: str) -> str:
    return read_sql(DUCKDB_SQL_DIR / filename)


def bigquery_sql(filename: str) -> str:
    return read_sql(BIGQUERY_SQL_DIR / filename)
