"""SQL dialect abstraction (Phase 5A.0 scaffold).

A single seam for which sqlglot dialect the parse / transpile / SQL-rendering
paths use. V0.1 hardcodes DuckDB everywhere; this module is the additive
first step toward multi-database support (Postgres / Snowflake / BigQuery).

Behavior today:
  - Default dialect is DuckDB (matches V0.1 behavior exactly).
  - Override via the GIBRAN_SQL_DIALECT environment variable.
  - Unknown values raise ValueError at lookup time rather than silently
    falling back -- a typo'd dialect name should fail loudly so the user
    fixes their config, not silently emit DuckDB SQL against Snowflake.

What this DOES NOT do (deliberately):
  - Pick a connection driver (DuckDB-only execution path still applies).
  - Translate emitted SQL between dialects (compile path is unchanged).
  - Add migration files for non-DuckDB engines.

Those land in 5A.1-5A.4. This module exists so the sqlglot call sites
in execution/sql.py and governance/redaction.py read the dialect from one
place instead of literal strings, and so the result cache key includes
the dialect (preventing cross-dialect cache collisions when 5A.2+ lands).
"""
from __future__ import annotations

import os
from enum import Enum


class Dialect(str, Enum):
    """Supported SQL dialects.

    Inherits from str so values are JSON-serializable and compare equal to
    their string form (`Dialect.DUCKDB == "duckdb"`), which lets the
    enum drop straight into sqlglot's `dialect=` parameter without a
    `.value` lookup.
    """
    DUCKDB = "duckdb"
    POSTGRES = "postgres"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"


# Module-level cache of the resolved dialect. Lazy + memoized so the env
# var is read once per process. Tests that want to override mid-process
# call `_reset_active_dialect()` (private; not part of the public API).
_ACTIVE: Dialect | None = None


def active_dialect() -> Dialect:
    """Return the active SQL dialect for this process.

    Reads GIBRAN_SQL_DIALECT once and caches. Unknown values raise
    ValueError -- a misconfigured dialect should fail loudly, not
    silently fall back to DuckDB.
    """
    global _ACTIVE
    if _ACTIVE is not None:
        return _ACTIVE
    raw = os.environ.get("GIBRAN_SQL_DIALECT", "").strip().lower()
    if not raw:
        _ACTIVE = Dialect.DUCKDB
        return _ACTIVE
    try:
        _ACTIVE = Dialect(raw)
    except ValueError as e:
        valid = ", ".join(d.value for d in Dialect)
        raise ValueError(
            f"GIBRAN_SQL_DIALECT={raw!r} is not a recognized dialect; "
            f"valid choices: {valid}"
        ) from e
    return _ACTIVE


def _reset_active_dialect() -> None:
    """Clear the memoized dialect. Test-only -- not part of public API.

    Tests that set GIBRAN_SQL_DIALECT via monkeypatch need this so the
    new env-var value is observed instead of the cached one.
    """
    global _ACTIVE
    _ACTIVE = None
