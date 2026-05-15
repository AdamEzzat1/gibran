"""Source-type dispatch: turn a registered `source_id` into the FROM-clause
snippet for that source, and a data-version token for cache invalidation.

V1 assumption was "source_id == DuckDB relation name" -- which only works
for `duckdb_table` and `sql_view` source types. For parquet/csv sources
users had to manually `CREATE VIEW orders AS SELECT * FROM 'path.parquet'`
before any `gibran check` or DSL query would work.

This helper looks up the source's `source_type` + `uri` and dispatches
through the active `ExecutionEngine` for the FROM-clause fragment:

  duckdb_table  ->  "<uri>"                 (quoted identifier)
  sql_view      ->  "<uri>"                 (quoted identifier)
  parquet       ->  read_parquet('<uri>')   (DuckDB file-scan)
  csv           ->  read_csv('<uri>')       (DuckDB file-scan)

Phase 5A.1: the actual rendering moved into `DuckDBEngine.file_scan_sql`.
This module now wraps the engine and preserves backward-compatible
function signatures so existing callers (drift detector, applier,
example_values, dsl/compile) don't need to change yet.

`SourceDispatchError` is defined in `gibran.execution.engine` and
re-exported here for backward compatibility -- existing imports of
`from gibran._source_dispatch import SourceDispatchError` keep working.

Phase 2B addition: `source_data_version` closes the result-cache
stale-row hole by probing the source's data state at lookup time, and
`touch_source` lets users bump the version for sources that gibran
can't probe (duckdb_table / sql_view).
"""
from __future__ import annotations

import os

import duckdb

from gibran.execution.engine import DuckDBEngine, SourceDispatchError

__all__ = [
    "SourceDispatchError",
    "build_from_clause",
    "from_clause_for_source",
    "source_data_version",
    "touch_source",
]


def source_data_version(
    con: duckdb.DuckDBPyConnection, source_id: str
) -> str:
    """Return an opaque token representing this source's data state.

    The cache key includes this token so a parquet rewrite / duckdb_table
    overwrite invalidates cached results even between sync/check bumps.

    Per source-type:
      parquet / csv   -- os.stat(uri).st_mtime_ns. Cheap (sub-millisecond)
                         and reliable for file-backed sources.
      duckdb_table    -- value from gibran_table_versions, or "0" if the
                         source has never been touched. The user runs
                         `gibran touch <source>` after an external write.
      sql_view        -- same as duckdb_table for V1: an opaque token
                         touched manually. Recursive derivation from the
                         view's referenced tables is Phase 3 work.

    Returns "missing" for parquet/csv when the file is unreadable -- a
    distinct value so the cache treats deleted files as "definitely
    changed since last time" (any future state, including re-appearance,
    is also a change).
    """
    row = con.execute(
        "SELECT source_type, uri FROM gibran_sources WHERE source_id = ?",
        [source_id],
    ).fetchone()
    if row is None:
        raise SourceDispatchError(f"unknown source: {source_id!r}")
    source_type, uri = row

    if source_type in ("parquet", "csv"):
        try:
            return str(os.stat(uri).st_mtime_ns)
        except FileNotFoundError:
            return "missing"
    if source_type in ("duckdb_table", "sql_view"):
        v = con.execute(
            "SELECT version FROM gibran_table_versions WHERE source_id = ?",
            [source_id],
        ).fetchone()
        return v[0] if v else "0"
    raise SourceDispatchError(
        f"unrecognized source_type {source_type!r} for source {source_id!r}"
    )


def touch_source(
    con: duckdb.DuckDBPyConnection, source_id: str
) -> str:
    """Bump the source's data-version token. Used by `gibran touch` and
    by tests / programmatic invalidation. Returns the new version.

    Validates that the source exists. For source types that derive their
    version from the file system (parquet/csv), touching is a no-op --
    the file's own mtime is authoritative and a touch can't influence it.
    Returns the current file mtime in that case so callers always get a
    meaningful return value.
    """
    import uuid as _uuid

    row = con.execute(
        "SELECT source_type FROM gibran_sources WHERE source_id = ?",
        [source_id],
    ).fetchone()
    if row is None:
        raise SourceDispatchError(f"unknown source: {source_id!r}")
    source_type = row[0]
    if source_type in ("parquet", "csv"):
        return source_data_version(con, source_id)

    new_version = _uuid.uuid4().hex
    # DuckDB's ON CONFLICT parser rejects `updated_at = CURRENT_TIMESTAMP`
    # in the SET clause (it tries to bind CURRENT_TIMESTAMP as a column).
    # Pass CURRENT_TIMESTAMP through the VALUES list and reuse it via
    # EXCLUDED.updated_at in the conflict path.
    con.execute(
        "INSERT INTO gibran_table_versions (source_id, version, updated_at) "
        "VALUES (?, ?, CURRENT_TIMESTAMP) "
        "ON CONFLICT (source_id) DO UPDATE SET "
        "  version = EXCLUDED.version, updated_at = EXCLUDED.updated_at",
        [source_id, new_version],
    )
    return new_version


def from_clause_for_source(
    con: duckdb.DuckDBPyConnection, source_id: str
) -> str:
    """Return the FROM-clause snippet that scans this source.

    Result is one of:
      "table_or_view_name"           (quoted identifier, for duckdb_table / sql_view)
      read_parquet('path/uri')        (for parquet)
      read_csv('path/uri')            (for csv)

    Raises SourceDispatchError if the source is not registered or has an
    unrecognized source_type. The caller is responsible for catching this
    and surfacing it as appropriate (e.g. CompileError in the DSL path).
    """
    row = con.execute(
        "SELECT source_type, uri FROM gibran_sources WHERE source_id = ?",
        [source_id],
    ).fetchone()
    if row is None:
        raise SourceDispatchError(f"unknown source: {source_id!r}")
    source_type, uri = row
    return DuckDBEngine(con).file_scan_sql(source_type, uri)


def build_from_clause(source_type: str, uri: str) -> str:
    """Pure-function form of from_clause_for_source: takes (source_type, uri)
    directly, without a DB lookup. Used by the drift detector at sync time
    where the source isn't in `gibran_sources` yet."""
    return DuckDBEngine().file_scan_sql(source_type, uri)
