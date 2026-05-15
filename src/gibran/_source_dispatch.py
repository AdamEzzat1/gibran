"""Source-type dispatch: turn a registered `source_id` into the FROM-clause
snippet for that source.

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
example_values, dsl/compile) don't need to change yet. 5A.1b migrates
those callers to pass an engine directly.

`SourceDispatchError` is defined in `gibran.execution.engine` and
re-exported here for backward compatibility -- existing imports of
`from gibran._source_dispatch import SourceDispatchError` keep working.
"""
from __future__ import annotations

import duckdb

from gibran.execution.engine import DuckDBEngine, SourceDispatchError

__all__ = [
    "SourceDispatchError",
    "build_from_clause",
    "from_clause_for_source",
]


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
