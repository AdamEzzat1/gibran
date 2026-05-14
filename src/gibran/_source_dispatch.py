"""Source-type dispatch: turn a registered `source_id` into the FROM-clause
snippet for that source.

V1 assumption was "source_id == DuckDB relation name" -- which only works
for `duckdb_table` and `sql_view` source types. For parquet/csv sources
users had to manually `CREATE VIEW orders AS SELECT * FROM 'path.parquet'`
before any `gibran check` or DSL query would work.

This helper looks up the source's `source_type` + `uri` and returns the
appropriate FROM-clause fragment:

  duckdb_table  ->  "<uri>"                 (quoted identifier)
  sql_view      ->  "<uri>"                 (quoted identifier)
  parquet       ->  read_parquet('<uri>')   (file-scan)
  csv           ->  read_csv('<uri>')       (file-scan)

The returned string is ready to drop into `FROM <here>` -- callers do not
need to wrap it in additional quotes / parens.

Used by both `observability/runner.py` (the quality + freshness rule
evaluators) and `dsl/compile.py` (the FROM clause emitter), so the two
paths share one mapping and can't diverge.
"""
from __future__ import annotations

import duckdb

from gibran._sql import qident, render_literal


class SourceDispatchError(ValueError):
    pass


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
    return _build_from(source_type, uri)


def _build_from(source_type: str, uri: str) -> str:
    if source_type in ("duckdb_table", "sql_view"):
        # For relational sources, the uri IS the relation name. Quote as an
        # identifier (handles names with underscores, hyphens, mixed case).
        return qident(uri)
    if source_type == "parquet":
        return f"read_parquet({render_literal(uri)})"
    if source_type == "csv":
        # `header=true, auto_detect=true` are DuckDB defaults; we let it
        # infer types. Users with non-standard CSVs should register a
        # `sql_view` source instead.
        return f"read_csv({render_literal(uri)})"
    raise SourceDispatchError(
        f"unrecognized source_type {source_type!r} (expected one of "
        f"duckdb_table / sql_view / parquet / csv)"
    )
