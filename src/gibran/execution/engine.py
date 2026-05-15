"""Execution-engine protocol + shared error class (Phase 5A.1 / 5A.2 / 5A.1b / 5A.3 / 5A.4).

Defines the seam through which dialect-specific SQL behaviors (file
scans, schema introspection, query execution) get dispatched. Engine
implementations live in `gibran.execution.engines.*`:

  - `engines.duckdb.DuckDBEngine`        (V0.1 default; Phase 5A.1)
  - `engines.postgres.PostgresEngine`    (Phase 5A.2)
  - `engines.snowflake.SnowflakeEngine`  (Phase 5A.3 -- adapter code only;
                                          end-to-end verification deferred
                                          pending paid Snowflake account)
  - `engines.bigquery.BigQueryEngine`    (Phase 5A.4 -- adapter code only;
                                          end-to-end verification deferred
                                          pending GCP project)

This module re-exports the engine classes so existing callers
(`from gibran.execution.engine import DuckDBEngine`) continue to work
across the split. New code is encouraged to import from the per-engine
modules directly.

Protocol surface as of 5A.1b:
  - `dialect`              — which SQL dialect this engine targets
  - `file_scan_sql`        — render a FROM-clause fragment (5A.1)
  - `execute`              — run a statement, no result expected (5A.1b)
  - `query`                — run a SELECT, return (rows, column_names) (5A.1b)
  - `fetchone`             — run a SELECT expecting at most one row (5A.1b)
  - `commit`               — commit pending writes (no-op on auto-commit
                             engines like DuckDB; real on Postgres unless
                             the connection is in autocommit mode) (5A.1b)

All execute-side methods use DuckDB-style `?` placeholders. Engines that
use a different placeholder style (e.g. psycopg's `%s`) translate
internally so callers don't have to.
"""
from __future__ import annotations

from typing import Protocol

from gibran.execution.dialect import Dialect


class SourceDispatchError(ValueError):
    """Source could not be resolved to a FROM-clause fragment.

    Raised when a `source_id` is not registered, or when an engine
    doesn't support the requested `source_type` (e.g. PostgresEngine
    asked to scan a parquet file without `pg_parquet` installed).

    Re-exported from `gibran._source_dispatch` for backward
    compatibility with existing callers.
    """


class ExecutionEngine(Protocol):
    """The cross-dialect surface gibran needs from its execution backend.

    Engines are constructed around an optional connection (None is fine
    for the pure operations like `file_scan_sql`; required for the
    execute / query / fetchone methods).
    """

    @property
    def dialect(self) -> Dialect: ...

    def file_scan_sql(self, source_type: str, uri: str) -> str:
        """Return a SQL fragment that scans `uri` as a `source_type`.

        Result drops directly into a FROM clause. Raises
        `SourceDispatchError` if the engine doesn't support the
        requested source type."""
        ...

    def execute(self, sql: str, params: tuple | list = ()) -> None:
        """Run a statement that produces no result (DDL, INSERT, etc.).

        Placeholders use `?` regardless of engine. Engines whose driver
        uses a different style translate internally."""
        ...

    def query(
        self, sql: str, params: tuple | list = ()
    ) -> tuple[list[tuple], list[str]]:
        """Run a SELECT and return (rows, column_names).

        For SELECTs that return zero rows, the first element is `[]` and
        the second still carries the column names from the query plan.
        """
        ...

    def fetchone(self, sql: str, params: tuple | list = ()) -> tuple | None:
        """Run a SELECT expecting at most one row. Returns None when the
        query produced no rows."""
        ...

    def commit(self) -> None:
        """Commit pending writes. No-op for engines whose connection is
        in auto-commit mode (DuckDB by default; Postgres when
        `autocommit=True`)."""
        ...


# ---------------------------------------------------------------------------
# Re-exports -- preserve `from gibran.execution.engine import DuckDBEngine`
# across the engine-module split. New code should prefer importing from
# `gibran.execution.engines.duckdb` / `.postgres` directly.
#
# We use PEP 562 `__getattr__` so engine modules load LAZILY on first
# access. Eager imports here caused a circular import: each engine
# module imports `SourceDispatchError` from this file, and re-exporting
# the engines back here at import time meant Python tried to load each
# engine before this module had finished initializing.
# ---------------------------------------------------------------------------

_ENGINE_REEXPORTS = {
    "DuckDBEngine": ("gibran.execution.engines.duckdb", "DuckDBEngine"),
    "PostgresEngine": ("gibran.execution.engines.postgres", "PostgresEngine"),
    "SnowflakeEngine": ("gibran.execution.engines.snowflake", "SnowflakeEngine"),
    "BigQueryEngine": ("gibran.execution.engines.bigquery", "BigQueryEngine"),
}


def __getattr__(name: str):
    if name in _ENGINE_REEXPORTS:
        import importlib
        module_path, attr = _ENGINE_REEXPORTS[name]
        module = importlib.import_module(module_path)
        return getattr(module, attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "BigQueryEngine",
    "Dialect",
    "DuckDBEngine",
    "ExecutionEngine",
    "PostgresEngine",
    "SnowflakeEngine",
    "SourceDispatchError",
]
