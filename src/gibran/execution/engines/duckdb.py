"""DuckDB execution engine. The V0.1 default and only fully-supported engine.

Wraps an in-process `DuckDBPyConnection`. The connection is optional for
the pure-rendering method (`file_scan_sql`); methods that actually run
SQL against the database (`execute`, `query`, `fetchone`) require a
non-None connection and raise `NoConnectionError` otherwise.
"""
from __future__ import annotations

import duckdb

from gibran._sql import qident, render_literal
from gibran.execution.dialect import Dialect
from gibran.execution.engine import SourceDispatchError


class NoConnectionError(RuntimeError):
    """Engine was asked to execute SQL but holds no connection."""


class DuckDBEngine:
    """DuckDB execution engine.

    `file_scan_sql` is pure (works without a connection -- used by the
    drift detector at sync time before any source is registered).
    `execute` / `query` / `fetchone` / `commit` require a connection.

    DuckDB auto-commits by default, so `commit()` is a no-op.
    """

    dialect: Dialect = Dialect.DUCKDB

    def __init__(self, con: duckdb.DuckDBPyConnection | None = None) -> None:
        self.con = con

    # -- pure (no connection required) --------------------------------------

    def file_scan_sql(self, source_type: str, uri: str) -> str:
        if source_type in ("duckdb_table", "sql_view"):
            return qident(uri)
        if source_type == "parquet":
            return f"read_parquet({render_literal(uri)})"
        if source_type == "csv":
            return f"read_csv({render_literal(uri)})"
        raise SourceDispatchError(
            f"unrecognized source_type {source_type!r} for DuckDBEngine "
            f"(expected one of duckdb_table / sql_view / parquet / csv)"
        )

    # -- execute-side (connection required) ---------------------------------

    def _require_con(self) -> duckdb.DuckDBPyConnection:
        if self.con is None:
            raise NoConnectionError(
                "DuckDBEngine has no connection; pass a DuckDBPyConnection "
                "to the constructor before calling execute / query / fetchone."
            )
        return self.con

    def execute(self, sql: str, params: tuple | list = ()) -> None:
        con = self._require_con()
        if params:
            con.execute(sql, list(params))
        else:
            con.execute(sql)

    def query(
        self, sql: str, params: tuple | list = ()
    ) -> tuple[list[tuple], list[str]]:
        con = self._require_con()
        cur = con.execute(sql, list(params)) if params else con.execute(sql)
        rows = cur.fetchall()
        col_names = [d[0] for d in cur.description] if cur.description else []
        return rows, col_names

    def fetchone(self, sql: str, params: tuple | list = ()) -> tuple | None:
        con = self._require_con()
        cur = con.execute(sql, list(params)) if params else con.execute(sql)
        return cur.fetchone()

    def commit(self) -> None:
        # DuckDB auto-commits by default; commit() is a no-op so callers
        # can write `engine.commit()` portably without engine-aware
        # branching.
        pass
