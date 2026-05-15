"""Postgres execution engine (Phase 5A.2 + 5A.1b).

Implements the `ExecutionEngine` protocol for PostgreSQL. Scope:

  - `dialect` -> Dialect.POSTGRES so sqlglot parses / emits Postgres SQL
  - `file_scan_sql` for `duckdb_table` / `sql_view` source types (both
    map to `qident(uri)` -- "the uri IS the relation name", same as the
    DuckDB engine).
  - `file_scan_sql` for `parquet` / `csv` raises a clear `SourceDispatchError`
    explaining the limitation. Postgres has no `read_parquet()` /
    `read_csv()` equivalent in core; users would need a foreign-table
    setup (postgres_fdw, file_fdw) or the `pg_parquet` extension.
  - `execute` / `query` / `fetchone` / `commit` for actually running
    SQL against Postgres (added in 5A.1b).

Placeholder translation:
  Gibran's internal SQL uses `?` placeholders (DuckDB style). psycopg
  uses `%s` (format style) or `%(name)s` (pyformat). PostgresEngine
  translates `?` -> `%s` inside `execute` / `query` / `fetchone` so
  callers don't have to think about it. A small state machine skips
  quoted strings so a literal `?` inside `'foo?bar'` is preserved.

Transactions / commit:
  `connect(url)` opens the connection in autocommit mode so single-
  statement INSERTs (audit log, etc.) behave the same way they do on
  DuckDB. `engine.commit()` is therefore a no-op for typical use.
  Callers that need explicit transactions can pass their own
  non-autocommit connection and call `engine.commit()` after a batch.

Dependency:
  Requires `psycopg>=3.1` (installed via `pip install gibran[postgres]`).
  The import is lazy so that headless installs without the extras don't
  fail at import time -- `from gibran.execution.engines.postgres import
  PostgresEngine` works even without psycopg; the connection-side
  constructor raises a clear ImportError if psycopg is missing.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from gibran._sql import qident
from gibran.execution.dialect import Dialect
from gibran.execution.engine import SourceDispatchError

if TYPE_CHECKING:  # pragma: no cover -- typing-only import
    import psycopg


_PSYCOPG_INSTALL_HINT = (
    "PostgresEngine requires psycopg>=3.1. "
    "Install via `pip install gibran[postgres]` (recommended) or "
    "`pip install 'psycopg[binary]>=3.1'`."
)


class NoConnectionError(RuntimeError):
    """Engine was asked to execute SQL but holds no connection."""


def translate_qmark_placeholders(sql: str) -> str:
    """Translate DuckDB-style `?` placeholders to psycopg `%s`.

    Also doubles every literal `%` to `%%`. psycopg's parameter
    substitution treats `%` as a format directive and requires
    doubling regardless of whether the `%` appears inside a string
    literal -- a `LIKE 'a%b'` would otherwise be misparsed.

    Walks the SQL string with a tiny state machine that tracks whether
    we're inside a single-quoted string literal, a double-quoted
    identifier, or a `--` line comment. `?` is only replaced outside
    those scopes (a literal `?` in `WHERE name = 'who?'` is part of
    the value, not a placeholder).

    Limitations:
      - Doesn't handle dollar-quoted strings ($$ ... $$). Internal
        gibran SQL doesn't use them. If a future caller needs them,
        extend this function.
      - The `%%` doubling is applied EVERYWHERE (even inside comments)
        because we don't track whether the SQL will later be passed
        through psycopg's parameter substitution. Over-doubling in a
        comment is harmless.
    """
    out: list[str] = []
    i = 0
    n = len(sql)
    in_single = False
    in_double = False
    in_line_comment = False
    while i < n:
        c = sql[i]
        # `%` -> `%%` everywhere (psycopg parameter substitution).
        # Must be handled first so it applies inside string literals too.
        if c == "%":
            out.append("%%")
            i += 1
            continue
        # `?` -> `%s` ONLY outside literals / comments (a literal `?`
        # inside `'who?'` is part of the value, not a placeholder).
        if c == "?" and not (in_single or in_double or in_line_comment):
            out.append("%s")
            i += 1
            continue
        # Otherwise: track state and pass character through.
        if in_line_comment:
            out.append(c)
            if c == "\n":
                in_line_comment = False
        elif in_single:
            out.append(c)
            if c == "'":
                # `''` is an escaped single quote inside the literal,
                # not the literal's terminator.
                if i + 1 < n and sql[i + 1] == "'":
                    out.append("'")
                    i += 2
                    continue
                in_single = False
        elif in_double:
            out.append(c)
            if c == '"':
                in_double = False
        elif c == "'":
            out.append(c)
            in_single = True
        elif c == '"':
            out.append(c)
            in_double = True
        elif c == "-" and i + 1 < n and sql[i + 1] == "-":
            out.append("--")
            i += 2
            in_line_comment = True
            continue
        else:
            out.append(c)
        i += 1
    return "".join(out)


class PostgresEngine:
    """PostgreSQL execution engine. Wraps a `psycopg.Connection`.

    `file_scan_sql` is pure (works without a connection). The
    execute-side methods (`execute`, `query`, `fetchone`, `commit`)
    require a non-None connection and raise `NoConnectionError`
    otherwise.

    Constructor verifies psycopg is importable when a connection is
    passed, so a missing extras install fails with a clear hint at
    construction time rather than deep in a call chain.
    """

    dialect: Dialect = Dialect.POSTGRES

    def __init__(self, con: "psycopg.Connection | None" = None) -> None:
        if con is not None:
            try:
                import psycopg as _psycopg  # noqa: F401 -- import-side verification
            except ImportError as e:
                raise ImportError(_PSYCOPG_INSTALL_HINT) from e
        self.con = con

    # -- pure (no connection required) --------------------------------------

    def file_scan_sql(self, source_type: str, uri: str) -> str:
        if source_type in ("duckdb_table", "sql_view"):
            return qident(uri)
        if source_type == "parquet":
            raise SourceDispatchError(
                f"PostgresEngine does not support {source_type!r} sources. "
                f"Postgres has no native `read_parquet()`. Options: "
                f"(a) use a foreign-table setup (postgres_fdw / file_fdw); "
                f"(b) install the `pg_parquet` extension and expose the file "
                f"as a foreign table; "
                f"(c) load the parquet into a regular table and register it "
                f"as source_type='duckdb_table' (legacy name; means 'named "
                f"relational source')."
            )
        if source_type == "csv":
            raise SourceDispatchError(
                f"PostgresEngine does not support {source_type!r} sources. "
                f"Postgres has no native `read_csv()` for SELECT-time scans. "
                f"Options: "
                f"(a) `COPY` the CSV into a regular table and register it as "
                f"source_type='duckdb_table'; "
                f"(b) use `file_fdw` to expose the file as a foreign table."
            )
        raise SourceDispatchError(
            f"unrecognized source_type {source_type!r} for PostgresEngine "
            f"(expected one of duckdb_table / sql_view; "
            f"parquet / csv are not natively supported on Postgres)."
        )

    # -- execute-side (connection required) ---------------------------------

    def _require_con(self) -> Any:
        if self.con is None:
            raise NoConnectionError(
                "PostgresEngine has no connection; pass a psycopg.Connection "
                "to the constructor before calling execute / query / fetchone."
            )
        return self.con

    def execute(self, sql: str, params: tuple | list = ()) -> None:
        con = self._require_con()
        translated = translate_qmark_placeholders(sql)
        with con.cursor() as cur:
            cur.execute(translated, tuple(params) if params else None)

    def query(
        self, sql: str, params: tuple | list = ()
    ) -> tuple[list[tuple], list[str]]:
        con = self._require_con()
        translated = translate_qmark_placeholders(sql)
        with con.cursor() as cur:
            cur.execute(translated, tuple(params) if params else None)
            rows = cur.fetchall()
            col_names = [d.name for d in cur.description] if cur.description else []
        return rows, col_names

    def fetchone(self, sql: str, params: tuple | list = ()) -> tuple | None:
        con = self._require_con()
        translated = translate_qmark_placeholders(sql)
        with con.cursor() as cur:
            cur.execute(translated, tuple(params) if params else None)
            return cur.fetchone()

    def commit(self) -> None:
        # If the connection is in autocommit mode (the default for
        # connections opened via `connect(url)` below), this is a no-op.
        # For non-autocommit connections passed in by callers, it
        # commits the current transaction.
        if self.con is None:
            return
        if getattr(self.con, "autocommit", False):
            return
        self.con.commit()


def connect(url: str) -> Any:
    """Convenience constructor: open a psycopg connection from a URL.

    Opens the connection in `autocommit=True` mode so single-statement
    writes (audit log INSERTs, schema migrations) behave the same way
    they do on DuckDB. Callers that want explicit transactions can
    construct their own connection with `autocommit=False` and pass it
    to `PostgresEngine(con=...)`.
    """
    try:
        import psycopg
    except ImportError as e:
        raise ImportError(_PSYCOPG_INSTALL_HINT) from e
    return psycopg.connect(url, autocommit=True)
