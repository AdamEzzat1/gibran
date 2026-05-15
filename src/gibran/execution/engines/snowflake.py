"""Snowflake execution engine (Phase 5A.3).

Implements the `ExecutionEngine` protocol for Snowflake. Scope:

  - `dialect` -> Dialect.SNOWFLAKE so sqlglot parses / emits Snowflake SQL
  - `file_scan_sql` for `duckdb_table` / `sql_view` -> qident (double-quote
    identifiers; Snowflake accepts the same syntax DuckDB / Postgres use).
  - `file_scan_sql` for `parquet` / `csv` raises `SourceDispatchError`
    suggesting Snowflake stages as the workaround.
  - `execute` / `query` / `fetchone` / `commit` against a
    snowflake.connector.SnowflakeConnection.

Placeholder handling:
  The Snowflake connector supports `qmark` paramstyle natively (set
  `paramstyle = 'qmark'` at the connector level). gibran's SQL uses `?`
  everywhere, so we set paramstyle=qmark in `connect()` and no
  translation is needed. Callers that pass their own connection with a
  different paramstyle would need to handle translation themselves.

Identifier quoting:
  Snowflake uses double-quotes for identifiers (same as DuckDB and
  Postgres). The shared `qident` helper from gibran._sql works without
  modification.

Dependency:
  Requires `snowflake-connector-python>=3.0` (Apache 2.0 licensed,
  installed via `pip install gibran[snowflake]`). Import is lazy so
  the module is importable without the extras installed -- the
  connection-side constructor raises a clear ImportError otherwise.

Trademark note: "Snowflake" is a trademark of Snowflake Inc. This
adapter is independent of Snowflake Inc. and uses the name
descriptively (per nominative-use principles). gibran does not imply
endorsement.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from gibran._sql import qident
from gibran.execution.dialect import Dialect
from gibran.execution.engine import SourceDispatchError

if TYPE_CHECKING:  # pragma: no cover -- typing-only import
    import snowflake.connector


_SNOWFLAKE_INSTALL_HINT = (
    "SnowflakeEngine requires snowflake-connector-python>=3.0. "
    "Install via `pip install gibran[snowflake]` (recommended) or "
    "`pip install snowflake-connector-python`."
)


class NoConnectionError(RuntimeError):
    """Engine was asked to execute SQL but holds no connection."""


class SnowflakeEngine:
    """Snowflake execution engine. Wraps a SnowflakeConnection.

    `file_scan_sql` is pure (works without a connection). Execute-side
    methods require a connection and raise `NoConnectionError` otherwise.
    """

    dialect: Dialect = Dialect.SNOWFLAKE

    def __init__(
        self, con: "snowflake.connector.SnowflakeConnection | None" = None
    ) -> None:
        if con is not None:
            try:
                import snowflake.connector as _sc  # noqa: F401 -- import verification
            except ImportError as e:
                raise ImportError(_SNOWFLAKE_INSTALL_HINT) from e
        self.con = con

    # -- pure (no connection required) --------------------------------------

    def file_scan_sql(self, source_type: str, uri: str) -> str:
        if source_type in ("duckdb_table", "sql_view"):
            # Snowflake uses the same double-quoted identifier syntax as
            # DuckDB / Postgres. qident is dialect-neutral here.
            return qident(uri)
        if source_type == "parquet":
            raise SourceDispatchError(
                f"SnowflakeEngine does not support {source_type!r} as a "
                f"direct file scan. Snowflake's file-scan model uses stages: "
                f"(a) create an external stage pointing at your file's "
                f"location (S3 / GCS / Azure / internal); "
                f"(b) reference via `@stage_name/file.parquet` and use "
                f"`COPY INTO` to load, or query directly with "
                f"`SELECT $1, $2 FROM @stage_name/file.parquet "
                f"(FILE_FORMAT => 'PARQUET_FORMAT')`; "
                f"(c) register the loaded table as source_type='duckdb_table' "
                f"(legacy name; means 'named relational source')."
            )
        if source_type == "csv":
            raise SourceDispatchError(
                f"SnowflakeEngine does not support {source_type!r} as a "
                f"direct file scan. Snowflake stage-based options: "
                f"(a) `PUT` the CSV to an internal stage, then "
                f"`COPY INTO` a regular table; "
                f"(b) use an external stage + `COPY INTO`; "
                f"(c) register the loaded table as source_type='duckdb_table'."
            )
        raise SourceDispatchError(
            f"unrecognized source_type {source_type!r} for SnowflakeEngine "
            f"(expected one of duckdb_table / sql_view; "
            f"parquet / csv require stage-based loading -- see above)."
        )

    # -- execute-side (connection required) ---------------------------------

    def _require_con(self) -> Any:
        if self.con is None:
            raise NoConnectionError(
                "SnowflakeEngine has no connection; pass a "
                "snowflake.connector.SnowflakeConnection to the constructor "
                "before calling execute / query / fetchone."
            )
        return self.con

    def execute(self, sql: str, params: tuple | list = ()) -> None:
        con = self._require_con()
        cur = con.cursor()
        try:
            if params:
                cur.execute(sql, tuple(params))
            else:
                cur.execute(sql)
        finally:
            cur.close()

    def query(
        self, sql: str, params: tuple | list = ()
    ) -> tuple[list[tuple], list[str]]:
        con = self._require_con()
        cur = con.cursor()
        try:
            if params:
                cur.execute(sql, tuple(params))
            else:
                cur.execute(sql)
            rows = cur.fetchall()
            col_names = (
                [d[0] for d in cur.description] if cur.description else []
            )
        finally:
            cur.close()
        return rows, col_names

    def fetchone(self, sql: str, params: tuple | list = ()) -> tuple | None:
        con = self._require_con()
        cur = con.cursor()
        try:
            if params:
                cur.execute(sql, tuple(params))
            else:
                cur.execute(sql)
            return cur.fetchone()
        finally:
            cur.close()

    def commit(self) -> None:
        # Snowflake auto-commits by default for DDL/DML statements (each
        # statement is its own transaction unless wrapped in BEGIN/COMMIT).
        # Mirror DuckDB's "commit is a safe no-op" semantics; callers that
        # need explicit transactions can call con.commit() themselves.
        if self.con is None:
            return
        try:
            self.con.commit()
        except Exception:
            # Some Snowflake connection states don't allow explicit commit
            # (e.g. autocommit mode). Treat as no-op rather than surfacing
            # an error from the portable engine method.
            pass


def connect(url: str, **kwargs: Any) -> Any:
    """Open a Snowflake connection.

    `url` accepts the Snowflake-supported form:
        snowflake://user:pass@account/database/schema?warehouse=wh&role=r

    Additional connection kwargs (e.g. `private_key`, `authenticator`)
    can be passed via `**kwargs` and are forwarded to
    `snowflake.connector.connect()`.

    Sets `paramstyle='qmark'` on the connector module so the `?`
    placeholders gibran uses internally work without translation.
    """
    try:
        import snowflake.connector
    except ImportError as e:
        raise ImportError(_SNOWFLAKE_INSTALL_HINT) from e
    # Set qmark paramstyle at the module level so `?` placeholders work.
    # This is a module-wide setting in the snowflake connector; it
    # affects all connections in this process.
    snowflake.connector.paramstyle = "qmark"
    parsed = _parse_snowflake_url(url)
    parsed.update(kwargs)
    return snowflake.connector.connect(**parsed)


def _parse_snowflake_url(url: str) -> dict[str, str]:
    """Parse `snowflake://user:pass@account/database/schema?warehouse=wh&role=r`
    into the kwargs that `snowflake.connector.connect()` expects.

    Minimal parser -- handles the common shape but doesn't try to be a
    full URL parser. For advanced auth (private keys, OAuth), callers
    should open the connection themselves and pass it to
    `SnowflakeEngine(con=...)`.
    """
    from urllib.parse import urlparse, parse_qs

    p = urlparse(url)
    if p.scheme != "snowflake":
        raise ValueError(
            f"expected snowflake:// URL, got scheme {p.scheme!r}"
        )
    out: dict[str, str] = {}
    if p.username:
        out["user"] = p.username
    if p.password:
        out["password"] = p.password
    if p.hostname:
        out["account"] = p.hostname
    # Path is like '/database/schema' -- split on '/'.
    parts = [seg for seg in p.path.split("/") if seg]
    if len(parts) >= 1:
        out["database"] = parts[0]
    if len(parts) >= 2:
        out["schema"] = parts[1]
    # Query params: warehouse, role
    for key, vals in parse_qs(p.query).items():
        if vals:
            out[key] = vals[0]
    return out
