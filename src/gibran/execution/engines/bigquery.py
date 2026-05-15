"""BigQuery execution engine (Phase 5A.4).

Implements the `ExecutionEngine` protocol for Google BigQuery. Scope:

  - `dialect` -> Dialect.BIGQUERY so sqlglot parses / emits BigQuery SQL
  - `file_scan_sql` for `duckdb_table` / `sql_view` uses BACKTICK
    identifier quoting (BigQuery's syntax; double-quotes don't work
    for BQ identifiers).
  - `file_scan_sql` for `parquet` / `csv` raises `SourceDispatchError`
    suggesting external tables or `bq load` as the workaround.
  - `execute` / `query` / `fetchone` / `commit` via BigQuery's
    db-api wrapper (`google.cloud.bigquery.dbapi`).

Identifier quoting:
  BigQuery uses BACKTICKS for identifiers, not double-quotes. Three-part
  names like `project.dataset.table` are supported via single
  backtick-quoted form (`` `project.dataset.table` ``).

  IMPORTANT: gibran's shared `_sql.qident` returns double-quoted
  identifiers, which is what every other engine uses. The COMPILE PATH
  in `dsl/compile.py` and `governance/ast.py` still emits double-quoted
  identifiers via qident. Until those paths are made dialect-aware
  (see 5D follow-up), running DSL queries through a BigQueryEngine
  will produce SQL that BigQuery rejects. Source-dispatch (this engine's
  `file_scan_sql`) IS dialect-correct -- it's the rest of the compile
  path that's deferred.

Placeholder handling:
  BigQuery's db-api uses `pyformat` paramstyle (`%s` for positional,
  `%(name)s` for named). gibran's SQL uses `?` placeholders, so we
  translate via `translate_qmark_placeholders` (reused from the
  Postgres engine -- same translation rules apply since psycopg and
  BQ-dbapi both use pyformat).

Dependency:
  Requires `google-cloud-bigquery>=3.0` (Apache 2.0 licensed, installed
  via `pip install gibran[bigquery]`). Import is lazy so the module
  is importable without the extras installed -- the connection-side
  constructor raises a clear ImportError otherwise.

Cost note: BigQuery charges per byte scanned. Running gibran against
BigQuery without thinking about WHERE-clause partition pruning, column
selection (no SELECT *), and result-set limits can rack up bills fast.
The existing gibran guardrails (column-level governance forces explicit
column enumeration; intent.limit caps result-set size) help but don't
prevent expensive full-table scans.

Trademark note: "BigQuery" is a trademark of Google LLC. This adapter
is independent of Google LLC and uses the name descriptively (per
nominative-use principles). gibran does not imply endorsement.

DEFERRED to follow-ups:
  - Dialect-aware identifier quoting in compile/AST paths (5D)
  - Dataset/project resolution from `gibran init --engine bigquery://...`
  - Cost-estimation pre-flight (BigQuery's `dry_run` mode lets you
    estimate scan bytes before charging)
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from gibran.execution.dialect import Dialect
from gibran.execution.engine import SourceDispatchError
from gibran.execution.engines.postgres import translate_qmark_placeholders

if TYPE_CHECKING:  # pragma: no cover -- typing-only import
    import google.cloud.bigquery


_BIGQUERY_INSTALL_HINT = (
    "BigQueryEngine requires google-cloud-bigquery>=3.0. "
    "Install via `pip install gibran[bigquery]` (recommended) or "
    "`pip install google-cloud-bigquery`."
)


class NoConnectionError(RuntimeError):
    """Engine was asked to execute SQL but holds no connection."""


def bqident(name: str) -> str:
    """Quote a BigQuery identifier using backtick syntax.

    Refuses identifiers containing a backtick (BQ's escape character)
    to eliminate the injection-via-identifier vector -- same posture
    as the shared `qident` for double-quote dialects.

    Three-part names (`project.dataset.table`) are accepted as-is in
    a single backtick-quoted form, which BigQuery supports. If a
    caller wants per-segment quoting (`` `project`.`dataset`.`table` ``)
    they can split + bqident each segment + join with '.'.
    """
    if "`" in name:
        raise ValueError(f"identifier contains backtick: {name!r}")
    return f"`{name}`"


class BigQueryEngine:
    """BigQuery execution engine. Wraps a `google.cloud.bigquery.dbapi`
    connection (which itself wraps a `bigquery.Client`).

    `file_scan_sql` is pure (works without a connection). Execute-side
    methods require a connection and raise `NoConnectionError`
    otherwise.
    """

    dialect: Dialect = Dialect.BIGQUERY

    def __init__(
        self, con: "google.cloud.bigquery.dbapi.Connection | None" = None
    ) -> None:
        if con is not None:
            try:
                import google.cloud.bigquery  # noqa: F401 -- import verification
            except ImportError as e:
                raise ImportError(_BIGQUERY_INSTALL_HINT) from e
        self.con = con

    # -- pure (no connection required) --------------------------------------

    def file_scan_sql(self, source_type: str, uri: str) -> str:
        if source_type in ("duckdb_table", "sql_view"):
            # BigQuery uses backticks. The uri may be a bare table name
            # ('orders') or a three-part name ('project.dataset.orders');
            # bqident accepts either form.
            return bqident(uri)
        if source_type == "parquet":
            raise SourceDispatchError(
                f"BigQueryEngine does not support {source_type!r} as a "
                f"direct file scan. BigQuery's file-scan model uses "
                f"external tables or load jobs: "
                f"(a) create a BigLake / external table pointing at the "
                f"file's GCS location, then reference the external "
                f"table as source_type='duckdb_table'; "
                f"(b) `bq load` the file into a regular table; "
                f"(c) for ad-hoc scans, use BigQuery's federated query "
                f"capability on a GCS URI (limited; document carefully)."
            )
        if source_type == "csv":
            raise SourceDispatchError(
                f"BigQueryEngine does not support {source_type!r} as a "
                f"direct file scan. Options: "
                f"(a) create a BigLake / external table over the CSV "
                f"in GCS, register as source_type='duckdb_table'; "
                f"(b) `bq load --source_format=CSV` into a regular table."
            )
        raise SourceDispatchError(
            f"unrecognized source_type {source_type!r} for BigQueryEngine "
            f"(expected one of duckdb_table / sql_view; "
            f"parquet / csv require external-table or load-job setup)."
        )

    # -- execute-side (connection required) ---------------------------------

    def _require_con(self) -> Any:
        if self.con is None:
            raise NoConnectionError(
                "BigQueryEngine has no connection; pass a "
                "google.cloud.bigquery.dbapi.Connection to the constructor "
                "before calling execute / query / fetchone."
            )
        return self.con

    def execute(self, sql: str, params: tuple | list = ()) -> None:
        con = self._require_con()
        translated = translate_qmark_placeholders(sql)
        cur = con.cursor()
        try:
            if params:
                cur.execute(translated, tuple(params))
            else:
                cur.execute(translated)
        finally:
            cur.close()

    def query(
        self, sql: str, params: tuple | list = ()
    ) -> tuple[list[tuple], list[str]]:
        con = self._require_con()
        translated = translate_qmark_placeholders(sql)
        cur = con.cursor()
        try:
            if params:
                cur.execute(translated, tuple(params))
            else:
                cur.execute(translated)
            rows = list(cur.fetchall())
            # BigQuery dbapi cursor.description is a list of Column
            # objects; use .name for the column label.
            col_names = (
                [d.name for d in cur.description] if cur.description else []
            )
        finally:
            cur.close()
        return rows, col_names

    def fetchone(self, sql: str, params: tuple | list = ()) -> tuple | None:
        con = self._require_con()
        translated = translate_qmark_placeholders(sql)
        cur = con.cursor()
        try:
            if params:
                cur.execute(translated, tuple(params))
            else:
                cur.execute(translated)
            row = cur.fetchone()
            return tuple(row) if row is not None else None
        finally:
            cur.close()

    def commit(self) -> None:
        # BigQuery is a query-as-a-service model; there are no client-side
        # transactions in the SQL-OLTP sense. Each query is its own job.
        # The dbapi connection's commit() is a no-op in practice; we
        # mirror that for portable callers.
        pass


def connect(project: str | None = None, **client_kwargs: Any) -> Any:
    """Open a BigQuery dbapi connection.

    `project` is the GCP project ID for billing; if None, the BigQuery
    client picks it up from `GOOGLE_CLOUD_PROJECT` / Application Default
    Credentials. `client_kwargs` are forwarded to `bigquery.Client()`
    (e.g. `credentials=`, `location=`).
    """
    try:
        from google.cloud import bigquery
        from google.cloud.bigquery import dbapi
    except ImportError as e:
        raise ImportError(_BIGQUERY_INSTALL_HINT) from e
    client = bigquery.Client(project=project, **client_kwargs)
    return dbapi.Connection(client=client)
