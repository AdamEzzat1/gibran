"""Phase 5A.1 / 5A.1b -- ExecutionEngine protocol + DuckDBEngine.

5A.1 scope: the DuckDB engine wraps the existing source-dispatch
behavior. This file pins that the engine produces byte-identical
FROM-clause fragments to what the pre-5A.1 hardcoded `_build_from`
produced, plus protocol conformance checks and backward-compat
re-exports of `SourceDispatchError`.

5A.1b scope: execute / query / fetchone / commit methods on DuckDBEngine.
PostgresEngine equivalents are tested in test_postgres_engine.py (unit)
and test_postgres_engine_integration.py (against a real Postgres).
"""
from __future__ import annotations

import pytest

from gibran.execution.dialect import Dialect
from gibran.execution.engine import (
    DuckDBEngine,
    ExecutionEngine,
    SourceDispatchError,
)


# ---------------------------------------------------------------------------
# DuckDBEngine.file_scan_sql -- per source_type
# ---------------------------------------------------------------------------


def test_duckdb_engine_dialect_attribute():
    engine = DuckDBEngine()
    assert engine.dialect is Dialect.DUCKDB


def test_file_scan_duckdb_table_returns_quoted_identifier():
    engine = DuckDBEngine()
    assert engine.file_scan_sql("duckdb_table", "orders") == '"orders"'


def test_file_scan_sql_view_returns_quoted_identifier():
    engine = DuckDBEngine()
    assert engine.file_scan_sql("sql_view", "v_recent_orders") == '"v_recent_orders"'


def test_file_scan_table_name_with_special_chars_is_quoted():
    """Names with underscores / hyphens / mixed case round-trip through
    qident. This is the property that broke before _source_dispatch
    started using qident -- a regression test pins it."""
    engine = DuckDBEngine()
    assert engine.file_scan_sql("duckdb_table", "Orders-East") == '"Orders-East"'


def test_file_scan_parquet_returns_read_parquet():
    engine = DuckDBEngine()
    out = engine.file_scan_sql("parquet", "data/orders.parquet")
    assert out == "read_parquet('data/orders.parquet')"


def test_file_scan_csv_returns_read_csv():
    engine = DuckDBEngine()
    out = engine.file_scan_sql("csv", "data/orders.csv")
    assert out == "read_csv('data/orders.csv')"


def test_file_scan_uri_with_single_quote_is_escaped():
    """Path with a single quote must be safely escaped (via render_literal)
    so a malicious path can't break out of the SQL string literal."""
    engine = DuckDBEngine()
    out = engine.file_scan_sql("parquet", "data/o'rders.parquet")
    # render_literal escapes ' to '' (SQL standard)
    assert "''" in out
    assert out.startswith("read_parquet('")
    assert out.endswith("')")


def test_file_scan_unknown_source_type_raises():
    engine = DuckDBEngine()
    with pytest.raises(SourceDispatchError) as exc:
        engine.file_scan_sql("postgres_table", "orders")
    msg = str(exc.value)
    assert "postgres_table" in msg
    assert "duckdb_table" in msg  # error lists valid options


# ---------------------------------------------------------------------------
# Connection handling
# ---------------------------------------------------------------------------


def test_engine_works_without_connection():
    """file_scan_sql is pure given (source_type, uri) -- no con needed.
    This is what the drift detector relies on at sync time, before any
    source is registered."""
    engine = DuckDBEngine(con=None)
    assert engine.file_scan_sql("parquet", "a.parquet") == "read_parquet('a.parquet')"


def test_engine_with_connection_stores_it():
    """When a connection is provided, the engine holds the reference for
    methods that will need it (added in 5A.1b)."""
    import duckdb
    con = duckdb.connect(":memory:")
    engine = DuckDBEngine(con=con)
    assert engine.con is con


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_duckdb_engine_satisfies_protocol():
    """Structural-subtyping check: DuckDBEngine implements the
    ExecutionEngine protocol so that downstream type-checking sees it
    as a valid engine implementation. Future engines (Postgres, etc.)
    will also need to pass this shape."""
    engine: ExecutionEngine = DuckDBEngine()
    assert hasattr(engine, "dialect")
    assert hasattr(engine, "file_scan_sql")
    assert callable(engine.file_scan_sql)


# ---------------------------------------------------------------------------
# Backward-compat: re-exports
# ---------------------------------------------------------------------------


def test_source_dispatch_error_reexported():
    """SourceDispatchError moved to gibran.execution.engine but is
    re-exported from gibran._source_dispatch so existing callers
    (sync/applier, sync/drift, sync/example_values, dsl/compile, and
    tests) keep working without import changes."""
    from gibran._source_dispatch import SourceDispatchError as ReExported
    from gibran.execution.engine import SourceDispatchError as Canonical
    assert ReExported is Canonical


def test_build_from_clause_delegates_to_engine():
    """The legacy `build_from_clause(source_type, uri)` function must
    produce identical output to `DuckDBEngine().file_scan_sql(...)` --
    that's the contract of the 5A.1 refactor (preserve behavior,
    change dispatch path).
    """
    from gibran._source_dispatch import build_from_clause

    engine = DuckDBEngine()
    for source_type, uri in [
        ("duckdb_table", "orders"),
        ("sql_view", "v_recent"),
        ("parquet", "data/x.parquet"),
        ("csv", "data/x.csv"),
    ]:
        assert build_from_clause(source_type, uri) == engine.file_scan_sql(
            source_type, uri
        )


# ---------------------------------------------------------------------------
# 5A.1b -- execute / query / fetchone / commit on DuckDBEngine
# ---------------------------------------------------------------------------


@pytest.fixture
def duck_engine():
    """An engine wrapping a fresh in-memory DuckDB with a small test table."""
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE t (id INTEGER, label VARCHAR)")
    con.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    return DuckDBEngine(con)


def test_query_returns_rows_and_columns(duck_engine):
    rows, cols = duck_engine.query("SELECT id, label FROM t ORDER BY id")
    assert rows == [(1, "a"), (2, "b"), (3, "c")]
    assert cols == ["id", "label"]


def test_query_with_params(duck_engine):
    rows, cols = duck_engine.query(
        "SELECT label FROM t WHERE id = ?", [2]
    )
    assert rows == [("b",)]
    assert cols == ["label"]


def test_query_empty_result_returns_columns_anyway(duck_engine):
    rows, cols = duck_engine.query("SELECT id, label FROM t WHERE id = 999")
    assert rows == []
    assert cols == ["id", "label"]


def test_fetchone_returns_single_row(duck_engine):
    row = duck_engine.fetchone("SELECT label FROM t WHERE id = ?", [1])
    assert row == ("a",)


def test_fetchone_returns_none_for_empty_result(duck_engine):
    assert duck_engine.fetchone("SELECT id FROM t WHERE id = 999") is None


def test_execute_runs_ddl(duck_engine):
    duck_engine.execute("CREATE TABLE other (x INTEGER)")
    duck_engine.execute("INSERT INTO other VALUES (?)", [42])
    rows, _ = duck_engine.query("SELECT x FROM other")
    assert rows == [(42,)]


def test_commit_is_noop_on_duckdb(duck_engine):
    """DuckDB auto-commits; engine.commit() should be a safe no-op so
    callers can write portable code that calls commit() without
    branching on dialect."""
    duck_engine.commit()  # should not raise
    # Subsequent reads still work
    rows, _ = duck_engine.query("SELECT COUNT(*) FROM t")
    assert rows == [(3,)]


def test_execute_methods_require_connection():
    """An engine constructed without a connection raises a clear error
    on execute / query / fetchone, not a deep AttributeError."""
    from gibran.execution.engines.duckdb import NoConnectionError

    engine = DuckDBEngine(con=None)
    with pytest.raises(NoConnectionError):
        engine.execute("SELECT 1")
    with pytest.raises(NoConnectionError):
        engine.query("SELECT 1")
    with pytest.raises(NoConnectionError):
        engine.fetchone("SELECT 1")


# ---------------------------------------------------------------------------
# 5A.1b -- run_sql_query accepts both connection and engine
# ---------------------------------------------------------------------------


def test_run_sql_query_accepts_engine_directly():
    """The entry point used to take `con: DuckDBPyConnection`; after
    5A.1b it accepts either. Pass an engine and verify the same result
    shape comes back. Reuses the analyst_west fixture from
    test_execution_sql to inherit a known-working governance config."""
    from tests.test_execution_sql import _make_db_with_orders_data, _ident

    from gibran.execution.sql import run_sql_query
    from gibran.governance.default import DefaultGovernance

    con = _make_db_with_orders_data()
    engine = DuckDBEngine(con)
    gov = DefaultGovernance(con)

    # Pass engine -- should work identically to passing con
    result = run_sql_query(
        engine, gov, _ident("analyst_west", region="west"),
        "SELECT order_id FROM orders",
    )
    assert result.status == "ok"
    # analyst_west sees only west rows: o1, o3
    order_ids = {row[0] for row in result.rows}
    assert order_ids == {"o1", "o3"}


def test_run_sql_query_still_accepts_raw_connection():
    """Backward-compat: old callers passing `con: DuckDBPyConnection`
    must keep working without changes."""
    from tests.test_execution_sql import _make_db_with_orders_data, _ident

    from gibran.execution.sql import run_sql_query
    from gibran.governance.default import DefaultGovernance

    con = _make_db_with_orders_data()
    gov = DefaultGovernance(con)

    # Pass raw con -- gets auto-wrapped in DuckDBEngine internally
    result = run_sql_query(
        con, gov, _ident("analyst_west", region="west"),
        "SELECT order_id FROM orders",
    )
    assert result.status == "ok"
    order_ids = {row[0] for row in result.rows}
    assert order_ids == {"o1", "o3"}


def test_run_sql_query_engine_and_con_produce_identical_results():
    """The two entry-point shapes (engine vs con) must produce the same
    QueryResult contents for the same logical query. This is the
    backward-compat invariant."""
    from tests.test_execution_sql import _make_db_with_orders_data, _ident

    from gibran.execution.sql import run_sql_query
    from gibran.governance.default import DefaultGovernance

    con = _make_db_with_orders_data()
    gov = DefaultGovernance(con)
    identity = _ident("analyst_west", region="west")

    sql = "SELECT order_id, amount FROM orders"
    result_con = run_sql_query(con, gov, identity, sql)
    result_eng = run_sql_query(DuckDBEngine(con), gov, identity, sql)

    assert result_con.status == result_eng.status == "ok"
    assert set(result_con.rows) == set(result_eng.rows)
    assert result_con.columns == result_eng.columns
