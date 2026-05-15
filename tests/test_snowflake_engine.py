"""Phase 5A.3 -- SnowflakeEngine unit tests.

Scope: pure code paths that don't require a real Snowflake account.
  - dialect attribute
  - file_scan_sql for each source_type (including the rejected
    parquet/csv cases that explain stage-based alternatives)
  - Constructor behavior with / without a connection
  - URL parser for `snowflake://...` strings
  - Protocol conformance

DEFERRED: actually executing SQL against a live Snowflake account.
That requires a paid Snowflake account and the snowflake-connector-python
extras (`pip install gibran[snowflake]`). The integration test stub in
`tests/test_snowflake_engine_integration.py` is gated on a credential
env var and skips by default -- it will never auto-run or incur cost
unless a developer explicitly opts in.

Trademark note: "Snowflake" appears here in nominative use. gibran is
not affiliated with or endorsed by Snowflake Inc.
"""
from __future__ import annotations

import pytest

from gibran.execution.dialect import Dialect
from gibran.execution.engine import ExecutionEngine, SourceDispatchError
from gibran.execution.engines.snowflake import (
    SnowflakeEngine,
    _parse_snowflake_url,
)


# ---------------------------------------------------------------------------
# dialect + file_scan_sql for supported source types
# ---------------------------------------------------------------------------


def test_snowflake_engine_dialect_is_snowflake():
    assert SnowflakeEngine().dialect is Dialect.SNOWFLAKE


def test_file_scan_duckdb_table_returns_quoted_identifier():
    """Snowflake uses the same double-quoted identifier syntax as
    DuckDB / Postgres. qident is dialect-neutral here."""
    engine = SnowflakeEngine()
    assert engine.file_scan_sql("duckdb_table", "ORDERS") == '"ORDERS"'


def test_file_scan_sql_view_returns_quoted_identifier():
    engine = SnowflakeEngine()
    assert engine.file_scan_sql("sql_view", "v_recent_orders") == '"v_recent_orders"'


def test_file_scan_table_name_case_preserved_by_quoting():
    """Snowflake folds unquoted identifiers to UPPERCASE; double-quoting
    preserves case as-given. Pin this so a future refactor doesn't
    accidentally drop the quoting."""
    engine = SnowflakeEngine()
    assert engine.file_scan_sql("duckdb_table", "MixedCase") == '"MixedCase"'


# ---------------------------------------------------------------------------
# file_scan_sql for unsupported source types
# ---------------------------------------------------------------------------


def test_file_scan_parquet_raises_with_stage_guidance():
    """Snowflake's file model is stage-based, not direct file scan. The
    error must explain the stage-based alternative so users have a
    clear next step."""
    engine = SnowflakeEngine()
    with pytest.raises(SourceDispatchError) as exc:
        engine.file_scan_sql("parquet", "data/x.parquet")
    msg = str(exc.value)
    assert "Snowflake" in msg
    assert "parquet" in msg
    # Stage-based workaround must be mentioned
    assert any(hint in msg.lower() for hint in ("stage", "@", "copy into"))


def test_file_scan_csv_raises_with_stage_guidance():
    engine = SnowflakeEngine()
    with pytest.raises(SourceDispatchError) as exc:
        engine.file_scan_sql("csv", "data/x.csv")
    msg = str(exc.value)
    assert "Snowflake" in msg
    assert any(hint in msg.lower() for hint in ("stage", "copy into", "put"))


def test_file_scan_unknown_source_type_raises():
    engine = SnowflakeEngine()
    with pytest.raises(SourceDispatchError) as exc:
        engine.file_scan_sql("bigquery_external", "proj.ds.tbl")
    msg = str(exc.value)
    assert "bigquery_external" in msg
    assert "SnowflakeEngine" in msg


# ---------------------------------------------------------------------------
# Construction without snowflake-connector-python installed
# ---------------------------------------------------------------------------


def test_snowflake_engine_constructible_without_connection():
    """Module must be importable AND the no-connection constructor must
    work, even on installs that don't have snowflake-connector-python.
    This is what lets tooling introspect available engines without
    forcing the [snowflake] extras (and its ~30MB of dependencies)."""
    engine = SnowflakeEngine()
    assert engine.con is None
    assert engine.dialect is Dialect.SNOWFLAKE


def test_snowflake_engine_with_connection_requires_connector(monkeypatch):
    """If a connection is passed but the connector module can't be
    imported, the constructor must fail with a clear install hint --
    not a deep, misleading ImportError at first use."""
    import builtins
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("snowflake"):
            raise ImportError("simulated missing snowflake-connector-python")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    class _Sentinel:
        pass

    with pytest.raises(ImportError) as exc:
        SnowflakeEngine(con=_Sentinel())  # type: ignore[arg-type]
    msg = str(exc.value).lower()
    assert "snowflake" in msg
    assert "gibran[snowflake]" in msg or "snowflake-connector-python" in msg


# ---------------------------------------------------------------------------
# URL parser
# ---------------------------------------------------------------------------


def test_parse_url_minimal():
    """`snowflake://user:pass@account` -> {user, password, account}."""
    out = _parse_snowflake_url("snowflake://alice:s3cret@acme-x")
    assert out == {"user": "alice", "password": "s3cret", "account": "acme-x"}


def test_parse_url_with_database_and_schema():
    out = _parse_snowflake_url(
        "snowflake://alice:s3cret@acme-x/analytics/public"
    )
    assert out["database"] == "analytics"
    assert out["schema"] == "public"


def test_parse_url_with_warehouse_and_role():
    out = _parse_snowflake_url(
        "snowflake://alice:s3cret@acme-x/analytics/public"
        "?warehouse=compute_wh&role=analyst"
    )
    assert out["warehouse"] == "compute_wh"
    assert out["role"] == "analyst"


def test_parse_url_wrong_scheme_raises():
    with pytest.raises(ValueError) as exc:
        _parse_snowflake_url("postgres://alice:p@host/db")
    assert "snowflake" in str(exc.value).lower()


# ---------------------------------------------------------------------------
# Protocol conformance + cross-engine invariants
# ---------------------------------------------------------------------------


def test_snowflake_engine_satisfies_protocol():
    engine: ExecutionEngine = SnowflakeEngine()
    assert hasattr(engine, "dialect")
    assert hasattr(engine, "file_scan_sql")
    assert callable(engine.file_scan_sql)


def test_all_engines_have_distinct_dialects():
    """Cache keys (5A.0) and dialect-aware emitters (5D) depend on
    every engine reporting a distinct Dialect value."""
    from gibran.execution.engines.duckdb import DuckDBEngine
    from gibran.execution.engines.postgres import PostgresEngine

    dialects = {
        DuckDBEngine().dialect,
        PostgresEngine().dialect,
        SnowflakeEngine().dialect,
    }
    assert len(dialects) == 3
