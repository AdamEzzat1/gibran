"""Phase 5A.4 -- BigQueryEngine unit tests.

Scope: pure code paths that don't require a real GCP project.
  - dialect attribute
  - bqident (backtick quoting; refuses backtick-in-name)
  - file_scan_sql for each source_type (with the stage-equivalent
    guidance for the rejected parquet/csv cases)
  - Constructor behavior with / without a connection
  - Protocol conformance
  - Cross-engine invariants (4 distinct dialects)

DEFERRED: actually executing SQL against a live BigQuery project. That
requires a GCP project with billing enabled, the google-cloud-bigquery
extras (`pip install gibran[bigquery]`), and Application Default
Credentials configured. The integration test stub in
`tests/test_bigquery_engine_integration.py` is gated on an env var and
skips by default -- it will never auto-run or incur cost unless a
developer explicitly opts in.

Trademark note: "BigQuery" is a trademark of Google LLC. gibran is not
affiliated with or endorsed by Google LLC.
"""
from __future__ import annotations

import pytest

from gibran.execution.dialect import Dialect
from gibran.execution.engine import ExecutionEngine, SourceDispatchError
from gibran.execution.engines.bigquery import BigQueryEngine, bqident


# ---------------------------------------------------------------------------
# bqident helper
# ---------------------------------------------------------------------------


def test_bqident_quotes_simple_name():
    assert bqident("orders") == "`orders`"


def test_bqident_quotes_three_part_name():
    """BigQuery accepts a single backtick-quoted three-part name."""
    assert bqident("my-project.analytics.orders") == "`my-project.analytics.orders`"


def test_bqident_quotes_mixed_case_preserved():
    """Backtick quoting preserves case as-given (BigQuery's identifiers
    are case-sensitive when quoted)."""
    assert bqident("MyTable") == "`MyTable`"


def test_bqident_refuses_backtick_in_name():
    """Backticks in identifier names would be the injection vector --
    same posture as the shared qident's double-quote rejection."""
    with pytest.raises(ValueError) as exc:
        bqident("evil`name")
    assert "backtick" in str(exc.value).lower()


# ---------------------------------------------------------------------------
# dialect + file_scan_sql for supported source types
# ---------------------------------------------------------------------------


def test_bigquery_engine_dialect_is_bigquery():
    assert BigQueryEngine().dialect is Dialect.BIGQUERY


def test_file_scan_duckdb_table_uses_backticks():
    """BigQuery requires backticks, NOT double-quotes. If a future
    refactor wires file_scan_sql to the shared `qident`, this test
    would fail loudly."""
    engine = BigQueryEngine()
    assert engine.file_scan_sql("duckdb_table", "orders") == "`orders`"


def test_file_scan_sql_view_uses_backticks():
    engine = BigQueryEngine()
    assert engine.file_scan_sql("sql_view", "v_recent") == "`v_recent`"


def test_file_scan_three_part_name():
    """`project.dataset.table` is a valid bare uri for BigQuery sources.
    Backtick-quoted as one unit (BigQuery accepts that form)."""
    engine = BigQueryEngine()
    out = engine.file_scan_sql("duckdb_table", "my-proj.analytics.orders")
    assert out == "`my-proj.analytics.orders`"


# ---------------------------------------------------------------------------
# file_scan_sql for unsupported source types
# ---------------------------------------------------------------------------


def test_file_scan_parquet_raises_with_external_table_guidance():
    """BigQuery's file model is external-table-based. Error must
    suggest BigLake / external tables / bq load."""
    engine = BigQueryEngine()
    with pytest.raises(SourceDispatchError) as exc:
        engine.file_scan_sql("parquet", "gs://bucket/x.parquet")
    msg = str(exc.value)
    assert "BigQueryEngine" in msg
    assert "parquet" in msg
    assert any(
        hint in msg.lower()
        for hint in ("external", "biglake", "bq load", "federated")
    )


def test_file_scan_csv_raises_with_load_guidance():
    engine = BigQueryEngine()
    with pytest.raises(SourceDispatchError) as exc:
        engine.file_scan_sql("csv", "gs://bucket/x.csv")
    msg = str(exc.value)
    assert "BigQueryEngine" in msg
    assert any(
        hint in msg.lower()
        for hint in ("external", "bq load", "biglake")
    )


def test_file_scan_unknown_source_type_raises():
    engine = BigQueryEngine()
    with pytest.raises(SourceDispatchError) as exc:
        engine.file_scan_sql("snowflake_stage", "@s/x.parquet")
    assert "snowflake_stage" in str(exc.value)
    assert "BigQueryEngine" in str(exc.value)


# ---------------------------------------------------------------------------
# Construction without google-cloud-bigquery installed
# ---------------------------------------------------------------------------


def test_bigquery_engine_constructible_without_connection():
    """Module importable + no-connection constructor works without the
    [bigquery] extras. Lets tooling introspect engines without forcing
    the (heavy) google-cloud SDK."""
    engine = BigQueryEngine()
    assert engine.con is None
    assert engine.dialect is Dialect.BIGQUERY


def test_bigquery_engine_with_connection_requires_sdk(monkeypatch):
    """Passing a connection while the SDK is missing raises a clear
    ImportError with install hint, not a deep AttributeError."""
    import builtins
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("google"):
            raise ImportError("simulated missing google-cloud-bigquery")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    class _Sentinel:
        pass

    with pytest.raises(ImportError) as exc:
        BigQueryEngine(con=_Sentinel())  # type: ignore[arg-type]
    msg = str(exc.value).lower()
    assert "bigquery" in msg or "google" in msg
    assert "gibran[bigquery]" in str(exc.value) or "google-cloud-bigquery" in str(exc.value)


# ---------------------------------------------------------------------------
# Protocol + cross-engine invariants
# ---------------------------------------------------------------------------


def test_bigquery_engine_satisfies_protocol():
    engine: ExecutionEngine = BigQueryEngine()
    assert hasattr(engine, "dialect")
    assert hasattr(engine, "file_scan_sql")
    assert callable(engine.file_scan_sql)


def test_all_four_engines_have_distinct_dialects():
    """With BigQuery added, gibran has four engines -- each must
    report a distinct Dialect for cache keys / dialect-aware emitters
    to function correctly."""
    from gibran.execution.engines.duckdb import DuckDBEngine
    from gibran.execution.engines.postgres import PostgresEngine
    from gibran.execution.engines.snowflake import SnowflakeEngine

    dialects = {
        DuckDBEngine().dialect,
        PostgresEngine().dialect,
        SnowflakeEngine().dialect,
        BigQueryEngine().dialect,
    }
    assert len(dialects) == 4


def test_bigquery_diverges_from_other_engines_on_identifier_quoting():
    """The key behavioral difference: DuckDB / Postgres / Snowflake use
    double-quoted identifiers; BigQuery uses backticks. Pinning this
    divergence prevents a future refactor from accidentally unifying
    the quoting style and breaking BigQuery."""
    from gibran.execution.engines.duckdb import DuckDBEngine
    from gibran.execution.engines.postgres import PostgresEngine
    from gibran.execution.engines.snowflake import SnowflakeEngine

    bq_out = BigQueryEngine().file_scan_sql("duckdb_table", "orders")
    assert bq_out == "`orders`"

    for engine in (DuckDBEngine(), PostgresEngine(), SnowflakeEngine()):
        assert engine.file_scan_sql("duckdb_table", "orders") == '"orders"'
