"""Phase 5A.2 -- PostgresEngine unit tests.

These tests cover the pure-function side of PostgresEngine: dialect
attribute, file_scan_sql per source_type (including the rejected
parquet / csv cases), error message helpfulness, and protocol
conformance. None of these require a running Postgres -- they're safe
to run in any environment.

Integration tests against a real Postgres are in
`tests/test_postgres_engine_integration.py`, which skip gracefully
when psycopg isn't installed or `GIBRAN_POSTGRES_URL` isn't set.
"""
from __future__ import annotations

import pytest

from gibran.execution.dialect import Dialect
from gibran.execution.engine import (
    ExecutionEngine,
    SourceDispatchError,
)
from gibran.execution.engines.postgres import PostgresEngine


# ---------------------------------------------------------------------------
# dialect + file_scan_sql for supported source types
# ---------------------------------------------------------------------------


def test_postgres_engine_dialect_is_postgres():
    assert PostgresEngine().dialect is Dialect.POSTGRES


def test_file_scan_duckdb_table_returns_quoted_identifier():
    """The legacy `duckdb_table` source_type means 'named relational
    source' -- PostgresEngine accepts it and returns a quoted identifier
    (same shape as DuckDB; qident is dialect-neutral)."""
    engine = PostgresEngine()
    assert engine.file_scan_sql("duckdb_table", "orders") == '"orders"'


def test_file_scan_sql_view_returns_quoted_identifier():
    engine = PostgresEngine()
    assert engine.file_scan_sql("sql_view", "v_recent_orders") == '"v_recent_orders"'


def test_file_scan_table_name_with_special_chars_is_quoted():
    """Pin that qident handles mixed-case / hyphenated names correctly
    on the Postgres path too."""
    engine = PostgresEngine()
    assert engine.file_scan_sql("duckdb_table", "Orders-East") == '"Orders-East"'


# ---------------------------------------------------------------------------
# file_scan_sql for unsupported source types (parquet / csv)
# ---------------------------------------------------------------------------


def test_file_scan_parquet_raises_with_helpful_message():
    """Parquet isn't supported on Postgres natively. The error must
    explain the limitation AND list the user's escape hatches -- this is
    the gibran style: when you can't do what was asked, say what they
    can do instead."""
    engine = PostgresEngine()
    with pytest.raises(SourceDispatchError) as exc:
        engine.file_scan_sql("parquet", "data/x.parquet")
    msg = str(exc.value)
    assert "PostgresEngine" in msg
    assert "parquet" in msg
    # The error must mention at least one workaround so the user has a
    # clear next step.
    assert any(
        hint in msg.lower()
        for hint in ("foreign", "fdw", "pg_parquet", "load")
    )


def test_file_scan_csv_raises_with_helpful_message():
    engine = PostgresEngine()
    with pytest.raises(SourceDispatchError) as exc:
        engine.file_scan_sql("csv", "data/x.csv")
    msg = str(exc.value)
    assert "PostgresEngine" in msg
    assert "csv" in msg
    # COPY or file_fdw should be suggested.
    assert any(hint in msg.lower() for hint in ("copy", "fdw"))


def test_file_scan_unknown_source_type_raises():
    engine = PostgresEngine()
    with pytest.raises(SourceDispatchError) as exc:
        engine.file_scan_sql("snowflake_stage", "@my_stage/x.parquet")
    msg = str(exc.value)
    assert "snowflake_stage" in msg
    assert "PostgresEngine" in msg


# ---------------------------------------------------------------------------
# Construction without psycopg installed
# ---------------------------------------------------------------------------


def test_postgres_engine_constructible_without_connection():
    """The module must be importable AND the no-connection constructor
    must work, even on installs that don't have psycopg. This is what
    lets tooling introspect available engines without forcing the
    [postgres] extras on every user."""
    engine = PostgresEngine()
    assert engine.con is None
    assert engine.dialect is Dialect.POSTGRES


def test_postgres_engine_with_connection_requires_psycopg(monkeypatch):
    """If a connection is passed but psycopg can't be imported, the
    constructor must fail with a clear install hint -- not a deep,
    misleading ImportError at first use."""
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "psycopg":
            raise ImportError("simulated missing psycopg")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    # Use a sentinel object that quacks like a connection (we never touch it)
    class _Sentinel:
        pass

    with pytest.raises(ImportError) as exc:
        PostgresEngine(con=_Sentinel())  # type: ignore[arg-type]
    msg = str(exc.value)
    assert "psycopg" in msg.lower()
    # Install hint must mention either the extras group or pip install
    assert "gibran[postgres]" in msg or "psycopg" in msg


# ---------------------------------------------------------------------------
# Protocol conformance + interaction with DuckDBEngine
# ---------------------------------------------------------------------------


def test_postgres_engine_satisfies_protocol():
    engine: ExecutionEngine = PostgresEngine()
    assert hasattr(engine, "dialect")
    assert hasattr(engine, "file_scan_sql")
    assert callable(engine.file_scan_sql)


def test_two_engines_have_distinct_dialects():
    """Sanity: DuckDBEngine and PostgresEngine report different dialects.
    Cache keys (5A.0) and dialect-aware emitters (5D) depend on this."""
    from gibran.execution.engines.duckdb import DuckDBEngine

    assert DuckDBEngine().dialect is Dialect.DUCKDB
    assert PostgresEngine().dialect is Dialect.POSTGRES
    assert DuckDBEngine().dialect is not PostgresEngine().dialect


def test_engines_diverge_on_parquet():
    """The key behavioral difference: DuckDB renders parquet as a
    file-scan; Postgres rejects it. Pinning this divergence prevents
    a future PR from accidentally making one engine fall back to the
    other's behavior."""
    from gibran.execution.engines.duckdb import DuckDBEngine

    duck_result = DuckDBEngine().file_scan_sql("parquet", "x.parquet")
    assert duck_result == "read_parquet('x.parquet')"

    with pytest.raises(SourceDispatchError):
        PostgresEngine().file_scan_sql("parquet", "x.parquet")


# ---------------------------------------------------------------------------
# Re-export from engine.py keeps backward-compat
# ---------------------------------------------------------------------------


def test_postgres_engine_reexported_from_engine_module():
    """`from gibran.execution.engine import PostgresEngine` works,
    matching the established DuckDBEngine import pattern."""
    from gibran.execution.engine import PostgresEngine as ReExported
    assert ReExported is PostgresEngine


# ---------------------------------------------------------------------------
# 5A.1b -- placeholder translation `?` -> `%s`
# ---------------------------------------------------------------------------


def test_translate_simple_placeholders():
    from gibran.execution.engines.postgres import translate_qmark_placeholders
    assert translate_qmark_placeholders(
        "INSERT INTO t VALUES (?, ?, ?)"
    ) == "INSERT INTO t VALUES (%s, %s, %s)"


def test_translate_preserves_question_in_single_quoted_literal():
    """A literal `?` inside a single-quoted string must NOT be translated.
    Without this, `WHERE name = 'who?'` would corrupt the value."""
    from gibran.execution.engines.postgres import translate_qmark_placeholders
    assert translate_qmark_placeholders(
        "SELECT * FROM t WHERE name = 'who?' AND id = ?"
    ) == "SELECT * FROM t WHERE name = 'who?' AND id = %s"


def test_translate_handles_escaped_single_quote():
    """SQL-standard `''` escapes a single quote inside a literal.
    `WHERE name = 'don''t?'` -- the `?` is still inside the literal
    because the `''` doesn't close it."""
    from gibran.execution.engines.postgres import translate_qmark_placeholders
    assert translate_qmark_placeholders(
        "WHERE name = 'don''t?' AND id = ?"
    ) == "WHERE name = 'don''t?' AND id = %s"


def test_translate_preserves_question_in_double_quoted_identifier():
    """Double-quoted identifiers preserve their contents (Postgres
    allows `?` in identifier names via quoting; uncommon but legal)."""
    from gibran.execution.engines.postgres import translate_qmark_placeholders
    assert translate_qmark_placeholders(
        'SELECT "col?" FROM t WHERE id = ?'
    ) == 'SELECT "col?" FROM t WHERE id = %s'


def test_translate_skips_question_in_line_comment():
    from gibran.execution.engines.postgres import translate_qmark_placeholders
    sql = "SELECT id FROM t -- can this have ?\nWHERE id = ?"
    out = translate_qmark_placeholders(sql)
    assert "-- can this have ?" in out  # comment preserved
    assert "WHERE id = %s" in out  # real placeholder translated


def test_translate_doubles_literal_percent():
    """psycopg treats `%` as the start of its own format syntax. A
    literal `%` (e.g. in LIKE patterns) must be doubled to survive
    parameter substitution."""
    from gibran.execution.engines.postgres import translate_qmark_placeholders
    assert translate_qmark_placeholders(
        "WHERE name LIKE 'a%b' AND id = ?"
    ) == "WHERE name LIKE 'a%%b' AND id = %s"


def test_translate_empty_string():
    from gibran.execution.engines.postgres import translate_qmark_placeholders
    assert translate_qmark_placeholders("") == ""


def test_translate_no_placeholders_or_specials():
    from gibran.execution.engines.postgres import translate_qmark_placeholders
    assert translate_qmark_placeholders(
        "SELECT 1"
    ) == "SELECT 1"
