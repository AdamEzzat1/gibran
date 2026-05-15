"""Phase 5A.0 — dialect-abstraction scaffold tests.

The scaffold is additive and non-breaking: DuckDB stays the default,
sqlglot call sites read the dialect from one place, and the result-cache
key includes the dialect so future engines won't collide in the cache.

Scope of THIS test file is the helper + the threaded-through behavior.
The actual engine adapters (Postgres / Snowflake / BigQuery) are tested
separately in tests/test_dialect_matrix.py when 5A.2-5A.4 lands.
"""
from __future__ import annotations

import pytest

from gibran.execution import dialect as dialect_mod
from gibran.execution.dialect import Dialect, active_dialect
from gibran.execution.result_cache import cache_key
from gibran.governance.types import IdentityContext


@pytest.fixture(autouse=True)
def _reset_dialect_cache(monkeypatch):
    """Ensure each test starts with a fresh dialect lookup.

    The active dialect is memoized at module scope; tests that monkeypatch
    the env var need the memo cleared so the new value is observed.
    """
    dialect_mod._reset_active_dialect()
    yield
    dialect_mod._reset_active_dialect()


# ---------------------------------------------------------------------------
# Helper behavior
# ---------------------------------------------------------------------------


def test_default_dialect_is_duckdb(monkeypatch):
    monkeypatch.delenv("GIBRAN_SQL_DIALECT", raising=False)
    assert active_dialect() is Dialect.DUCKDB


def test_env_var_overrides_default(monkeypatch):
    monkeypatch.setenv("GIBRAN_SQL_DIALECT", "postgres")
    assert active_dialect() is Dialect.POSTGRES


def test_env_var_is_case_insensitive(monkeypatch):
    monkeypatch.setenv("GIBRAN_SQL_DIALECT", "PostGres")
    assert active_dialect() is Dialect.POSTGRES


def test_env_var_whitespace_stripped(monkeypatch):
    monkeypatch.setenv("GIBRAN_SQL_DIALECT", "  bigquery  ")
    assert active_dialect() is Dialect.BIGQUERY


def test_empty_env_var_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("GIBRAN_SQL_DIALECT", "")
    assert active_dialect() is Dialect.DUCKDB


def test_unknown_dialect_raises_loudly(monkeypatch):
    monkeypatch.setenv("GIBRAN_SQL_DIALECT", "oracle")
    with pytest.raises(ValueError) as exc:
        active_dialect()
    msg = str(exc.value).lower()
    assert "oracle" in msg
    # Error must list valid choices so the user can fix their config.
    assert "duckdb" in msg
    assert "postgres" in msg


def test_dialect_value_is_lowercase_string():
    """Sanity check: each enum value is the lowercase form sqlglot expects."""
    assert Dialect.DUCKDB.value == "duckdb"
    assert Dialect.POSTGRES.value == "postgres"
    assert Dialect.SNOWFLAKE.value == "snowflake"
    assert Dialect.BIGQUERY.value == "bigquery"


def test_dialect_is_str_subclass():
    """Dialect inherits from str so it can drop into sqlglot's `dialect=`
    parameter without a `.value` lookup. This test pins that behavior."""
    assert isinstance(Dialect.DUCKDB, str)
    assert Dialect.DUCKDB == "duckdb"


def test_active_dialect_is_memoized(monkeypatch):
    """First call reads the env; subsequent calls return cached value
    even if the env changes (the user's process inherits one config)."""
    monkeypatch.setenv("GIBRAN_SQL_DIALECT", "postgres")
    first = active_dialect()
    monkeypatch.setenv("GIBRAN_SQL_DIALECT", "snowflake")
    second = active_dialect()
    assert first is second is Dialect.POSTGRES


# ---------------------------------------------------------------------------
# Cache-key dialect inclusion
# ---------------------------------------------------------------------------


def _ident() -> IdentityContext:
    return IdentityContext(
        user_id="analyst_west",
        role_id="analyst",
        attributes={},
        source="cli",
    )


def test_cache_key_includes_dialect(monkeypatch):
    """Same SQL + identity + generations should produce DIFFERENT cache keys
    when the dialect differs. Without this, a query compiled for Postgres
    would collide with the same string compiled for DuckDB, returning the
    wrong cached rows when engines are mixed.
    """
    sql = "SELECT region, SUM(amount) FROM orders GROUP BY region"
    ident = _ident()

    key_duck = cache_key(sql, ident, "cg1", "hg1", dialect="duckdb")
    key_pg = cache_key(sql, ident, "cg1", "hg1", dialect="postgres")
    assert key_duck != key_pg


def test_cache_key_defaults_to_active_dialect(monkeypatch):
    """When `dialect=` is omitted, the key picks up the active dialect from
    the env var. Ensures the cache is dialect-correct even when callers
    don't explicitly pass dialect through."""
    sql = "SELECT 1"
    ident = _ident()

    monkeypatch.setenv("GIBRAN_SQL_DIALECT", "duckdb")
    dialect_mod._reset_active_dialect()
    default_key_duck = cache_key(sql, ident, "cg1", "hg1")
    explicit_key_duck = cache_key(sql, ident, "cg1", "hg1", dialect="duckdb")
    assert default_key_duck == explicit_key_duck

    monkeypatch.setenv("GIBRAN_SQL_DIALECT", "postgres")
    dialect_mod._reset_active_dialect()
    default_key_pg = cache_key(sql, ident, "cg1", "hg1")
    assert default_key_pg != default_key_duck


# ---------------------------------------------------------------------------
# Non-regression: the threaded sqlglot calls still work for DuckDB
# ---------------------------------------------------------------------------


def test_sql_parser_still_works_with_helper(monkeypatch):
    """Run a parse that hits one of the rewritten call sites (_inject_filter).
    If the helper threading broke anything, this would raise."""
    monkeypatch.delenv("GIBRAN_SQL_DIALECT", raising=False)
    dialect_mod._reset_active_dialect()
    from gibran.execution.sql import _inject_filter

    out = _inject_filter(
        "SELECT region, amount FROM orders",
        "amount > 0",
    )
    # Output should be valid DuckDB SQL with the filter injected
    assert "WHERE" in out.upper()
    assert "amount > 0" in out


def test_redactor_still_works_with_helper(monkeypatch):
    monkeypatch.delenv("GIBRAN_SQL_DIALECT", raising=False)
    dialect_mod._reset_active_dialect()
    from gibran.governance.redaction import redact_sql_literals

    out = redact_sql_literals(
        "SELECT order_id FROM orders WHERE customer_email = 'a@b.com'",
        frozenset({"customer_email"}),
    )
    # The literal must be redacted; the column reference stays
    assert "a@b.com" not in out
    assert "<redacted>" in out
