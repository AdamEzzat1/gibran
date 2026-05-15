"""Phase 5D -- dialect-aware emitter tests.

Pins the per-dialect SQL output for the 4 time-related emitter methods
across all 4 supported engines. If any dialect's syntax for one of these
shifts (e.g. BigQuery adds support for the unquoted `INTERVAL 'X days'`
form), this test would catch it.

Scope: just the emitter registry. Call-site migration (the existing
hardcoded `INTERVAL '28 days'` in sync/applier.py for rolling_window,
the `DATE_DIFF('second', ...)` calls in observability/default.py) is
documented as a follow-up.
"""
from __future__ import annotations

import pytest

from gibran.dsl.dialect_emit import emitter_for
from gibran.execution.dialect import Dialect


ALL_DIALECTS = [
    Dialect.DUCKDB,
    Dialect.POSTGRES,
    Dialect.SNOWFLAKE,
    Dialect.BIGQUERY,
]


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dialect", ALL_DIALECTS)
def test_emitter_exists_for_every_dialect(dialect):
    """Every Dialect enum value must have a registered emitter -- a
    missing one would only surface at runtime when a query is dispatched."""
    em = emitter_for(dialect)
    assert em.dialect is dialect


def test_each_dialect_has_distinct_emitter():
    """Just a sanity check that the registry doesn't accidentally share
    instances across dialects."""
    emitters = [emitter_for(d) for d in ALL_DIALECTS]
    assert len({id(e) for e in emitters}) == 4


# ---------------------------------------------------------------------------
# now()
# ---------------------------------------------------------------------------


def test_now_duckdb_postgres_use_lowercase_now():
    assert emitter_for(Dialect.DUCKDB).now() == "now()"
    assert emitter_for(Dialect.POSTGRES).now() == "now()"


def test_now_snowflake_bigquery_use_current_timestamp():
    assert emitter_for(Dialect.SNOWFLAKE).now() == "CURRENT_TIMESTAMP()"
    assert emitter_for(Dialect.BIGQUERY).now() == "CURRENT_TIMESTAMP()"


# ---------------------------------------------------------------------------
# current_date()
# ---------------------------------------------------------------------------


def test_current_date_per_dialect():
    # DuckDB / Postgres: no parens (CURRENT_DATE is a keyword)
    assert emitter_for(Dialect.DUCKDB).current_date() == "CURRENT_DATE"
    assert emitter_for(Dialect.POSTGRES).current_date() == "CURRENT_DATE"
    # Snowflake / BigQuery: function call with parens
    assert emitter_for(Dialect.SNOWFLAKE).current_date() == "CURRENT_DATE()"
    assert emitter_for(Dialect.BIGQUERY).current_date() == "CURRENT_DATE()"


# ---------------------------------------------------------------------------
# interval(amount, unit)
# ---------------------------------------------------------------------------


def test_interval_quoted_form_for_duckdb_postgres_snowflake():
    """Three dialects share the `INTERVAL '<n> <unit>s'` form."""
    for d in (Dialect.DUCKDB, Dialect.POSTGRES, Dialect.SNOWFLAKE):
        em = emitter_for(d)
        assert em.interval(28, "days") == "INTERVAL '28 days'"
        assert em.interval(1, "hour") == "INTERVAL '1 hours'"
        assert em.interval(3, "month") == "INTERVAL '3 months'"


def test_interval_unquoted_uppercase_form_for_bigquery():
    """BigQuery: `INTERVAL <n> <UNIT>` -- no quotes, singular uppercase."""
    em = emitter_for(Dialect.BIGQUERY)
    assert em.interval(28, "days") == "INTERVAL 28 DAY"
    assert em.interval(1, "hour") == "INTERVAL 1 HOUR"
    assert em.interval(3, "month") == "INTERVAL 3 MONTH"


def test_interval_normalizes_unit_aliases():
    """Both `day` and `days` and `DAYS` resolve to the same emission."""
    em = emitter_for(Dialect.DUCKDB)
    a = em.interval(28, "days")
    b = em.interval(28, "day")
    c = em.interval(28, "DAYS")
    assert a == b == c


def test_interval_rejects_unknown_unit():
    em = emitter_for(Dialect.DUCKDB)
    with pytest.raises(ValueError) as exc:
        em.interval(1, "fortnight")
    assert "fortnight" in str(exc.value)


def test_interval_coerces_amount_to_int():
    """Defensive: a float amount silently rounds via int() rather than
    rendering `INTERVAL '28.0 days'` (which DuckDB accepts but is ugly)."""
    em = emitter_for(Dialect.DUCKDB)
    assert em.interval(28.7, "days") == "INTERVAL '28 days'"


# ---------------------------------------------------------------------------
# date_diff_seconds(start, end)
# ---------------------------------------------------------------------------


def test_date_diff_seconds_duckdb():
    em = emitter_for(Dialect.DUCKDB)
    assert em.date_diff_seconds("a", "b") == "DATE_DIFF('second', a, b)"


def test_date_diff_seconds_postgres_uses_extract_epoch():
    """Postgres has no DATE_DIFF; EXTRACT(EPOCH FROM (end - start))
    is the standard idiom, cast to INT for parity."""
    em = emitter_for(Dialect.POSTGRES)
    assert em.date_diff_seconds("a", "b") == (
        "CAST(EXTRACT(EPOCH FROM (b - a)) AS INTEGER)"
    )


def test_date_diff_seconds_snowflake_uses_datediff_one_word():
    em = emitter_for(Dialect.SNOWFLAKE)
    assert em.date_diff_seconds("a", "b") == "DATEDIFF('second', a, b)"


def test_date_diff_seconds_bigquery_has_end_first():
    """BigQuery's TIMESTAMP_DIFF argument order is (end, start, UNIT)
    -- opposite of every other dialect. Pin this so a future refactor
    doesn't accidentally swap the args."""
    em = emitter_for(Dialect.BIGQUERY)
    assert em.date_diff_seconds("a", "b") == "TIMESTAMP_DIFF(b, a, SECOND)"


# ---------------------------------------------------------------------------
# Divergence matrix -- single test that captures all the differences
# ---------------------------------------------------------------------------


def test_realistic_time_window_expression_per_dialect():
    """Compose a realistic time-window predicate (`WHERE ts > now() -
    INTERVAL 28 days`) and pin the per-dialect shape.

    Honest note: DuckDB and Postgres emit IDENTICAL SQL for this
    expression -- both use `now()` and the quoted interval form. They
    only diverge on `date_diff_seconds` (different functions entirely).
    Snowflake and BigQuery each differ from the DuckDB/Postgres pair.
    """
    expressions = {
        d: f"WHERE ts > {emitter_for(d).now()} - {emitter_for(d).interval(28, 'days')}"
        for d in ALL_DIALECTS
    }
    # Three distinct shapes: {DuckDB,Postgres}, Snowflake, BigQuery.
    assert len(set(expressions.values())) == 3
    # DuckDB and Postgres legitimately match here.
    assert expressions[Dialect.DUCKDB] == expressions[Dialect.POSTGRES]
    # Snowflake and BigQuery each differ.
    assert expressions[Dialect.SNOWFLAKE] != expressions[Dialect.DUCKDB]
    assert expressions[Dialect.BIGQUERY] != expressions[Dialect.SNOWFLAKE]
    # Spot-check the specific shapes:
    assert "now()" in expressions[Dialect.DUCKDB]
    assert "INTERVAL '28 days'" in expressions[Dialect.DUCKDB]
    assert "CURRENT_TIMESTAMP()" in expressions[Dialect.SNOWFLAKE]
    assert "INTERVAL 28 DAY" in expressions[Dialect.BIGQUERY]


def test_date_diff_produces_four_distinct_outputs():
    """The OTHER load-bearing test: date_diff IS where all 4 dialects
    genuinely differ (DATE_DIFF / EXTRACT / DATEDIFF / TIMESTAMP_DIFF).
    This is where the registry earns its keep."""
    diffs = {
        d: emitter_for(d).date_diff_seconds("a", "b")
        for d in ALL_DIALECTS
    }
    # Four distinct outputs: pinned divergence across all dialects.
    assert len(set(diffs.values())) == 4
