"""Tests for the pattern-template NL layer.

Two layers of coverage:
  1. `nl_to_intent` pure function: each registered pattern hits, and
     resolution against AllowedSchema works (or fails cleanly without
     inventing).
  2. End-to-end via `run_nl_query`: text -> intent -> execute.

Tier 5 invariants this file pins:
  * If no pattern matches and resolves, returns None. Never invents.
  * Slot resolution requires the named metric / dim / column to exist
    on the AllowedSchema; typos return None rather than guessing.
  * Patterns are tried in registration order; more specific shapes win.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from gibran.governance.default import DefaultGovernance
from gibran.governance.types import IdentityContext
from gibran.nl.patterns import nl_to_intent
from gibran.nl.runner import run_nl_query
from gibran.sync.applier import apply as apply_config
from gibran.sync.example_values import populate_example_values
from gibran.sync.loader import load as load_config
from gibran.sync.migrations import apply_all as apply_migrations


FIXTURES = Path(__file__).parent / "fixtures"
MIGRATIONS = Path(__file__).parent.parent / "migrations"


def _populated_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    apply_migrations(con, MIGRATIONS)
    apply_config(con, load_config(FIXTURES / "gibran.yaml"))
    con.execute(
        "CREATE TABLE orders ("
        "order_id VARCHAR, amount DECIMAL(18,2), order_date TIMESTAMP, "
        "status VARCHAR, region VARCHAR, customer_email VARCHAR)"
    )
    con.execute(
        "INSERT INTO orders VALUES "
        "('o1', 100, TIMESTAMP '2026-01-05', 'paid', 'west', 'a@x'),"
        "('o2', 200, TIMESTAMP '2026-01-10', 'paid', 'east', 'b@x'),"
        "('o3',  50, TIMESTAMP '2026-02-15', 'paid', 'west', 'c@x')"
    )
    # Populate example_values so the "for <value>" pattern has something
    # to bind against (it looks up known examples to disambiguate which
    # column to filter on).
    populate_example_values(con, load_config(FIXTURES / "gibran.yaml").config)
    return con


def _schema(con: duckdb.DuckDBPyConnection):
    gov = DefaultGovernance(con)
    ident = IdentityContext(
        user_id="aw", role_id="analyst_west",
        attributes={"region": "west"}, source="test",
    )
    return gov.preview_schema(ident, "orders")


# ---------------------------------------------------------------------------
# Pattern: count_of_thing
# ---------------------------------------------------------------------------

class TestCountOfThing:
    def test_count_of_orders(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("count of orders", schema)
        assert m is not None
        assert m.intent["metrics"] == ["order_count"]
        assert "dimensions" not in m.intent or m.intent["dimensions"] == []

    def test_how_many_orders(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("how many orders", schema)
        assert m is not None
        assert m.intent["metrics"] == ["order_count"]

    def test_total_orders(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("total orders", schema)
        assert m is not None


# ---------------------------------------------------------------------------
# Pattern: metric_by_dim
# ---------------------------------------------------------------------------

class TestMetricByDim:
    def test_gross_revenue_by_region(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue by region", schema)
        assert m is not None
        assert m.intent["metrics"] == ["gross_revenue"]
        assert m.intent["dimensions"] == [{"id": "orders.region"}]

    def test_show_me_prefix_accepted(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("show me gross revenue by region", schema)
        assert m is not None
        assert m.intent["metrics"] == ["gross_revenue"]
        assert m.intent["dimensions"] == [{"id": "orders.region"}]


# ---------------------------------------------------------------------------
# Pattern: metric_by_grain
# ---------------------------------------------------------------------------

class TestMetricByGrain:
    def test_revenue_by_month(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue by month", schema)
        assert m is not None
        assert m.intent["dimensions"] == [
            {"id": "orders.order_date", "grain": "month"}
        ]

    def test_revenue_by_weekly_synonym(self) -> None:
        # "weekly" -> grain "week"
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue by weekly", schema)
        assert m is not None
        assert m.intent["dimensions"][0]["grain"] == "week"


# ---------------------------------------------------------------------------
# Pattern: metric_over_time
# ---------------------------------------------------------------------------

class TestMetricOverTime:
    def test_trend(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue trend", schema)
        assert m is not None
        assert m.intent["metrics"] == ["gross_revenue"]
        # Always month grain regardless of phrasing -- see pattern docstring.
        assert m.intent["dimensions"] == [
            {"id": "orders.order_date", "grain": "month"}
        ]

    def test_over_time(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue over time", schema)
        assert m is not None
        assert m.intent["dimensions"][0]["grain"] == "month"

    def test_across_time(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue across time", schema)
        assert m is not None
        assert m.intent["dimensions"][0]["grain"] == "month"

    def test_show_me_prefix(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("show me gross revenue trend", schema)
        assert m is not None
        assert m.intent["metrics"] == ["gross_revenue"]


# ---------------------------------------------------------------------------
# Pattern: top_n_by_metric
# ---------------------------------------------------------------------------

class TestTopN:
    def test_top_n(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("top 5 region by gross revenue", schema)
        assert m is not None
        assert m.intent["limit"] == 5
        assert m.intent["metrics"] == ["gross_revenue"]
        assert m.intent["order_by"][0]["direction"] == "desc"

    def test_biggest_synonym(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("biggest 3 region by gross revenue", schema)
        assert m is not None
        assert m.intent["limit"] == 3
        assert m.intent["order_by"][0]["direction"] == "desc"

    def test_largest_synonym(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("largest 2 region by gross revenue", schema)
        assert m is not None
        assert m.intent["limit"] == 2

    def test_highest_synonym(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("highest 10 region by gross revenue", schema)
        assert m is not None
        assert m.intent["order_by"][0]["direction"] == "desc"


# ---------------------------------------------------------------------------
# Pattern: bottom_n_by_metric
# ---------------------------------------------------------------------------

class TestBottomN:
    def test_bottom_n(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("bottom 5 region by gross revenue", schema)
        assert m is not None
        assert m.intent["limit"] == 5
        assert m.intent["metrics"] == ["gross_revenue"]
        assert m.intent["dimensions"] == [{"id": "orders.region"}]
        # Key distinction from top_n: ASC instead of DESC.
        assert m.intent["order_by"][0]["direction"] == "asc"
        assert m.intent["order_by"][0]["key"] == "gross_revenue"

    def test_smallest_synonym(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("smallest 3 region by gross revenue", schema)
        assert m is not None
        assert m.intent["order_by"][0]["direction"] == "asc"

    def test_lowest_synonym(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("lowest 1 region by gross revenue", schema)
        assert m is not None
        assert m.intent["limit"] == 1

    def test_unresolvable_dim_returns_none(self) -> None:
        # bogus dim -- pattern raises NoMatch, falls through, eventually None.
        schema = _schema(_populated_db())
        m = nl_to_intent("bottom 5 bogus_dim by gross revenue", schema)
        assert m is None


# ---------------------------------------------------------------------------
# Pattern: metric_in_period
# ---------------------------------------------------------------------------

class TestMetricInPeriod:
    def test_year_only(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue in 2026", schema)
        assert m is not None
        assert m.intent["metrics"] == ["gross_revenue"]
        assert m.intent["filters"] == [{
            "op": "and",
            "args": [
                {"op": "gte", "column": "order_date", "value": "2026-01-01"},
                {"op": "lt", "column": "order_date", "value": "2027-01-01"},
            ],
        }]

    def test_month_and_year(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue in January 2026", schema)
        assert m is not None
        assert m.intent["filters"] == [{
            "op": "and",
            "args": [
                {"op": "gte", "column": "order_date", "value": "2026-01-01"},
                {"op": "lt", "column": "order_date", "value": "2026-02-01"},
            ],
        }]

    def test_december_rolls_year_in_upper_bound(self) -> None:
        # December's half-open upper bound is January of next year -- this
        # is the only month where the year increments.
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue in December 2026", schema)
        assert m is not None
        assert m.intent["filters"][0]["args"][1] == {
            "op": "lt", "column": "order_date", "value": "2027-01-01",
        }

    def test_month_abbreviation_accepted(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue in Feb 2026", schema)
        assert m is not None
        assert m.intent["filters"][0]["args"][0]["value"] == "2026-02-01"

    def test_unrecognized_month_returns_none(self) -> None:
        # "foo" isn't a month name -- pattern raises NoMatch, falls through
        # to single_metric which also fails (no metric named "gross revenue
        # in foo 2026"). Tier 5 invariant: no fabrication.
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue in foo 2026", schema)
        assert m is None

    def test_end_to_end_filter_applied(self) -> None:
        # analyst_west sees only west rows; in 2026 those are o1 (Jan, $100)
        # and o3 (Feb, $50). Restricting to February drops o1.
        con = _populated_db()
        gov = DefaultGovernance(con)
        ident = IdentityContext(
            user_id="aw", role_id="analyst_west",
            attributes={"region": "west"}, source="test",
        )
        result = run_nl_query(
            con, gov, ident, "gross revenue in February 2026", "orders",
        )
        assert result.match is not None
        assert result.run_result is not None
        qr = result.run_result.query_result
        assert qr is not None and qr.status == "ok"
        # One scalar row: only o3 (50, west, Feb 15) matches. Compare on the
        # numeric value -- DuckDB returns Decimal for the SUM of a DECIMAL
        # column, and Decimal('50.00') == 50 is True via numeric coercion.
        assert len(qr.rows) == 1
        assert qr.rows[0][0] == 50


# ---------------------------------------------------------------------------
# Pattern: metric_filtered_by_value (uses example_values)
# ---------------------------------------------------------------------------

class TestMetricFilteredByValue:
    def test_filter_resolves_against_example_values(self) -> None:
        # `west` is in region's example_values (after populate_example_values)
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue for west", schema)
        assert m is not None
        assert m.intent["filters"][0] == {
            "op": "eq", "column": "region", "value": "west",
        }

    def test_filter_unresolved_falls_through(self) -> None:
        # `mars` is not a known region value -- no pattern resolves.
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue for mars", schema)
        # Falls through to single_metric; "gross revenue for mars" treated
        # as a single phrase -> no metric match; result is None.
        assert m is None


# ---------------------------------------------------------------------------
# Pattern: single_metric (catch-all)
# ---------------------------------------------------------------------------

class TestSingleMetric:
    def test_bare_metric_name(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue", schema)
        assert m is not None
        assert m.intent["metrics"] == ["gross_revenue"]

    def test_show_me_prefix(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("show me gross revenue", schema)
        assert m is not None
        assert m.intent["metrics"] == ["gross_revenue"]


# ---------------------------------------------------------------------------
# No-match contract (the Tier 5 invariant)
# ---------------------------------------------------------------------------

class TestNoMatchSafety:
    def test_invented_metric_returns_none(self) -> None:
        # `bogus_metric_that_doesnt_exist` is not on the schema -- no
        # pattern resolves it. The matcher returns None instead of
        # inventing a metric.
        schema = _schema(_populated_db())
        m = nl_to_intent("show me bogus_metric_that_doesnt_exist", schema)
        assert m is None

    def test_unrelated_text_returns_none(self) -> None:
        schema = _schema(_populated_db())
        assert nl_to_intent("what's for dinner", schema) is None
        assert nl_to_intent("please buy milk", schema) is None

    def test_empty_text_returns_none(self) -> None:
        schema = _schema(_populated_db())
        assert nl_to_intent("", schema) is None

    def test_punctuation_stripped(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("show me gross revenue?", schema)
        assert m is not None
        assert m.intent["metrics"] == ["gross_revenue"]


# ---------------------------------------------------------------------------
# End-to-end via run_nl_query
# ---------------------------------------------------------------------------

class TestRunNLQuery:
    def test_end_to_end_executes(self) -> None:
        con = _populated_db()
        gov = DefaultGovernance(con)
        ident = IdentityContext(
            user_id="aw", role_id="analyst_west",
            attributes={"region": "west"}, source="test",
        )
        result = run_nl_query(
            con, gov, ident, "gross revenue by region", "orders",
        )
        assert result.match is not None
        assert result.run_result is not None
        qr = result.run_result.query_result
        assert qr is not None
        assert qr.status == "ok"
        # analyst_west sees only west region; one row expected.
        assert len(qr.rows or ()) == 1

    def test_no_match_returns_none(self) -> None:
        con = _populated_db()
        gov = DefaultGovernance(con)
        ident = IdentityContext(
            user_id="aw", role_id="analyst_west",
            attributes={"region": "west"}, source="test",
        )
        result = run_nl_query(
            con, gov, ident, "what's for lunch tomorrow", "orders",
        )
        assert result.match is None
        assert result.run_result is None
