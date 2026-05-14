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

from datetime import date
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
        # No filter -- this is the bare-form path. The 2-word adjective
        # form goes through count_with_condition.
        assert "filters" not in m.intent or m.intent["filters"] == []

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
# Pattern: count_with_condition
# ---------------------------------------------------------------------------

class TestCountWithCondition:
    def test_count_of_paid_orders(self) -> None:
        # "paid" is in status column's example_values after populate_example_values.
        schema = _schema(_populated_db())
        m = nl_to_intent("count of paid orders", schema)
        assert m is not None
        assert m.intent["metrics"] == ["order_count"]
        assert m.intent["filters"] == [
            {"op": "eq", "column": "status", "value": "paid"},
        ]

    def test_how_many_paid_orders(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("how many paid orders", schema)
        assert m is not None
        assert m.intent["filters"][0]["column"] == "status"

    def test_total_paid_orders(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("total paid orders", schema)
        assert m is not None
        assert m.intent["filters"][0]["value"] == "paid"

    def test_unrecognized_condition_falls_through_to_bare_count(self) -> None:
        # "bogus" isn't in any example_values -- count_with_condition
        # raises NoMatch, count_of_thing catches the same input as
        # ".+" and returns the bare count (no filter).
        schema = _schema(_populated_db())
        m = nl_to_intent("count of bogus orders", schema)
        assert m is not None
        assert m.intent["metrics"] == ["order_count"]
        assert "filters" not in m.intent or m.intent["filters"] == []


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
# Pattern: metric_by_type_keyword (unique / max / min / avg / first / last / median)
# ---------------------------------------------------------------------------

class TestMetricByTypeKeyword:
    def test_unique_routes_to_count_distinct(self) -> None:
        # The fixture has unique_customers (count_distinct on customer_email).
        schema = _schema(_populated_db())
        m = nl_to_intent("unique customers", schema)
        assert m is not None
        assert m.intent["metrics"] == ["unique_customers"]

    def test_distinct_synonym(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("distinct customers", schema)
        assert m is not None
        assert m.intent["metrics"] == ["unique_customers"]

    def test_max_routes_to_max_type_only(self) -> None:
        # "amount" appears in many metric display names (Max Order Amount,
        # Min Order Amount, Average Order Amount, ...). The pattern must
        # filter by type=max FIRST so only max_amount is a candidate.
        schema = _schema(_populated_db())
        m = nl_to_intent("max order amount", schema)
        assert m is not None
        assert m.intent["metrics"] == ["max_amount"]

    def test_min_routes_to_min_type_only(self) -> None:
        # Symmetric to max -- if the type filter weren't applied first
        # this could resolve to max_amount alphabetically.
        schema = _schema(_populated_db())
        m = nl_to_intent("min order amount", schema)
        assert m is not None
        assert m.intent["metrics"] == ["min_amount"]

    def test_average_synonyms(self) -> None:
        schema = _schema(_populated_db())
        for phrase in ("average order amount", "avg amount", "mean amount"):
            m = nl_to_intent(phrase, schema)
            assert m is not None, f"failed: {phrase!r}"
            assert m.intent["metrics"] == ["avg_amount"], phrase

    def test_first_last_route_to_value_aggregates(self) -> None:
        schema = _schema(_populated_db())
        m_first = nl_to_intent("first order amount", schema)
        m_last = nl_to_intent("last order amount", schema)
        assert m_first is not None and m_first.intent["metrics"] == ["first_amount"]
        assert m_last is not None and m_last.intent["metrics"] == ["last_amount"]

    def test_median_routes_to_median_type(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("median amount", schema)
        assert m is not None
        assert m.intent["metrics"] == ["median_amount"]

    def test_wrong_type_for_keyword_returns_none(self) -> None:
        # "max gross revenue" -- gross_revenue is type=sum, not max.
        # No fabrication; returns None rather than coercing.
        schema = _schema(_populated_db())
        m = nl_to_intent("max gross revenue", schema)
        assert m is None

    def test_keyword_with_no_matching_metric_returns_none(self) -> None:
        # No count_distinct metric on amount -> "unique amount" no-match.
        schema = _schema(_populated_db())
        m = nl_to_intent("unique amount", schema)
        assert m is None


# ---------------------------------------------------------------------------
# Pattern: multi_metric
# ---------------------------------------------------------------------------

class TestMultiMetric:
    def test_two_metrics_bare(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue and order_count", schema)
        assert m is not None
        assert m.intent["metrics"] == ["gross_revenue", "order_count"]
        assert "dimensions" not in m.intent or m.intent["dimensions"] == []

    def test_two_metrics_with_by_dim(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue and order_count by region", schema)
        assert m is not None
        assert m.intent["metrics"] == ["gross_revenue", "order_count"]
        assert m.intent["dimensions"] == [{"id": "orders.region"}]

    def test_show_me_prefix(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("show me gross revenue and order_count", schema)
        assert m is not None
        assert m.intent["metrics"] == ["gross_revenue", "order_count"]

    def test_duplicate_metrics_rejected(self) -> None:
        # "X and X" is a configuration mistake; no fabricated dedupe.
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue and gross_revenue", schema)
        assert m is None

    def test_unresolved_second_metric_returns_none(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue and bogus_metric", schema)
        assert m is None


# ---------------------------------------------------------------------------
# Pattern: metric_by_dim_and_grain (one named dim + temporal at grain)
# ---------------------------------------------------------------------------

class TestMetricByDimAndGrain:
    def test_dim_plus_month_grain(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue by region by month", schema)
        assert m is not None
        assert m.intent["metrics"] == ["gross_revenue"]
        assert m.intent["dimensions"] == [
            {"id": "orders.region"},
            {"id": "orders.order_date", "grain": "month"},
        ]

    def test_dim_plus_year_grain(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue by region by year", schema)
        assert m is not None
        assert m.intent["dimensions"][1] == {
            "id": "orders.order_date", "grain": "year"
        }

    def test_redundant_temporal_dim_falls_through(self) -> None:
        # "<temporal_dim> by <grain>" -- the user is specifying the temporal
        # dim twice (once as the named dim, once implicitly via grain). The
        # pattern raises NoMatch and falls through to metric_by_grain, which
        # handles "<metric> by <grain>" correctly.
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue by order_date by month", schema)
        # metric_by_grain doesn't match (extra " by month" tail).
        # multi_metric doesn't match (no " and ").
        # Falls through -- returns None or matches some other pattern.
        # The point: it doesn't produce a 2-dim intent that double-counts
        # the temporal dim.
        assert m is None or m.intent.get("dimensions") != [
            {"id": "orders.order_date"},
            {"id": "orders.order_date", "grain": "month"},
        ]


# ---------------------------------------------------------------------------
# Pattern: metric_by_two_dims
# ---------------------------------------------------------------------------

class TestMetricByTwoDims:
    def test_two_dims_by_separator(self) -> None:
        # Fixture only has 2 dims (orders.region categorical + orders.order_date
        # temporal). Using both with " by " separator.
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue by region by order_date", schema)
        assert m is not None
        assert m.intent["metrics"] == ["gross_revenue"]
        assert m.intent["dimensions"] == [
            {"id": "orders.region"},
            {"id": "orders.order_date"},
        ]

    def test_two_dims_comma_separator(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue by region, order_date", schema)
        assert m is not None
        assert m.intent["dimensions"] == [
            {"id": "orders.region"},
            {"id": "orders.order_date"},
        ]

    def test_duplicate_dims_rejected(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue by region by region", schema)
        assert m is None


# ---------------------------------------------------------------------------
# Pattern: metric_where_between (inclusive numeric range)
# ---------------------------------------------------------------------------

class TestMetricWhereBetween:
    def test_amount_between(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent(
            "gross revenue where amount between 50 and 200", schema,
        )
        assert m is not None
        # NOTE: "count of orders where ..." would NOT route here because
        # count_of_thing matches first and ignores the trailing text (it
        # picks the first count-type metric regardless of the noun). For
        # an amount-bounded count, the user writes "order_count where ..."
        # to skip the count_of_thing keyword prefix.
        assert m.intent["filters"] == [{
            "op": "and",
            "args": [
                {"op": "gte", "column": "amount", "value": 50},
                {"op": "lte", "column": "amount", "value": 200},
            ],
        }]

    def test_float_bounds(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent(
            "gross revenue where amount between 0.5 and 99.99", schema,
        )
        assert m is not None
        assert m.intent["filters"][0]["args"][0]["value"] == 0.5
        assert m.intent["filters"][0]["args"][1]["value"] == 99.99

    def test_unknown_column_returns_none(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent(
            "gross revenue where bogus between 0 and 100", schema,
        )
        assert m is None


# ---------------------------------------------------------------------------
# Pattern: metric_where (single comparison filter on a numeric column)
# ---------------------------------------------------------------------------

class TestMetricWhere:
    def test_amount_greater_than(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue where amount > 100", schema)
        assert m is not None
        assert m.intent["filters"] == [
            {"op": "gt", "column": "amount", "value": 100},
        ]

    def test_amount_gte(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue where amount >= 50.5", schema)
        assert m is not None
        assert m.intent["filters"][0] == {
            "op": "gte", "column": "amount", "value": 50.5,
        }

    def test_neq(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue where amount != 0", schema)
        assert m is not None
        assert m.intent["filters"][0] == {
            "op": "neq", "column": "amount", "value": 0,
        }

    def test_unknown_column_returns_none(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue where bogus > 100", schema)
        assert m is None


# ---------------------------------------------------------------------------
# Pattern: metric_as_percent_of (routes to ratio metric by num/denom)
# ---------------------------------------------------------------------------

class TestMetricAsPercentOf:
    def test_routes_to_ratio_by_components(self) -> None:
        # avg_order_value is type=ratio with numerator=gross_revenue,
        # denominator=order_count. So "gross revenue as percent of
        # order count" routes here.
        schema = _schema(_populated_db())
        m = nl_to_intent(
            "gross revenue as percent of order count", schema,
        )
        assert m is not None
        assert m.intent["metrics"] == ["avg_order_value"]

    def test_percent_sign_synonym(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent(
            "gross revenue as % of order count", schema,
        )
        assert m is not None
        assert m.intent["metrics"] == ["avg_order_value"]

    def test_wrong_direction_returns_none(self) -> None:
        # "order count as percent of gross revenue" would mean
        # order_count / gross_revenue, but avg_order_value is the
        # OPPOSITE direction. No ratio metric matches, so NoMatch.
        schema = _schema(_populated_db())
        m = nl_to_intent(
            "order count as percent of gross revenue", schema,
        )
        assert m is None

    def test_unknown_components_return_none(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent(
            "bogus as percent of gross revenue", schema,
        )
        assert m is None

    def test_show_me_prefix(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent(
            "show me gross revenue as percent of order count", schema,
        )
        assert m is not None
        assert m.intent["metrics"] == ["avg_order_value"]


# ---------------------------------------------------------------------------
# Pattern: metric_filter_compound (two AND-ed eq filters)
# ---------------------------------------------------------------------------

class TestMetricFilterCompound:
    def test_two_filters_different_columns(self) -> None:
        # "west" -> region, "paid" -> status -- two columns, two filters.
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue for west and paid", schema)
        assert m is not None
        cols = {f["column"] for f in m.intent["filters"]}
        assert cols == {"region", "status"}
        vals = {f["value"] for f in m.intent["filters"]}
        assert vals == {"west", "paid"}

    def test_show_me_prefix(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("show me gross revenue for west and paid", schema)
        assert m is not None
        assert len(m.intent["filters"]) == 2

    def test_unresolved_value_returns_none(self) -> None:
        # "bogus" isn't in any example_values -- no fabrication.
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue for bogus and paid", schema)
        assert m is None


# ---------------------------------------------------------------------------
# Pattern: metric_in_date_range (explicit YYYY-MM-DD to YYYY-MM-DD)
# ---------------------------------------------------------------------------

class TestMetricInDateRange:
    def test_full_date_range(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent(
            "gross revenue from 2026-01-01 to 2026-02-01", schema,
        )
        assert m is not None
        assert m.intent["filters"] == [{
            "op": "and",
            "args": [
                {"op": "gte", "column": "order_date", "value": "2026-01-01"},
                {"op": "lt", "column": "order_date", "value": "2026-02-01"},
            ],
        }]

    def test_invalid_date_format_returns_none(self) -> None:
        schema = _schema(_populated_db())
        # "bogus" doesn't match the ISO YYYY-MM-DD regex.
        m = nl_to_intent("gross revenue from 2026-01-01 to bogus", schema)
        assert m is None

    def test_show_me_prefix(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent(
            "show me gross revenue from 2026-01-01 to 2026-02-01", schema,
        )
        assert m is not None
        assert m.intent["metrics"] == ["gross_revenue"]


# ---------------------------------------------------------------------------
# Pattern: metric_this_period (this week|month|quarter|year)
# ---------------------------------------------------------------------------

class TestMetricThisPeriod:
    @pytest.fixture(autouse=True)
    def _fix_today(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Pin today to Thursday, May 14, 2026 (Q2). Week starts Mon May 11.
        monkeypatch.setattr(
            "gibran.nl.patterns._today",
            lambda: date(2026, 5, 14),
        )

    def test_this_year(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue this year", schema)
        assert m is not None
        assert m.intent["filters"][0]["args"] == [
            {"op": "gte", "column": "order_date", "value": "2026-01-01"},
            {"op": "lt",  "column": "order_date", "value": "2027-01-01"},
        ]

    def test_this_quarter_q2(self) -> None:
        # May 14 is in Q2 -- runs Apr 1 to Jul 1.
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue this quarter", schema)
        assert m is not None
        assert m.intent["filters"][0]["args"] == [
            {"op": "gte", "column": "order_date", "value": "2026-04-01"},
            {"op": "lt",  "column": "order_date", "value": "2026-07-01"},
        ]

    def test_this_month(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue this month", schema)
        assert m is not None
        assert m.intent["filters"][0]["args"] == [
            {"op": "gte", "column": "order_date", "value": "2026-05-01"},
            {"op": "lt",  "column": "order_date", "value": "2026-06-01"},
        ]

    def test_this_week_iso_monday_start(self) -> None:
        # Thu May 14 -> Monday of that week is May 11; next Monday is May 18.
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue this week", schema)
        assert m is not None
        assert m.intent["filters"][0]["args"] == [
            {"op": "gte", "column": "order_date", "value": "2026-05-11"},
            {"op": "lt",  "column": "order_date", "value": "2026-05-18"},
        ]

    def test_unknown_period_returns_none(self) -> None:
        # "decade" isn't in THIS_PERIOD_WORDS -- the regex won't match.
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue this decade", schema)
        assert m is None


# ---------------------------------------------------------------------------
# Pattern: top_n_with_having
# ---------------------------------------------------------------------------

class TestTopNWithHaving:
    def test_top_n_with_having_same_metric(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent(
            "top 5 region by gross revenue where gross revenue > 100",
            schema,
        )
        assert m is not None
        assert m.intent["metrics"] == ["gross_revenue"]
        assert m.intent["limit"] == 5
        assert m.intent["order_by"][0]["direction"] == "desc"
        assert m.intent["having"] == [{
            "op": "gt", "metric": "gross_revenue", "value": 100,
        }]

    def test_top_n_with_having_gte(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent(
            "top 3 region by gross revenue where gross revenue >= 50",
            schema,
        )
        assert m is not None
        assert m.intent["having"][0]["op"] == "gte"
        assert m.intent["having"][0]["value"] == 50

    def test_biggest_synonym_and_eq(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent(
            "biggest 2 region by order_count where order_count = 1",
            schema,
        )
        assert m is not None
        assert m.intent["having"][0]["op"] == "eq"

    def test_having_on_different_metric_projects_both(self) -> None:
        # If the HAVING metric differs from the ordering metric, both
        # are projected (HAVING needs the SELECT alias to refer to).
        schema = _schema(_populated_db())
        m = nl_to_intent(
            "top 5 region by gross_revenue where order_count > 0",
            schema,
        )
        assert m is not None
        assert set(m.intent["metrics"]) == {"gross_revenue", "order_count"}

    def test_unknown_metric_in_having_returns_none(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent(
            "top 5 region by gross revenue where bogus > 100",
            schema,
        )
        assert m is None


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
# Pattern: metric_last_n_period (clock-dependent; tests pin date via monkeypatch)
# ---------------------------------------------------------------------------

class TestMetricLastNPeriod:
    @pytest.fixture(autouse=True)
    def _fix_today(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Pin date.today() at 2026-05-14 so the assertions on filter
        # values are deterministic. The pattern uses gibran.nl.patterns._today
        # specifically to make this monkeypatch trivial.
        monkeypatch.setattr(
            "gibran.nl.patterns._today",
            lambda: date(2026, 5, 14),
        )

    def test_last_30_days(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue last 30 days", schema)
        assert m is not None
        # start = 2026-05-14 - 30 days = 2026-04-14
        # end   = 2026-05-14 + 1 day  = 2026-05-15 (exclusive upper bound)
        assert m.intent["filters"] == [{
            "op": "and",
            "args": [
                {"op": "gte", "column": "order_date", "value": "2026-04-14"},
                {"op": "lt", "column": "order_date", "value": "2026-05-15"},
            ],
        }]

    def test_last_3_months_uses_30_day_approximation(self) -> None:
        # Phase 1 approximation: 3 months = 90 days.
        # 2026-05-14 - 90 days = 2026-02-13.
        # Phase 3's relative_time_filter will replace with calendar math.
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue last 3 months", schema)
        assert m is not None
        assert m.intent["filters"][0]["args"][0]["value"] == "2026-02-13"

    def test_past_synonym(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue past 7 days", schema)
        assert m is not None
        # 2026-05-14 - 7 days = 2026-05-07
        assert m.intent["filters"][0]["args"][0]["value"] == "2026-05-07"

    def test_singular_unit_accepted(self) -> None:
        # "1 day" (singular) should match the same regex as "30 days".
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue last 1 day", schema)
        assert m is not None
        assert m.intent["filters"][0]["args"][0]["value"] == "2026-05-13"

    def test_last_1_year(self) -> None:
        # 1 year = 365 days (approximation).
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue last 1 year", schema)
        assert m is not None
        # 2026-05-14 - 365 days = 2025-05-14
        assert m.intent["filters"][0]["args"][0]["value"] == "2025-05-14"

    def test_invalid_metric_returns_none(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("bogus_metric last 30 days", schema)
        assert m is None


# ---------------------------------------------------------------------------
# Pattern: metric_excluding_value (uses example_values, emits neq)
# ---------------------------------------------------------------------------

class TestMetricExcludingValue:
    def test_excluding_value(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue excluding paid", schema)
        assert m is not None
        assert m.intent["filters"] == [
            {"op": "neq", "column": "status", "value": "paid"},
        ]

    def test_excluding_value_with_trailing_noun(self) -> None:
        # The optional trailing noun is matched but discarded -- both
        # phrasings produce the same filter.
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue excluding paid orders", schema)
        assert m is not None
        assert m.intent["filters"][0] == {
            "op": "neq", "column": "status", "value": "paid",
        }

    def test_show_me_prefix(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("show me gross revenue excluding paid orders", schema)
        assert m is not None
        assert m.intent["metrics"] == ["gross_revenue"]

    def test_unrecognized_value_returns_none(self) -> None:
        # "bogus" isn't an example value -- pattern raises NoMatch and
        # nothing else matches. Tier 5 invariant: no fabrication.
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue excluding bogus", schema)
        assert m is None


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
# Pattern: metric_anomalies (routes to an anomaly_query metric)
# ---------------------------------------------------------------------------

class TestMetricAnomalies:
    def test_anomalies_in_revenue_routes_to_anomaly_query(self) -> None:
        # The fixture has revenue_anomalies (anomaly_query metric).
        schema = _schema(_populated_db())
        m = nl_to_intent("anomalies in revenue", schema)
        assert m is not None
        assert m.intent["metrics"] == ["revenue_anomalies"]

    def test_singular_anomaly_accepted(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("anomaly in revenue", schema)
        assert m is not None
        assert m.intent["metrics"] == ["revenue_anomalies"]

    def test_display_name_match(self) -> None:
        # "Revenue Anomalies" is the display name.
        schema = _schema(_populated_db())
        m = nl_to_intent("anomalies in Revenue Anomalies", schema)
        assert m is not None
        assert m.intent["metrics"] == ["revenue_anomalies"]

    def test_no_matching_anomaly_metric_returns_none(self) -> None:
        # "amount" -- no anomaly_query metric whose name contains amount.
        schema = _schema(_populated_db())
        m = nl_to_intent("anomalies in amount", schema)
        assert m is None

    def test_show_me_prefix(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("show me anomalies in revenue", schema)
        assert m is not None
        assert m.intent["metrics"] == ["revenue_anomalies"]


# ---------------------------------------------------------------------------
# Pattern: metric_period_over_period (yoy / mom / vs last year / etc.)
# ---------------------------------------------------------------------------

class TestMetricPeriodOverPeriod:
    def test_revenue_yoy(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("revenue yoy", schema)
        assert m is not None
        assert m.intent["metrics"] == ["revenue_yoy"]
        # period_over_period requires the period_dim at matching grain
        # in the intent -- auto-added by the pattern.
        assert m.intent["dimensions"] == [
            {"id": "orders.order_date", "grain": "year"}
        ]

    def test_vs_last_year_routes_to_yoy(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("revenue vs last year", schema)
        assert m is not None
        assert m.intent["metrics"] == ["revenue_yoy"]

    def test_year_over_year_long_form(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("revenue year over year", schema)
        assert m is not None
        assert m.intent["metrics"] == ["revenue_yoy"]

    def test_mom_routes_to_revenue_mom(self) -> None:
        # Same noun "revenue" but mom keyword routes to month metric.
        schema = _schema(_populated_db())
        m = nl_to_intent("revenue mom", schema)
        assert m is not None
        assert m.intent["metrics"] == ["revenue_mom"]
        assert m.intent["dimensions"] == [
            {"id": "orders.order_date", "grain": "month"}
        ]

    def test_vs_last_month_routes_to_mom(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("revenue vs last month", schema)
        assert m is not None
        assert m.intent["metrics"] == ["revenue_mom"]

    def test_no_matching_period_metric_returns_none(self) -> None:
        # No qoq metric in fixture -- pattern matches "vs last quarter"
        # but finds no period_over_period metric with quarter hint.
        schema = _schema(_populated_db())
        m = nl_to_intent("revenue qoq", schema)
        assert m is None


# ---------------------------------------------------------------------------
# Pattern: metric_distribution
# ---------------------------------------------------------------------------

class TestMetricDistribution:
    def test_percentile_metric_resolves(self) -> None:
        # p95_amount is type=percentile in the fixture -- routes here.
        schema = _schema(_populated_db())
        m = nl_to_intent("p95_amount distribution", schema)
        assert m is not None
        assert m.intent["metrics"] == ["p95_amount"]
        assert m.intent.get("dimensions", []) == []
        assert m.intent.get("filters", []) == []

    def test_display_name_resolves(self) -> None:
        # "P95 Order Amount" is p95_amount's display_name.
        schema = _schema(_populated_db())
        m = nl_to_intent("P95 Order Amount distribution", schema)
        assert m is not None
        assert m.intent["metrics"] == ["p95_amount"]

    def test_show_me_prefix(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("show me p95_amount distribution", schema)
        assert m is not None
        assert m.intent["metrics"] == ["p95_amount"]

    def test_non_distribution_metric_returns_none(self) -> None:
        # gross_revenue is type=sum -- the metric resolves, but the
        # pattern raises NoMatch because the type isn't median or
        # percentile. Falls through to single_metric, which returns the
        # bare metric (no shape change). The pattern's job is to refuse
        # to fabricate a "distribution" for a non-distribution metric;
        # it lets bare metric pass since "gross revenue distribution" as
        # text is still a recognizable metric reference.
        schema = _schema(_populated_db())
        m = nl_to_intent("gross revenue distribution", schema)
        # Falls through to single_metric -- which fails because
        # "gross revenue distribution" isn't a valid metric phrase.
        assert m is None

    def test_unresolvable_returns_none(self) -> None:
        schema = _schema(_populated_db())
        m = nl_to_intent("bogus_metric distribution", schema)
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
