"""Tests for Tier 2 Item 5 aggregate primitives: weighted_avg,
stddev_samp, stddev_pop, count_distinct, count_distinct_approx, mode.

These are scalar primitives that compose with `GROUP BY` exactly like
sum/avg/min/max -- a single function call in the SELECT projection.
The compiler treats them under the standard aggregate code path; this
file pins per-primitive: Pydantic validation, applier SQL rendering,
and end-to-end execution against synthetic data.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from gibran.sync.applier import _render_expression
from gibran.sync.yaml_schema import MetricConfig


MIGRATIONS = Path(__file__).parent.parent / "migrations"


def _src_with_widget_table() -> duckdb.DuckDBPyConnection:
    """Migrated DB plus a `widgets` table with two numeric cols + a label
    suitable for testing every aggregate primitive."""
    from gibran.sync.migrations import apply_all as apply_migrations

    con = duckdb.connect(":memory:")
    apply_migrations(con, MIGRATIONS)
    con.execute(
        "CREATE TABLE widgets ("
        "  id VARCHAR, value DOUBLE, weight DOUBLE, label VARCHAR)"
    )
    con.execute(
        "INSERT INTO widgets VALUES "
        "('a', 10, 1, 'red'),"
        "('b', 20, 2, 'red'),"
        "('c', 30, 3, 'blue'),"
        "('d', 30, 3, 'blue'),"
        "('e', 30, 3, 'blue')"
    )
    return con


# ---------------------------------------------------------------------------
# weighted_avg
# ---------------------------------------------------------------------------

class TestWeightedAvg:
    def test_pydantic_requires_expression_and_weight(self) -> None:
        with pytest.raises(ValueError, match="weighted_avg requires"):
            MetricConfig(
                id="m", source="s", display_name="m", type="weighted_avg",
                expression="value",  # weight_column missing
            )

    def test_renders_sum_value_times_weight_over_sum_weight(self) -> None:
        m = MetricConfig(
            id="m", source="s", display_name="m", type="weighted_avg",
            expression="value", weight_column="weight",
        )
        sql = _render_expression(m)
        assert sql == "SUM((value) * weight) / NULLIF(SUM(weight), 0)"

    def test_executes_against_real_data(self) -> None:
        con = _src_with_widget_table()
        # Manual SQL using the same expression -- verify the formula.
        result = con.execute(
            "SELECT SUM((value) * weight) / NULLIF(SUM(weight), 0) AS w_avg "
            "FROM widgets"
        ).fetchone()[0]
        # Expected: (10*1 + 20*2 + 30*3 + 30*3 + 30*3) / (1+2+3+3+3)
        #         = (10 + 40 + 90 + 90 + 90) / 12
        #         = 320 / 12 ~= 26.67
        assert abs(result - 320 / 12) < 1e-9


# ---------------------------------------------------------------------------
# stddev_samp, stddev_pop
# ---------------------------------------------------------------------------

class TestStdDev:
    def test_stddev_samp_renders(self) -> None:
        m = MetricConfig(
            id="m", source="s", display_name="m", type="stddev_samp",
            expression="value",
        )
        assert _render_expression(m) == "STDDEV_SAMP(value)"

    def test_stddev_pop_renders(self) -> None:
        m = MetricConfig(
            id="m", source="s", display_name="m", type="stddev_pop",
            expression="value",
        )
        assert _render_expression(m) == "STDDEV_POP(value)"

    def test_stddev_requires_expression(self) -> None:
        with pytest.raises(ValueError, match="stddev_samp requires"):
            MetricConfig(
                id="m", source="s", display_name="m", type="stddev_samp",
            )

    def test_stddev_pop_executes(self) -> None:
        con = _src_with_widget_table()
        result = con.execute("SELECT STDDEV_POP(value) FROM widgets").fetchone()[0]
        # values: [10, 20, 30, 30, 30], mean = 24
        # variance_pop = mean of (x - 24)^2 = (196 + 16 + 36 + 36 + 36)/5 = 64
        # stddev_pop = 8.0
        assert abs(result - 8.0) < 1e-9


# ---------------------------------------------------------------------------
# count_distinct / count_distinct_approx
# ---------------------------------------------------------------------------

class TestCountDistinct:
    def test_count_distinct_renders(self) -> None:
        m = MetricConfig(
            id="m", source="s", display_name="m", type="count_distinct",
            column="label",
        )
        assert _render_expression(m) == "COUNT(DISTINCT label)"

    def test_count_distinct_approx_renders(self) -> None:
        m = MetricConfig(
            id="m", source="s", display_name="m", type="count_distinct_approx",
            column="label",
        )
        assert _render_expression(m) == "APPROX_COUNT_DISTINCT(label)"

    def test_count_distinct_requires_column(self) -> None:
        with pytest.raises(ValueError, match="count_distinct requires"):
            MetricConfig(
                id="m", source="s", display_name="m", type="count_distinct",
            )

    def test_count_distinct_executes(self) -> None:
        con = _src_with_widget_table()
        n = con.execute(
            "SELECT COUNT(DISTINCT label) FROM widgets"
        ).fetchone()[0]
        assert n == 2  # red + blue

    def test_count_distinct_approx_executes(self) -> None:
        con = _src_with_widget_table()
        n = con.execute(
            "SELECT APPROX_COUNT_DISTINCT(label) FROM widgets"
        ).fetchone()[0]
        # Approximate -- but with only 2 distinct values, HLL returns
        # exactly 2 (well below its precision threshold for error).
        assert n == 2


# ---------------------------------------------------------------------------
# mode
# ---------------------------------------------------------------------------

class TestMode:
    def test_mode_renders(self) -> None:
        m = MetricConfig(
            id="m", source="s", display_name="m", type="mode", column="label",
        )
        assert _render_expression(m) == "MODE(label)"

    def test_mode_requires_column(self) -> None:
        with pytest.raises(ValueError, match="mode requires"):
            MetricConfig(
                id="m", source="s", display_name="m", type="mode",
            )

    def test_mode_executes(self) -> None:
        con = _src_with_widget_table()
        # blue appears 3x, red 2x -> mode is blue
        result = con.execute("SELECT MODE(label) FROM widgets").fetchone()[0]
        assert result == "blue"
