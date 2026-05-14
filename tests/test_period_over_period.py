"""Tests for the period_over_period metric primitive.

Covers:
  - Pydantic validation: required fields, mutually-exclusive shapes
  - Loader cross-entity validation: base_metric exists, same source,
    period_dim is temporal
  - Applier: metric_config JSON stored on gibran_metric_versions
  - Compiler: emits LAG window function with correct DATE_TRUNC
  - DSL validator: rejects intents missing the period_dim
  - End-to-end execution against DuckDB with fixture data
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

from gibran.dsl.compile import Catalog, CompileError, compile_intent
from gibran.dsl.run import run_dsl_query
from gibran.dsl.types import QueryIntent
from gibran.dsl.validate import IntentValidationError, validate_intent
from gibran.governance.default import DefaultGovernance
from gibran.governance.identity import CLIResolver
from gibran.observability.default import DefaultObservability
from gibran.observability.runner import run_checks
from gibran.sync.applier import apply as apply_config
from gibran.sync.loader import ConfigValidationError, load as load_config
from gibran.sync.migrations import apply_all as apply_migrations
from gibran.sync.yaml_schema import MetricConfig


FIXTURES = Path(__file__).parent / "fixtures"
MIGRATIONS = Path(__file__).parent.parent / "migrations"


def _populated_db_with_orders() -> duckdb.DuckDBPyConnection:
    """Catalog + governance + orders rows spanning multiple months so the
    LAG produces meaningful values."""
    con = duckdb.connect(":memory:")
    apply_migrations(con, MIGRATIONS)
    apply_config(con, load_config(FIXTURES / "gibran.yaml"))
    con.execute(
        "CREATE TABLE orders ("
        "order_id VARCHAR, amount DECIMAL(18,2), order_date TIMESTAMP, "
        "status VARCHAR, region VARCHAR, customer_email VARCHAR)"
    )
    # Three months of paid revenue:
    #   2026-01: 100      -> no prior period
    #   2026-02: 100+200=300  delta=300-100=200
    #   2026-03: 400          delta=400-300=100
    con.execute(
        "INSERT INTO orders VALUES "
        "('o1', 100.00, TIMESTAMP '2026-01-15 10:00:00', 'paid', 'west', 'a@x'), "
        "('o2', 100.00, TIMESTAMP '2026-02-10 10:00:00', 'paid', 'west', 'b@x'), "
        "('o3', 200.00, TIMESTAMP '2026-02-20 10:00:00', 'paid', 'west', 'c@x'), "
        "('o4', 400.00, TIMESTAMP '2026-03-05 10:00:00', 'paid', 'west', 'd@x')"
    )
    # Disable freshness rule (data is from 2026-01..03, not "now").
    con.execute("DELETE FROM gibran_freshness_rules")
    return con


# ---------------------------------------------------------------------------
# Pydantic validation
# ---------------------------------------------------------------------------

class TestPydanticValidation:
    def test_minimal_valid_config_parses(self) -> None:
        m = MetricConfig(
            id="revenue_mom",
            source="orders",
            display_name="Revenue MoM",
            type="period_over_period",
            base_metric="gross_revenue",
            period_dim="orders.order_date",
            period_unit="month",
            comparison="delta",
        )
        assert m.base_metric == "gross_revenue"
        assert m.period_unit == "month"

    def test_missing_required_fields_rejected(self) -> None:
        with pytest.raises(ValidationError, match="period_over_period requires"):
            MetricConfig(
                id="x", source="orders", display_name="X",
                type="period_over_period",
                # base_metric missing
                period_dim="orders.order_date",
                period_unit="month",
                comparison="delta",
            )

    def test_expression_field_rejected(self) -> None:
        with pytest.raises(
            ValidationError, match="period_over_period cannot have"
        ):
            MetricConfig(
                id="x", source="orders", display_name="X",
                type="period_over_period",
                base_metric="gross_revenue",
                period_dim="orders.order_date",
                period_unit="month",
                comparison="delta",
                expression="amount",  # forbidden
            )

    def test_unknown_period_unit_rejected(self) -> None:
        with pytest.raises(ValidationError):
            MetricConfig(
                id="x", source="orders", display_name="X",
                type="period_over_period",
                base_metric="gross_revenue",
                period_dim="orders.order_date",
                period_unit="decade",  # not in Literal
                comparison="delta",
            )

    def test_unknown_comparison_rejected(self) -> None:
        with pytest.raises(ValidationError):
            MetricConfig(
                id="x", source="orders", display_name="X",
                type="period_over_period",
                base_metric="gross_revenue",
                period_dim="orders.order_date",
                period_unit="month",
                comparison="lift",  # not in Literal
            )


# ---------------------------------------------------------------------------
# Loader / applier
# ---------------------------------------------------------------------------

class TestLoaderValidation:
    def _make_yaml(self, tmp_path: Path, extra: str) -> Path:
        base = (FIXTURES / "gibran.yaml").read_text(encoding="utf-8")
        out = tmp_path / "gibran.yaml"
        out.write_text(base + extra, encoding="utf-8")
        return out

    def test_unknown_base_metric_rejected(self, tmp_path: Path) -> None:
        path = self._make_yaml(tmp_path, """
  - id: bad_mom
    source: orders
    display_name: Bad
    type: period_over_period
    base_metric: ghost_metric
    period_dim: orders.order_date
    period_unit: month
    comparison: delta
""")
        with pytest.raises(ConfigValidationError, match="base_metric"):
            load_config(path)

    def test_unknown_period_dim_rejected(self, tmp_path: Path) -> None:
        path = self._make_yaml(tmp_path, """
  - id: bad_mom
    source: orders
    display_name: Bad
    type: period_over_period
    base_metric: gross_revenue
    period_dim: orders.ghost_dim
    period_unit: month
    comparison: delta
""")
        with pytest.raises(ConfigValidationError, match="period_dim"):
            load_config(path)

    def test_non_temporal_period_dim_rejected(self, tmp_path: Path) -> None:
        path = self._make_yaml(tmp_path, """
  - id: bad_mom
    source: orders
    display_name: Bad
    type: period_over_period
    base_metric: gross_revenue
    period_dim: orders.region
    period_unit: month
    comparison: delta
""")
        with pytest.raises(ConfigValidationError, match="temporal"):
            load_config(path)


class TestApplierStoresMetricConfig:
    def test_metric_config_persisted_on_version(self) -> None:
        con = _populated_db_with_orders()
        row = con.execute(
            "SELECT metric_config FROM gibran_metric_versions "
            "WHERE metric_id = 'revenue_mom' AND effective_to IS NULL"
        ).fetchone()
        assert row is not None
        cfg = json.loads(row[0])
        assert cfg == {
            "base_metric": "gross_revenue",
            "period_dim": "orders.order_date",
            "period_unit": "month",
            "comparison": "delta",
        }


# ---------------------------------------------------------------------------
# Compiler
# ---------------------------------------------------------------------------

class TestCompiler:
    def test_compile_emits_lag_window_function(self) -> None:
        con = _populated_db_with_orders()
        intent = QueryIntent(
            source="orders",
            metrics=["revenue_mom"],
            dimensions=[{"id": "orders.order_date", "grain": "month"}],
        )
        sql = compile_intent(intent, Catalog(con)).render()
        assert "LAG(" in sql
        # base metric is gross_revenue -> SUM(amount) FILTER (WHERE status='paid')
        assert "SUM(amount)" in sql
        # window orders by DATE_TRUNC('month', "order_date")
        assert "DATE_TRUNC('month'" in sql
        assert '"order_date"' in sql

    def test_compile_ratio_comparison_emits_nullif(self) -> None:
        con = _populated_db_with_orders()
        # Switch the metric's comparison to ratio via direct UPDATE
        # (alternative would be re-syncing yaml; this is faster).
        con.execute(
            "UPDATE gibran_metric_versions SET metric_config = ? "
            "WHERE metric_id = 'revenue_mom' AND effective_to IS NULL",
            [json.dumps({
                "base_metric": "gross_revenue",
                "period_dim": "orders.order_date",
                "period_unit": "month",
                "comparison": "ratio",
            })],
        )
        intent = QueryIntent(
            source="orders",
            metrics=["revenue_mom"],
            dimensions=[{"id": "orders.order_date", "grain": "month"}],
        )
        sql = compile_intent(intent, Catalog(con)).render()
        assert "LAG(" in sql
        assert "NULLIF(" in sql

    def test_compile_pct_change_comparison(self) -> None:
        con = _populated_db_with_orders()
        con.execute(
            "UPDATE gibran_metric_versions SET metric_config = ? "
            "WHERE metric_id = 'revenue_mom' AND effective_to IS NULL",
            [json.dumps({
                "base_metric": "gross_revenue",
                "period_dim": "orders.order_date",
                "period_unit": "month",
                "comparison": "pct_change",
            })],
        )
        intent = QueryIntent(
            source="orders",
            metrics=["revenue_mom"],
            dimensions=[{"id": "orders.order_date", "grain": "month"}],
        )
        sql = compile_intent(intent, Catalog(con)).render()
        # pct_change = (base - lag) / NULLIF(lag, 0)
        assert "LAG(" in sql
        assert "NULLIF(" in sql

    def test_filter_on_pop_metric_rejected(self) -> None:
        con = _populated_db_with_orders()
        con.execute(
            "UPDATE gibran_metric_versions SET filter_sql = 'status = ''paid''' "
            "WHERE metric_id = 'revenue_mom' AND effective_to IS NULL"
        )
        intent = QueryIntent(
            source="orders",
            metrics=["revenue_mom"],
            dimensions=[{"id": "orders.order_date", "grain": "month"}],
        )
        with pytest.raises(CompileError, match="filter_sql is not supported"):
            compile_intent(intent, Catalog(con))


# ---------------------------------------------------------------------------
# DSL validation
# ---------------------------------------------------------------------------

class TestDSLValidation:
    def test_missing_period_dim_rejected(self) -> None:
        con = _populated_db_with_orders()
        gov = DefaultGovernance(con, observability=DefaultObservability(con))
        identity = CLIResolver(
            user_id="alice", role_id="analyst_west", attributes={"region": "west"},
        ).resolve(None)
        schema = gov.preview_schema(identity, "orders")
        intent = QueryIntent(
            source="orders",
            metrics=["revenue_mom"],
            # NO dimensions -> the period_dim is missing
        )
        with pytest.raises(IntentValidationError, match="requires intent.dimensions"):
            validate_intent(intent, schema, con=con)

    def test_wrong_grain_rejected(self) -> None:
        con = _populated_db_with_orders()
        gov = DefaultGovernance(con, observability=DefaultObservability(con))
        identity = CLIResolver(
            user_id="alice", role_id="analyst_west", attributes={"region": "west"},
        ).resolve(None)
        schema = gov.preview_schema(identity, "orders")
        intent = QueryIntent(
            source="orders",
            metrics=["revenue_mom"],
            dimensions=[{"id": "orders.order_date", "grain": "day"}],  # wrong grain
        )
        with pytest.raises(IntentValidationError, match="grain"):
            validate_intent(intent, schema, con=con)

    def test_correct_dim_and_grain_passes(self) -> None:
        con = _populated_db_with_orders()
        gov = DefaultGovernance(con, observability=DefaultObservability(con))
        identity = CLIResolver(
            user_id="alice", role_id="analyst_west", attributes={"region": "west"},
        ).resolve(None)
        schema = gov.preview_schema(identity, "orders")
        intent = QueryIntent(
            source="orders",
            metrics=["revenue_mom"],
            dimensions=[{"id": "orders.order_date", "grain": "month"}],
        )
        # No exception: passes.
        validate_intent(intent, schema, con=con)


# ---------------------------------------------------------------------------
# End-to-end execution against DuckDB
# ---------------------------------------------------------------------------

class TestEndToEnd:
    def test_revenue_mom_returns_expected_deltas(self) -> None:
        con = _populated_db_with_orders()
        obs = DefaultObservability(con)
        gov = DefaultGovernance(con, observability=obs)
        identity = CLIResolver(
            user_id="alice", role_id="analyst_west", attributes={"region": "west"},
        ).resolve(None)
        # Populate source_health so quality consultation passes.
        run_checks(con, "orders", obs)

        result = run_dsl_query(
            con, gov, identity,
            {
                "source": "orders",
                "metrics": ["revenue_mom"],
                "dimensions": [{"id": "orders.order_date", "grain": "month"}],
                "order_by": [{"key": "orders.order_date", "direction": "asc"}],
            },
        )
        assert result.pre_compile_error is None, result.pre_compile_error
        assert result.query_result is not None
        assert result.query_result.status == "ok", (
            f"{result.query_result.deny_reason} "
            f"{result.query_result.deny_detail} "
            f"{result.query_result.error_message}"
        )
        # Three months of data; first has no prior period so LAG returns NULL.
        # 2026-01: 100, prev=NULL  -> 100 - NULL = NULL
        # 2026-02: 300, prev=100   -> 300 - 100 = 200
        # 2026-03: 400, prev=300   -> 400 - 300 = 100
        rows = result.query_result.rows
        assert rows is not None
        assert len(rows) == 3
        # rows is ((order_date_truncated, revenue_mom), ...)
        deltas = [r[1] for r in rows]
        assert deltas[0] is None
        assert float(deltas[1]) == 200.0
        assert float(deltas[2]) == 100.0
