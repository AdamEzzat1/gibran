"""Tests for cohort_retention and funnel -- the two CTE-based shape
primitives that ride on the Tier 3 CTE infrastructure.

Shape primitives are different from scalar primitives (count/sum/ratio/
percentile/etc): they emit a fixed multi-column output shape and have
their own dimensions baked into the SQL. Constraints enforced by
`dsl.validate`:
  * a shape primitive must be the only metric in the intent
  * intent.dimensions, filters, having, order_by must all be empty
  * (the compiler emits the whole query; user-supplied modifiers have
    nowhere to plug in)
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from gibran.dsl.compile import Catalog, CompileError, compile_intent
from gibran.dsl.run import run_dsl_query
from gibran.dsl.types import QueryIntent
from gibran.dsl.validate import IntentValidationError, validate_intent
from gibran.governance.default import DefaultGovernance
from gibran.governance.types import DenyReason, IdentityContext
from gibran.sync.applier import apply as apply_config
from gibran.sync.loader import (
    ConfigValidationError,
    load as load_config,
)
from gibran.sync.migrations import apply_all as apply_migrations
from gibran.sync.yaml_schema import MetricConfig


FIXTURES = Path(__file__).parent / "fixtures"
MIGRATIONS = Path(__file__).parent.parent / "migrations"


def _populated_db() -> duckdb.DuckDBPyConnection:
    """Migrated + synced DB + a real `orders` table with 6 rows across 3
    customers spanning 3 months, so cohort retention has real data to
    bucket."""
    con = duckdb.connect(":memory:")
    apply_migrations(con, MIGRATIONS)
    apply_config(con, load_config(FIXTURES / "gibran.yaml"))
    con.execute(
        "CREATE TABLE orders ("
        "  order_id VARCHAR, amount DECIMAL(18,2), order_date TIMESTAMP,"
        "  status VARCHAR, region VARCHAR, customer_email VARCHAR"
        ")"
    )
    # alice: first order Jan, returns Feb + Mar
    # bob:   first order Jan, returns Feb only
    # carol: first order Feb, returns Mar
    con.execute(
        "INSERT INTO orders VALUES "
        "('o1',  10, TIMESTAMP '2026-01-05', 'paid', 'west', 'alice'),"
        "('o2',  20, TIMESTAMP '2026-02-10', 'paid', 'west', 'alice'),"
        "('o3',  30, TIMESTAMP '2026-03-15', 'paid', 'west', 'alice'),"
        "('o4',  40, TIMESTAMP '2026-01-20', 'paid', 'west', 'bob'),"
        "('o5',  50, TIMESTAMP '2026-02-25', 'pending', 'west', 'bob'),"
        "('o6',  60, TIMESTAMP '2026-02-08', 'paid', 'west', 'carol'),"
        "('o7',  70, TIMESTAMP '2026-03-12', 'pending', 'west', 'carol')"
    )
    return con


def _admin(con: duckdb.DuckDBPyConnection) -> tuple[IdentityContext, DefaultGovernance]:
    """Provision an allow-everything admin role for tests that need
    direct access to PII (`customer_email` is the entity column)."""
    con.execute("INSERT INTO gibran_roles (role_id, display_name) VALUES ('admin', 'Admin')")
    con.execute(
        "INSERT INTO gibran_policies "
        "(policy_id, role_id, source_id, default_column_mode) "
        "VALUES ('admin_orders', 'admin', 'orders', 'allow')"
    )
    ident = IdentityContext(
        user_id="admin", role_id="admin", attributes={}, source="test"
    )
    return ident, DefaultGovernance(con)


# ---------------------------------------------------------------------------
# cohort_retention: Pydantic validation
# ---------------------------------------------------------------------------

class TestCohortRetentionValidation:
    def test_minimum_config_accepted(self) -> None:
        m = MetricConfig(
            id="r", source="orders", display_name="R", type="cohort_retention",
            entity_column="customer_email", event_column="order_date",
            cohort_grain="month", retention_grain="month",
        )
        assert m.type == "cohort_retention"

    def test_missing_required_field_rejected(self) -> None:
        with pytest.raises(ValueError, match="cohort_retention requires"):
            MetricConfig(
                id="r", source="orders", display_name="R",
                type="cohort_retention",
                entity_column="customer_email",
                # event_column missing
                cohort_grain="month", retention_grain="month",
            )

    def test_non_positive_max_periods_rejected(self) -> None:
        with pytest.raises(ValueError, match="max_periods"):
            MetricConfig(
                id="r", source="orders", display_name="R",
                type="cohort_retention",
                entity_column="customer_email", event_column="order_date",
                cohort_grain="month", retention_grain="month",
                max_periods=0,
            )

    def test_extra_scalar_fields_rejected(self) -> None:
        with pytest.raises(ValueError, match="cannot have"):
            MetricConfig(
                id="r", source="orders", display_name="R",
                type="cohort_retention",
                entity_column="customer_email", event_column="order_date",
                cohort_grain="month", retention_grain="month",
                expression="amount",  # not allowed
            )


# ---------------------------------------------------------------------------
# cohort_retention: loader cross-entity check
# ---------------------------------------------------------------------------

class TestCohortRetentionLoaderRejection:
    def test_unknown_entity_column_rejected(self, tmp_path: Path) -> None:
        yaml_path = tmp_path / "gibran.yaml"
        yaml_path.write_text(
            (FIXTURES / "gibran.yaml").read_text(encoding="utf-8")
            .replace(
                "entity_column: customer_email",
                "entity_column: ghost_col",
            ),
            encoding="utf-8",
        )
        with pytest.raises(ConfigValidationError, match="ghost_col"):
            load_config(yaml_path)


# ---------------------------------------------------------------------------
# cohort_retention: applier persists metric_config JSON
# ---------------------------------------------------------------------------

class TestCohortRetentionApplier:
    def test_metric_config_persisted_as_json(self) -> None:
        con = _populated_db()
        cfg_json = con.execute(
            "SELECT metric_config FROM gibran_metric_versions "
            "WHERE metric_id = 'customer_retention' AND effective_to IS NULL"
        ).fetchone()[0]
        assert cfg_json is not None
        cfg = json.loads(cfg_json)
        assert cfg["entity_column"] == "customer_email"
        assert cfg["event_column"] == "order_date"
        assert cfg["cohort_grain"] == "month"
        assert cfg["retention_grain"] == "month"
        assert cfg["max_periods"] == 12


# ---------------------------------------------------------------------------
# cohort_retention: compiler emits 3-CTE shape
# ---------------------------------------------------------------------------

class TestCohortRetentionCompile:
    def test_emits_three_ctes(self) -> None:
        con = _populated_db()
        intent = QueryIntent(source="orders", metrics=["customer_retention"])
        compiled = compile_intent(intent, Catalog(con))
        cte_names = [c.name for c in compiled.ctes]
        assert cte_names == ["cohorts", "retention", "cohort_sizes"]

    def test_rendered_sql_starts_with_with_clause(self) -> None:
        con = _populated_db()
        intent = QueryIntent(source="orders", metrics=["customer_retention"])
        sql = compile_intent(intent, Catalog(con)).render()
        assert sql.startswith("WITH cohorts AS")
        assert "DATE_DIFF" in sql
        assert "DATE_TRUNC" in sql

    def test_compile_fails_without_metric_config(self) -> None:
        # Simulate stale catalog state: zero out the metric_config column.
        con = _populated_db()
        con.execute(
            "UPDATE gibran_metric_versions SET metric_config = NULL "
            "WHERE metric_id = 'customer_retention' AND effective_to IS NULL"
        )
        intent = QueryIntent(source="orders", metrics=["customer_retention"])
        with pytest.raises(CompileError, match="metric_config"):
            compile_intent(intent, Catalog(con))


# ---------------------------------------------------------------------------
# cohort_retention: DSL validator rejects bad shapes
# ---------------------------------------------------------------------------

class TestCohortRetentionDslValidation:
    def _schema(self, con: duckdb.DuckDBPyConnection):
        gov = DefaultGovernance(con)
        # Use the admin identity so all metrics are visible.
        ident = IdentityContext(
            user_id="admin", role_id="admin", attributes={}, source="test"
        )
        # admin role + policy must exist for preview_schema to return non-empty.
        # _populated_db doesn't create them; the loader cohort schema fetch
        # is happening at the schema level here. We grab the schema with
        # the analyst_west role which exists in the fixture.
        ident = IdentityContext(
            user_id="aw", role_id="analyst_west", attributes={"region": "west"},
            source="test",
        )
        return gov.preview_schema(ident, "orders")

    def test_rejects_combination_with_other_metric(self) -> None:
        con = _populated_db()
        schema = self._schema(con)
        intent = QueryIntent(
            source="orders",
            metrics=["customer_retention", "order_count"],
        )
        with pytest.raises(IntentValidationError, match="only metric"):
            validate_intent(intent, schema, con=con)

    def test_rejects_intent_dimensions(self) -> None:
        con = _populated_db()
        schema = self._schema(con)
        intent = QueryIntent(
            source="orders",
            metrics=["customer_retention"],
            dimensions=[{"id": "orders.region"}],
        )
        with pytest.raises(IntentValidationError, match="dimensions"):
            validate_intent(intent, schema, con=con)

    def test_rejects_intent_filters(self) -> None:
        con = _populated_db()
        schema = self._schema(con)
        intent = QueryIntent(
            source="orders",
            metrics=["customer_retention"],
            filters=[{"op": "eq", "column": "region", "value": "west"}],
        )
        with pytest.raises(IntentValidationError, match="filters"):
            validate_intent(intent, schema, con=con)


# ---------------------------------------------------------------------------
# cohort_retention: end-to-end execution + governance
# ---------------------------------------------------------------------------

class TestCohortRetentionEndToEnd:
    def test_executes_and_returns_expected_shape(self) -> None:
        con = _populated_db()
        ident, gov = _admin(con)
        result = run_dsl_query(
            con, gov, ident,
            {"source": "orders", "metrics": ["customer_retention"]},
        )
        assert result.pre_compile_error is None
        qr = result.query_result
        assert qr is not None
        assert qr.status == "ok", (qr.deny_reason, qr.error_message)
        # 5 output columns per the documented shape.
        assert qr.columns == (
            "cohort_start", "periods_since_cohort", "retained_count",
            "cohort_size", "retention_rate",
        )
        # Jan cohort has 2 entities (alice + bob); their period-0 count is 2
        # (the first event itself counts as period 0).
        jan_p0 = [
            r for r in qr.rows if r[1] == 0 and r[0].month == 1
        ]
        assert jan_p0, qr.rows
        assert jan_p0[0][3] == 2  # cohort_size for the January cohort

    def test_governance_denies_when_entity_column_is_pii(self) -> None:
        # `customer_email` is classified `pii` in the fixture; the
        # cohort metric reads it as `entity_column` inside the CTEs.
        # external_partner has customer_email denied -> the CTE-walking
        # governance check should still trip COLUMN_DENIED.
        con = _populated_db()
        gov = DefaultGovernance(con)
        ident = IdentityContext(
            user_id="ep", role_id="external_partner",
            attributes={}, source="test",
        )
        result = run_dsl_query(
            con, gov, ident,
            {"source": "orders", "metrics": ["customer_retention"]},
        )
        qr = result.query_result
        assert qr is not None
        assert qr.status == "denied"
        assert qr.deny_reason is DenyReason.COLUMN_DENIED
        assert "customer_email" in (qr.deny_detail or "")


# ---------------------------------------------------------------------------
# funnel: Pydantic + applier + compile
# ---------------------------------------------------------------------------

class TestFunnelValidation:
    def test_minimum_config_accepted(self) -> None:
        m = MetricConfig(
            id="f", source="orders", display_name="F", type="funnel",
            funnel_entity_column="customer_email",
            funnel_event_order_column="order_date",
            funnel_steps=[
                {"name": "a", "condition": "status = 'pending'"},
                {"name": "b", "condition": "status = 'paid'"},
            ],
        )
        assert m.type == "funnel"

    def test_single_step_rejected(self) -> None:
        with pytest.raises(ValueError, match="at least 2 steps"):
            MetricConfig(
                id="f", source="orders", display_name="F", type="funnel",
                funnel_entity_column="customer_email",
                funnel_event_order_column="order_date",
                funnel_steps=[{"name": "only", "condition": "TRUE"}],
            )

    def test_duplicate_step_name_rejected(self) -> None:
        with pytest.raises(ValueError, match="more than once"):
            MetricConfig(
                id="f", source="orders", display_name="F", type="funnel",
                funnel_entity_column="customer_email",
                funnel_event_order_column="order_date",
                funnel_steps=[
                    {"name": "dup", "condition": "status = 'pending'"},
                    {"name": "dup", "condition": "status = 'paid'"},
                ],
            )

    def test_step_missing_required_keys_rejected(self) -> None:
        with pytest.raises(ValueError, match="name.*condition"):
            MetricConfig(
                id="f", source="orders", display_name="F", type="funnel",
                funnel_entity_column="customer_email",
                funnel_event_order_column="order_date",
                funnel_steps=[
                    {"name": "missing_condition"},
                    {"name": "b", "condition": "TRUE"},
                ],
            )


class TestFunnelCompile:
    def test_emits_one_cte_per_step_plus_counts(self) -> None:
        con = _populated_db()
        intent = QueryIntent(source="orders", metrics=["paid_funnel"])
        compiled = compile_intent(intent, Catalog(con))
        names = [c.name for c in compiled.ctes]
        # 2 steps in the fixture + step_counts aggregator
        assert names == ["step_0", "step_1", "step_counts"]
        # The conversion ratios use FIRST_VALUE and LAG window functions.
        sql = compiled.render()
        assert "LAG(entity_count)" in sql
        assert "FIRST_VALUE(entity_count)" in sql


class TestFunnelEndToEnd:
    def test_paid_funnel_returns_step_counts(self) -> None:
        con = _populated_db()
        ident, gov = _admin(con)
        result = run_dsl_query(
            con, gov, ident,
            {"source": "orders", "metrics": ["paid_funnel"]},
        )
        qr = result.query_result
        assert qr is not None
        assert qr.status == "ok", (qr.deny_reason, qr.error_message)
        assert qr.columns == (
            "step_name", "step_index", "entity_count",
            "conversion_from_previous", "conversion_from_first",
        )
        # Step "ordered" should see all 3 entities (alice, bob, carol).
        # Step "paid" sees only entities that have a paid status after
        # their first ordered event: alice, bob (initial Jan = paid),
        # carol (initial Feb = paid). All three qualify -> count == 3.
        rows_by_step = {r[0]: r for r in qr.rows}
        assert rows_by_step["ordered"][2] == 3
        assert rows_by_step["paid"][2] == 3


# ---------------------------------------------------------------------------
# cohort_filter: Pydantic validation
# ---------------------------------------------------------------------------

class TestCohortFilterValidation:
    def test_minimum_config_accepted(self) -> None:
        m = MetricConfig(
            id="c", source="orders", display_name="C", type="cohort_filter",
            entity_column="customer_email",
            cohort_condition="order_date >= '2026-01-01' AND order_date < '2026-02-01'",
            result_condition="order_date >= '2026-02-01' AND order_date < '2026-03-01'",
        )
        assert m.type == "cohort_filter"

    def test_missing_entity_column_rejected(self) -> None:
        with pytest.raises(ValueError, match="cohort_filter requires"):
            MetricConfig(
                id="c", source="orders", display_name="C", type="cohort_filter",
                cohort_condition="status = 'paid'",
                result_condition="status = 'paid'",
            )

    def test_missing_cohort_condition_rejected(self) -> None:
        with pytest.raises(ValueError, match="cohort_filter requires"):
            MetricConfig(
                id="c", source="orders", display_name="C", type="cohort_filter",
                entity_column="customer_email",
                # cohort_condition missing
                result_condition="status = 'paid'",
            )

    def test_extra_scalar_fields_rejected(self) -> None:
        with pytest.raises(ValueError, match="cannot have"):
            MetricConfig(
                id="c", source="orders", display_name="C", type="cohort_filter",
                entity_column="customer_email",
                cohort_condition="status = 'paid'",
                result_condition="status = 'paid'",
                expression="amount",
            )

    def test_cohort_filter_cannot_be_materialized(self) -> None:
        with pytest.raises(ValueError, match="cannot be materialized"):
            MetricConfig(
                id="c", source="orders", display_name="C", type="cohort_filter",
                entity_column="customer_email",
                cohort_condition="status = 'paid'",
                result_condition="status = 'paid'",
                materialized=[],
            )


# ---------------------------------------------------------------------------
# cohort_filter: end-to-end query against synthetic data
# ---------------------------------------------------------------------------

class TestCohortFilterEndToEnd:
    def test_jan_to_feb_returners_via_fixture_metric(self) -> None:
        # The fixture metric jan_to_feb_returners filters paid Jan orders
        # then intersects with paid Feb orders. From _populated_db:
        #   alice: Jan paid + Feb paid  -> in cohort, in result -> COUNT
        #   bob:   Jan paid + Feb pending -> in cohort, NOT in result
        #   carol: Feb paid (no Jan)   -> NOT in cohort
        # Expected count = 1 (just alice).
        con = _populated_db()
        ident, gov = _admin(con)
        intent = QueryIntent(
            source="orders", metrics=["jan_to_feb_returners"],
        )
        result = run_dsl_query(con, gov, ident, intent.model_dump())
        assert result.query_result is not None
        qr = result.query_result
        assert qr.status == "ok"
        assert qr.columns == ("jan_to_feb_returners",)
        assert qr.rows == ((1,),)

    def test_compile_emits_two_ctes(self) -> None:
        # The compiled SQL should have the cohort + result CTEs and a
        # COUNT JOIN against them.
        con = _populated_db()
        intent = QueryIntent(
            source="orders", metrics=["jan_to_feb_returners"],
        )
        compiled = compile_intent(intent, Catalog(con))
        cte_names = [c.name for c in compiled.ctes]
        assert cte_names == ["cohort", "result"]
        rendered = compiled.render()
        assert "JOIN result USING" in rendered
        assert "COUNT(*)" in rendered

    def test_cannot_combine_with_dimensions(self) -> None:
        # Shape-primitive contract: single metric, no dimensions /
        # filters / having / order_by. Default ShapePrimitive
        # validate_intent enforces.
        con = _populated_db()
        ident, gov = _admin(con)
        intent = QueryIntent(
            source="orders",
            metrics=["jan_to_feb_returners"],
            dimensions=[{"id": "orders.region"}],
        )
        with pytest.raises(IntentValidationError, match="cannot be combined with intent.dimensions"):
            validate_intent(intent, gov.preview_schema(ident, "orders"))


# ---------------------------------------------------------------------------
# anomaly_query: Pydantic validation
# ---------------------------------------------------------------------------

class TestAnomalyQueryValidation:
    def test_minimum_config_accepted(self) -> None:
        m = MetricConfig(
            id="a", source="orders", display_name="A",
            type="anomaly_query", rule_id="orders_revenue_anomaly",
        )
        assert m.type == "anomaly_query"

    def test_missing_rule_id_rejected(self) -> None:
        with pytest.raises(ValueError, match="anomaly_query requires .rule_id."):
            MetricConfig(
                id="a", source="orders", display_name="A", type="anomaly_query",
            )

    def test_extra_scalar_fields_rejected(self) -> None:
        with pytest.raises(ValueError, match="cannot have"):
            MetricConfig(
                id="a", source="orders", display_name="A", type="anomaly_query",
                rule_id="orders_revenue_anomaly", expression="amount",
            )

    def test_anomaly_query_cannot_be_materialized(self) -> None:
        with pytest.raises(ValueError, match="cannot be materialized"):
            MetricConfig(
                id="a", source="orders", display_name="A", type="anomaly_query",
                rule_id="orders_revenue_anomaly", materialized=[],
            )


# ---------------------------------------------------------------------------
# anomaly_query: end-to-end query against synthetic gibran_quality_runs rows
# ---------------------------------------------------------------------------

class TestAnomalyQueryEndToEnd:
    def test_queries_failed_runs_only(self) -> None:
        # The fixture defines orders_revenue_anomaly + revenue_anomalies
        # metric. Insert synthetic runs (1 passing, 2 failing); the query
        # should return only the 2 failing ones.
        con = _populated_db()
        ident, gov = _admin(con)
        # Synthetic run rows -- 1 pass + 2 fails on the right rule + 1
        # fail on an unrelated rule (must be excluded).
        con.execute(
            "INSERT INTO gibran_quality_runs "
            "(run_id, rule_id, rule_kind, passed, observed_value, ran_at) VALUES "
            "('r1', 'orders_revenue_anomaly', 'quality', TRUE, "
            "  '{\"value\": 100}'::JSON, TIMESTAMP '2026-04-01'), "
            "('r2', 'orders_revenue_anomaly', 'quality', FALSE, "
            "  '{\"value\": 999}'::JSON, TIMESTAMP '2026-04-02'), "
            "('r3', 'orders_revenue_anomaly', 'quality', FALSE, "
            "  '{\"value\": 50}'::JSON, TIMESTAMP '2026-04-03'), "
            "('r4', 'unrelated_rule',        'quality', FALSE, "
            "  '{\"value\": 7}'::JSON, TIMESTAMP '2026-04-04')"
        )
        intent = QueryIntent(source="orders", metrics=["revenue_anomalies"])
        result = run_dsl_query(con, gov, ident, intent.model_dump())
        qr = result.query_result
        assert qr is not None and qr.status == "ok"
        assert qr.columns == (
            "run_id", "observed_value", "ran_at", "detected_anomaly",
        )
        # 2 failing runs for the right rule -- ordered DESC, so r3 first.
        run_ids = [r[0] for r in qr.rows]
        assert run_ids == ["r3", "r2"]
        # detected_anomaly is `NOT passed`, so TRUE for the 2 failures.
        assert all(r[3] is True for r in qr.rows)

    def test_compile_emits_select_from_quality_runs(self) -> None:
        con = _populated_db()
        intent = QueryIntent(source="orders", metrics=["revenue_anomalies"])
        compiled = compile_intent(intent, Catalog(con))
        rendered = compiled.render()
        assert "FROM gibran_quality_runs" in rendered
        assert "rule_id = 'orders_revenue_anomaly'" in rendered
        assert "passed = FALSE" in rendered

    def test_cannot_combine_with_dimensions(self) -> None:
        con = _populated_db()
        ident, gov = _admin(con)
        intent = QueryIntent(
            source="orders", metrics=["revenue_anomalies"],
            dimensions=[{"id": "orders.region"}],
        )
        with pytest.raises(IntentValidationError, match="cannot be combined with intent.dimensions"):
            validate_intent(intent, gov.preview_schema(ident, "orders"))
