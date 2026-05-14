"""End-to-end DSL tests: types, semantic validation, compilation, runner."""
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

from gibran.dsl.compile import Catalog, CompileError, compile_intent
from gibran.dsl.run import run_dsl_query
from gibran.dsl.types import DimensionRef, HavingClause, OrderBy, QueryIntent
from gibran.dsl.validate import IntentValidationError, validate_intent
from gibran.governance.default import DefaultGovernance
from gibran.governance.types import DenyReason, IdentityContext
from gibran.observability.default import DefaultObservability
from gibran.sync.applier import apply as apply_config
from gibran.sync.loader import load as load_config
from gibran.sync.migrations import apply_all as apply_migrations


FIXTURES = Path(__file__).parent / "fixtures"
MIGRATIONS = Path(__file__).parent.parent / "migrations"


def _populated_db_with_data() -> duckdb.DuckDBPyConnection:
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
        "('o1', 100.00, '2025-01-15 10:00:00', 'paid',    'west',  'a@x.com'), "
        "('o2', 200.00, '2025-01-20 11:00:00', 'paid',    'east',  'b@x.com'), "
        "('o3',  50.00, '2025-02-01 12:00:00', 'pending', 'west',  'c@x.com'), "
        "('o4', 300.00, '2025-02-15 13:00:00', 'paid',    'north', 'd@x.com')"
    )
    # Pre-record passing health so the obs layer doesn't deny everything
    obs = DefaultObservability(con)
    obs.record_run("orders_amount_not_null", "quality", True)
    obs.record_run("orders_freshness_24h", "freshness", True)
    return con


def _ident(role: str, **attrs: str) -> IdentityContext:
    return IdentityContext(
        user_id=f"u_{role}", role_id=role, attributes=dict(attrs), source="test"
    )


# ---------------------------------------------------------------------------
# Pydantic types
# ---------------------------------------------------------------------------

class TestQueryIntentParsing:
    def test_minimal_intent(self) -> None:
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["gross_revenue"]
        })
        assert intent.source == "orders"
        assert intent.metrics == ["gross_revenue"]
        assert intent.dimensions == []
        assert intent.limit == 1000

    def test_dimensions_string_shorthand_coerced(self) -> None:
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["gross_revenue"],
            "dimensions": ["orders.region"]
        })
        assert intent.dimensions == [DimensionRef(id="orders.region", grain=None)]

    def test_dimensions_full_form_with_grain(self) -> None:
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["gross_revenue"],
            "dimensions": [{"id": "orders.order_date", "grain": "month"}],
        })
        assert intent.dimensions == [
            DimensionRef(id="orders.order_date", grain="month")
        ]

    def test_order_by_default_direction_asc(self) -> None:
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["gross_revenue"],
            "order_by": [{"key": "gross_revenue"}],
        })
        assert intent.order_by == [OrderBy(key="gross_revenue", direction="asc")]

    def test_empty_projection_rejected(self) -> None:
        with pytest.raises(ValidationError, match="metric or one dimension"):
            QueryIntent.model_validate({"source": "orders"})

    def test_limit_bounds_enforced(self) -> None:
        with pytest.raises(ValidationError):
            QueryIntent.model_validate({
                "source": "orders", "metrics": ["gross_revenue"], "limit": 0
            })
        with pytest.raises(ValidationError):
            QueryIntent.model_validate({
                "source": "orders", "metrics": ["gross_revenue"], "limit": 100000
            })

    def test_invalid_grain_rejected(self) -> None:
        with pytest.raises(ValidationError):
            QueryIntent.model_validate({
                "source": "orders", "metrics": ["gross_revenue"],
                "dimensions": [{"id": "orders.order_date", "grain": "millennium"}],
            })

    def test_extra_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            QueryIntent.model_validate({
                "source": "orders", "metrics": ["gross_revenue"], "extra_field": 42
            })


# ---------------------------------------------------------------------------
# Semantic validator against AllowedSchema
# ---------------------------------------------------------------------------

class TestValidateIntent:
    def _schema_for(self, role: str = "analyst_west", region: str = "west"):
        con = _populated_db_with_data()
        gov = DefaultGovernance(con, observability=DefaultObservability(con))
        return gov.preview_schema(_ident(role, region=region), "orders"), con

    def test_valid_intent_passes(self) -> None:
        schema, _ = self._schema_for()
        intent = QueryIntent.model_validate({
            "source": "orders",
            "metrics": ["gross_revenue"],
            "dimensions": [{"id": "orders.order_date", "grain": "month"}],
            "filters": [{"op": "gte", "column": "order_date", "value": "2025-01-01"}],
            "order_by": [{"key": "gross_revenue", "direction": "desc"}],
            "limit": 12,
        })
        validate_intent(intent, schema)  # no raise

    def test_wrong_source_rejected(self) -> None:
        schema, _ = self._schema_for()
        intent = QueryIntent.model_validate({
            "source": "ghost_source", "metrics": ["gross_revenue"]
        })
        with pytest.raises(IntentValidationError, match="intent.source"):
            validate_intent(intent, schema)

    def test_unknown_metric_rejected(self) -> None:
        schema, _ = self._schema_for()
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["ghost_metric"]
        })
        with pytest.raises(IntentValidationError, match="metric .* not in AllowedSchema"):
            validate_intent(intent, schema)

    def test_unknown_dimension_rejected(self) -> None:
        schema, _ = self._schema_for()
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["gross_revenue"],
            "dimensions": ["orders.ghost"],
        })
        with pytest.raises(IntentValidationError, match="dimension .* not in AllowedSchema"):
            validate_intent(intent, schema)

    def test_grain_on_non_temporal_dimension_rejected(self) -> None:
        schema, _ = self._schema_for()
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["gross_revenue"],
            "dimensions": [{"id": "orders.region", "grain": "month"}],
        })
        with pytest.raises(IntentValidationError, match="grain"):
            validate_intent(intent, schema)

    def test_attr_ref_in_filter_rejected(self) -> None:
        schema, _ = self._schema_for()
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["gross_revenue"],
            "filters": [{"op": "eq", "column": "region", "value": {"$attr": "region"}}],
        })
        with pytest.raises(IntentValidationError, match="filter AST invalid"):
            validate_intent(intent, schema)

    def test_order_by_key_not_in_projection_rejected(self) -> None:
        schema, _ = self._schema_for()
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["gross_revenue"],
            "order_by": [{"key": "order_count"}],  # not in this intent's metrics
        })
        with pytest.raises(IntentValidationError, match="order_by.key"):
            validate_intent(intent, schema)

    def test_dimension_invisible_to_role_rejected(self) -> None:
        schema, _ = self._schema_for(role="external_partner")
        # external_partner sees only granted columns; orders.region's column
        # is 'region' which is NOT granted to external_partner.
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["gross_revenue"],
            "dimensions": ["orders.region"],
        })
        with pytest.raises(IntentValidationError, match="dimension"):
            validate_intent(intent, schema)


# ---------------------------------------------------------------------------
# Compiler
# ---------------------------------------------------------------------------

class TestCompileIntent:
    def test_simple_metric_with_filter(self) -> None:
        con = _populated_db_with_data()
        cat = Catalog(con)
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["gross_revenue"],
            "filters": [{"op": "gte", "column": "order_date", "value": "2025-01-01"}],
        })
        sql = compile_intent(intent, cat).render()
        # The metric's filter lives INSIDE the aggregate via FILTER (WHERE...)
        assert "SUM(amount) FILTER (WHERE status = 'paid')" in sql
        # The user's DSL filter lives in the query's WHERE clause
        assert '"order_date" >= ' in sql
        assert 'FROM "orders"' in sql
        assert "LIMIT 1000" in sql

    def test_dimension_with_time_grain(self) -> None:
        con = _populated_db_with_data()
        cat = Catalog(con)
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["gross_revenue"],
            "dimensions": [{"id": "orders.order_date", "grain": "month"}],
        })
        sql = compile_intent(intent, cat).render()
        assert "DATE_TRUNC('month', \"order_date\")" in sql
        assert "GROUP BY 1" in sql

    def test_ratio_metric_expands_to_resolved_expression(self) -> None:
        con = _populated_db_with_data()
        cat = Catalog(con)
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["avg_order_value"]
        })
        sql = compile_intent(intent, cat).render()
        # The literal template must NOT appear (it was a placeholder)
        assert "{gross_revenue}" not in sql
        assert "{order_count}" not in sql
        # Resolved: numerator is SUM(amount) FILTER..., denominator is COUNT(*)
        assert "SUM(amount)" in sql
        assert "COUNT(*)" in sql
        # Divide-by-zero guard
        assert "NULLIF" in sql

    def test_ratio_metric_executes_against_duckdb(self) -> None:
        """The pre-existing string-shape test missed that the literal
        template made the SQL invalid. This test executes the compiled SQL
        and asserts a numeric result -- proves the fix actually fixes."""
        con = _populated_db_with_data()
        cat = Catalog(con)
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["avg_order_value"]
        })
        sql = compile_intent(intent, cat).render()
        # Strip the LIMIT to keep aggregation semantics; execute directly
        result = con.execute(sql).fetchone()
        # gross_revenue (SUM amount FILTER paid) / order_count (COUNT *) over all rows:
        # paid rows: o1=100, o2=200, o4=300 -> SUM=600
        # total rows: 4 -> COUNT=4
        # ratio = 600/4 = 150.0
        assert float(result[0]) == 150.0

    def test_order_by_renders(self) -> None:
        con = _populated_db_with_data()
        cat = Catalog(con)
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["gross_revenue"],
            "order_by": [{"key": "gross_revenue", "direction": "desc"}],
        })
        sql = compile_intent(intent, cat).render()
        assert 'ORDER BY "gross_revenue" DESC' in sql

    def test_unknown_source_raises_compile_error(self) -> None:
        con = _populated_db_with_data()
        cat = Catalog(con)
        intent = QueryIntent.model_construct(
            source="ghost", metrics=["x"], dimensions=[],
            filters=[], order_by=[], limit=1000,
        )
        with pytest.raises(CompileError, match="unknown source"):
            compile_intent(intent, cat)


# ---------------------------------------------------------------------------
# Runner end-to-end
# ---------------------------------------------------------------------------

class TestRunDSLQuery:
    def test_happy_path_returns_filtered_rows(self) -> None:
        con = _populated_db_with_data()
        gov = DefaultGovernance(con, observability=DefaultObservability(con))
        result = run_dsl_query(
            con, gov, _ident("analyst_west", region="west"),
            {"source": "orders", "metrics": ["gross_revenue"]},
        )
        assert result.stage == "executed"
        assert result.query_result is not None
        assert result.query_result.status == "ok"
        # Only west (region='west') rows that match status='paid':
        # o1 (west, paid, 100) -> sum = 100
        assert float(result.query_result.rows[0][0]) == 100.0

    def test_dimension_breakdown(self) -> None:
        con = _populated_db_with_data()
        gov = DefaultGovernance(con, observability=DefaultObservability(con))
        result = run_dsl_query(
            con, gov, _ident("analyst_west", region="west"),
            {
                "source": "orders",
                "metrics": ["gross_revenue"],
                "dimensions": ["orders.region"],
            },
        )
        assert result.query_result.status == "ok"
        # Region filter from policy means only 'west' shows up
        regions = {row[0] for row in result.query_result.rows}
        assert regions == {"west"}

    def test_invalid_intent_pre_compile_failure(self) -> None:
        con = _populated_db_with_data()
        gov = DefaultGovernance(con, observability=DefaultObservability(con))
        result = run_dsl_query(
            con, gov, _ident("analyst_west", region="west"),
            {"source": "orders", "metrics": ["ghost_metric"]},
        )
        assert result.stage == "validated"
        assert result.query_result is None
        assert "ghost_metric" in (result.pre_compile_error or "")

    def test_empty_projection_pre_compile_failure(self) -> None:
        con = _populated_db_with_data()
        gov = DefaultGovernance(con, observability=DefaultObservability(con))
        result = run_dsl_query(
            con, gov, _ident("analyst_west", region="west"),
            {"source": "orders"},  # no metrics, no dimensions
        )
        assert result.stage == "parsed"
        assert result.query_result is None

    def test_attr_ref_in_filter_rejected(self) -> None:
        con = _populated_db_with_data()
        gov = DefaultGovernance(con, observability=DefaultObservability(con))
        result = run_dsl_query(
            con, gov, _ident("analyst_west", region="west"),
            {
                "source": "orders",
                "metrics": ["gross_revenue"],
                "filters": [
                    {"op": "eq", "column": "region", "value": {"$attr": "region"}}
                ],
            },
        )
        assert result.stage == "validated"
        assert "attribute" in (result.pre_compile_error or "").lower()

    def test_audit_log_captures_intent_for_successful_query(self) -> None:
        con = _populated_db_with_data()
        gov = DefaultGovernance(con, observability=DefaultObservability(con))
        result = run_dsl_query(
            con, gov, _ident("analyst_west", region="west", user="alice"),
            {"source": "orders", "metrics": ["gross_revenue"]},
        )
        row = con.execute(
            "SELECT nl_prompt, status FROM gibran_query_log WHERE query_id = ?",
            [result.query_result.query_id],
        ).fetchone()
        assert row[1] == "ok"
        # nl_prompt holds the intent JSON
        assert '"source": "orders"' in row[0] or '"source":"orders"' in row[0]
        assert "gross_revenue" in row[0]

    def test_audit_log_for_pre_compile_failure(self) -> None:
        con = _populated_db_with_data()
        gov = DefaultGovernance(con, observability=DefaultObservability(con))
        result = run_dsl_query(
            con, gov, _ident("analyst_west", region="west"),
            {"source": "orders", "metrics": ["ghost"]},
        )
        row = con.execute(
            "SELECT status, deny_reason, generated_sql, nl_prompt "
            "FROM gibran_query_log WHERE query_id = ?",
            [result.query_id],
        ).fetchone()
        assert row[0] == "error"
        assert "ghost" in (row[1] or "")
        assert row[2] == ""  # no SQL was generated
        assert "ghost" in (row[3] or "")  # intent JSON preserved

    def test_governance_still_runs_on_compiled_sql(self) -> None:
        """If the DSL passes validate_intent but the compiled SQL touches
        a column the role doesn't have access to, governance.evaluate
        (called inside run_sql_query) must still deny."""
        con = _populated_db_with_data()
        gov = DefaultGovernance(con, observability=DefaultObservability(con))
        # external_partner has grants {order_id, amount, order_date} but NOT status.
        # gross_revenue metric internally filters on status='paid'. So the compiled
        # SQL references the 'status' column (inside FILTER WHERE), which
        # external_partner cannot see.
        result = run_dsl_query(
            con, gov, _ident("external_partner"),
            {"source": "orders", "metrics": ["gross_revenue"]},
        )
        assert result.stage == "executed"
        assert result.query_result.status == "denied"
        assert result.query_result.deny_reason is DenyReason.COLUMN_DENIED
        assert result.query_result.deny_detail == "status"


# ---------------------------------------------------------------------------
# FILTER (WHERE ...) clauses for metric filters
# ---------------------------------------------------------------------------

class TestFilterAggregateRendering:
    """Metric-level filters live INSIDE the aggregate via FILTER (WHERE ...).
    They do NOT pollute the query's WHERE clause -- this matters when two
    metrics with different filters appear in the same query."""

    def test_metric_filter_is_inside_aggregate(self) -> None:
        con = _populated_db_with_data()
        cat = Catalog(con)
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["gross_revenue"]
        })
        sql = compile_intent(intent, cat).render()
        assert "SUM(amount) FILTER (WHERE status = 'paid')" in sql
        # No WHERE clause should be present (no DSL filters, metric filter is FILTER'd)
        assert "\nWHERE " not in sql

    def test_count_metric_with_no_filter_no_filter_clause(self) -> None:
        con = _populated_db_with_data()
        cat = Catalog(con)
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["order_count"]
        })
        sql = compile_intent(intent, cat).render()
        assert 'COUNT(*) AS "order_count"' in sql
        # order_count has no filter -> no FILTER clause attached
        assert "FILTER (WHERE" not in sql

    def test_two_metrics_different_filters_dont_contaminate(self) -> None:
        """If both gross_revenue (status=paid) and order_count (no filter)
        appear in the same query, the FILTER syntax isolates them: order_count
        sees ALL rows, gross_revenue sees only paid ones."""
        con = _populated_db_with_data()
        cat = Catalog(con)
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["order_count", "gross_revenue"],
        })
        sql = compile_intent(intent, cat).render()
        result = con.execute(sql).fetchone()
        # order_count = 4 (all rows), gross_revenue = 100+200+300 = 600 (paid only)
        assert result[0] == 4
        assert float(result[1]) == 600.0


# ---------------------------------------------------------------------------
# Expression metric template resolution
# ---------------------------------------------------------------------------

class TestExpressionMetricTemplate:
    def test_expression_metric_template_resolves(self) -> None:
        con = _populated_db_with_data()
        cat = Catalog(con)
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["revenue_per_paid_order"]
        })
        sql = compile_intent(intent, cat).render()
        assert "{gross_revenue}" not in sql
        assert "{order_count}" not in sql
        assert "SUM(amount)" in sql
        assert "COUNT(*)" in sql
        assert "NULLIF" in sql  # user-authored guard in the template

    def test_expression_metric_executes_correctly(self) -> None:
        con = _populated_db_with_data()
        cat = Catalog(con)
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["revenue_per_paid_order"]
        })
        sql = compile_intent(intent, cat).render()
        result = con.execute(sql).fetchone()
        # Same math as avg_order_value over all rows: 600 / 4 = 150
        assert float(result[0]) == 150.0

    def test_expression_metric_cycle_detected(self) -> None:
        """An expression metric whose template references itself produces an
        infinite-recursion-style cycle. The compiler's _seen tracker catches
        it (sync.loader doesn't track expression deps in V1)."""
        con = _populated_db_with_data()
        # Manually insert a self-referencing expression metric
        con.execute(
            "INSERT INTO gibran_metrics "
            "(metric_id, source_id, display_name, metric_type, current_version) "
            "VALUES ('self_ref', 'orders', 'Self Ref', 'expression', 1)"
        )
        con.execute(
            "INSERT INTO gibran_metric_versions "
            "(metric_id, version, expression) "
            "VALUES ('self_ref', 1, '{self_ref} + 1')"
        )
        cat = Catalog(con)
        # Build the intent via model_construct (skip validation) since validate_intent
        # would reject (the metric isn't in the policy's AllowedSchema for the
        # default analyst_west role -- but the compiler-level cycle check is the
        # behavior we're pinning).
        intent = QueryIntent.model_construct(
            source="orders", metrics=["self_ref"], dimensions=[],
            filters=[], having=[], order_by=[], limit=1000,
        )
        with pytest.raises(CompileError, match="cycle"):
            compile_intent(intent, cat)


# ---------------------------------------------------------------------------
# HAVING clauses
# ---------------------------------------------------------------------------

class TestHavingValidation:
    def test_having_metric_must_be_in_intent_metrics(self) -> None:
        con = _populated_db_with_data()
        from gibran.governance.default import DefaultGovernance
        gov = DefaultGovernance(con, observability=DefaultObservability(con))
        schema = gov.preview_schema(_ident("analyst_west", region="west"), "orders")

        intent = QueryIntent.model_validate({
            "source": "orders",
            "metrics": ["gross_revenue"],
            "having": [{"op": "gt", "metric": "order_count", "value": 0}],
        })
        with pytest.raises(IntentValidationError, match="having.metric"):
            validate_intent(intent, schema)

    def test_having_value_must_be_scalar_for_comparison_op(self) -> None:
        with pytest.raises(ValidationError, match="scalar"):
            HavingClause.model_validate(
                {"op": "gt", "metric": "x", "value": [1, 2]}
            )

    def test_having_value_must_be_list_for_in(self) -> None:
        with pytest.raises(ValidationError, match="non-empty list"):
            HavingClause.model_validate(
                {"op": "in", "metric": "x", "value": 42}
            )


class TestHavingCompilation:
    def test_having_emits_after_group_by(self) -> None:
        con = _populated_db_with_data()
        cat = Catalog(con)
        intent = QueryIntent.model_validate({
            "source": "orders",
            "metrics": ["gross_revenue"],
            "dimensions": ["orders.region"],
            "having": [{"op": "gt", "metric": "gross_revenue", "value": 150}],
        })
        sql = compile_intent(intent, cat).render()
        # HAVING clause references the metric alias
        assert 'HAVING ("gross_revenue" > 150)' in sql
        # And appears after GROUP BY in the SQL
        assert sql.index("GROUP BY") < sql.index("HAVING")

    def test_having_with_in_op(self) -> None:
        con = _populated_db_with_data()
        cat = Catalog(con)
        intent = QueryIntent.model_validate({
            "source": "orders",
            "metrics": ["order_count"],
            "dimensions": ["orders.region"],
            "having": [{"op": "in", "metric": "order_count", "value": [1, 2]}],
        })
        sql = compile_intent(intent, cat).render()
        assert '"order_count" IN (1, 2)' in sql

    def test_having_executes_against_duckdb(self) -> None:
        con = _populated_db_with_data()
        cat = Catalog(con)
        # Group by region across ALL rows (no governance row filter here -- this
        # is a direct compile-execute test, not through run_dsl_query).
        intent = QueryIntent.model_validate({
            "source": "orders",
            "metrics": ["gross_revenue"],
            "dimensions": ["orders.region"],
            "having": [{"op": "gt", "metric": "gross_revenue", "value": 150}],
            "order_by": [{"key": "orders.region"}],
        })
        sql = compile_intent(intent, cat).render()
        rows = con.execute(sql).fetchall()
        # paid gross revenue by region:
        #   west: 100 (o1)
        #   east: 200 (o2)
        #   north: 300 (o4)
        # HAVING > 150 -> {east, north}
        regions = [r[0] for r in rows]
        assert regions == ["east", "north"]

    def test_having_through_runner_with_governance(self) -> None:
        """End-to-end: HAVING combined with governance row filter."""
        con = _populated_db_with_data()
        from gibran.governance.default import DefaultGovernance
        gov = DefaultGovernance(con, observability=DefaultObservability(con))
        # analyst_west sees only west rows. After grouping, only {west: 100}
        # exists. HAVING > 50 keeps it. HAVING > 200 drops it.
        result_kept = run_dsl_query(
            con, gov, _ident("analyst_west", region="west"),
            {
                "source": "orders", "metrics": ["gross_revenue"],
                "dimensions": ["orders.region"],
                "having": [{"op": "gt", "metric": "gross_revenue", "value": 50}],
            },
        )
        assert result_kept.query_result.status == "ok"
        assert len(result_kept.query_result.rows) == 1

        result_dropped = run_dsl_query(
            con, gov, _ident("analyst_west", region="west"),
            {
                "source": "orders", "metrics": ["gross_revenue"],
                "dimensions": ["orders.region"],
                "having": [{"op": "gt", "metric": "gross_revenue", "value": 200}],
            },
        )
        assert result_dropped.query_result.status == "ok"
        assert len(result_dropped.query_result.rows) == 0


# ---------------------------------------------------------------------------
# Percentile metric primitive
# ---------------------------------------------------------------------------

class TestPercentileMetric:
    def test_percentile_pydantic_requires_column_and_p(self) -> None:
        from gibran.sync.yaml_schema import MetricConfig
        with pytest.raises(ValidationError, match="percentile requires"):
            MetricConfig.model_validate({
                "id": "x", "source": "orders", "display_name": "X",
                "type": "percentile", "p": 0.5,
            })

    def test_percentile_pydantic_rejects_p_outside_unit_interval(self) -> None:
        from gibran.sync.yaml_schema import MetricConfig
        for bad in (0, 1, 1.5, -0.1):
            with pytest.raises(ValidationError, match="must be in"):
                MetricConfig.model_validate({
                    "id": "x", "source": "orders", "display_name": "X",
                    "type": "percentile", "column": "amount", "p": bad,
                })

    def test_percentile_compiles_to_quantile_cont(self) -> None:
        con = _populated_db_with_data()
        cat = Catalog(con)
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["p95_amount"]
        })
        sql = compile_intent(intent, cat).render()
        assert "QUANTILE_CONT(amount, 0.95)" in sql

    def test_percentile_executes_correctly(self) -> None:
        con = _populated_db_with_data()
        cat = Catalog(con)
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["p95_amount"]
        })
        sql = compile_intent(intent, cat).render()
        result = con.execute(sql).fetchone()
        # 4 rows of amount in {100, 200, 50, 300}; p95 ≈ 285 with linear interp
        assert 250.0 <= float(result[0]) <= 300.0

    def test_percentile_combines_with_dimension(self) -> None:
        """Percentile is a regular aggregate -- works with GROUP BY."""
        con = _populated_db_with_data()
        cat = Catalog(con)
        intent = QueryIntent.model_validate({
            "source": "orders",
            "metrics": ["p95_amount"],
            "dimensions": ["orders.region"],
        })
        sql = compile_intent(intent, cat).render()
        assert "GROUP BY" in sql
        rows = con.execute(sql).fetchall()
        # 3 distinct regions: west, east, north
        assert len(rows) == 3

    def test_percentile_through_runner_with_governance(self) -> None:
        con = _populated_db_with_data()
        from gibran.governance.default import DefaultGovernance
        gov = DefaultGovernance(con, observability=DefaultObservability(con))
        result = run_dsl_query(
            con, gov, _ident("analyst_west", region="west"),
            {"source": "orders", "metrics": ["p95_amount"]},
        )
        assert result.query_result.status == "ok"
        # Only west rows: amounts {100, 50}; p95 ≈ 97.5
        assert 50.0 <= float(result.query_result.rows[0][0]) <= 100.0


# ---------------------------------------------------------------------------
# Rolling-window metric primitive
# ---------------------------------------------------------------------------

class TestRollingWindowMetric:
    def test_rolling_window_pydantic_requires_all_fields(self) -> None:
        from gibran.sync.yaml_schema import MetricConfig
        with pytest.raises(ValidationError, match="rolling_window requires"):
            MetricConfig.model_validate({
                "id": "x", "source": "orders", "display_name": "X",
                "type": "rolling_window",
                "column": "amount", "aggregate": "sum",
                # missing window + order_by_column
            })

    def test_rolling_window_compiles_to_over_clause(self) -> None:
        con = _populated_db_with_data()
        cat = Catalog(con)
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["revenue_7d_rolling"]
        })
        sql = compile_intent(intent, cat).render()
        assert "OVER" in sql
        assert "ORDER BY order_date" in sql
        assert "RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW" in sql
        # FILTER (WHERE ...) for metric filter
        assert "FILTER (WHERE status = 'paid')" in sql

    def test_rolling_window_executes_per_row(self) -> None:
        """Window function emits one result per source row (no GROUP BY)."""
        con = _populated_db_with_data()
        cat = Catalog(con)
        intent = QueryIntent.model_validate({
            "source": "orders", "metrics": ["revenue_7d_rolling"],
            "limit": 100,
        })
        sql = compile_intent(intent, cat).render()
        rows = con.execute(sql).fetchall()
        # Fixture has 4 rows; window emits per-row -> 4 rolling values
        assert len(rows) == 4

    def test_rolling_window_forbids_dimensions(self) -> None:
        """rolling_window + dimensions is a V1 validation error."""
        con = _populated_db_with_data()
        from gibran.governance.default import DefaultGovernance
        gov = DefaultGovernance(con, observability=DefaultObservability(con))
        result = run_dsl_query(
            con, gov, _ident("analyst_west", region="west"),
            {
                "source": "orders",
                "metrics": ["revenue_7d_rolling"],
                "dimensions": ["orders.region"],
            },
        )
        assert result.stage == "validated"
        assert "rolling_window" in (result.pre_compile_error or "")
        assert "dimensions" in (result.pre_compile_error or "")

    def test_rolling_window_through_runner(self) -> None:
        con = _populated_db_with_data()
        from gibran.governance.default import DefaultGovernance
        gov = DefaultGovernance(con, observability=DefaultObservability(con))
        result = run_dsl_query(
            con, gov, _ident("analyst_west", region="west"),
            {"source": "orders", "metrics": ["revenue_7d_rolling"], "limit": 100},
        )
        assert result.query_result.status == "ok"
        # analyst_west sees only west rows (o1, o3) -> 2 per-row results
        assert len(result.query_result.rows) == 2
