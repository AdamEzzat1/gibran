"""End-to-end DefaultGovernance tests against a fully-synced fixture catalog."""
from pathlib import Path

import duckdb
import pytest

from rumi.governance.default import DefaultGovernance
from rumi.governance.types import DenyReason, IdentityContext
from rumi.observability.default import DefaultObservability
from rumi.sync.applier import apply as apply_config
from rumi.sync.loader import load as load_config
from rumi.sync.migrations import apply_all as apply_migrations


FIXTURES = Path(__file__).parent / "fixtures"
MIGRATIONS = Path(__file__).parent.parent / "migrations"


def _populated_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    apply_migrations(con, MIGRATIONS)
    apply_config(con, load_config(FIXTURES / "rumi.yaml"))
    return con


def _ident(role: str, **attrs: str) -> IdentityContext:
    return IdentityContext(
        user_id=f"user_{role}", role_id=role, attributes=dict(attrs), source="test"
    )


# ---------------------------------------------------------------------------
# preview_schema
# ---------------------------------------------------------------------------

class TestPreviewSchema:
    def test_analyst_west_sees_all_columns_default_allow(self) -> None:
        gov = DefaultGovernance(_populated_db())
        schema = gov.preview_schema(_ident("analyst_west", region="west"), "orders")
        assert schema.source_id == "orders"
        assert schema.source_display_name == "Orders"
        names = {c.name for c in schema.columns}
        assert names == {
            "order_id", "amount", "order_date", "status", "region", "customer_email"
        }
        assert {d.dimension_id for d in schema.dimensions} == {
            "orders.region", "orders.order_date"
        }
        assert {m.metric_id for m in schema.metrics} == {
            "order_count", "gross_revenue", "avg_order_value", "revenue_per_paid_order",
            "p95_amount", "revenue_7d_rolling",
        }

    def test_external_partner_sees_only_granted_columns_default_deny(self) -> None:
        gov = DefaultGovernance(_populated_db())
        schema = gov.preview_schema(_ident("external_partner"), "orders")
        names = {c.name for c in schema.columns}
        assert names == {"order_id", "amount", "order_date"}
        # PII column 'customer_email' must NOT be in this schema
        assert "customer_email" not in names
        # Dimensions only include allowed columns
        dim_columns = {d.column_name for d in schema.dimensions}
        assert dim_columns <= names

    def test_unknown_role_returns_empty_schema(self) -> None:
        gov = DefaultGovernance(_populated_db())
        schema = gov.preview_schema(_ident("nonexistent_role"), "orders")
        assert schema.columns == ()
        assert schema.dimensions == ()
        assert schema.metrics == ()

    def test_unknown_source_raises(self) -> None:
        gov = DefaultGovernance(_populated_db())
        with pytest.raises(ValueError, match="unknown source"):
            gov.preview_schema(_ident("analyst_west"), "ghost_source")

    def test_metric_views_carry_dependencies(self) -> None:
        gov = DefaultGovernance(_populated_db())
        schema = gov.preview_schema(_ident("analyst_west", region="west"), "orders")
        avg = next(m for m in schema.metrics if m.metric_id == "avg_order_value")
        assert set(avg.depends_on) == {"order_count", "gross_revenue"}
        order_count = next(m for m in schema.metrics if m.metric_id == "order_count")
        assert order_count.depends_on == ()

    def test_example_values_never_populated_in_v1(self) -> None:
        gov = DefaultGovernance(_populated_db())
        schema = gov.preview_schema(_ident("analyst_west", region="west"), "orders")
        for col in schema.columns:
            assert col.example_values is None

    def test_cache_version_is_returned(self) -> None:
        gov = DefaultGovernance(_populated_db())
        schema = gov.preview_schema(_ident("analyst_west", region="west"), "orders")
        # source.schema_version=1 (default), policy.schema_version=1 (default)
        assert schema.cache_version == (1, 1)


# ---------------------------------------------------------------------------
# evaluate
# ---------------------------------------------------------------------------

class TestEvaluate:
    def test_allowed_with_attribute_filter_injected(self) -> None:
        gov = DefaultGovernance(_populated_db())
        decision = gov.evaluate(
            _ident("analyst_west", region="west"),
            frozenset({"orders"}),
            frozenset({"order_date", "amount", "region"}),
            ("gross_revenue",),
        )
        assert decision.allowed is True
        assert decision.deny_reason is None
        assert decision.injected_filter_sql == '("region" = \'west\')'
        assert decision.metric_versions == (("gross_revenue", 1),)

    def test_external_partner_allowed_for_granted_columns(self) -> None:
        gov = DefaultGovernance(_populated_db())
        decision = gov.evaluate(
            _ident("external_partner"),
            frozenset({"orders"}),
            frozenset({"order_id", "amount"}),
            (),
        )
        assert decision.allowed is True
        assert decision.injected_filter_sql == '("region" = \'west\')'

    def test_column_denied_for_pii_outside_grants(self) -> None:
        gov = DefaultGovernance(_populated_db())
        decision = gov.evaluate(
            _ident("external_partner"),
            frozenset({"orders"}),
            frozenset({"order_id", "customer_email"}),
            (),
        )
        assert decision.allowed is False
        assert decision.deny_reason is DenyReason.COLUMN_DENIED
        assert decision.deny_detail == "customer_email"

    def test_no_policy_for_unknown_role(self) -> None:
        gov = DefaultGovernance(_populated_db())
        decision = gov.evaluate(
            _ident("ghost_role"),
            frozenset({"orders"}),
            frozenset({"amount"}),
            (),
        )
        assert decision.allowed is False
        assert decision.deny_reason is DenyReason.NO_POLICY
        assert "ghost_role" in (decision.deny_detail or "")

    def test_unknown_metric_denied(self) -> None:
        gov = DefaultGovernance(_populated_db())
        decision = gov.evaluate(
            _ident("analyst_west", region="west"),
            frozenset({"orders"}),
            frozenset(),
            ("ghost_metric",),
        )
        assert decision.allowed is False
        assert decision.deny_reason is DenyReason.METRIC_DENIED
        assert decision.deny_detail == "ghost_metric"

    def test_attribute_missing_denied(self) -> None:
        gov = DefaultGovernance(_populated_db())
        # analyst_west's policy references {"$attr": "region"}, but identity has no attrs
        decision = gov.evaluate(
            _ident("analyst_west"),  # no region attribute
            frozenset({"orders"}),
            frozenset({"amount"}),
            (),
        )
        assert decision.allowed is False
        assert decision.deny_reason is DenyReason.ATTRIBUTE_MISSING
        assert decision.deny_detail == "region"

    def test_cross_source_raises_not_implemented(self) -> None:
        gov = DefaultGovernance(_populated_db())
        with pytest.raises(NotImplementedError, match="cross-source"):
            gov.evaluate(
                _ident("analyst_west", region="west"),
                frozenset({"orders", "other"}),
                frozenset(),
                (),
            )

    def test_no_row_filter_means_no_injected_sql(self) -> None:
        # Construct a synthetic policy with no row_filter to check this path
        con = _populated_db()
        # Add a role + policy without a row_filter
        con.execute("INSERT INTO rumi_roles VALUES ('admin', 'Admin')")
        con.execute(
            "INSERT INTO rumi_policies (policy_id, role_id, source_id, default_column_mode) "
            "VALUES ('admin_orders', 'admin', 'orders', 'allow')"
        )
        gov = DefaultGovernance(con)
        decision = gov.evaluate(
            _ident("admin"),
            frozenset({"orders"}),
            frozenset({"amount"}),
            (),
        )
        assert decision.allowed is True
        assert decision.injected_filter_sql is None

    def test_compiled_filter_produces_executable_sql(self) -> None:
        """The injected SQL must be valid DuckDB so it can be ANDed into a query."""
        con = _populated_db()
        gov = DefaultGovernance(con)
        decision = gov.evaluate(
            _ident("analyst_west", region="west"),
            frozenset({"orders"}),
            frozenset({"region"}),
            (),
        )
        assert decision.injected_filter_sql is not None
        # Build a synthetic table and apply the filter
        con.execute(
            "CREATE TABLE _orders_demo (region VARCHAR);"
            "INSERT INTO _orders_demo VALUES ('west'), ('east');"
        )
        rows = con.execute(
            f"SELECT region FROM _orders_demo WHERE {decision.injected_filter_sql}"
        ).fetchall()
        assert rows == [("west",)]


# ---------------------------------------------------------------------------
# validate_alternatives
# ---------------------------------------------------------------------------

class TestValidateAlternatives:
    def test_returns_one_decision_per_candidate(self) -> None:
        gov = DefaultGovernance(_populated_db())
        identity = _ident("external_partner")
        candidates = (
            (frozenset({"order_id", "amount"}), ()),           # allowed
            (frozenset({"customer_email"}), ()),                # column denied
            (frozenset(), ("ghost_metric",)),                   # metric denied
        )
        decisions = gov.validate_alternatives(
            identity, frozenset({"orders"}), candidates
        )
        assert len(decisions) == 3
        assert decisions[0].allowed is True
        assert decisions[1].deny_reason is DenyReason.COLUMN_DENIED
        assert decisions[2].deny_reason is DenyReason.METRIC_DENIED

    def test_empty_candidates_returns_empty_tuple(self) -> None:
        gov = DefaultGovernance(_populated_db())
        decisions = gov.validate_alternatives(
            _ident("analyst_west", region="west"), frozenset({"orders"}), ()
        )
        assert decisions == ()


# ---------------------------------------------------------------------------
# evaluate with observability
# ---------------------------------------------------------------------------

def _populated_with_passing_health() -> tuple[duckdb.DuckDBPyConnection, DefaultObservability]:
    con = _populated_db()
    obs = DefaultObservability(con)
    obs.record_run("orders_amount_not_null", "quality", True)
    obs.record_run("orders_freshness_24h", "freshness", True)
    return con, obs


class TestEvaluateWithObservability:
    def test_clean_health_allows_query(self) -> None:
        con, obs = _populated_with_passing_health()
        gov = DefaultGovernance(con, observability=obs)
        decision = gov.evaluate(
            _ident("analyst_west", region="west"),
            frozenset({"orders"}),
            frozenset({"order_id", "amount"}),
            (),
        )
        assert decision.allowed is True
        assert decision.quality_holds == ()

    def test_failed_quality_rule_blocks_query(self) -> None:
        con, obs = _populated_with_passing_health()
        # Now record a failure for the not_null rule
        obs.record_run("orders_amount_not_null", "quality", False, {"null_count": 5})
        gov = DefaultGovernance(con, observability=obs)
        decision = gov.evaluate(
            _ident("analyst_west", region="west"),
            frozenset({"orders"}),
            frozenset({"order_id"}),
            (),
        )
        assert decision.allowed is False
        assert decision.deny_reason is DenyReason.QUALITY_BLOCK
        assert "orders_amount_not_null" in (decision.deny_detail or "")
        assert "orders_amount_not_null" in decision.quality_holds

    def test_stale_freshness_blocks_query(self) -> None:
        con, obs = _populated_with_passing_health()
        # Replace the fresh freshness run with a stale one (default window 300s)
        con.execute(
            "DELETE FROM rumi_quality_runs WHERE rule_id = 'orders_freshness_24h'"
        )
        con.execute(
            "INSERT INTO rumi_quality_runs "
            "(run_id, rule_id, rule_kind, passed, ran_at) "
            "VALUES ('stale1', 'orders_freshness_24h', 'freshness', TRUE, "
            "now() - INTERVAL '2 hours')"
        )
        gov = DefaultGovernance(con, observability=obs)
        decision = gov.evaluate(
            _ident("analyst_west", region="west"),
            frozenset({"orders"}),
            frozenset({"order_id"}),
            (),
        )
        assert decision.allowed is False
        assert decision.deny_reason is DenyReason.FRESHNESS_BLOCK
        assert "stale" in (decision.deny_detail or "").lower() or \
               "orders_freshness_24h" in (decision.deny_detail or "")

    def test_never_run_rule_blocks_query(self) -> None:
        # No record_run calls -> all rules show as never_run
        con = _populated_db()
        obs = DefaultObservability(con)
        gov = DefaultGovernance(con, observability=obs)
        decision = gov.evaluate(
            _ident("analyst_west", region="west"),
            frozenset({"orders"}),
            frozenset({"order_id"}),
            (),
        )
        assert decision.allowed is False
        # The first failure could be either quality or freshness depending on
        # row order. Either way the query is blocked.
        assert decision.deny_reason in (
            DenyReason.QUALITY_BLOCK, DenyReason.FRESHNESS_BLOCK
        )
        assert len(decision.quality_holds) >= 1

    def test_no_policy_beats_quality_block(self) -> None:
        # Even with broken health, NO_POLICY for an unknown role wins
        # (you cannot leak the existence of governance failures to roles
        # that have no relationship with the source).
        con = _populated_db()
        obs = DefaultObservability(con)
        # Record a failing quality run to make the source unhealthy
        obs.record_run("orders_amount_not_null", "quality", False)
        gov = DefaultGovernance(con, observability=obs)
        decision = gov.evaluate(
            _ident("ghost_role"),
            frozenset({"orders"}),
            frozenset({"order_id"}),
            (),
        )
        assert decision.allowed is False
        assert decision.deny_reason is DenyReason.NO_POLICY

    def test_no_observability_skips_health_check(self) -> None:
        # Constructed without observability -> existing behavior preserved
        con = _populated_db()
        gov = DefaultGovernance(con)  # no obs param
        decision = gov.evaluate(
            _ident("analyst_west", region="west"),
            frozenset({"orders"}),
            frozenset({"order_id"}),
            (),
        )
        assert decision.allowed is True
