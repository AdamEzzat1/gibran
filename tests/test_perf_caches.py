"""Tests for the Tier 4 Item 15 perf primitives: plan cache, result
cache, and materialized metrics.

All three are opt-in / fallback-safe -- the bare correctness path
remains uncached + un-materialized. These tests pin the cache hit/miss
shape and the materialized-table routing.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from gibran.dsl.compile import Catalog, compile_intent
from gibran.dsl.plan_cache import (
    PlanCache,
    bump_catalog_generation,
    catalog_generation,
    compile_intent_cached,
    default_cache,
)
from gibran.dsl.run import run_dsl_query
from gibran.dsl.types import QueryIntent
from gibran.execution.result_cache import (
    CachedResult,
    ResultCache,
    bump_source_health_generation,
    cache_key as result_cache_key,
    lookup as result_lookup,
    source_health_generation,
    store as result_store,
)
from gibran.execution.sql import run_sql_query
from gibran.governance.default import DefaultGovernance
from gibran.governance.types import IdentityContext
from gibran.sync.applier import apply as apply_config
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
    return con


def _admin(con: duckdb.DuckDBPyConnection) -> IdentityContext:
    con.execute(
        "INSERT INTO gibran_roles (role_id, display_name) "
        "VALUES ('admin', 'Admin')"
    )
    con.execute(
        "INSERT INTO gibran_policies "
        "(policy_id, role_id, source_id, default_column_mode) "
        "VALUES ('admin_orders', 'admin', 'orders', 'allow')"
    )
    return IdentityContext(
        user_id="admin", role_id="admin", attributes={}, source="test"
    )


# ---------------------------------------------------------------------------
# PlanCache
# ---------------------------------------------------------------------------

class TestPlanCache:
    def test_hit_after_miss(self) -> None:
        cache = PlanCache(max_size=8)
        con = _populated_db()
        catalog = Catalog(con)
        intent = QueryIntent(source="orders", metrics=["order_count"])
        assert cache.hits == 0
        compile_intent_cached(intent, catalog, cache=cache)
        assert cache.hits == 0 and cache.misses == 1
        compile_intent_cached(intent, catalog, cache=cache)
        assert cache.hits == 1 and cache.misses == 1

    def test_different_intents_miss(self) -> None:
        cache = PlanCache(max_size=8)
        con = _populated_db()
        catalog = Catalog(con)
        compile_intent_cached(
            QueryIntent(source="orders", metrics=["order_count"]),
            catalog, cache=cache,
        )
        compile_intent_cached(
            QueryIntent(source="orders", metrics=["gross_revenue"]),
            catalog, cache=cache,
        )
        assert cache.misses == 2

    def test_lru_eviction(self) -> None:
        cache = PlanCache(max_size=2)
        con = _populated_db()
        catalog = Catalog(con)
        intents = [
            QueryIntent(source="orders", metrics=["order_count"]),
            QueryIntent(source="orders", metrics=["gross_revenue"]),
            QueryIntent(source="orders", metrics=["avg_order_value"]),
        ]
        for i in intents:
            compile_intent_cached(i, catalog, cache=cache)
        # First one evicted; refetching it counts as a miss.
        compile_intent_cached(intents[0], catalog, cache=cache)
        assert cache.misses == 4

    def test_catalog_generation_bumps_invalidate(self) -> None:
        # Same intent, same Python process, but a sync between calls
        # bumps the catalog generation and forces a recompile.
        cache = PlanCache(max_size=8)
        con = _populated_db()
        catalog = Catalog(con)
        intent = QueryIntent(source="orders", metrics=["order_count"])
        compile_intent_cached(intent, catalog, cache=cache)
        compile_intent_cached(intent, catalog, cache=cache)
        assert cache.hits == 1
        bump_catalog_generation(con)
        compile_intent_cached(intent, catalog, cache=cache)
        assert cache.misses == 2  # the previous hit was invalidated


# ---------------------------------------------------------------------------
# ResultCache
# ---------------------------------------------------------------------------

class TestResultCache:
    def test_lookup_returns_stored_value(self) -> None:
        cache = ResultCache(max_size=4)
        con = _populated_db()
        ident = _admin(con)
        cg = catalog_generation(con)
        hg = source_health_generation(con)
        key = result_cache_key("SELECT 1", ident, cg, hg)
        result_store(key, CachedResult(rows=((1,),), columns=("x",)), cache=cache)
        assert cache.get(key) is not None

    def test_health_generation_bump_invalidates(self) -> None:
        # After bumping source-health generation, the same lookup key
        # changes, so the previously-stored result isn't returned.
        cache = ResultCache(max_size=4)
        con = _populated_db()
        ident = _admin(con)
        key1, _ = result_lookup(con, "SELECT 1", ident, cache=cache)
        result_store(key1, CachedResult(rows=((1,),), columns=("x",)), cache=cache)
        bump_source_health_generation(con)
        key2, cached = result_lookup(con, "SELECT 1", ident, cache=cache)
        assert key1 != key2
        assert cached is None

    def test_end_to_end_cache_hit_skips_execute(self) -> None:
        # Run the same query twice; the second time should hit the
        # result cache. We can't observe the cache directly via
        # run_sql_query's return value, but we CAN observe by mutating
        # the underlying source between calls -- the cached result
        # should NOT reflect the new row.
        con = _populated_db()
        ident = _admin(con)
        gov = DefaultGovernance(con)
        first = run_sql_query(
            con, gov, ident,
            "SELECT COUNT(*) AS n FROM orders",
        )
        assert first.rows == ((3,),)
        # Mutate the source AFTER the first query.
        con.execute(
            "INSERT INTO orders VALUES "
            "('o4', 99, TIMESTAMP '2026-03-01', 'paid', 'west', 'd@x')"
        )
        second = run_sql_query(
            con, gov, ident,
            "SELECT COUNT(*) AS n FROM orders",
        )
        # Cached: returns the original row count of 3 even though the
        # source now has 4 rows.
        assert second.rows == ((3,),)


# ---------------------------------------------------------------------------
# Catalog / health generation bumps wired through sync + check
# ---------------------------------------------------------------------------

class TestGenerationBumps:
    def test_apply_config_bumps_catalog_generation(self) -> None:
        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        before = catalog_generation(con)
        apply_config(con, load_config(FIXTURES / "gibran.yaml"))
        after = catalog_generation(con)
        assert before != after

    def test_run_checks_bumps_health_generation(self) -> None:
        from gibran.observability.default import DefaultObservability
        from gibran.observability.runner import run_checks
        con = _populated_db()
        # Need to also create source_health table state via observability
        obs = DefaultObservability(con)
        before = source_health_generation(con)
        run_checks(con, "orders", obs)
        after = source_health_generation(con)
        assert before != after


# ---------------------------------------------------------------------------
# Materialized metrics
# ---------------------------------------------------------------------------

class TestMaterializedMetrics:
    def test_validation_rejects_incompatible_metric_types(self) -> None:
        from gibran.sync.yaml_schema import MetricConfig
        with pytest.raises(ValueError, match="cannot be materialized"):
            MetricConfig(
                id="bad", source="orders", display_name="bad",
                type="cohort_retention",
                entity_column="customer_email", event_column="order_date",
                cohort_grain="month", retention_grain="month",
                materialized=[],
            )

    def test_apply_creates_materialized_table(self, tmp_path: Path) -> None:
        # Author a YAML with a materialized metric and verify the table
        # is created with the expected shape.
        yaml_path = tmp_path / "gibran.yaml"
        text = (FIXTURES / "gibran.yaml").read_text(encoding="utf-8")
        text = text.replace(
            "    description: Sum of paid order amounts.",
            "    description: Sum of paid order amounts.\n"
            "    materialized: [orders.region]",
        )
        yaml_path.write_text(text, encoding="utf-8")
        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        # Provision orders table BEFORE apply (the materialize step
        # needs the source to exist).
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
        apply_config(con, load_config(yaml_path))
        rows = con.execute(
            "SELECT * FROM gibran_mat_gross_revenue ORDER BY \"orders.region\""
        ).fetchall()
        # 2 regions: west (100+50=150) and east (200).
        regions = {r[0]: float(r[1]) for r in rows}
        assert regions == {"west": 150.0, "east": 200.0}

    def test_compile_routes_to_materialized_table(self) -> None:
        from gibran.sync.yaml_schema import MetricConfig
        con = _populated_db()
        # Register a fake materialized metric (no real materialization
        # for the test; we're just checking compile-time routing).
        con.execute(
            "INSERT INTO gibran_metrics "
            "(metric_id, source_id, display_name, metric_type) "
            "VALUES ('m_revenue_by_region', 'orders', 'Revenue by Region', 'sum')"
        )
        cfg = {"materialized": ["orders.region"]}
        con.execute(
            "INSERT INTO gibran_metric_versions "
            "(metric_id, version, expression, metric_config) "
            "VALUES ('m_revenue_by_region', 1, 'SUM(amount)', ?)",
            [json.dumps(cfg)],
        )
        intent = QueryIntent(
            source="orders", metrics=["m_revenue_by_region"],
            dimensions=[{"id": "orders.region"}],
        )
        compiled = compile_intent(intent, Catalog(con))
        rendered = compiled.render()
        assert "gibran_mat_m_revenue_by_region" in rendered
        # Source-table reference should NOT appear -- we routed away.
        assert "FROM \"orders\"" not in rendered
