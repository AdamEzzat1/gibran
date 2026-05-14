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
        #
        # Phase 2B contract: this only holds because we don't call
        # `gibran touch orders` between mutations. A touch would invalidate
        # the cache and the second query would return 4. See
        # TestDataVersionInvalidation below.
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
# Phase 2B: data-version tracking closes the stale-cache hole
# ---------------------------------------------------------------------------

class TestDataVersionInvalidation:
    def test_duckdb_table_initial_version_is_zero(self) -> None:
        # An un-touched source has no row in gibran_table_versions and
        # source_data_version returns the sentinel "0".
        from gibran._source_dispatch import source_data_version

        con = _populated_db()
        assert source_data_version(con, "orders") == "0"

    def test_touch_bumps_duckdb_table_version(self) -> None:
        from gibran._source_dispatch import source_data_version, touch_source

        con = _populated_db()
        v0 = source_data_version(con, "orders")
        v1 = touch_source(con, "orders")
        assert v0 != v1
        # Re-touching produces yet another version.
        v2 = touch_source(con, "orders")
        assert v1 != v2
        # The reader now sees the latest token.
        assert source_data_version(con, "orders") == v2

    def test_touch_parquet_returns_mtime_no_table_versions_row(
        self, tmp_path
    ) -> None:
        # parquet uses file mtime as version; touch is a no-op (returns
        # the mtime without inserting into gibran_table_versions).
        from gibran._source_dispatch import source_data_version, touch_source

        con = _populated_db()
        parquet = tmp_path / "pq_test.parquet"
        con.execute(f"COPY (SELECT 1 AS x) TO '{parquet}' (FORMAT PARQUET)")
        con.execute(
            "INSERT INTO gibran_sources "
            "(source_id, display_name, source_type, uri, primary_grain) "
            "VALUES (?, ?, ?, ?, ?)",
            ["pq_test", "PQ Test", "parquet", str(parquet), None],
        )
        v1 = source_data_version(con, "pq_test")
        v2 = touch_source(con, "pq_test")
        assert v1 == v2  # touch was a no-op
        # No row inserted into table_versions for parquet sources.
        n = con.execute(
            "SELECT COUNT(*) FROM gibran_table_versions WHERE source_id = 'pq_test'"
        ).fetchone()[0]
        assert n == 0

    def test_parquet_mtime_bump_changes_data_version(self, tmp_path) -> None:
        # Bump the file mtime via os.utime without rewriting contents --
        # the cache should treat that as a data change.
        import os
        import time
        from gibran._source_dispatch import source_data_version

        con = _populated_db()
        parquet = tmp_path / "pq_test.parquet"
        con.execute(f"COPY (SELECT 1 AS x) TO '{parquet}' (FORMAT PARQUET)")
        con.execute(
            "INSERT INTO gibran_sources "
            "(source_id, display_name, source_type, uri, primary_grain) "
            "VALUES (?, ?, ?, ?, ?)",
            ["pq_test", "PQ Test", "parquet", str(parquet), None],
        )
        v1 = source_data_version(con, "pq_test")
        # Bump mtime by 1 second forward (some filesystems have 1-second
        # mtime resolution; nanoseconds aren't always reliable).
        time.sleep(0.01)
        new_mtime = os.stat(parquet).st_mtime + 1
        os.utime(parquet, (new_mtime, new_mtime))
        v2 = source_data_version(con, "pq_test")
        assert v1 != v2

    def test_sql_view_uses_table_versions_not_recursive(self) -> None:
        # V1 contract: sql_view sources are treated like duckdb_table for
        # version lookup (manual touch required). Recursive derivation
        # from the view's underlying tables is Phase 3 work.
        from gibran._source_dispatch import source_data_version, touch_source

        con = _populated_db()
        # Pretend "orders" is registered as sql_view by patching the
        # source row; the data hasn't changed, only the source_type metadata.
        con.execute(
            "UPDATE gibran_sources SET source_type = 'sql_view' "
            "WHERE source_id = 'orders'"
        )
        assert source_data_version(con, "orders") == "0"
        # A manual touch bumps the version, same as duckdb_table.
        v1 = touch_source(con, "orders")
        assert source_data_version(con, "orders") == v1
        # But mutating the underlying data (a real INSERT) does NOT bump
        # the version -- that's the documented V1 limitation.
        con.execute(
            "INSERT INTO orders VALUES "
            "('o4', 99, TIMESTAMP '2026-03-01', 'paid', 'west', 'd@x')"
        )
        assert source_data_version(con, "orders") == v1

    def test_touch_unknown_source_raises(self) -> None:
        from gibran._source_dispatch import SourceDispatchError, touch_source

        con = _populated_db()
        with pytest.raises(SourceDispatchError, match="unknown source"):
            touch_source(con, "nonexistent")

    def test_end_to_end_touch_invalidates_cache(self) -> None:
        # Companion to TestResultCache.test_end_to_end_cache_hit_skips_execute:
        # WITH a touch between mutations, the cache invalidates and the
        # second query reflects the new row.
        from gibran._source_dispatch import touch_source

        con = _populated_db()
        ident = _admin(con)
        gov = DefaultGovernance(con)
        first = run_sql_query(con, gov, ident, "SELECT COUNT(*) AS n FROM orders")
        assert first.rows == ((3,),)
        con.execute(
            "INSERT INTO orders VALUES "
            "('o4', 99, TIMESTAMP '2026-03-01', 'paid', 'west', 'd@x')"
        )
        touch_source(con, "orders")
        second = run_sql_query(con, gov, ident, "SELECT COUNT(*) AS n FROM orders")
        # The touch invalidated the cache; the new query sees 4 rows.
        assert second.rows == ((4,),)


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


# ---------------------------------------------------------------------------
# Phase 2C: incremental materialization
# ---------------------------------------------------------------------------

class TestIncrementalMaterializationValidation:
    """Pydantic-level checks on the new YAML fields."""

    def test_incremental_requires_watermark_column(self) -> None:
        from gibran.sync.yaml_schema import MetricConfig
        with pytest.raises(ValueError, match="requires `watermark_column`"):
            MetricConfig(
                id="m", source="orders", display_name="m",
                type="sum", expression="amount",
                materialized=["orders.region"],
                materialized_strategy="incremental",
            )

    def test_watermark_without_incremental_rejected(self) -> None:
        from gibran.sync.yaml_schema import MetricConfig
        with pytest.raises(
            ValueError,
            match="watermark_column is only meaningful with "
                  "materialized_strategy=incremental",
        ):
            MetricConfig(
                id="m", source="orders", display_name="m",
                type="sum", expression="amount",
                materialized=["orders.region"],
                watermark_column="order_date",
            )

    def test_scalar_incremental_rejected(self) -> None:
        # Empty materialized list (scalar) + incremental is meaningless --
        # nothing to incrementally update.
        from gibran.sync.yaml_schema import MetricConfig
        with pytest.raises(
            ValueError, match="scalar materialization .* incompatible",
        ):
            MetricConfig(
                id="m", source="orders", display_name="m",
                type="sum", expression="amount",
                materialized=[],
                materialized_strategy="incremental",
                watermark_column="order_date",
            )

    def test_negative_grace_rejected(self) -> None:
        from gibran.sync.yaml_schema import MetricConfig
        with pytest.raises(
            ValueError, match="late_arrival_grace_seconds must be >= 0",
        ):
            MetricConfig(
                id="m", source="orders", display_name="m",
                type="sum", expression="amount",
                materialized=["orders.region"],
                materialized_strategy="incremental",
                watermark_column="order_date",
                late_arrival_grace_seconds=-5,
            )

    def test_strategy_without_materialized_rejected(self) -> None:
        # materialized_strategy on a non-materialized metric is a
        # configuration mistake -- caught at validation.
        from gibran.sync.yaml_schema import MetricConfig
        with pytest.raises(
            ValueError, match="require `materialized` to be set",
        ):
            MetricConfig(
                id="m", source="orders", display_name="m",
                type="sum", expression="amount",
                materialized_strategy="full",
            )


def _incremental_yaml_fixture(tmp_path: Path) -> Path:
    """Materializes gross_revenue by region with incremental strategy
    over order_date. Used by the incremental refresh tests below."""
    yaml_path = tmp_path / "gibran.yaml"
    text = (FIXTURES / "gibran.yaml").read_text(encoding="utf-8")
    text = text.replace(
        "    description: Sum of paid order amounts.",
        "    description: Sum of paid order amounts.\n"
        "    materialized: [orders.region]\n"
        "    materialized_strategy: incremental\n"
        "    watermark_column: order_date",
    )
    yaml_path.write_text(text, encoding="utf-8")
    return yaml_path


def _provision_orders(con: duckdb.DuckDBPyConnection) -> None:
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


class TestIncrementalMaterializationRefresh:
    """End-to-end behavior: apply, mutate, re-materialize, check sums."""

    def test_first_run_does_full_build_and_records_watermark(
        self, tmp_path: Path,
    ) -> None:
        from gibran.sync.applier import _materialize_metrics
        yaml_path = _incremental_yaml_fixture(tmp_path)
        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        _provision_orders(con)
        apply_config(con, load_config(yaml_path))
        # After apply (which calls _materialize_metrics), the mat table
        # has aggregates for all regions and the state row records the
        # latest watermark.
        rows = con.execute(
            "SELECT * FROM gibran_mat_gross_revenue ORDER BY \"orders.region\""
        ).fetchall()
        assert {r[0]: float(r[1]) for r in rows} == {
            "west": 150.0, "east": 200.0,
        }
        state = con.execute(
            "SELECT last_refresh_watermark FROM gibran_mat_state "
            "WHERE metric_id = 'gross_revenue'"
        ).fetchone()
        assert state is not None
        # The recorded watermark is MAX(order_date) over the source.
        # The exact serialized form depends on DuckDB's CAST(... AS VARCHAR)
        # for TIMESTAMP -- assert the year/month/day are present.
        assert "2026-02-15" in state[0]

    def test_incremental_picks_up_new_rows_for_existing_dim(
        self, tmp_path: Path,
    ) -> None:
        # The bug-class test: dim "west" has rows in the original data
        # AND new rows after the watermark. Incremental refresh must
        # produce the CORRECT new aggregate (old + new), not just the
        # aggregate of new rows.
        from gibran.sync.applier import _materialize_metrics
        yaml_path = _incremental_yaml_fixture(tmp_path)
        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        _provision_orders(con)
        validated = load_config(yaml_path)
        apply_config(con, validated)
        # After initial apply: west=150, east=200.
        # Insert a new row that should bump west by 25.
        con.execute(
            "INSERT INTO orders VALUES "
            "('o4', 25, TIMESTAMP '2026-03-01', 'paid', 'west', 'd@x')"
        )
        _materialize_metrics(con, validated.config.metrics)
        rows = con.execute(
            "SELECT * FROM gibran_mat_gross_revenue ORDER BY \"orders.region\""
        ).fetchall()
        # west should be 150 + 25 = 175 (NOT 25, which would be the bug).
        # east should be unchanged at 200.
        assert {r[0]: float(r[1]) for r in rows} == {
            "west": 175.0, "east": 200.0,
        }

    def test_incremental_leaves_untouched_dims_alone(
        self, tmp_path: Path,
    ) -> None:
        # If new data only arrives for "west", the "east" row in the
        # mat table must not be touched (no DELETE + re-INSERT for it).
        from gibran.sync.applier import _materialize_metrics
        yaml_path = _incremental_yaml_fixture(tmp_path)
        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        _provision_orders(con)
        validated = load_config(yaml_path)
        apply_config(con, validated)
        # Mutate east in the source table to simulate something the
        # mat table should NOT pick up (it's not in the watermark window).
        # This is a bit contrived -- in practice you can't update history
        # silently -- but it pins the "untouched dims preserved" contract.
        con.execute(
            "UPDATE orders SET amount = 999 "
            "WHERE order_id = 'o2'"
        )
        _materialize_metrics(con, validated.config.metrics)
        rows = con.execute(
            "SELECT * FROM gibran_mat_gross_revenue ORDER BY \"orders.region\""
        ).fetchall()
        # east remains 200 (the original aggregate) because no new row
        # was inserted with order_date > last watermark.
        assert {r[0]: float(r[1]) for r in rows} == {
            "west": 150.0, "east": 200.0,
        }

    def test_force_full_rebuilds_everything(self, tmp_path: Path) -> None:
        # `gibran materialize --full` bypasses the watermark and rebuilds
        # from scratch, picking up backdated mutations.
        from gibran.sync.applier import _materialize_metrics
        yaml_path = _incremental_yaml_fixture(tmp_path)
        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        _provision_orders(con)
        validated = load_config(yaml_path)
        apply_config(con, validated)
        # Same silent mutation as above.
        con.execute("UPDATE orders SET amount = 999 WHERE order_id = 'o2'")
        _materialize_metrics(
            con, validated.config.metrics, force_full=True,
        )
        rows = con.execute(
            "SELECT * FROM gibran_mat_gross_revenue ORDER BY \"orders.region\""
        ).fetchall()
        # east is now 999 (the new amount) because force_full re-aggregated.
        assert {r[0]: float(r[1]) for r in rows} == {
            "west": 150.0, "east": 999.0,
        }

    def test_late_arriving_row_within_grace_picked_up(
        self, tmp_path: Path,
    ) -> None:
        # late_arrival_grace_seconds = 86400 (1 day) means rows with
        # order_date as late as 1 day BEFORE last_refresh_watermark are
        # re-evaluated. A row backdated 1 hour into the past should be
        # picked up.
        from gibran.sync.applier import _materialize_metrics
        yaml_path = tmp_path / "gibran.yaml"
        text = (FIXTURES / "gibran.yaml").read_text(encoding="utf-8")
        text = text.replace(
            "    description: Sum of paid order amounts.",
            "    description: Sum of paid order amounts.\n"
            "    materialized: [orders.region]\n"
            "    materialized_strategy: incremental\n"
            "    watermark_column: order_date\n"
            "    late_arrival_grace_seconds: 86400",
        )
        yaml_path.write_text(text, encoding="utf-8")
        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        _provision_orders(con)
        validated = load_config(yaml_path)
        apply_config(con, validated)
        # Last watermark is 2026-02-15. Insert a row dated 1 hour
        # earlier than that (within grace).
        con.execute(
            "INSERT INTO orders VALUES "
            "('o4', 10, TIMESTAMP '2026-02-15 00:00:00', 'paid', 'east', 'd@x')"
        )
        _materialize_metrics(con, validated.config.metrics)
        rows = con.execute(
            "SELECT * FROM gibran_mat_gross_revenue ORDER BY \"orders.region\""
        ).fetchall()
        # east should be 200 + 10 = 210 because the backdated row was
        # within the grace window.
        assert {r[0]: float(r[1]) for r in rows}["east"] == 210.0
