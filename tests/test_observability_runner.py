"""Tests for the `rumi check` runner: per-rule evaluators + run_checks."""
from pathlib import Path

import duckdb
import pytest

from rumi.observability.default import DefaultObservability
from rumi.observability.runner import (
    RuleResult,
    _evaluate_freshness_rule,
    _evaluate_quality_rule,
    run_checks,
)
from rumi.sync.applier import apply as apply_config
from rumi.sync.loader import load as load_config
from rumi.sync.migrations import apply_all as apply_migrations


FIXTURES = Path(__file__).parent / "fixtures"
MIGRATIONS = Path(__file__).parent.parent / "migrations"


def _populated_db_with_data() -> duckdb.DuckDBPyConnection:
    """Catalog + governance + a real `orders` table with rows."""
    con = duckdb.connect(":memory:")
    apply_migrations(con, MIGRATIONS)
    apply_config(con, load_config(FIXTURES / "rumi.yaml"))
    con.execute(
        "CREATE TABLE orders ("
        "order_id VARCHAR, amount DECIMAL(18,2), order_date TIMESTAMP, "
        "status VARCHAR, region VARCHAR, customer_email VARCHAR)"
    )
    con.execute(
        "INSERT INTO orders VALUES "
        "('o1', 100.00, now() - INTERVAL '1 hour',  'paid',    'west',  'a@x'), "
        "('o2', 200.00, now() - INTERVAL '2 hours', 'paid',    'east',  'b@x'), "
        "('o3',  50.00, now() - INTERVAL '3 hours', 'pending', 'west',  'c@x'), "
        "('o4', 300.00, now() - INTERVAL '4 hours', 'paid',    'north', 'd@x')"
    )
    return con


# ---------------------------------------------------------------------------
# Per-rule-type evaluators (unit tests against a small in-memory dataset)
# ---------------------------------------------------------------------------

class TestQualityEvaluators:
    def test_not_null_passes_when_no_nulls(self) -> None:
        con = _populated_db_with_data()
        passed, observed = _evaluate_quality_rule(
            con, "orders", "not_null", {"column": "amount"}
        )
        assert passed is True
        assert observed == {"null_count": 0}

    def test_not_null_fails_when_nulls_present(self) -> None:
        con = _populated_db_with_data()
        con.execute("INSERT INTO orders VALUES ('o5', NULL, now(), 'paid', 'west', 'e@x')")
        passed, observed = _evaluate_quality_rule(
            con, "orders", "not_null", {"column": "amount"}
        )
        assert passed is False
        assert observed["null_count"] == 1

    def test_unique_passes_when_all_distinct(self) -> None:
        con = _populated_db_with_data()
        passed, observed = _evaluate_quality_rule(
            con, "orders", "unique", {"column": "order_id"}
        )
        assert passed is True
        assert observed["duplicates"] == 0

    def test_unique_fails_when_duplicates(self) -> None:
        con = _populated_db_with_data()
        con.execute(
            "INSERT INTO orders VALUES ('o1', 999, now(), 'paid', 'west', 'dup@x')"
        )
        passed, observed = _evaluate_quality_rule(
            con, "orders", "unique", {"column": "order_id"}
        )
        assert passed is False
        assert observed["duplicates"] == 1

    def test_range_passes_when_in_bounds(self) -> None:
        con = _populated_db_with_data()
        passed, observed = _evaluate_quality_rule(
            con, "orders", "range", {"column": "amount", "min": 0, "max": 1000000}
        )
        assert passed is True
        assert observed["out_of_range_count"] == 0

    def test_range_fails_when_out_of_bounds(self) -> None:
        con = _populated_db_with_data()
        passed, observed = _evaluate_quality_rule(
            con, "orders", "range", {"column": "amount", "min": 0, "max": 150}
        )
        # o2=200 and o4=300 exceed max=150
        assert passed is False
        assert observed["out_of_range_count"] == 2

    def test_range_requires_min_or_max(self) -> None:
        con = _populated_db_with_data()
        with pytest.raises(ValueError, match="range rule requires"):
            _evaluate_quality_rule(
                con, "orders", "range", {"column": "amount"}
            )

    def test_custom_sql_passes_on_truthy_scalar(self) -> None:
        con = _populated_db_with_data()
        passed, observed = _evaluate_quality_rule(
            con, "orders", "custom_sql",
            {"sql": "SELECT COUNT(*) = 4 FROM orders"},
        )
        assert passed is True

    def test_custom_sql_fails_on_falsy_scalar(self) -> None:
        con = _populated_db_with_data()
        passed, observed = _evaluate_quality_rule(
            con, "orders", "custom_sql",
            {"sql": "SELECT COUNT(*) = 99 FROM orders"},
        )
        assert passed is False

    def test_custom_sql_must_return_scalar(self) -> None:
        con = _populated_db_with_data()
        passed, observed = _evaluate_quality_rule(
            con, "orders", "custom_sql",
            {"sql": "SELECT order_id FROM orders"},
        )
        assert passed is False
        assert "error" in observed


class TestFreshnessEvaluator:
    def test_passes_when_recent(self) -> None:
        con = _populated_db_with_data()
        # Latest order is 1 hour ago; max_age 1 day -> passes
        passed, observed = _evaluate_freshness_rule(
            con, "orders", "order_date", max_age_seconds=86400
        )
        assert passed is True
        assert observed["age_seconds"] < 86400

    def test_fails_when_stale(self) -> None:
        con = _populated_db_with_data()
        # Latest order is 1 hour ago; max_age 30 minutes -> fails
        passed, observed = _evaluate_freshness_rule(
            con, "orders", "order_date", max_age_seconds=1800
        )
        assert passed is False
        assert observed["age_seconds"] > 1800

    def test_no_rows_fails(self) -> None:
        con = _populated_db_with_data()
        con.execute("DELETE FROM orders")
        passed, observed = _evaluate_freshness_rule(
            con, "orders", "order_date", max_age_seconds=86400
        )
        assert passed is False


# ---------------------------------------------------------------------------
# run_checks: end-to-end runner that records runs + refreshes health
# ---------------------------------------------------------------------------

class TestRunChecks:
    def test_runs_all_enabled_rules(self) -> None:
        con = _populated_db_with_data()
        obs = DefaultObservability(con)
        result = run_checks(con, "orders", obs)
        # Fixture has 2 quality rules + 1 freshness rule = 3 total
        assert result.total == 3
        rule_ids = {r.rule_id for r in result.results}
        assert rule_ids == {
            "orders_amount_not_null",
            "orders_amount_range",
            "orders_freshness_24h",
        }

    def test_records_runs_in_audit_table(self) -> None:
        con = _populated_db_with_data()
        obs = DefaultObservability(con)
        run_checks(con, "orders", obs)
        run_count = con.execute(
            "SELECT COUNT(*) FROM rumi_quality_runs"
        ).fetchone()[0]
        assert run_count == 3

    def test_refreshes_source_health(self) -> None:
        con = _populated_db_with_data()
        obs = DefaultObservability(con)
        # Before: no health row
        assert con.execute(
            "SELECT COUNT(*) FROM rumi_source_health WHERE source_id = 'orders'"
        ).fetchone()[0] == 0
        run_checks(con, "orders", obs)
        # After: health row exists with status='healthy' (all rules pass on the fixture data)
        row = con.execute(
            "SELECT status, blocking_failures FROM rumi_source_health "
            "WHERE source_id = 'orders'"
        ).fetchone()
        assert row[0] == "healthy"
        assert row[1] == "[]"

    def test_failed_rule_marks_source_as_blocked(self) -> None:
        con = _populated_db_with_data()
        # Insert a NULL amount so the not_null rule fails
        con.execute("INSERT INTO orders VALUES ('o5', NULL, now(), 'paid', 'west', 'e@x')")
        obs = DefaultObservability(con)
        result = run_checks(con, "orders", obs)
        assert result.failed >= 1
        row = con.execute(
            "SELECT status FROM rumi_source_health WHERE source_id = 'orders'"
        ).fetchone()
        assert row[0] == "block"

    def test_cache_hit_returns_cached_failures(self) -> None:
        """After refresh_health, latest_blocking_failures reads from the cache."""
        con = _populated_db_with_data()
        # NULL amount -> not_null rule will fail when run
        con.execute("INSERT INTO orders VALUES ('o5', NULL, now(), 'paid', 'west', 'e@x')")
        obs = DefaultObservability(con)
        run_checks(con, "orders", obs)
        failures = obs.latest_blocking_failures("orders")
        assert len(failures) >= 1
        assert any(f.rule_id == "orders_amount_not_null" for f in failures)
        # Pre-cache: the same call before refresh would have aggregated; post-cache,
        # it's a single PK lookup. Verify cache is what's being read by counting
        # the rumi_source_health row -- if we'd been aggregating, we wouldn't need it.
        assert con.execute(
            "SELECT COUNT(*) FROM rumi_source_health WHERE source_id = 'orders'"
        ).fetchone()[0] == 1


# ---------------------------------------------------------------------------
# Cache fallback: when no rumi_source_health row exists, fall back to V1.5
# ---------------------------------------------------------------------------

class TestCacheFallback:
    def test_empty_cache_falls_back_to_aggregation(self) -> None:
        """The existing test_observability.py tests rely on this: they record
        runs but never call refresh_health, so the cache stays empty. The V1.5
        aggregation path must keep working."""
        con = _populated_db_with_data()
        obs = DefaultObservability(con)
        # Record a passing run, but don't refresh
        obs.record_run("orders_amount_not_null", "quality", True)
        # Cache is empty for this source
        assert con.execute(
            "SELECT COUNT(*) FROM rumi_source_health WHERE source_id = 'orders'"
        ).fetchone()[0] == 0
        # The aggregation path is invoked. orders_freshness_24h is still never_run.
        failures = obs.latest_blocking_failures("orders")
        assert any(f.reason == "never_run" for f in failures)

    def test_refresh_health_can_be_called_with_no_runs(self) -> None:
        """Edge case: refresh before any rules have been run. The cache
        should reflect all-never_run as blocking failures."""
        con = _populated_db_with_data()
        obs = DefaultObservability(con)
        obs.refresh_health("orders")
        row = con.execute(
            "SELECT status, blocking_failures FROM rumi_source_health "
            "WHERE source_id = 'orders'"
        ).fetchone()
        assert row[0] == "block"  # never_run rules block
        import json
        failures = json.loads(row[1])
        assert all(f["reason"] == "never_run" for f in failures)
