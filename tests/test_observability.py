"""DefaultObservability tests.

Covers:
  - latest_blocking_failures classification (never_run / rule_failed / stale_check)
  - severity='warn' rules do not appear in blocking failures
  - disabled rules excluded
  - record_run creates rows
  - per-rule staleness_seconds override
"""
from pathlib import Path

import duckdb
import pytest

from gibran.observability.default import DefaultObservability
from gibran.observability.types import resolve_staleness_seconds
from gibran.sync.applier import apply as apply_config
from gibran.sync.loader import load as load_config
from gibran.sync.migrations import apply_all as apply_migrations


FIXTURES = Path(__file__).parent / "fixtures"
MIGRATIONS = Path(__file__).parent.parent / "migrations"


def _populated_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    apply_migrations(con, MIGRATIONS)
    apply_config(con, load_config(FIXTURES / "gibran.yaml"))
    return con


# ---------------------------------------------------------------------------
# resolve_staleness_seconds defaults
# ---------------------------------------------------------------------------

class TestStalenessDefaults:
    def test_cheap_quality_default_600(self) -> None:
        assert resolve_staleness_seconds("quality", "cheap", None) == 600

    def test_expensive_quality_default_3600(self) -> None:
        assert resolve_staleness_seconds("quality", "expensive", None) == 3600

    def test_freshness_default_300(self) -> None:
        assert resolve_staleness_seconds("freshness", None, None) == 300

    def test_explicit_value_wins(self) -> None:
        assert resolve_staleness_seconds("quality", "cheap", 42) == 42
        assert resolve_staleness_seconds("freshness", None, 42) == 42


# ---------------------------------------------------------------------------
# latest_blocking_failures classification
# ---------------------------------------------------------------------------

class TestLatestBlockingFailures:
    def test_never_run_rules_appear_as_never_run(self) -> None:
        con = _populated_db()
        obs = DefaultObservability(con)
        failures = obs.latest_blocking_failures("orders")
        # Fixture has 2 blocking rules on orders: orders_amount_not_null (quality)
        # and orders_freshness_24h (freshness). orders_amount_range is severity=warn.
        rule_ids = {f.rule_id for f in failures}
        assert rule_ids == {"orders_amount_not_null", "orders_freshness_24h"}
        assert all(f.reason == "never_run" for f in failures)

    def test_passing_runs_within_window_yield_no_failures(self) -> None:
        con = _populated_db()
        obs = DefaultObservability(con)
        obs.record_run("orders_amount_not_null", "quality", True)
        obs.record_run("orders_freshness_24h", "freshness", True)
        assert obs.latest_blocking_failures("orders") == ()

    def test_failed_run_appears_as_rule_failed(self) -> None:
        con = _populated_db()
        obs = DefaultObservability(con)
        obs.record_run("orders_amount_not_null", "quality", False, {"null_count": 17})
        obs.record_run("orders_freshness_24h", "freshness", True)
        failures = obs.latest_blocking_failures("orders")
        assert len(failures) == 1
        f = failures[0]
        assert f.rule_id == "orders_amount_not_null"
        assert f.rule_kind == "quality"
        assert f.reason == "rule_failed"

    def test_stale_run_appears_as_stale_check(self) -> None:
        con = _populated_db()
        obs = DefaultObservability(con)
        # Pass orders_amount_not_null normally
        obs.record_run("orders_amount_not_null", "quality", True)
        # Insert a stale freshness run using DuckDB's own clock (avoids
        # Python/DuckDB timezone drift in the comparison)
        con.execute(
            "INSERT INTO gibran_quality_runs "
            "(run_id, rule_id, rule_kind, passed, ran_at) "
            "VALUES ('stale1', 'orders_freshness_24h', 'freshness', TRUE, "
            "now() - INTERVAL '2 hours')"
        )
        failures = obs.latest_blocking_failures("orders")
        assert len(failures) == 1
        f = failures[0]
        assert f.rule_id == "orders_freshness_24h"
        assert f.reason == "stale_check"
        assert f.seconds_overdue is not None
        assert f.seconds_overdue > 0

    def test_warn_severity_rule_never_blocks(self) -> None:
        con = _populated_db()
        obs = DefaultObservability(con)
        # Pass the blocking rules; orders_amount_range is severity=warn and should not appear
        obs.record_run("orders_amount_not_null", "quality", True)
        obs.record_run("orders_freshness_24h", "freshness", True)
        # Even if we record a failing run for the warn rule, it's not blocking
        obs.record_run("orders_amount_range", "quality", False, {"out_of_range": 5})
        failures = obs.latest_blocking_failures("orders")
        rule_ids = {f.rule_id for f in failures}
        assert "orders_amount_range" not in rule_ids
        assert failures == ()  # all blocking rules pass

    def test_disabled_rule_excluded_from_blocking(self) -> None:
        con = _populated_db()
        obs = DefaultObservability(con)
        # Disable the blocking quality rule
        con.execute(
            "UPDATE gibran_quality_rules SET enabled = FALSE "
            "WHERE rule_id = 'orders_amount_not_null'"
        )
        # Pass freshness so only the now-disabled quality rule could block
        obs.record_run("orders_freshness_24h", "freshness", True)
        failures = obs.latest_blocking_failures("orders")
        rule_ids = {f.rule_id for f in failures}
        assert "orders_amount_not_null" not in rule_ids

    def test_per_rule_staleness_override(self) -> None:
        con = _populated_db()
        obs = DefaultObservability(con)
        # Pass quality
        obs.record_run("orders_amount_not_null", "quality", True)
        # Override freshness staleness to 60 seconds
        con.execute(
            "UPDATE gibran_freshness_rules SET staleness_seconds = 60 "
            "WHERE rule_id = 'orders_freshness_24h'"
        )
        # Insert a freshness run from 90s ago (stale by override, not by default 300s)
        con.execute(
            "INSERT INTO gibran_quality_runs "
            "(run_id, rule_id, rule_kind, passed, ran_at) "
            "VALUES ('o1', 'orders_freshness_24h', 'freshness', TRUE, "
            "now() - INTERVAL '90 seconds')"
        )
        failures = obs.latest_blocking_failures("orders")
        assert len(failures) == 1
        assert failures[0].reason == "stale_check"

    def test_unknown_source_returns_empty(self) -> None:
        con = _populated_db()
        obs = DefaultObservability(con)
        # No rules defined for this source -> nothing to fail
        assert obs.latest_blocking_failures("ghost_source") == ()


# ---------------------------------------------------------------------------
# record_run
# ---------------------------------------------------------------------------

class TestRecordRun:
    def test_creates_row_with_unique_id(self) -> None:
        con = _populated_db()
        obs = DefaultObservability(con)
        run_id_a = obs.record_run("orders_amount_not_null", "quality", True)
        run_id_b = obs.record_run("orders_amount_not_null", "quality", True)
        assert run_id_a != run_id_b
        rows = con.execute(
            "SELECT rule_id, passed FROM gibran_quality_runs "
            "WHERE rule_id = 'orders_amount_not_null'"
        ).fetchall()
        assert len(rows) == 2

    def test_observed_value_persisted_as_json(self) -> None:
        con = _populated_db()
        obs = DefaultObservability(con)
        obs.record_run(
            "orders_amount_not_null", "quality", False,
            observed_value={"null_count": 17, "checked_rows": 1000},
        )
        ov = con.execute(
            "SELECT CAST(observed_value AS VARCHAR) FROM gibran_quality_runs "
            "WHERE rule_id = 'orders_amount_not_null'"
        ).fetchone()[0]
        assert "null_count" in ov
        assert "17" in ov

    def test_latest_wins_when_two_runs(self) -> None:
        con = _populated_db()
        obs = DefaultObservability(con)
        obs.record_run("orders_amount_not_null", "quality", False)
        obs.record_run("orders_amount_not_null", "quality", True)  # latest=passed
        obs.record_run("orders_freshness_24h", "freshness", True)
        # Second run wins; no blocking failures
        assert obs.latest_blocking_failures("orders") == ()
