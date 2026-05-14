"""Tests for the Tier 4 strategic primitives bundled in commit `5xxx`:

  * multi_stage_filter shape primitive
  * anomaly rule type (6th quality-rule type)
  * break-glass role marker
  * webhook alerting on block-severity rule failures
  * in-process rate limiter
  * access-pattern anomaly detection
  * approval workflow
  * query timeout via env var

Each section keeps its surface tight -- one or two tests pinning the
interesting behavior, not exhaustive coverage. The full suite stays
under 90s.
"""
from __future__ import annotations

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import duckdb
import pytest

from gibran.dsl.compile import Catalog, compile_intent
from gibran.dsl.types import QueryIntent
from gibran.execution.sql import run_sql_query
from gibran.governance.default import DefaultGovernance
from gibran.governance.rate_limit import RateLimiter
from gibran.governance.types import DenyReason, IdentityContext
from gibran.observability.access_anomaly import detect_access_anomalies
from gibran.observability.default import DefaultObservability
from gibran.observability.runner import _evaluate_quality_rule, run_checks
from gibran.sync.applier import apply as apply_config
from gibran.sync.approval import approve, list_pending, submit_change
from gibran.sync.loader import load as load_config
from gibran.sync.migrations import apply_all as apply_migrations
from gibran.sync.yaml_schema import MetricConfig, QualityRuleConfig


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
        "('o3', 300, TIMESTAMP '2026-02-15', 'pending', 'west', 'c@x'),"
        "('o4',  50, TIMESTAMP '2026-02-20', 'paid', 'west', 'a@x')"
    )
    return con


def _admin(con: duckdb.DuckDBPyConnection, is_break_glass: bool = False):
    con.execute(
        "INSERT INTO gibran_roles (role_id, display_name, is_break_glass) "
        "VALUES ('admin', 'Admin', ?)",
        [is_break_glass],
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
# multi_stage_filter
# ---------------------------------------------------------------------------

class TestMultiStageFilterValidation:
    def test_requires_exactly_one_of_top_n_or_top_percentile(self) -> None:
        with pytest.raises(ValueError, match="exactly ONE"):
            MetricConfig(
                id="m", source="s", display_name="m",
                type="multi_stage_filter",
                msf_entity_column="customer_email",
                msf_ranking_expression="SUM(amount)",
                msf_result_expression="COUNT(*)",
                top_n=10, top_percentile=0.1,  # both set -> reject
            )

    def test_top_percentile_out_of_range_rejected(self) -> None:
        with pytest.raises(ValueError, match="top_percentile must be"):
            MetricConfig(
                id="m", source="s", display_name="m",
                type="multi_stage_filter",
                msf_entity_column="customer_email",
                msf_ranking_expression="SUM(amount)",
                msf_result_expression="COUNT(*)",
                top_percentile=1.5,
            )


class TestMultiStageFilterCompile:
    def test_compiles_to_two_cte_shape(self) -> None:
        con = _populated_db()
        # Author a one-off multi_stage_filter and persist it directly
        # rather than thread it through the fixture (the test scope is
        # just compilation, not catalog persistence).
        con.execute(
            "INSERT INTO gibran_metrics "
            "(metric_id, source_id, display_name, metric_type) "
            "VALUES ('top_spender_orders', 'orders', 'Top Spender Orders', "
            "'multi_stage_filter')"
        )
        cfg = {
            "entity_column": "customer_email",
            "ranking_expression": "SUM(amount)",
            "result_expression": "COUNT(*)",
            "top_n": 1, "top_percentile": None,
        }
        con.execute(
            "INSERT INTO gibran_metric_versions "
            "(metric_id, version, expression, metric_config) "
            "VALUES ('top_spender_orders', 1, "
            "'multi_stage_filter[customer_email/top_n=1]', ?)",
            [json.dumps(cfg)],
        )
        intent = QueryIntent(source="orders", metrics=["top_spender_orders"])
        compiled = compile_intent(intent, Catalog(con))
        names = [c.name for c in compiled.ctes]
        assert names == ["ranked", "top_entities"]
        sql = compiled.render()
        assert "PERCENT_RANK" not in sql        # top_n path used ROW_NUMBER/LIMIT
        assert "LIMIT 1" in sql or "LIMIT 1\n" in sql


# ---------------------------------------------------------------------------
# anomaly rule type
# ---------------------------------------------------------------------------

class TestAnomalyRuleValidation:
    def test_requires_sql_n_sigma_trailing(self) -> None:
        with pytest.raises(ValueError, match="anomaly requires"):
            QualityRuleConfig(
                id="q", source="orders", type="anomaly",
                config={"sql": "SELECT 1"},  # missing n_sigma + trailing_periods
                severity="warn",
            )


class TestAnomalyRuleEvaluation:
    def test_bootstrap_passes_with_no_history(self) -> None:
        # Brand-new rule, no history runs yet -> always pass.
        con = _populated_db()
        passed, observed = _evaluate_quality_rule(
            con, "orders", "anomaly",
            {"sql": "SELECT AVG(amount) FROM orders",
             "n_sigma": 3, "trailing_periods": 5},
            rule_id="orders_amount_anomaly",
        )
        assert passed is True
        assert observed["bootstrapping"] is True

    def test_flags_anomaly_when_current_value_is_far_from_history(self) -> None:
        con = _populated_db()
        rule_id = "orders_anomaly_rule"
        # Seed a constant historical baseline of value=100.
        for i in range(10):
            con.execute(
                "INSERT INTO gibran_quality_runs "
                "(run_id, rule_id, rule_kind, passed, observed_value) "
                "VALUES (?, ?, 'quality', TRUE, ?)",
                [f"r{i}", rule_id, json.dumps({"value": 100.0})],
            )
        # Now evaluate where the current value will be 1000 -- a clear
        # outlier vs. the 100-constant history.
        passed, observed = _evaluate_quality_rule(
            con, "orders", "anomaly",
            {"sql": "SELECT 1000",
             "n_sigma": 3, "trailing_periods": 10},
            rule_id=rule_id,
        )
        assert passed is False
        assert observed["value"] == 1000.0
        assert observed.get("constant_history") is True


# ---------------------------------------------------------------------------
# break-glass marker
# ---------------------------------------------------------------------------

class TestBreakGlassAuditFlag:
    def test_normal_role_writes_false(self) -> None:
        con = _populated_db()
        ident = _admin(con, is_break_glass=False)
        result = run_sql_query(
            con, DefaultGovernance(con), ident,
            "SELECT order_id FROM orders",
        )
        assert result.status == "ok"
        flag = con.execute(
            "SELECT is_break_glass FROM gibran_query_log WHERE query_id = ?",
            [result.query_id],
        ).fetchone()[0]
        assert flag is False

    def test_break_glass_role_writes_true(self) -> None:
        con = _populated_db()
        ident = _admin(con, is_break_glass=True)
        result = run_sql_query(
            con, DefaultGovernance(con), ident,
            "SELECT order_id FROM orders",
        )
        assert result.status == "ok"
        flag = con.execute(
            "SELECT is_break_glass FROM gibran_query_log WHERE query_id = ?",
            [result.query_id],
        ).fetchone()[0]
        assert flag is True


# ---------------------------------------------------------------------------
# webhook alerting
# ---------------------------------------------------------------------------

class _WebhookCapture(BaseHTTPRequestHandler):
    """Tiny HTTP server that records every POSTed payload to a class
    attribute. Threading-safe via the GIL + list.append atomicity."""
    captured: list[dict] = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        try:
            self.captured.append(json.loads(body))
        except Exception:
            self.captured.append({"_parse_error": body.decode()})
        self.send_response(204)
        self.end_headers()

    def log_message(self, *args, **kwargs):  # silence default stderr logging
        return


class TestWebhookAlerting:
    def test_block_severity_failure_fires_webhook(self) -> None:
        _WebhookCapture.captured = []
        server = ThreadingHTTPServer(("127.0.0.1", 0), _WebhookCapture)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            con = _populated_db()
            # Configure an always-failing custom_sql rule with webhook.
            con.execute(
                "INSERT INTO gibran_quality_rules "
                "(rule_id, source_id, rule_type, rule_config, cost_class, "
                "severity, enabled, alert_webhook) "
                "VALUES ('always_fail', 'orders', 'custom_sql', ?, "
                "'cheap', 'block', TRUE, ?)",
                [json.dumps({"sql": "SELECT FALSE"}),
                 f"http://127.0.0.1:{port}/alert"],
            )
            obs = DefaultObservability(con)
            run_checks(con, "orders", obs)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
        assert len(_WebhookCapture.captured) >= 1
        payload = _WebhookCapture.captured[0]
        assert payload["rule_id"] == "always_fail"
        assert payload["severity"] == "block"


# ---------------------------------------------------------------------------
# rate limiter
# ---------------------------------------------------------------------------

class TestRateLimiter:
    def test_burst_then_steady_state(self) -> None:
        rl = RateLimiter(tokens_per_second=10, burst=3)
        # First 3 succeed (burst), 4th fails immediately.
        assert rl.acquire("u1", "r1") is True
        assert rl.acquire("u1", "r1") is True
        assert rl.acquire("u1", "r1") is True
        assert rl.acquire("u1", "r1") is False

    def test_different_users_have_independent_buckets(self) -> None:
        rl = RateLimiter(tokens_per_second=10, burst=1)
        assert rl.acquire("u1", "r1") is True
        # u1 exhausted, but u2 has its own bucket.
        assert rl.acquire("u2", "r1") is True
        assert rl.acquire("u1", "r1") is False
        assert rl.acquire("u2", "r1") is False


class TestRateLimiterInGovernance:
    def test_evaluate_denies_when_rate_limited(self) -> None:
        con = _populated_db()
        ident = _admin(con)
        # Use a very SLOW refill rate (1 token per 100s) so the bucket
        # demonstrably can't refill between the two adjacent calls,
        # even on a slow CI runner.
        rl = RateLimiter(tokens_per_second=0.01, burst=1)
        gov = DefaultGovernance(con, rate_limiter=rl)
        first = gov.evaluate(
            ident, frozenset({"orders"}), frozenset({"order_id"}), (),
        )
        assert first.allowed is True
        second = gov.evaluate(
            ident, frozenset({"orders"}), frozenset({"order_id"}), (),
        )
        assert second.allowed is False
        assert second.deny_reason is DenyReason.RATE_LIMITED


# ---------------------------------------------------------------------------
# access-pattern anomaly detection
# ---------------------------------------------------------------------------

class TestAccessAnomaly:
    def test_flags_user_with_today_spike(self) -> None:
        con = _populated_db()
        # Seed 5 days of history at ~3 queries/day for user X.
        for d in range(1, 6):
            for q in range(3):
                con.execute(
                    "INSERT INTO gibran_query_log "
                    "(query_id, user_id, role_id, generated_sql, status, "
                    "created_at, is_break_glass) "
                    "VALUES (?, 'user_x', 'analyst_west', 'SELECT 1', 'ok', "
                    f"now() - INTERVAL '{d} day', FALSE)",
                    [f"q_x_{d}_{q}"],
                )
        # Today user_x runs 100 queries (a clear anomaly).
        for q in range(100):
            con.execute(
                "INSERT INTO gibran_query_log "
                "(query_id, user_id, role_id, generated_sql, status, "
                "is_break_glass) "
                "VALUES (?, 'user_x', 'analyst_west', 'SELECT 1', 'ok', FALSE)",
                [f"q_x_today_{q}"],
            )
        anomalies = detect_access_anomalies(
            con, trailing_days=10, n_sigma=3.0, min_history_days=3,
        )
        flagged = [a for a in anomalies if a.user_id == "user_x"]
        assert flagged, anomalies
        assert flagged[0].today_count == 100
        assert flagged[0].mean == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# approval workflow
# ---------------------------------------------------------------------------

class TestApprovalWorkflow:
    def test_submit_then_approve_round_trip(self) -> None:
        con = _populated_db()
        change_id = submit_change(
            con,
            change_type="policy",
            payload={"role": "ext", "source": "orders", "default_column_mode": "deny"},
            requested_by="alice",
            reason="contractor access removal",
        )
        # Listed as pending.
        pending = list_pending(con)
        assert len(pending) == 1
        assert pending[0].change_id == change_id

        approved = approve(con, change_id, approved_by="bob")
        assert approved.payload["role"] == "ext"
        # Now no longer pending.
        assert list_pending(con) == []

    def test_double_approve_rejected(self) -> None:
        con = _populated_db()
        change_id = submit_change(
            con, change_type="policy", payload={}, requested_by="alice",
        )
        approve(con, change_id, approved_by="bob")
        with pytest.raises(ValueError, match="already approved"):
            approve(con, change_id, approved_by="bob")


# ---------------------------------------------------------------------------
# query timeout via env var
# ---------------------------------------------------------------------------

class TestQueryTimeout:
    def test_env_var_path_does_not_crash(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The env var triggers a SET statement_timeout call in the
        # execution path. Some DuckDB builds don't expose that setting,
        # so the SET is wrapped in try/except. This test verifies that
        # the env-var path runs without crashing the query -- exact
        # timeout enforcement is best-effort per DuckDB version.
        monkeypatch.setenv("GIBRAN_QUERY_TIMEOUT_MS", "5000")
        con = _populated_db()
        ident = _admin(con)
        result = run_sql_query(
            con, DefaultGovernance(con), ident,
            "SELECT order_id FROM orders",
        )
        assert result.status == "ok"

    def test_invalid_env_var_ignored(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("GIBRAN_QUERY_TIMEOUT_MS", "not_a_number")
        con = _populated_db()
        ident = _admin(con)
        result = run_sql_query(
            con, DefaultGovernance(con), ident,
            "SELECT order_id FROM orders",
        )
        assert result.status == "ok"
