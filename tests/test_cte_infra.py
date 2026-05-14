"""Tests for CTE infrastructure: CompiledQuery dataclass + execution-layer
parsing relaxation.

The DSL compiler now returns CompiledQuery(ctes, main_sql) instead of a
bare string. For V1's single-SELECT primitives (count / sum / ratio /
percentile / rolling_window / period_over_period / expression) the
`ctes` tuple is empty and `.render()` returns just `main_sql` --
byte-identical to the old string output. New CTE-based primitives
(cohort_retention, funnel) populate `ctes`; this file's tests cover the
infrastructure those primitives ride on.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from gibran.dsl.compile import CTE, Catalog, CompiledQuery, compile_intent
from gibran.dsl.types import QueryIntent
from gibran.execution.sql import (
    UnsupportedQueryError,
    _parse_for_governance,
    run_sql_query,
)
from gibran.governance.default import DefaultGovernance
from gibran.governance.types import DenyReason, IdentityContext
from gibran.sync.applier import apply as apply_config
from gibran.sync.loader import load as load_config
from gibran.sync.migrations import apply_all as apply_migrations


FIXTURES = Path(__file__).parent / "fixtures"
MIGRATIONS = Path(__file__).parent.parent / "migrations"


# ---------------------------------------------------------------------------
# CompiledQuery dataclass: render()
# ---------------------------------------------------------------------------

class TestCompiledQueryRender:
    def test_render_with_no_ctes_returns_main_sql_verbatim(self) -> None:
        # The no-CTE shape is the V1 contract; .render() must be a no-op
        # so existing primitives produce byte-identical SQL.
        cq = CompiledQuery(ctes=(), main_sql="SELECT 1")
        assert cq.render() == "SELECT 1"

    def test_render_with_one_cte_emits_with_clause(self) -> None:
        cq = CompiledQuery(
            ctes=(CTE("a", "SELECT order_id FROM orders"),),
            main_sql="SELECT order_id FROM a",
        )
        rendered = cq.render()
        assert rendered.startswith("WITH a AS")
        assert "SELECT order_id FROM orders" in rendered
        assert rendered.rstrip().endswith("SELECT order_id FROM a")

    def test_render_with_multiple_ctes_preserves_order(self) -> None:
        # DuckDB requires CTEs in dependency-resolved order. The renderer
        # emits them in the order given -- the compiler is expected to
        # provide them already sorted (cohorts before retention, etc.).
        cq = CompiledQuery(
            ctes=(
                CTE("cohorts",
                    "SELECT user_id, MIN(order_date) AS first_order "
                    "FROM orders GROUP BY user_id"),
                CTE("retention",
                    "SELECT c.user_id, c.first_order, o.amount "
                    "FROM cohorts c JOIN orders o ON c.user_id = o.user_id",
                    depends_on=("cohorts",)),
            ),
            main_sql="SELECT first_order FROM retention",
        )
        rendered = cq.render()
        assert rendered.index("cohorts AS") < rendered.index("retention AS")
        assert "WITH cohorts AS" in rendered
        # The CTEs are comma-separated; rendering puts the comma at end-of-line
        # before the next CTE name. Either inline (`, retention`) or wrapped
        # (`,\nretention`) is valid -- SQL parsers accept both.
        assert "retention AS" in rendered
        assert rendered.count("AS (") == 2

    def test_compiled_query_is_frozen(self) -> None:
        cq = CompiledQuery(ctes=(), main_sql="SELECT 1")
        with pytest.raises((AttributeError, Exception)):
            cq.main_sql = "SELECT 2"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Execution layer: CTE-aware parsing
# ---------------------------------------------------------------------------

class TestParseGovernanceWithCtes:
    def test_simple_cte_resolves_source(self) -> None:
        source, cols = _parse_for_governance(
            "WITH a AS (SELECT order_id FROM orders) SELECT order_id FROM a"
        )
        assert source == "orders"
        assert "order_id" in cols

    def test_self_join_inside_cte_accepted(self) -> None:
        # This is the cohort-retention shape: a self-join on `orders`
        # buried inside a CTE. The single-source check correctly counts
        # `orders` once (the two references are the same source).
        source, _cols = _parse_for_governance(
            "WITH cohorts AS ("
            "  SELECT user_id, MIN(order_date) AS first_order "
            "  FROM orders GROUP BY user_id"
            ") "
            "SELECT c.first_order, o.amount "
            "FROM cohorts c "
            "JOIN orders o ON c.user_id = o.user_id"
        )
        assert source == "orders"

    def test_column_inside_cte_body_visible_to_governance(self) -> None:
        # Critical security property: a sensitive column inside a CTE
        # must be visible to the governance walker. Otherwise CTEs become
        # a side channel that lets queries reach PII the role can't
        # directly access.
        _source, cols = _parse_for_governance(
            "WITH a AS (SELECT customer_email FROM orders) "
            "SELECT customer_email FROM a"
        )
        assert "customer_email" in cols

    def test_cte_output_alias_not_treated_as_column(self) -> None:
        # `cohort_size` is the CTE's projected name, NOT a real column.
        # The governance walker must not try to authorize it.
        _source, cols = _parse_for_governance(
            "WITH a AS (SELECT COUNT(*) AS cohort_size FROM orders) "
            "SELECT cohort_size FROM a"
        )
        assert "cohort_size" not in cols

    def test_pass_through_cte_alias_still_governed(self) -> None:
        # A CTE that aliases a real column to itself (`SELECT amount AS
        # amount FROM orders`) -- governance should still see `amount`.
        # find_all walks into the CTE body, where the underlying
        # exp.Column for `amount` lives.
        _source, cols = _parse_for_governance(
            "WITH a AS (SELECT amount AS amount FROM orders) "
            "SELECT amount FROM a"
        )
        # `amount` is BOTH a real column inside the CTE AND a synthesized
        # alias in the outer reference. The synth-name filter would drop
        # it from the outer ref, but the inner exp.Column ref survives.
        # Net: amount IS visible to governance.
        assert "amount" in cols

    def test_multi_source_via_cte_rejected(self) -> None:
        # A CTE that scans a DIFFERENT source than the outer SELECT must
        # still fail the single-source check.
        with pytest.raises(UnsupportedQueryError, match="exactly one source"):
            _parse_for_governance(
                "WITH a AS (SELECT id FROM customers) "
                "SELECT amount FROM orders"
            )

    def test_select_star_inside_cte_rejected(self) -> None:
        with pytest.raises(UnsupportedQueryError, match="SELECT \\*"):
            _parse_for_governance(
                "WITH a AS (SELECT * FROM orders) SELECT order_id FROM a"
            )


# ---------------------------------------------------------------------------
# compile_intent returns a CompiledQuery
# ---------------------------------------------------------------------------

class TestCompileIntentReturnType:
    def test_returns_compiled_query_with_empty_ctes_for_simple_primitive(
        self,
    ) -> None:
        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        apply_config(con, load_config(FIXTURES / "gibran.yaml"))
        try:
            intent = QueryIntent(source="orders", metrics=["order_count"])
            result = compile_intent(intent, Catalog(con))
            assert isinstance(result, CompiledQuery)
            assert result.ctes == ()
            rendered = result.render()
            assert "SELECT" in rendered
            assert "FROM" in rendered
        finally:
            con.close()


# ---------------------------------------------------------------------------
# End-to-end: CTE-shaped query through governance + audit
# ---------------------------------------------------------------------------

def _populated_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    apply_migrations(con, MIGRATIONS)
    apply_config(con, load_config(FIXTURES / "gibran.yaml"))
    con.execute(
        "CREATE TABLE orders ("
        "  order_id VARCHAR, amount DECIMAL(18,2), order_date TIMESTAMP,"
        "  status VARCHAR, region VARCHAR, customer_email VARCHAR"
        ")"
    )
    con.execute(
        "INSERT INTO orders VALUES "
        "('o1', 10, TIMESTAMP '2026-01-01', 'paid', 'west', 'a@x.com'),"
        "('o2', 20, TIMESTAMP '2026-02-01', 'paid', 'west', 'b@x.com'),"
        "('o3', 30, TIMESTAMP '2026-02-15', 'paid', 'west', 'c@x.com')"
    )
    return con


def _admin_policy(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("INSERT INTO gibran_roles (role_id, display_name) VALUES ('admin', 'Admin')")
    con.execute(
        "INSERT INTO gibran_policies "
        "(policy_id, role_id, source_id, default_column_mode) "
        "VALUES ('admin_orders', 'admin', 'orders', 'allow')"
    )


def _admin() -> IdentityContext:
    return IdentityContext(
        user_id="admin", role_id="admin", attributes={}, source="test"
    )


class TestCteEndToEnd:
    def test_cte_query_executes_cleanly_and_audits(self) -> None:
        con = _populated_db()
        _admin_policy(con)
        gov = DefaultGovernance(con)
        result = run_sql_query(
            con, gov, _admin(),
            "WITH paid AS (SELECT order_id, amount FROM orders WHERE status = 'paid') "
            "SELECT order_id FROM paid",
        )
        assert result.status == "ok", (result.deny_reason, result.error_message)
        # Three paid rows -> three results from the CTE pass-through.
        assert result.rows is not None and len(result.rows) == 3
        # Audit row written with the CTE-shaped SQL preserved.
        row = con.execute(
            "SELECT generated_sql FROM gibran_query_log WHERE query_id = ?",
            [result.query_id],
        ).fetchone()
        assert row is not None
        assert "WITH paid AS" in row[0]

    def test_cte_referencing_denied_column_denied(self) -> None:
        # external_partner has `customer_email` denied. A CTE that reads
        # it internally must still be denied -- the governance walker
        # reaches into the CTE body and sees the sensitive reference.
        # This is the SECURITY guarantee that justifies allowing CTEs.
        con = _populated_db()
        gov = DefaultGovernance(con)
        ident = IdentityContext(
            user_id="ep", role_id="external_partner",
            attributes={}, source="test",
        )
        result = run_sql_query(
            con, gov, ident,
            "WITH p AS (SELECT customer_email FROM orders) "
            "SELECT customer_email FROM p",
        )
        assert result.status == "denied"
        assert result.deny_reason is DenyReason.COLUMN_DENIED
        assert "customer_email" in (result.deny_detail or "")

    def test_cte_self_join_executes(self) -> None:
        # Sanity check that the cohort-retention shape (CTE + self-join)
        # actually executes against DuckDB end-to-end. The query computes
        # for each order, the order_date of the same customer's first
        # order via a self-join on customer_email through a CTE.
        con = _populated_db()
        _admin_policy(con)
        gov = DefaultGovernance(con)
        result = run_sql_query(
            con, gov, _admin(),
            "WITH firsts AS ("
            "  SELECT customer_email, MIN(order_date) AS first_dt "
            "  FROM orders GROUP BY customer_email"
            ") "
            "SELECT o.order_id, f.first_dt "
            "FROM orders o "
            "JOIN firsts f ON o.customer_email = f.customer_email",
        )
        assert result.status == "ok", (result.deny_reason, result.error_message)
        assert result.rows is not None
