"""End-to-end SQL execution: parse + govern + rewrite + execute + audit."""
from pathlib import Path

import duckdb
import pytest

from gibran.execution.sql import (
    QueryParseError,
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


def _make_db_with_orders_data() -> duckdb.DuckDBPyConnection:
    """Build a DB with the fixture catalog AND a real orders table the
    rewritten queries can hit."""
    con = duckdb.connect(":memory:")
    apply_migrations(con, MIGRATIONS)
    apply_config(con, load_config(FIXTURES / "gibran.yaml"))
    con.execute(
        """
        CREATE TABLE orders (
            order_id VARCHAR,
            amount DECIMAL(18,2),
            order_date TIMESTAMP,
            status VARCHAR,
            region VARCHAR,
            customer_email VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO orders VALUES
            ('o1', 100.00, '2025-01-15 10:00:00', 'paid',    'west',  'a@example.com'),
            ('o2', 200.00, '2025-01-20 11:00:00', 'paid',    'east',  'b@example.com'),
            ('o3',  50.00, '2025-02-01 12:00:00', 'pending', 'west',  'c@example.com'),
            ('o4', 300.00, '2025-02-15 13:00:00', 'paid',    'north', 'd@example.com')
        """
    )
    return con


def _ident(role: str, user: str | None = None, **attrs: str) -> IdentityContext:
    return IdentityContext(
        user_id=user or f"u_{role}",
        role_id=role,
        attributes=dict(attrs),
        source="test",
    )


# ---------------------------------------------------------------------------
# Parser unit tests
# ---------------------------------------------------------------------------

class TestParseForGovernance:
    def test_extracts_source_and_columns(self) -> None:
        source, cols = _parse_for_governance(
            "SELECT order_id, amount FROM orders WHERE status = 'paid'"
        )
        assert source == "orders"
        assert cols == frozenset({"order_id", "amount", "status"})

    def test_aggregation_extracts_inner_column(self) -> None:
        source, cols = _parse_for_governance("SELECT SUM(amount) FROM orders")
        assert source == "orders"
        assert cols == frozenset({"amount"})

    def test_count_star_yields_zero_columns(self) -> None:
        source, cols = _parse_for_governance("SELECT COUNT(*) FROM orders")
        assert source == "orders"
        assert cols == frozenset()

    def test_alias_does_not_count_as_column(self) -> None:
        source, cols = _parse_for_governance(
            "SELECT amount AS total FROM orders"
        )
        assert cols == frozenset({"amount"})

    def test_qualified_columns_resolved_by_name(self) -> None:
        source, cols = _parse_for_governance(
            "SELECT o.order_id, o.amount FROM orders o WHERE o.status = 'paid'"
        )
        assert source == "orders"
        assert cols == frozenset({"order_id", "amount", "status"})

    def test_select_star_rejected(self) -> None:
        with pytest.raises(UnsupportedQueryError, match="SELECT \\*"):
            _parse_for_governance("SELECT * FROM orders")

    def test_join_rejected(self) -> None:
        with pytest.raises(UnsupportedQueryError, match="join"):
            _parse_for_governance(
                "SELECT a.x FROM a JOIN b ON a.id = b.id"
            )

    def test_subquery_rejected(self) -> None:
        with pytest.raises(UnsupportedQueryError, match="subquer"):
            _parse_for_governance(
                "SELECT amount FROM orders WHERE order_id IN (SELECT id FROM other)"
            )

    def test_cte_rejected(self) -> None:
        with pytest.raises(UnsupportedQueryError, match="CTE"):
            _parse_for_governance(
                "WITH a AS (SELECT amount FROM orders) SELECT amount FROM a"
            )

    def test_dml_rejected(self) -> None:
        with pytest.raises(UnsupportedQueryError, match="only SELECT"):
            _parse_for_governance("DELETE FROM orders WHERE order_id = 'x'")

    def test_garbage_sql_raises_parse_error(self) -> None:
        with pytest.raises((QueryParseError, UnsupportedQueryError)):
            _parse_for_governance("SELEC bad sql definitely not valid")


# ---------------------------------------------------------------------------
# End-to-end execution
# ---------------------------------------------------------------------------

class TestRunSQLQueryAllowed:
    def test_analyst_west_sees_only_west_rows(self) -> None:
        con = _make_db_with_orders_data()
        gov = DefaultGovernance(con)
        result = run_sql_query(
            con, gov,
            _ident("analyst_west", region="west"),
            "SELECT order_id, amount FROM orders",
        )
        assert result.status == "ok"
        order_ids = {row[0] for row in result.rows}
        assert order_ids == {"o1", "o3"}

    def test_filter_anded_with_user_where(self) -> None:
        con = _make_db_with_orders_data()
        gov = DefaultGovernance(con)
        result = run_sql_query(
            con, gov,
            _ident("analyst_west", region="west"),
            "SELECT order_id FROM orders WHERE status = 'paid'",
        )
        assert result.status == "ok"
        # west AND paid: just o1
        assert {row[0] for row in result.rows} == {"o1"}

    def test_aggregation_respects_filter(self) -> None:
        con = _make_db_with_orders_data()
        gov = DefaultGovernance(con)
        result = run_sql_query(
            con, gov,
            _ident("analyst_west", region="west"),
            "SELECT SUM(amount) FROM orders",
        )
        assert result.status == "ok"
        # west rows: 100 + 50 = 150
        assert float(result.rows[0][0]) == 150.0

    def test_count_star_no_column_access_required(self) -> None:
        con = _make_db_with_orders_data()
        gov = DefaultGovernance(con)
        # external_partner can NOT see customer_email; COUNT(*) doesn't need columns
        result = run_sql_query(
            con, gov,
            _ident("external_partner"),
            "SELECT COUNT(*) FROM orders",
        )
        assert result.status == "ok"
        # external_partner has region='west' literal filter -> 2 rows
        assert result.rows[0][0] == 2

    def test_external_partner_explicit_columns(self) -> None:
        con = _make_db_with_orders_data()
        gov = DefaultGovernance(con)
        result = run_sql_query(
            con, gov,
            _ident("external_partner"),
            "SELECT order_id, amount FROM orders",
        )
        assert result.status == "ok"
        order_ids = {row[0] for row in result.rows}
        assert order_ids == {"o1", "o3"}


class TestRunSQLQueryDenied:
    def test_pii_column_denied_for_external_partner(self) -> None:
        con = _make_db_with_orders_data()
        gov = DefaultGovernance(con)
        result = run_sql_query(
            con, gov,
            _ident("external_partner"),
            "SELECT customer_email FROM orders",
        )
        assert result.status == "denied"
        assert result.deny_reason is DenyReason.COLUMN_DENIED
        assert result.deny_detail == "customer_email"
        assert result.rows is None

    def test_unknown_role_denied(self) -> None:
        con = _make_db_with_orders_data()
        gov = DefaultGovernance(con)
        result = run_sql_query(
            con, gov,
            _ident("ghost"),
            "SELECT order_id FROM orders",
        )
        assert result.status == "denied"
        assert result.deny_reason is DenyReason.NO_POLICY

    def test_missing_attribute_denied(self) -> None:
        con = _make_db_with_orders_data()
        gov = DefaultGovernance(con)
        # analyst_west's policy uses {"$attr": "region"}
        result = run_sql_query(
            con, gov,
            _ident("analyst_west"),  # no region attr
            "SELECT order_id FROM orders",
        )
        assert result.status == "denied"
        assert result.deny_reason is DenyReason.ATTRIBUTE_MISSING


class TestRunSQLQueryErrors:
    def test_select_star_recorded_as_error(self) -> None:
        con = _make_db_with_orders_data()
        gov = DefaultGovernance(con)
        result = run_sql_query(
            con, gov,
            _ident("analyst_west", region="west"),
            "SELECT * FROM orders",
        )
        assert result.status == "error"
        assert "SELECT *" in (result.error_message or "")

    def test_dml_recorded_as_error(self) -> None:
        con = _make_db_with_orders_data()
        gov = DefaultGovernance(con)
        result = run_sql_query(
            con, gov,
            _ident("analyst_west", region="west"),
            "DELETE FROM orders WHERE order_id = 'o1'",
        )
        assert result.status == "error"
        # Verify nothing actually got deleted
        count = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        assert count == 4


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------

class TestAuditLog:
    def test_allowed_query_writes_log_row(self) -> None:
        con = _make_db_with_orders_data()
        gov = DefaultGovernance(con)
        result = run_sql_query(
            con, gov,
            _ident("analyst_west", region="west", user="alice"),
            "SELECT order_id FROM orders",
        )
        log = con.execute(
            "SELECT user_id, role_id, status, generated_sql, row_count, deny_reason "
            "FROM gibran_query_log WHERE query_id = ?",
            [result.query_id],
        ).fetchone()
        assert log[0] == "alice"
        assert log[1] == "analyst_west"
        assert log[2] == "ok"
        assert "region" in log[3]                 # rewritten SQL has injected filter
        assert log[4] == 2                        # west rows
        assert log[5] is None

    def test_denied_query_writes_log_row(self) -> None:
        con = _make_db_with_orders_data()
        gov = DefaultGovernance(con)
        result = run_sql_query(
            con, gov,
            _ident("external_partner"),
            "SELECT customer_email FROM orders",
        )
        log = con.execute(
            "SELECT status, deny_reason, row_count, generated_sql "
            "FROM gibran_query_log WHERE query_id = ?",
            [result.query_id],
        ).fetchone()
        assert log[0] == "denied"
        assert log[1] == "policy:no_column_access:customer_email"
        assert log[2] is None
        # Original SQL preserved (not the rewritten one — deny short-circuits before rewrite)
        assert "customer_email" in log[3]

    def test_error_query_writes_log_row(self) -> None:
        con = _make_db_with_orders_data()
        gov = DefaultGovernance(con)
        result = run_sql_query(
            con, gov,
            _ident("analyst_west", region="west"),
            "SELECT * FROM orders",
        )
        log = con.execute(
            "SELECT status, deny_reason, row_count "
            "FROM gibran_query_log WHERE query_id = ?",
            [result.query_id],
        ).fetchone()
        assert log[0] == "error"
        assert log[1] is None
        assert log[2] is None

    def test_each_query_gets_unique_id(self) -> None:
        con = _make_db_with_orders_data()
        gov = DefaultGovernance(con)
        ident = _ident("analyst_west", region="west")
        ids = set()
        for _ in range(5):
            result = run_sql_query(
                con, gov, ident, "SELECT COUNT(*) FROM orders"
            )
            ids.add(result.query_id)
        assert len(ids) == 5
        rows = con.execute(
            "SELECT COUNT(*) FROM gibran_query_log WHERE user_id = ?",
            [ident.user_id],
        ).fetchone()
        assert rows[0] == 5
