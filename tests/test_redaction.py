"""Tests for audit-log literal redaction.

Three layers:
  1. `redact_sql_literals` -- pure function over sqlglot AST.
  2. `redact_intent_literals` -- pure function over DSL intent JSON.
  3. End-to-end through `run_sql_query` and `run_dsl_query`, verifying
     that what lands in `gibran_query_log` has no leaked literals.

The fixture YAML already ships with `customer_email` classified as
`pii`; we reuse it everywhere instead of constructing synthetic state.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

from gibran.dsl.run import run_dsl_query
from gibran.execution.sql import run_sql_query
from gibran.governance.default import DefaultGovernance
from gibran.governance.redaction import (
    lookup_sensitive_columns,
    redact_audit_payload,
    redact_intent_literals,
    redact_sql_literals,
)
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
    # Provision a real `orders` table so run_sql_query / run_dsl_query
    # execute end-to-end (governance accepts duckdb_table sources by
    # relation name).
    con.execute(
        "CREATE TABLE orders ("
        "  order_id VARCHAR, amount DECIMAL(18,2), order_date TIMESTAMP,"
        "  status VARCHAR, region VARCHAR, customer_email VARCHAR"
        ")"
    )
    con.execute(
        "INSERT INTO orders VALUES "
        "('o1', 10, TIMESTAMP '2026-01-01', 'paid', 'west', 'alice@x.com'),"
        "('o2', 20, TIMESTAMP '2026-02-01', 'paid', 'west', 'bob@y.com')"
    )
    return con


def _admin_identity() -> IdentityContext:
    return IdentityContext(
        user_id="admin", role_id="admin", attributes={}, source="test"
    )


def _admin_policy(con: duckdb.DuckDBPyConnection) -> None:
    # Allow-everything policy for the admin role so raw-SQL paths can
    # touch customer_email without triggering COLUMN_DENIED.
    con.execute("INSERT INTO gibran_roles VALUES ('admin', 'Admin')")
    con.execute(
        "INSERT INTO gibran_policies (policy_id, role_id, source_id, default_column_mode) "
        "VALUES ('admin_orders', 'admin', 'orders', 'allow')"
    )


# ---------------------------------------------------------------------------
# Pure-function: SQL redactor
# ---------------------------------------------------------------------------

class TestRedactSqlLiterals:
    SENSITIVE = frozenset({"customer_email"})

    def test_eq_lhs_sensitive(self) -> None:
        out = redact_sql_literals(
            "SELECT order_id FROM orders WHERE customer_email = 'alice@x.com'",
            self.SENSITIVE,
        )
        assert "alice@x.com" not in out
        assert "<redacted>" in out

    def test_eq_rhs_sensitive_also_redacts(self) -> None:
        # 'alice@x.com' = customer_email -- same predicate, swapped sides.
        out = redact_sql_literals(
            "SELECT order_id FROM orders WHERE 'alice@x.com' = customer_email",
            self.SENSITIVE,
        )
        assert "alice@x.com" not in out
        assert "<redacted>" in out

    def test_in_list_all_redacted(self) -> None:
        out = redact_sql_literals(
            "SELECT order_id FROM orders WHERE customer_email IN ('a@x.com', 'b@x.com')",
            self.SENSITIVE,
        )
        assert "a@x.com" not in out
        assert "b@x.com" not in out
        # Both literals replaced
        assert out.count("<redacted>") == 2

    def test_between_both_endpoints_redacted(self) -> None:
        out = redact_sql_literals(
            "SELECT order_id FROM orders "
            "WHERE customer_email BETWEEN 'a@x.com' AND 'z@x.com'",
            self.SENSITIVE,
        )
        assert "a@x.com" not in out
        assert "z@x.com" not in out
        assert out.count("<redacted>") == 2

    def test_like_redacted(self) -> None:
        # LIKE isn't in the DSL whitelist but can appear in raw SQL.
        out = redact_sql_literals(
            "SELECT order_id FROM orders WHERE customer_email LIKE 'alice%'",
            self.SENSITIVE,
        )
        assert "alice" not in out
        assert "<redacted>" in out

    def test_public_column_unaffected(self) -> None:
        out = redact_sql_literals(
            "SELECT order_id FROM orders WHERE amount > 100",
            self.SENSITIVE,
        )
        assert "100" in out

    def test_mixed_predicates_only_sensitive_redacted(self) -> None:
        out = redact_sql_literals(
            "SELECT order_id FROM orders "
            "WHERE customer_email = 'alice@x.com' AND amount > 100",
            self.SENSITIVE,
        )
        assert "alice@x.com" not in out
        assert "100" in out  # amount is public, untouched
        assert "<redacted>" in out

    def test_unparseable_sql_returned_as_is(self) -> None:
        garbage = "this is not sql alice@x.com"
        # If sqlglot can't parse, we accept the leak rather than crashing
        # the audit write (fail-open) -- the same string comes back.
        out = redact_sql_literals(garbage, self.SENSITIVE)
        assert out == garbage

    def test_empty_sensitive_columns_short_circuits(self) -> None:
        sql = "SELECT order_id FROM orders WHERE customer_email = 'x'"
        assert redact_sql_literals(sql, frozenset()) == sql

    def test_empty_input_returned_as_is(self) -> None:
        assert redact_sql_literals("", self.SENSITIVE) == ""

    def test_select_projection_with_sensitive_column_no_literal(self) -> None:
        # No literal to redact; column reference alone is fine.
        out = redact_sql_literals(
            "SELECT customer_email FROM orders WHERE amount > 100",
            self.SENSITIVE,
        )
        assert "customer_email" in out
        assert "100" in out


# ---------------------------------------------------------------------------
# Pure-function: intent JSON redactor
# ---------------------------------------------------------------------------

class TestRedactIntentLiterals:
    SENSITIVE = frozenset({"customer_email"})

    def _redact(self, intent: dict) -> dict:
        return json.loads(redact_intent_literals(json.dumps(intent), self.SENSITIVE))

    def test_eq_value_replaced(self) -> None:
        out = self._redact({
            "source": "orders",
            "filters": [{"op": "eq", "column": "customer_email", "value": "alice@x.com"}],
        })
        assert out["filters"][0]["value"] == "<redacted>"

    def test_in_values_all_replaced(self) -> None:
        out = self._redact({
            "source": "orders",
            "filters": [{
                "op": "in",
                "column": "customer_email",
                "value": ["a@x.com", "b@x.com"],
            }],
        })
        assert out["filters"][0]["value"] == ["<redacted>", "<redacted>"]

    def test_between_values_replaced(self) -> None:
        out = self._redact({
            "source": "orders",
            "filters": [{
                "op": "between",
                "column": "customer_email",
                "value": ["a", "z"],
            }],
        })
        assert out["filters"][0]["value"] == ["<redacted>", "<redacted>"]

    def test_public_column_value_unchanged(self) -> None:
        out = self._redact({
            "source": "orders",
            "filters": [{"op": "eq", "column": "amount", "value": 100}],
        })
        assert out["filters"][0]["value"] == 100

    def test_nested_and_recurses(self) -> None:
        out = self._redact({
            "source": "orders",
            "filters": [{
                "op": "and",
                "args": [
                    {"op": "eq", "column": "customer_email", "value": "alice@x.com"},
                    {"op": "eq", "column": "amount", "value": 100},
                ],
            }],
        })
        and_node = out["filters"][0]
        assert and_node["args"][0]["value"] == "<redacted>"
        assert and_node["args"][1]["value"] == 100

    def test_nested_not_recurses(self) -> None:
        out = self._redact({
            "source": "orders",
            "filters": [{
                "op": "not",
                "args": [{"op": "eq", "column": "customer_email", "value": "alice"}],
            }],
        })
        assert out["filters"][0]["args"][0]["value"] == "<redacted>"

    def test_is_null_no_value_to_redact(self) -> None:
        # No `value` key in is_null ASTs -- redactor must leave them alone.
        intent = {
            "source": "orders",
            "filters": [{"op": "is_null", "column": "customer_email"}],
        }
        out = self._redact(intent)
        assert out["filters"][0] == {"op": "is_null", "column": "customer_email"}

    def test_invalid_json_returned_as_is(self) -> None:
        garbage = "{not json"
        assert redact_intent_literals(garbage, self.SENSITIVE) == garbage

    def test_missing_filters_returned_as_is(self) -> None:
        # A JSON object without `filters[]` has nothing to redact.
        s = json.dumps({"source": "orders", "metrics": ["gross_revenue"]})
        assert redact_intent_literals(s, self.SENSITIVE) == s

    def test_empty_sensitive_columns_short_circuits(self) -> None:
        s = json.dumps({
            "source": "orders",
            "filters": [{"op": "eq", "column": "customer_email", "value": "x"}],
        })
        assert redact_intent_literals(s, frozenset()) == s


# ---------------------------------------------------------------------------
# lookup_sensitive_columns
# ---------------------------------------------------------------------------

class TestLookupSensitiveColumns:
    def test_scoped_to_source(self) -> None:
        con = _populated_db()
        cols = lookup_sensitive_columns(con, "orders")
        assert cols == frozenset({"customer_email"})

    def test_global_lookup_when_source_none(self) -> None:
        con = _populated_db()
        cols = lookup_sensitive_columns(con, None)
        assert "customer_email" in cols

    def test_unknown_source_returns_empty(self) -> None:
        con = _populated_db()
        assert lookup_sensitive_columns(con, "no_such_source") == frozenset()


# ---------------------------------------------------------------------------
# End-to-end: raw SQL path
# ---------------------------------------------------------------------------

class TestRawSqlAuditRedaction:
    def test_select_with_sensitive_filter_redacts_in_audit_log(self) -> None:
        con = _populated_db()
        _admin_policy(con)
        gov = DefaultGovernance(con)
        result = run_sql_query(
            con, gov, _admin_identity(),
            "SELECT order_id FROM orders WHERE customer_email = 'alice@x.com'",
        )
        assert result.status == "ok"
        stored = con.execute(
            "SELECT generated_sql FROM gibran_query_log WHERE query_id = ?",
            [result.query_id],
        ).fetchone()[0]
        assert "alice@x.com" not in stored
        assert "<redacted>" in stored

    def test_denied_query_still_redacts(self) -> None:
        # A denied query still writes generated_sql to the audit log; that
        # write must redact too (deny detail is a separate field).
        con = _populated_db()
        gov = DefaultGovernance(con)
        # external_partner has no access to customer_email
        ident = IdentityContext(
            user_id="ep", role_id="external_partner", attributes={}, source="test"
        )
        result = run_sql_query(
            con, gov, ident,
            "SELECT customer_email FROM orders WHERE customer_email = 'alice@x.com'",
        )
        assert result.status == "denied"
        stored = con.execute(
            "SELECT generated_sql FROM gibran_query_log WHERE query_id = ?",
            [result.query_id],
        ).fetchone()[0]
        assert "alice@x.com" not in stored

    def test_public_filter_not_touched(self) -> None:
        con = _populated_db()
        _admin_policy(con)
        gov = DefaultGovernance(con)
        result = run_sql_query(
            con, gov, _admin_identity(),
            "SELECT order_id FROM orders WHERE amount > 5",
        )
        assert result.status == "ok"
        stored = con.execute(
            "SELECT generated_sql FROM gibran_query_log WHERE query_id = ?",
            [result.query_id],
        ).fetchone()[0]
        # `amount > 5` should appear in some form (sqlglot may reformat)
        assert "5" in stored
        assert "<redacted>" not in stored


# ---------------------------------------------------------------------------
# End-to-end: DSL path
# ---------------------------------------------------------------------------

class TestDslAuditRedaction:
    def test_dsl_filter_redacts_nl_prompt(self) -> None:
        # External partner can see order_id + amount + order_date, can filter
        # on those too. We can't filter on customer_email through a DSL
        # path that would pass governance (the column is denied). So this
        # test exercises a PRE-COMPILE FAILURE: the intent references the
        # sensitive column, validate_intent rejects it, the failing
        # nl_prompt still needs to be redacted.
        con = _populated_db()
        gov = DefaultGovernance(con)
        ident = IdentityContext(
            user_id="ep", role_id="external_partner", attributes={}, source="test"
        )
        intent = {
            "source": "orders",
            "metrics": ["order_count"],
            "filters": [{
                "op": "eq",
                "column": "customer_email",
                "value": "alice@x.com",
            }],
        }
        result = run_dsl_query(con, gov, ident, intent)
        # Validation should have rejected it before SQL emission.
        assert result.pre_compile_error is not None
        stored = con.execute(
            "SELECT nl_prompt FROM gibran_query_log WHERE query_id = ?",
            [result.query_id],
        ).fetchone()[0]
        assert "alice@x.com" not in stored
        assert "<redacted>" in stored

    def test_dsl_successful_run_redacts_both_fields(self) -> None:
        # Admin can SELECT customer_email; a DSL intent with a sensitive
        # filter that DOES pass governance must redact BOTH nl_prompt
        # (the intent JSON) AND generated_sql (the compiled query).
        # The fixture DSL grammar requires filters on intent.columns, but
        # external_partner has customer_email denied. So we use admin.
        con = _populated_db()
        _admin_policy(con)
        gov = DefaultGovernance(con)
        intent = {
            "source": "orders",
            "metrics": ["order_count"],
            "filters": [{
                "op": "eq",
                "column": "customer_email",
                "value": "alice@x.com",
            }],
        }
        result = run_dsl_query(con, gov, _admin_identity(), intent)
        # The DSL compiler should produce SQL; whether it executes is
        # secondary -- the audit row is what we care about.
        stored = con.execute(
            "SELECT nl_prompt, generated_sql FROM gibran_query_log "
            "WHERE query_id = ?",
            [result.query_id],
        ).fetchone()
        nl_prompt, generated_sql = stored
        assert "alice@x.com" not in (nl_prompt or "")
        assert "alice@x.com" not in (generated_sql or "")


# ---------------------------------------------------------------------------
# redact_audit_payload chokepoint
# ---------------------------------------------------------------------------

class TestRedactAuditPayload:
    def test_no_sensitive_columns_passes_through(self) -> None:
        # A catalog with no pii/restricted columns: redactor returns
        # input untouched (no DB lookup overhead matters here).
        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        # Don't sync any sources -> gibran_columns is empty.
        sql_in = "SELECT * FROM x WHERE email = 'a@x.com'"
        intent_in = '{"source":"x","filters":[{"op":"eq","column":"email","value":"a"}]}'
        sql_out, intent_out = redact_audit_payload(con, None, sql_in, intent_in)
        assert sql_out == sql_in
        assert intent_out == intent_in

    def test_redacts_both_when_sensitive_present(self) -> None:
        con = _populated_db()
        sql_in = "SELECT order_id FROM orders WHERE customer_email = 'a@x.com'"
        intent_in = json.dumps({
            "source": "orders",
            "filters": [{"op": "eq", "column": "customer_email", "value": "a@x.com"}],
        })
        sql_out, intent_out = redact_audit_payload(con, "orders", sql_in, intent_in)
        assert "a@x.com" not in sql_out
        assert "a@x.com" not in intent_out
