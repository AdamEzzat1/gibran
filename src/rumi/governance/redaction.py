"""Audit-log literal redaction for `rumi_query_log`.

The audit log records two fields that can carry user-supplied literal
values from filter predicates:

  - `generated_sql`     -- the (possibly rewritten) SQL that was executed
  - `nl_prompt`         -- for DSL-path queries, the JSON intent (per
                           rumi_constraints.md the field was repurposed
                           from its original NL-emission meaning)

If a predicate compares a `pii` or `restricted` column against a
literal, the literal value re-leaks via the audit log itself unless we
strip it before persistence. This module provides two redactors -- one
walks parsed SQL via sqlglot, the other walks the DSL intent JSON --
and a small DB-aware helper that resolves the sensitive-column set.

Design notes:

- The redactors are pure functions (no DB connection) parameterized on
  a `sensitive_columns: frozenset[str]`. Same split as
  governance.ast.validate_intent_ast / compile_intent_to_sql -- pure
  computation + a separate database-aware caller.

- Both redactors fail OPEN: if the SQL can't be parsed or the JSON
  can't be decoded, the input is returned unchanged. Losing the audit
  row entirely (by raising) is worse than logging the unparseable
  original; a parse failure is rare and usually means the query
  errored out anyway.

- The replacement value is the string literal `<redacted>` (no quotes
  in JSON; quoted via sqlglot in SQL). The redacted form is not
  intended to be executable -- the audit log records *what was tried*,
  not a replayable query.

- The lookup helper returns a global "all sensitive columns across all
  sources" set when source_id is None. That over-redacts on parse
  failures (we'd redact `email` in a source where it isn't sensitive
  if some OTHER source has a sensitive `email`), but under-redacting
  is the unacceptable direction for a security feature.
"""
from __future__ import annotations

import json
from typing import Any

import duckdb
import sqlglot
from sqlglot import exp


REDACTED = "<redacted>"
SENSITIVE_LEVELS = ("pii", "restricted")


# ---------------------------------------------------------------------------
# DB-aware lookup
# ---------------------------------------------------------------------------

def lookup_sensitive_columns(
    con: duckdb.DuckDBPyConnection, source_id: str | None = None
) -> frozenset[str]:
    """Resolve the set of column names whose sensitivity is in (pii, restricted).

    If `source_id` is given, scope to that source. If None (caller could not
    determine source -- e.g. raw SQL failed to parse), return the union
    across every source. Over-redaction is the safer failure mode.
    """
    if source_id is None:
        rows = con.execute(
            "SELECT DISTINCT column_name FROM rumi_columns "
            "WHERE sensitivity IN ('pii', 'restricted')"
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT column_name FROM rumi_columns "
            "WHERE source_id = ? AND sensitivity IN ('pii', 'restricted')",
            [source_id],
        ).fetchall()
    return frozenset(r[0] for r in rows)


# ---------------------------------------------------------------------------
# SQL redactor
# ---------------------------------------------------------------------------

# Binary comparisons whose RHS literal must be redacted when the LHS
# is a sensitive column (and vice versa). LIKE / ILIKE are not in the
# DSL operator whitelist but can appear in user-authored raw SQL, so
# they're covered here.
_BINARY_OP_CLASSES = (
    exp.EQ, exp.NEQ, exp.LT, exp.LTE, exp.GT, exp.GTE,
    exp.Like, exp.ILike,
)


def redact_sql_literals(sql: str, sensitive_columns: frozenset[str]) -> str:
    """Return `sql` with any literal compared to a sensitive column
    replaced by `'<redacted>'`. Returns input unchanged if sqlglot
    cannot parse it.

    V1 scope is constrained by the execution-path grammar -- no
    subqueries, no CTEs, no joins -- so column references are
    unambiguous (single-source). When CTE infra lands in Tier 3 the
    column->source resolution needs to be revisited: a sensitive
    column could appear inside a CTE that selects a different source.
    """
    if not sql or not sensitive_columns:
        return sql
    try:
        tree = sqlglot.parse_one(sql, dialect="duckdb")
    except Exception:
        return sql

    for predicate in tree.find_all(*_BINARY_OP_CLASSES):
        left, right = predicate.left, predicate.right
        if _is_sensitive_column(left, sensitive_columns):
            _redact_literals_in(right)
        if _is_sensitive_column(right, sensitive_columns):
            _redact_literals_in(left)

    for in_expr in tree.find_all(exp.In):
        if _is_sensitive_column(in_expr.this, sensitive_columns):
            for value in in_expr.expressions:
                _redact_literals_in(value)

    for between in tree.find_all(exp.Between):
        if _is_sensitive_column(between.this, sensitive_columns):
            _redact_literals_in(between.args.get("low"))
            _redact_literals_in(between.args.get("high"))

    return tree.sql(dialect="duckdb")


def _is_sensitive_column(
    node: exp.Expression | None, sensitive_columns: frozenset[str]
) -> bool:
    return isinstance(node, exp.Column) and node.name in sensitive_columns


def _redact_literals_in(node: exp.Expression | None) -> None:
    """Replace every Literal in the subtree rooted at `node` with a
    redacted string literal. Modifies the AST in place."""
    if node is None:
        return
    if isinstance(node, exp.Literal):
        node.replace(exp.Literal.string(REDACTED))
        return
    for lit in list(node.find_all(exp.Literal)):
        lit.replace(exp.Literal.string(REDACTED))


# ---------------------------------------------------------------------------
# Intent JSON redactor
# ---------------------------------------------------------------------------

_COMPARISON_OPS = frozenset({"eq", "neq", "lt", "lte", "gt", "gte"})
_SET_OPS = frozenset({"in", "not_in"})


def redact_intent_literals(
    intent_json: str, sensitive_columns: frozenset[str]
) -> str:
    """Walk a DSL intent's `filters[]` and replace `value` payloads where
    the predicate `column` is sensitive. Returns the original string if
    the input isn't valid JSON or isn't shaped like a DSL intent.

    The AST node shape is defined in `governance.ast`:
        {"op": "eq",  "column": <str>, "value": <scalar>}
        {"op": "in",  "column": <str>, "value": [<scalar>, ...]}
        {"op": "between", "column": <str>, "value": [<low>, <high>]}
        {"op": "and"|"or", "args": [<node>, ...]}
        {"op": "not", "args": [<node>]}
        {"op": "is_null"|"is_not_null", "column": <str>}
    """
    if not intent_json or not sensitive_columns:
        return intent_json
    try:
        intent = json.loads(intent_json)
    except (ValueError, TypeError):
        return intent_json
    if not isinstance(intent, dict):
        return intent_json

    filters = intent.get("filters")
    if isinstance(filters, list):
        for node in filters:
            _redact_intent_node(node, sensitive_columns)
        # sort_keys=True matches the canonical form emitted by run_dsl_query
        return json.dumps(intent, sort_keys=True, default=str)
    return intent_json


def _redact_intent_node(node: Any, sensitive_columns: frozenset[str]) -> None:
    if not isinstance(node, dict):
        return
    op = node.get("op")
    if not isinstance(op, str):
        return

    if op in ("and", "or", "not"):
        args = node.get("args")
        if isinstance(args, list):
            for child in args:
                _redact_intent_node(child, sensitive_columns)
        return

    column = node.get("column")
    if not isinstance(column, str) or column not in sensitive_columns:
        return

    if op in _COMPARISON_OPS:
        if "value" in node:
            node["value"] = REDACTED
        return

    if op in _SET_OPS:
        value = node.get("value")
        if isinstance(value, list):
            node["value"] = [REDACTED for _ in value]
        return

    if op == "between":
        value = node.get("value")
        if isinstance(value, list) and len(value) == 2:
            node["value"] = [REDACTED, REDACTED]
        return

    # is_null / is_not_null carry no values -- nothing to redact.


# ---------------------------------------------------------------------------
# One-shot chokepoint for audit-log writes
# ---------------------------------------------------------------------------

def redact_audit_payload(
    con: duckdb.DuckDBPyConnection,
    source_id: str | None,
    generated_sql: str,
    nl_prompt: str | None,
) -> tuple[str, str | None]:
    """Resolve sensitive columns once, then redact both audit fields.

    The two callers -- execution.sql._write_query_log and
    dsl.run._pre_compile_failure -- both flow through this helper before
    INSERTing into rumi_query_log, ensuring no path bypasses redaction.
    """
    sensitive = lookup_sensitive_columns(con, source_id)
    if not sensitive:
        return generated_sql, nl_prompt
    redacted_sql = redact_sql_literals(generated_sql, sensitive)
    redacted_intent = (
        redact_intent_literals(nl_prompt, sensitive) if nl_prompt else nl_prompt
    )
    return redacted_sql, redacted_intent
