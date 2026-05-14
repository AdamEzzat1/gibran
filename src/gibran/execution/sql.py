"""SQL execution path: parse, govern, rewrite, execute, audit.

V1 constraints (intentional, enforced before governance is consulted):
  - Top-level statement must be SELECT
  - Exactly one source (no joins)
  - No subqueries, no CTEs
  - No SELECT * (forces explicit column enumeration so governance can
    enforce column-level access)

A `gibran_query_log` row is written for every attempt -- allow, deny, or
error. The audit trail outlives the success of any individual query.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Literal

import duckdb
import sqlglot
from sqlglot import exp

from gibran.governance.redaction import redact_audit_payload
from gibran.governance.types import (
    DenyReason,
    GovernanceAPI,
    GovernanceDecision,
    IdentityContext,
)


class QueryParseError(ValueError):
    """SQL could not be parsed by sqlglot."""


class UnsupportedQueryError(ValueError):
    """SQL parsed but uses features the V1 runner does not support."""


@dataclass(frozen=True)
class QueryResult:
    query_id: str
    status: Literal["ok", "denied", "error"]
    rows: tuple[tuple, ...] | None
    columns: tuple[str, ...] | None
    deny_reason: DenyReason | None
    deny_detail: str | None
    error_message: str | None
    duration_ms: int
    rewritten_sql: str | None
    original_sql: str


def run_sql_query(
    con: duckdb.DuckDBPyConnection,
    governance: GovernanceAPI,
    identity: IdentityContext,
    sql: str,
    *,
    nl_prompt: str | None = None,
) -> QueryResult:
    """Execute a governed SQL query end-to-end.

    Returns a QueryResult; never raises (all errors are captured and
    written to the audit log).

    `nl_prompt` is recorded in gibran_query_log for traceability. Raw-SQL
    callers leave it None; the DSL runner passes the JSON intent so the
    audit log captures the user-authored intent alongside the compiled SQL.
    """
    query_id = str(uuid.uuid4())
    started_ns = time.monotonic_ns()

    try:
        source_id, requested_columns = _parse_for_governance(sql)
    except (QueryParseError, UnsupportedQueryError) as e:
        # source_id is unknown here -- redaction will fall back to the
        # global sensitive-column set in lookup_sensitive_columns.
        return _record_error(
            con, query_id, identity, sql, str(e), started_ns,
            nl_prompt=nl_prompt, source_id=None,
        )

    decision = governance.evaluate(
        identity,
        frozenset({source_id}),
        frozenset(requested_columns),
        (),  # no metric refs in raw SQL path
    )

    if not decision.allowed:
        return _record_denied(
            con, query_id, identity, sql, decision, started_ns,
            nl_prompt=nl_prompt, source_id=source_id,
        )

    rewritten = (
        _inject_filter(sql, decision.injected_filter_sql)
        if decision.injected_filter_sql
        else sql
    )

    try:
        cur = con.execute(rewritten)
        rows = cur.fetchall()
        col_names = [d[0] for d in cur.description] if cur.description else []
    except Exception as e:
        return _record_error(
            con, query_id, identity, rewritten,
            f"execution error: {e}", started_ns,
            nl_prompt=nl_prompt, source_id=source_id,
        )

    duration_ms = (time.monotonic_ns() - started_ns) // 1_000_000
    _write_query_log(
        con,
        query_id=query_id, identity=identity,
        nl_prompt=nl_prompt, generated_sql=rewritten,
        status="ok", deny_reason=None,
        row_count=len(rows), duration_ms=duration_ms,
        source_id=source_id,
    )
    return QueryResult(
        query_id=query_id,
        status="ok",
        rows=tuple(rows),
        columns=tuple(col_names),
        deny_reason=None,
        deny_detail=None,
        error_message=None,
        duration_ms=duration_ms,
        rewritten_sql=rewritten,
        original_sql=sql,
    )


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _parse_for_governance(sql: str) -> tuple[str, frozenset[str]]:
    try:
        parsed = sqlglot.parse_one(sql, dialect="duckdb")
    except Exception as e:
        raise QueryParseError(f"could not parse SQL: {e}") from e

    if not isinstance(parsed, exp.Select):
        raise UnsupportedQueryError(
            f"only SELECT supported in V1, got {type(parsed).__name__}"
        )

    # CTEs ARE supported in V1 (Tier 3 work). The DSL compiler may emit
    # `WITH a AS (...), b AS (...) SELECT ...` for multi-stage primitives
    # like cohort_retention and funnel. Joins must therefore be allowed
    # too -- those primitives self-join the same source inside a CTE.
    # Subqueries remain forbidden in V1: they introduce a column-scoping
    # complexity (nested SELECTs can rebind names) that the governance
    # walker doesn't handle yet. CTEs cover the same expressive territory
    # for the primitives we care about.
    if list(parsed.find_all(exp.Subquery)):
        raise UnsupportedQueryError(
            "subqueries not supported in V1 (use a CTE instead)"
        )

    # Collect CTE names so we can distinguish CTE references from real
    # tables in the source-extraction step. sqlglot represents a CTE
    # reference (`FROM cohorts`) as an exp.Table node identical in shape
    # to a real table reference -- the only way to tell them apart is by
    # whether the name appears in a `WITH ... AS (...)` binding.
    cte_names: set[str] = set()
    # sqlglot keys this as "with_" (trailing underscore) to avoid clashing
    # with the Python keyword.
    with_clause = parsed.args.get("with_") or parsed.args.get("with")
    if with_clause is not None:
        for cte in with_clause.expressions:
            # exp.CTE has .alias (the name after WITH) and .this (the inner SELECT)
            cte_names.add(cte.alias_or_name)

    # SELECT * is forbidden EVERYWHERE in the tree (including inside CTE
    # bodies). If a CTE selected `*`, governance couldn't enumerate which
    # columns the query reads without re-resolving the underlying schema.
    for select_node in parsed.find_all(exp.Select):
        for proj in select_node.expressions:
            if isinstance(proj, exp.Star):
                raise UnsupportedQueryError(
                    "SELECT * not supported; enumerate columns explicitly so "
                    "governance can enforce column-level access"
                )
            if isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star):
                raise UnsupportedQueryError(
                    "SELECT t.* not supported; enumerate columns explicitly"
                )

    # Collect every Table reference across the whole tree (outer SELECT
    # + every CTE body) and filter out the CTE-name references. What's
    # left are real table references -- governance's single-source unit.
    all_tables = list(parsed.find_all(exp.Table))
    real_tables = [t for t in all_tables if t.name not in cte_names]
    if not real_tables:
        raise UnsupportedQueryError("query must have a FROM clause")
    # Resolve each real-table reference to a source_id. For a relational
    # FROM (`FROM orders`, `FROM orders o`), `table.name` is the relation
    # and we use that -- the alias is local. For a file-scan FROM
    # (`FROM read_parquet('x.parquet') AS orders`), `table.name` is empty
    # (the relation is a function call) so we fall back to the alias,
    # which the DSL compiler attaches so governance can find the source.
    source_ids = {t.name or t.alias_or_name for t in real_tables}
    if len(source_ids) != 1:
        raise UnsupportedQueryError(
            f"V1 supports exactly one source across the query "
            f"(including all CTEs); got {sorted(source_ids)}"
        )
    [source_id] = source_ids

    # Column extraction. The subtle rule: alias names (from this SELECT
    # or from CTE projections) are valid references ONLY in HAVING /
    # ORDER BY positions, where SQL allows them. Everywhere else --
    # SELECT-list, WHERE, GROUP BY, FROM/JOIN conditions, and inside
    # function calls -- a Column reference is a REAL column even if its
    # name happens to match an alias somewhere (`amount AS amount` is a
    # real `amount` reference with a same-named alias on top).
    columns: set[str] = set()

    # Collect CTE inner SELECTs so we can scope each one separately.
    cte_inner_selects: list[exp.Select] = []
    if with_clause is not None:
        for cte in with_clause.expressions:
            inner = cte.this
            if isinstance(inner, exp.Select):
                cte_inner_selects.append(inner)

    def _is_in_having_or_order(col: exp.Expression, anchor: exp.Select) -> bool:
        """Walk col's ancestor chain; return True if HAVING or ORDER BY
        sits between col and the anchor SELECT (i.e. col is a reference
        in a HAVING/ORDER BY position of `anchor`)."""
        p = col.parent
        while p is not None and p is not anchor:
            if isinstance(p, (exp.Having, exp.Order)):
                return True
            p = p.parent
        return False

    def _inside_any_cte(node: exp.Expression) -> bool:
        anc = node.parent
        while anc is not None:
            if anc in cte_inner_selects:
                return True
            anc = anc.parent
        return False

    # Walk each CTE body with its own alias scope.
    for inner in cte_inner_selects:
        inner_aliases: set[str] = {
            p.alias for p in inner.expressions if isinstance(p, exp.Alias)
        }
        for col in inner.find_all(exp.Column):
            if not col.name or col.name == "*":
                continue
            if col.name in inner_aliases and _is_in_having_or_order(col, inner):
                continue  # self-reference to this CTE's projection alias
            columns.add(col.name)

    # Walk the outer SELECT. Two distinct filter sets:
    #   * `cte_output_names`: SYNTHESIZED projections from CTEs (those with
    #     an exp.Alias node), like `COUNT(*) AS cohort_size`. References
    #     to these in the OUTER SELECT (at any position) are CTE-output
    #     references, NOT real columns. Pass-through projections like
    #     `SELECT user_id FROM orders` are bare exp.Column nodes (no
    #     Alias) and intentionally don't go into this set: a later
    #     `o.user_id` reference IS a real column we want governance to
    #     see.
    #   * `outer_aliases`: aliases declared in the OUTER SELECT's
    #     projection. These are only valid as references in HAVING /
    #     ORDER BY -- so we apply that filter only there.
    cte_output_names: set[str] = set()
    for inner in cte_inner_selects:
        for p in inner.expressions:
            if isinstance(p, exp.Alias):
                cte_output_names.add(p.alias)
    outer_aliases: set[str] = {
        proj.alias for proj in parsed.expressions if isinstance(proj, exp.Alias)
    }

    for col in parsed.find_all(exp.Column):
        if _inside_any_cte(col):
            continue
        if not col.name or col.name == "*":
            continue
        if col.name in cte_output_names:
            continue  # reference to a synthesized CTE projection
        if col.name in outer_aliases and _is_in_having_or_order(col, parsed):
            continue  # self-reference to outer-SELECT alias
        columns.add(col.name)

    return source_id, frozenset(columns)


def _inject_filter(sql: str, injected_filter: str) -> str:
    parsed = sqlglot.parse_one(sql, dialect="duckdb")
    parsed = parsed.where(injected_filter, dialect="duckdb")
    return parsed.sql(dialect="duckdb")


# ---------------------------------------------------------------------------
# Audit log helpers
# ---------------------------------------------------------------------------

def _record_denied(
    con: duckdb.DuckDBPyConnection,
    query_id: str,
    identity: IdentityContext,
    sql: str,
    decision: GovernanceDecision,
    started_ns: int,
    *,
    nl_prompt: str | None = None,
    source_id: str | None = None,
) -> QueryResult:
    duration_ms = (time.monotonic_ns() - started_ns) // 1_000_000
    deny_reason_str: str | None
    if decision.deny_reason and decision.deny_detail:
        deny_reason_str = f"{decision.deny_reason.value}:{decision.deny_detail}"
    elif decision.deny_reason:
        deny_reason_str = decision.deny_reason.value
    else:
        deny_reason_str = None
    _write_query_log(
        con,
        query_id=query_id, identity=identity,
        nl_prompt=nl_prompt, generated_sql=sql,
        status="denied", deny_reason=deny_reason_str,
        row_count=None, duration_ms=duration_ms,
        source_id=source_id,
    )
    return QueryResult(
        query_id=query_id,
        status="denied",
        rows=None,
        columns=None,
        deny_reason=decision.deny_reason,
        deny_detail=decision.deny_detail,
        error_message=None,
        duration_ms=duration_ms,
        rewritten_sql=None,
        original_sql=sql,
    )


def _record_error(
    con: duckdb.DuckDBPyConnection,
    query_id: str,
    identity: IdentityContext,
    sql: str,
    message: str,
    started_ns: int,
    *,
    nl_prompt: str | None = None,
    source_id: str | None = None,
) -> QueryResult:
    duration_ms = (time.monotonic_ns() - started_ns) // 1_000_000
    _write_query_log(
        con,
        query_id=query_id, identity=identity,
        nl_prompt=nl_prompt, generated_sql=sql,
        status="error", deny_reason=None,
        row_count=None, duration_ms=duration_ms,
        source_id=source_id,
    )
    return QueryResult(
        query_id=query_id,
        status="error",
        rows=None,
        columns=None,
        deny_reason=None,
        deny_detail=None,
        error_message=message,
        duration_ms=duration_ms,
        rewritten_sql=None,
        original_sql=sql,
    )


def _write_query_log(
    con: duckdb.DuckDBPyConnection,
    *,
    query_id: str,
    identity: IdentityContext,
    nl_prompt: str | None,
    generated_sql: str,
    status: str,
    deny_reason: str | None,
    row_count: int | None,
    duration_ms: int,
    source_id: str | None = None,
) -> None:
    # Redact literals adjacent to sensitive columns BEFORE persistence.
    # source_id is None on the parse-failure path; redact_audit_payload
    # falls back to a global sensitive-column lookup (over-redacts).
    generated_sql, nl_prompt = redact_audit_payload(
        con, source_id, generated_sql, nl_prompt
    )
    con.execute(
        "INSERT INTO gibran_query_log "
        "(query_id, user_id, role_id, nl_prompt, generated_sql, "
        "status, deny_reason, row_count, duration_ms) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            query_id, identity.user_id, identity.role_id,
            nl_prompt, generated_sql, status, deny_reason,
            row_count, duration_ms,
        ],
    )
