"""SQL execution path: parse, govern, rewrite, execute, audit.

V1 constraints (intentional, enforced before governance is consulted):
  - Top-level statement must be SELECT
  - Exactly one source (no joins)
  - No subqueries, no CTEs
  - No SELECT * (forces explicit column enumeration so governance can
    enforce column-level access)

A `rumi_query_log` row is written for every attempt -- allow, deny, or
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

from rumi.governance.redaction import redact_audit_payload
from rumi.governance.types import (
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

    `nl_prompt` is recorded in rumi_query_log for traceability. Raw-SQL
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

    if list(parsed.find_all(exp.Join)):
        raise UnsupportedQueryError("joins not supported in V1 (single-source only)")
    if list(parsed.find_all(exp.Subquery)):
        raise UnsupportedQueryError("subqueries not supported in V1")
    if list(parsed.find_all(exp.With)):
        raise UnsupportedQueryError("CTEs not supported in V1")

    for proj in parsed.expressions:
        if isinstance(proj, exp.Star):
            raise UnsupportedQueryError(
                "SELECT * not supported; enumerate columns explicitly so "
                "governance can enforce column-level access"
            )
        if isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star):
            raise UnsupportedQueryError(
                "SELECT t.* not supported; enumerate columns explicitly"
            )

    from_clause = parsed.find(exp.From)
    if from_clause is None:
        raise UnsupportedQueryError("query must have a FROM clause")
    tables = list(from_clause.find_all(exp.Table))
    if len(tables) != 1:
        raise UnsupportedQueryError(
            f"V1 supports exactly one source; got {len(tables)}"
        )
    # Resolve the source identifier. For a relational FROM (`FROM orders`,
    # `FROM orders o`), `table.name` is the relation name and we use that
    # -- aliases like `o` are local to the query and not the governance key.
    # For a file-scan FROM (`FROM read_parquet('x.parquet') AS orders`),
    # `table.name` is empty because the relation is a function call; we
    # fall back to the alias, which the DSL compiler attaches precisely so
    # governance can locate the source.
    source_id = tables[0].name or tables[0].alias_or_name

    # Collect SELECT aliases so we can distinguish real column references
    # from alias references (which appear in HAVING / ORDER BY when DSL
    # compilation emits e.g. `HAVING ("gross_revenue" > 50)`). sqlglot's
    # `find_all(exp.Column)` returns both, so we filter by name.
    #
    # V1 limitation: a query like `SELECT amount AS amount FROM t` would
    # exclude the legitimate column reference. We don't generate such SQL
    # from the DSL (aliases like metric_id and dim_id are distinct from
    # column names), and user-authored raw SQL rarely uses `col AS col`.
    aliases: set[str] = set()
    for proj in parsed.expressions:
        if isinstance(proj, exp.Alias):
            aliases.add(proj.alias)

    columns: set[str] = set()
    for col in parsed.find_all(exp.Column):
        if col.name and col.name != "*" and col.name not in aliases:
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
        "INSERT INTO rumi_query_log "
        "(query_id, user_id, role_id, nl_prompt, generated_sql, "
        "status, deny_reason, row_count, duration_ms) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            query_id, identity.user_id, identity.role_id,
            nl_prompt, generated_sql, status, deny_reason,
            row_count, duration_ms,
        ],
    )
