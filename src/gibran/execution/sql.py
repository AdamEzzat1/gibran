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

    # Optional per-query timeout via env var. DuckDB's `statement_timeout`
    # is a session setting; we set it before execute and tolerate the case
    # where it's not supported on older DuckDB builds.
    timeout_ms = _query_timeout_ms()
    if timeout_ms is not None:
        try:
            con.execute(f"SET statement_timeout = '{timeout_ms}ms'")
        except Exception:
            pass  # older DuckDB; honor on a best-effort basis

    # Result-cache lookup. Audit-log row is STILL written on a cache hit
    # (we just skip the DuckDB execute). Set GIBRAN_DISABLE_RESULT_CACHE=1
    # to bypass caching for callers that need fresh-execute every time.
    from gibran.execution.result_cache import lookup as _cache_lookup, store as _cache_store, CachedResult
    import os as _os
    use_cache = _os.environ.get("GIBRAN_DISABLE_RESULT_CACHE", "") != "1"
    cache_key = None
    cached = None
    if use_cache:
        cache_key, cached = _cache_lookup(
            con, rewritten, identity, source_id=source_id,
        )

    if cached is not None:
        rows = list(cached.rows)
        col_names = list(cached.columns)
    else:
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
        if use_cache and cache_key is not None:
            _cache_store(cache_key, CachedResult(
                rows=tuple(rows), columns=tuple(col_names),
            ))

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

    # Column extraction. Subtle rules:
    #   * alias names (from this SELECT or from a CTE's projection) are
    #     valid references ONLY in HAVING / ORDER BY positions; elsewhere
    #     a same-named Column ref is a real column.
    #   * references to OTHER CTEs (via a table alias bound to a CTE name
    #     in the FROM/JOIN) are NOT real-column refs -- they're refs to
    #     synthesized CTE outputs and should be skipped. The walker has
    #     ALREADY found the underlying real columns inside that other
    #     CTE's body, so skipping the indirect reference here doesn't
    #     lose information.
    #   * for unprefixed Column refs in a CTE body that has NO real
    #     source in its FROM (e.g. a CTE that FROMs another CTE), every
    #     unprefixed ref must be a CTE-output ref -- skip it.
    columns: set[str] = set()

    # Collect CTE inner SELECTs.
    cte_inner_selects: list[exp.Select] = []
    if with_clause is not None:
        for cte in with_clause.expressions:
            inner = cte.this
            if isinstance(inner, exp.Select):
                cte_inner_selects.append(inner)

    def _is_in_having_or_order(col: exp.Expression, anchor: exp.Select) -> bool:
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

    def _from_aliases(select_node: exp.Select) -> tuple[set[str], bool]:
        """Return (table aliases bound to CTEs in this SELECT's FROM/JOIN,
        whether any real (non-CTE) source appears in the FROM/JOIN)."""
        cte_table_aliases: set[str] = set()
        has_real_source = False
        scopes = []
        from_ = select_node.args.get("from_") or select_node.args.get("from")
        if from_ is not None:
            scopes.append(from_)
        scopes.extend(select_node.args.get("joins") or [])
        for scope in scopes:
            for t in scope.find_all(exp.Table):
                if t.name in cte_names:
                    cte_table_aliases.add(t.alias_or_name)
                else:
                    has_real_source = True
        return cte_table_aliases, has_real_source

    def _walk_select_columns(select_node: exp.Select, *, outer_synth: set[str]) -> None:
        """Collect real-column refs from one SELECT scope. `outer_synth`
        is the alias-name set whose references are only valid in
        HAVING/ORDER BY of this SELECT (the SELECT's own aliases)."""
        cte_table_aliases, has_real_source = _from_aliases(select_node)
        for col in select_node.find_all(exp.Column):
            # Skip columns that belong to a DEEPER CTE -- they'll be
            # walked when that CTE's own scope is processed.
            if _inside_any_cte(col) and select_node not in cte_inner_selects:
                continue
            # When walking the OUTER SELECT, skip columns inside ANY CTE
            # (those were processed in their own pass).
            if select_node is parsed and _inside_any_cte(col):
                continue
            # When walking a specific CTE body, skip columns that are
            # inside a DIFFERENT CTE's body (shouldn't happen in V1 but
            # defensive).
            if select_node in cte_inner_selects:
                anc = col.parent
                while anc is not None:
                    if anc in cte_inner_selects and anc is not select_node:
                        break
                    anc = anc.parent
                else:
                    anc = None
                if anc is not None:
                    continue
            if not col.name or col.name == "*":
                continue
            # Self-reference to this SELECT's alias in HAVING/ORDER BY.
            if col.name in outer_synth and _is_in_having_or_order(col, select_node):
                continue
            # Explicit reference to a CTE-bound table alias (e.g. `r.entity`
            # where `r` aliases a CTE in this scope's FROM).
            if col.table and col.table in cte_table_aliases:
                continue
            # Unprefixed ref in a body whose FROM contains NO real source.
            # In SQL, an unprefixed col resolves to the only table in scope,
            # which here is a CTE -- so the ref is to that CTE's output.
            if not col.table and cte_table_aliases and not has_real_source:
                continue
            columns.add(col.name)

    # Walk each CTE body with its own alias scope.
    for inner in cte_inner_selects:
        inner_aliases: set[str] = {
            p.alias for p in inner.expressions if isinstance(p, exp.Alias)
        }
        _walk_select_columns(inner, outer_synth=inner_aliases)

    # Walk the outer SELECT.
    outer_aliases: set[str] = {
        proj.alias for proj in parsed.expressions if isinstance(proj, exp.Alias)
    }
    _walk_select_columns(parsed, outer_synth=outer_aliases)

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
    # Mark the audit row when the identity's role is a break-glass role.
    # This makes elevated-access usage searchable in the audit log
    # without changing the deny_reason semantics.
    is_break_glass = _is_break_glass_role(con, identity.role_id)
    con.execute(
        "INSERT INTO gibran_query_log "
        "(query_id, user_id, role_id, nl_prompt, generated_sql, "
        "status, deny_reason, row_count, duration_ms, is_break_glass) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            query_id, identity.user_id, identity.role_id,
            nl_prompt, generated_sql, status, deny_reason,
            row_count, duration_ms, is_break_glass,
        ],
    )


def _is_break_glass_role(
    con: duckdb.DuckDBPyConnection, role_id: str
) -> bool:
    row = con.execute(
        "SELECT is_break_glass FROM gibran_roles WHERE role_id = ?",
        [role_id],
    ).fetchone()
    return bool(row[0]) if row is not None else False


def _query_timeout_ms() -> int | None:
    """Return the per-query timeout in milliseconds from the
    GIBRAN_QUERY_TIMEOUT_MS env var, or None if unset / invalid. The
    env-var knob is intentional: query timeouts are a deployment
    concern (an analyst's 60s budget might be unacceptable for a
    dashboard's 2s SLO), not a per-query config."""
    import os
    raw = os.environ.get("GIBRAN_QUERY_TIMEOUT_MS")
    if not raw:
        return None
    try:
        v = int(raw)
        return v if v > 0 else None
    except ValueError:
        return None
