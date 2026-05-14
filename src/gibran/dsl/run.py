"""DSL runner: orchestrate Pydantic-parse + semantic-validate + compile + execute.

Returns a uniform DSLRunResult that wraps QueryResult from the execution
layer (when compilation succeeds and SQL runs) or carries early-failure
information (when parse / validate / compile fail).

Every attempt writes a gibran_query_log row. Pre-compile failures write
their own row (no SQL was generated). Compile-succeeded queries go
through execution.run_sql_query, which writes its own row -- with the
intent JSON captured in the nl_prompt field for traceability.
"""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Literal

import duckdb
from pydantic import ValidationError

from gibran.dsl.compile import Catalog, CompileError, compile_intent
from gibran.dsl.plan_cache import compile_intent_cached
from gibran.dsl.types import QueryIntent
from gibran.dsl.validate import IntentValidationError, validate_intent
from gibran.execution.sql import QueryResult, run_sql_query
from gibran.governance.redaction import redact_audit_payload
from gibran.governance.types import GovernanceAPI, IdentityContext


@dataclass(frozen=True)
class DSLRunResult:
    """Outcome of run_dsl_query.

    `query_result` is populated whenever we got far enough to call the
    execution layer (whether it succeeded or was denied). For pre-compile
    failures it's None and `pre_compile_error` carries the reason."""
    query_id: str
    stage: Literal["parsed", "validated", "compiled", "executed"]
    query_result: QueryResult | None
    pre_compile_error: str | None
    duration_ms: int


def run_dsl_query(
    con: duckdb.DuckDBPyConnection,
    governance: GovernanceAPI,
    identity: IdentityContext,
    raw_intent: dict[str, Any],
) -> DSLRunResult:
    started_ns = time.monotonic_ns()
    intent_json = json.dumps(raw_intent, default=str, sort_keys=True)

    # Pydantic structural parse
    try:
        intent = QueryIntent.model_validate(raw_intent)
    except ValidationError as e:
        return _pre_compile_failure(
            con, identity, intent_json, started_ns,
            stage="parsed", reason=f"intent_parse: {e}",
            raw_intent=raw_intent,
        )

    # Look up AllowedSchema for the intent's source
    try:
        schema = governance.preview_schema(identity, intent.source)
    except ValueError as e:
        return _pre_compile_failure(
            con, identity, intent_json, started_ns,
            stage="parsed", reason=f"unknown_source: {e}",
            raw_intent=raw_intent,
        )

    # Semantic validation against AllowedSchema. Pass `con` so primitive-
    # specific checks (e.g. period_over_period's period_dim requirement)
    # can read metric_config from the catalog.
    try:
        validate_intent(intent, schema, con=con)
    except IntentValidationError as e:
        return _pre_compile_failure(
            con, identity, intent_json, started_ns,
            stage="validated", reason=f"intent_invalid: {e}",
            raw_intent=raw_intent,
        )

    # Compile to SQL. compile_intent_cached short-circuits when the same
    # intent has been compiled before AND the catalog hasn't been re-synced
    # since. The cache is per-process; opt out via the env var if needed.
    catalog = Catalog(con)
    import os as _os
    try:
        if _os.environ.get("GIBRAN_DISABLE_PLAN_CACHE", "") == "1":
            sql = compile_intent(intent, catalog).render()
        else:
            sql = compile_intent_cached(intent, catalog).render()
    except CompileError as e:
        return _pre_compile_failure(
            con, identity, intent_json, started_ns,
            stage="compiled", reason=f"compile_failed: {e}",
            raw_intent=raw_intent,
        )

    # Hand off to execution. Pass intent_json as nl_prompt for audit traceability.
    query_result = run_sql_query(con, governance, identity, sql, nl_prompt=intent_json)
    duration_ms = (time.monotonic_ns() - started_ns) // 1_000_000
    return DSLRunResult(
        query_id=query_result.query_id,
        stage="executed",
        query_result=query_result,
        pre_compile_error=None,
        duration_ms=duration_ms,
    )


def _pre_compile_failure(
    con: duckdb.DuckDBPyConnection,
    identity: IdentityContext,
    intent_json: str,
    started_ns: int,
    *,
    stage: Literal["parsed", "validated", "compiled"],
    reason: str,
    raw_intent: dict[str, Any] | None = None,
) -> DSLRunResult:
    query_id = str(uuid.uuid4())
    duration_ms = (time.monotonic_ns() - started_ns) // 1_000_000
    # Redact literals in the intent JSON before audit-log persistence --
    # filter values for sensitive columns must not re-leak via nl_prompt.
    # We try to pull source from the raw intent (even on Pydantic failure
    # it might be present); if absent or non-string, fall back to a
    # global sensitive-column lookup.
    raw_source = None
    if isinstance(raw_intent, dict):
        candidate = raw_intent.get("source")
        if isinstance(candidate, str):
            raw_source = candidate
    _, redacted_intent = redact_audit_payload(con, raw_source, "", intent_json)
    con.execute(
        "INSERT INTO gibran_query_log "
        "(query_id, user_id, role_id, nl_prompt, generated_sql, status, "
        "deny_reason, row_count, duration_ms) "
        "VALUES (?, ?, ?, ?, ?, 'error', ?, NULL, ?)",
        [
            query_id, identity.user_id, identity.role_id,
            redacted_intent, "",          # no SQL was generated
            reason, duration_ms,
        ],
    )
    return DSLRunResult(
        query_id=query_id,
        stage=stage,
        query_result=None,
        pre_compile_error=reason,
        duration_ms=duration_ms,
    )
