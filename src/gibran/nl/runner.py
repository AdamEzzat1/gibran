"""NL runner: user text -> pattern match -> DSL run.

End-to-end orchestration for the pattern-template NL layer. Composes:

    nl_to_intent(text, schema) -> intent dict
    run_dsl_query(con, governance, identity, intent) -> DSLRunResult

If no pattern matches, returns None and the CLI prints "I don't know
how to answer that." Never invents a query.
"""
from __future__ import annotations

from dataclasses import dataclass

import duckdb

from gibran.dsl.run import DSLRunResult, run_dsl_query
from gibran.governance.types import GovernanceAPI, IdentityContext
from gibran.nl.patterns import MatchResult, nl_to_intent


@dataclass(frozen=True)
class NLResult:
    match: MatchResult | None
    run_result: DSLRunResult | None


def run_nl_query(
    con: duckdb.DuckDBPyConnection,
    governance: GovernanceAPI,
    identity: IdentityContext,
    text: str,
    source_id: str,
) -> NLResult:
    """Translate `text` to a DSL intent for `source_id` and execute it.

    Returns NLResult(match=None, run_result=None) when no pattern
    matches. Callers MUST handle that case explicitly -- the NL layer
    never fabricates a query.
    """
    schema = governance.preview_schema(identity, source_id)
    match = nl_to_intent(text, schema)
    if match is None:
        return NLResult(match=None, run_result=None)
    run_result = run_dsl_query(con, governance, identity, match.intent)
    return NLResult(match=match, run_result=run_result)
