"""Gibran DSL: the user-facing query surface.

In V1, the DSL is THE conversational surface — users type structured
query intents directly (no NL, no LLM). The DSL is forward-compatible
with adding an NL-on-top layer later (templates, embeddings, or LLM)
but that layer is not built.

Public API:
- QueryIntent / DimensionRef / OrderBy  (Pydantic models)
- IntentValidationError                  (raised by validate_intent)
- validate_intent(intent, schema)        (semantic check vs AllowedSchema)
- compile_intent(intent, catalog)        (DSL -> SQL string)
- run_dsl_query(con, gov, identity, raw_intent)  (orchestrator)
"""
from gibran.dsl.compile import CompileError, compile_intent
from gibran.dsl.run import DSLRunResult, run_dsl_query
from gibran.dsl.types import DimensionRef, HavingClause, OrderBy, QueryIntent
from gibran.dsl.validate import IntentValidationError, validate_intent

__all__ = [
    "CompileError",
    "DSLRunResult",
    "DimensionRef",
    "HavingClause",
    "IntentValidationError",
    "OrderBy",
    "QueryIntent",
    "compile_intent",
    "run_dsl_query",
    "validate_intent",
]
