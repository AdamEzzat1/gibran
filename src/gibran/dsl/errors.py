"""DSL error types.

Lives in its own module so both `dsl.validate` and `dsl.shape_primitives`
can import without a circular dependency. `dsl.validate` re-exports
`IntentValidationError` so the historical public import path
(`from gibran.dsl.validate import IntentValidationError`) keeps working.
"""
from __future__ import annotations


class IntentValidationError(ValueError):
    """Raised by `validate_intent` and by `ShapePrimitive.validate_intent`
    when an intent's shape conflicts with the schema or with primitive-
    specific invariants."""
