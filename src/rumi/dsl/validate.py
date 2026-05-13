"""Semantic validation: does this intent fit the user's AllowedSchema?

Pydantic models in dsl.types catch *structural* errors (wrong type,
missing field, extra field, out-of-range limit). This module catches
*semantic* errors that need the schema context:

  - source matches AllowedSchema.source_id
  - every metric id is in AllowedSchema.metrics
  - every dimension id is in AllowedSchema.dimensions
  - every filter column is in AllowedSchema.columns
  - every order_by key references something in the intent's projection
  - time grain only on temporal dimensions
  - filter AST nodes use only the whitelisted operators AND no $attr refs

Validation runs BEFORE compilation. If this passes, compilation cannot
fail for "missing reference" reasons -- only for internal-invariant
violations (which would be bugs)."""
from __future__ import annotations

from rumi.dsl.types import QueryIntent
from rumi.governance.ast import ASTValidationError, validate_intent_ast
from rumi.governance.types import AllowedSchema


class IntentValidationError(ValueError):
    pass


def validate_intent(intent: QueryIntent, schema: AllowedSchema) -> None:
    """Raise IntentValidationError if intent references something not in schema,
    if a time grain is applied to a non-temporal dimension, or if filter ASTs
    are malformed."""
    if intent.source != schema.source_id:
        raise IntentValidationError(
            f"intent.source {intent.source!r} != AllowedSchema.source {schema.source_id!r}"
        )

    schema_metric_ids = {m.metric_id for m in schema.metrics}
    schema_dim_index = {d.dimension_id: d for d in schema.dimensions}
    schema_column_names = frozenset(c.name for c in schema.columns)

    for metric_id in intent.metrics:
        if metric_id not in schema_metric_ids:
            raise IntentValidationError(
                f"metric {metric_id!r} not in AllowedSchema for source {schema.source_id!r}"
            )

    for dim in intent.dimensions:
        view = schema_dim_index.get(dim.id)
        if view is None:
            raise IntentValidationError(
                f"dimension {dim.id!r} not in AllowedSchema for source {schema.source_id!r}"
            )
        if dim.grain is not None and view.dim_type != "temporal":
            raise IntentValidationError(
                f"dimension {dim.id!r}: grain {dim.grain!r} only valid on temporal "
                f"dimensions (this dimension is {view.dim_type!r})"
            )

    for filter_node in intent.filters:
        try:
            validate_intent_ast(filter_node, schema_column_names)
        except ASTValidationError as e:
            raise IntentValidationError(f"filter AST invalid: {e}") from e

    projected_keys = (
        {m for m in intent.metrics}
        | {d.id for d in intent.dimensions}
    )
    for ob in intent.order_by:
        if ob.key not in projected_keys:
            raise IntentValidationError(
                f"order_by.key {ob.key!r} is not in the intent's projection "
                f"(must be one of metrics={sorted(intent.metrics)} or "
                f"dimensions={[d.id for d in intent.dimensions]})"
            )

    intent_metric_set = set(intent.metrics)
    for h in intent.having:
        if h.metric not in intent_metric_set:
            raise IntentValidationError(
                f"having.metric {h.metric!r} not in intent.metrics; "
                f"can only HAVING on a metric that's been selected "
                f"(intent.metrics={sorted(intent_metric_set)})"
            )

    # V1 constraint on rolling_window metrics: their SQL produces a per-row
    # window-function result (SUM(...) OVER (ORDER BY t RANGE ...)), which
    # doesn't compose with GROUP BY in the way regular aggregates do. Forbid
    # intent.dimensions on intents that include any rolling_window metric.
    rolling_metrics = [
        m for m in schema.metrics
        if m.metric_id in intent_metric_set and m.metric_type == "rolling_window"
    ]
    if rolling_metrics and intent.dimensions:
        raise IntentValidationError(
            f"rolling_window metrics ({[m.metric_id for m in rolling_metrics]}) "
            f"cannot be combined with dimensions in V1 -- the window function "
            f"is row-level and doesn't aggregate into groups. Remove dimensions "
            f"or use a non-rolling metric."
        )
