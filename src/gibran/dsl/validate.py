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
  - period_over_period metrics: the intent.dimensions must include the
    period_dim at the matching grain (the LAG window function references
    that grouping expression)

Validation runs BEFORE compilation. If this passes, compilation cannot
fail for "missing reference" reasons -- only for internal-invariant
violations (which would be bugs)."""
from __future__ import annotations

import json
from typing import TYPE_CHECKING

import duckdb

from gibran.dsl.types import QueryIntent
from gibran.governance.ast import ASTValidationError, validate_intent_ast
from gibran.governance.types import AllowedSchema


class IntentValidationError(ValueError):
    pass


def validate_intent(
    intent: QueryIntent,
    schema: AllowedSchema,
    con: "duckdb.DuckDBPyConnection | None" = None,
) -> None:
    """Raise IntentValidationError if intent references something not in schema,
    if a time grain is applied to a non-temporal dimension, or if filter ASTs
    are malformed.

    `con` is optional and used only for primitive-specific checks that
    need to read the catalog (e.g. period_over_period needs the metric's
    `metric_config` JSON to know which period_dim must appear in the
    intent). When `con` is None those checks are skipped -- callers that
    care about them MUST pass a connection."""
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

    # Shape-primitives (cohort_retention, funnel, multi_stage_filter) emit
    # a whole-query shape with their own dimensions baked in. They cannot
    # be combined with other metrics, intent.dimensions, intent.filters,
    # intent.having, or intent.order_by -- the compiler emits the whole
    # query, so user-supplied modifiers have nowhere to plug in.
    shape_metrics = [
        m for m in schema.metrics
        if m.metric_id in intent_metric_set
        and m.metric_type in ("cohort_retention", "funnel", "multi_stage_filter")
    ]
    if shape_metrics:
        sm = shape_metrics[0]
        if len(intent.metrics) != 1:
            raise IntentValidationError(
                f"{sm.metric_type} metric {sm.metric_id!r} must be the only "
                f"metric in the intent (it emits a whole-query shape and "
                f"doesn't compose with other metrics in V1)"
            )
        if intent.dimensions:
            raise IntentValidationError(
                f"{sm.metric_type} metric {sm.metric_id!r} cannot be "
                f"combined with intent.dimensions (the cohort/period or "
                f"funnel-step dimensions are emitted by the primitive itself)"
            )
        if intent.filters or intent.having or intent.order_by:
            raise IntentValidationError(
                f"{sm.metric_type} metric {sm.metric_id!r} cannot be "
                f"combined with intent.filters / having / order_by in V1 "
                f"(the primitive emits the whole query)"
            )

    # period_over_period metrics: their LAG window references DATE_TRUNC over
    # the metric's configured period_dim; the intent's dimension list must
    # therefore include that period_dim with a matching grain. Without this
    # check, the compiled SQL would emit a LAG ordered by a column that
    # isn't in the GROUP BY, producing nondeterministic / wrong results.
    pop_metrics = [
        m for m in schema.metrics
        if m.metric_id in intent_metric_set and m.metric_type == "period_over_period"
    ]
    if pop_metrics and con is not None:
        intent_dim_grain = {d.id: d.grain for d in intent.dimensions}
        for m in pop_metrics:
            cfg_row = con.execute(
                "SELECT metric_config FROM gibran_metric_versions "
                "WHERE metric_id = ? AND effective_to IS NULL",
                [m.metric_id],
            ).fetchone()
            if cfg_row is None or cfg_row[0] is None:
                raise IntentValidationError(
                    f"period_over_period metric {m.metric_id!r}: no "
                    f"metric_config in catalog (run `gibran sync`)"
                )
            cfg = json.loads(cfg_row[0])
            period_dim = cfg["period_dim"]
            period_unit = cfg["period_unit"]
            if period_dim not in intent_dim_grain:
                raise IntentValidationError(
                    f"period_over_period metric {m.metric_id!r} requires "
                    f"intent.dimensions to include {period_dim!r} (the "
                    f"period dimension); current intent dimensions: "
                    f"{[d.id for d in intent.dimensions]}"
                )
            if intent_dim_grain[period_dim] != period_unit:
                raise IntentValidationError(
                    f"period_over_period metric {m.metric_id!r} requires "
                    f"dimension {period_dim!r} at grain {period_unit!r}, "
                    f"got grain {intent_dim_grain[period_dim]!r}"
                )
