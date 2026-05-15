"""Shape-primitive protocol + registry.

A "shape primitive" is a metric whose compiled SQL produces a whole-query
shape with its own column layout (multiple CTEs feeding a final SELECT
that the user can't add dimensions / filters / having / order_by on top
of). The three current ones are `cohort_retention`, `funnel`, and
`multi_stage_filter`.

Before Phase 2A, `compile_intent` dispatched on `metric_type` via an
if/elif chain to call `_build_cohort_retention`, `_build_funnel`, and
`_build_multi_stage_filter` directly. The chain would have grown to ~6
branches by Phase 3, and made user-declared shape primitives in YAML
impossible (each new primitive needed a Python branch + an edit to
`dsl/validate.py` for its preconditions).

This module replaces the branch with a registry:

  1. `ShapePrimitive` -- abstract base class. Each concrete primitive
     declares `metric_type` (the catalog string the compiler matches on)
     and implements `build(meta, intent, from_clause) -> CompiledQuery`.
     A default `validate_intent(intent, metric_id)` enforces the V1
     shape contract (single metric, no dimensions / filters / having /
     order_by); subclasses can override for primitive-specific checks.

  2. `register_shape_primitive` -- decorator that instantiates the class
     and stores it in `SHAPE_PRIMITIVES` keyed on `metric_type`.

  3. `SHAPE_PRIMITIVES` -- read-only at runtime (mutated only at import
     time via the decorator). `compile_intent` and `dsl.validate` look
     up by metric_type.

The actual `build()` implementations remain in `dsl.compile` because they
need module-private helpers (`_MetricMeta`, `CompiledQuery`, the
`_build_*` functions). The concrete `ShapePrimitive` subclasses are
defined there too -- this module owns only the protocol surface.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from gibran.dsl.errors import IntentValidationError

if TYPE_CHECKING:
    from gibran.dsl.compile import CompiledQuery, _MetricMeta
    from gibran.dsl.types import QueryIntent


class ShapePrimitive:
    """Base class for whole-query shape primitives.

    Subclasses set the class-level `metric_type` and implement `build`.
    The default `validate_intent` enforces the V1 contract that all three
    current shape primitives share; override if a future primitive needs
    different preconditions (e.g. accepts a `WHERE` filter).
    """

    metric_type: ClassVar[str] = ""

    def validate_intent(self, intent: "QueryIntent", metric_id: str) -> None:
        """Default precondition check: this primitive must be the only
        metric, with no dimensions, no filters, no having, no order_by.

        Subclasses may override to relax (e.g. accept dimensions) or
        tighten (e.g. require a specific filter shape) these constraints.
        """
        if len(intent.metrics) != 1:
            raise IntentValidationError(
                f"{self.metric_type} metric {metric_id!r} must be the only "
                f"metric in the intent (it emits a whole-query shape and "
                f"doesn't compose with other metrics in V1)"
            )
        if intent.dimensions:
            raise IntentValidationError(
                f"{self.metric_type} metric {metric_id!r} cannot be "
                f"combined with intent.dimensions (the cohort/period or "
                f"funnel-step dimensions are emitted by the primitive itself)"
            )
        if intent.filters or intent.having or intent.order_by:
            raise IntentValidationError(
                f"{self.metric_type} metric {metric_id!r} cannot be "
                f"combined with intent.filters / having / order_by in V1 "
                f"(the primitive emits the whole query)"
            )

    def build(
        self,
        meta: "_MetricMeta",
        intent: "QueryIntent",
        from_clause: str,
    ) -> "CompiledQuery":
        raise NotImplementedError(
            f"{type(self).__name__} must implement build()"
        )


SHAPE_PRIMITIVES: dict[str, ShapePrimitive] = {}


def register_shape_primitive(cls: type[ShapePrimitive]) -> type[ShapePrimitive]:
    """Class decorator: instantiate and register in `SHAPE_PRIMITIVES`.

    Registration is at import time; ordering doesn't matter since lookup
    is by `metric_type`. Re-registering the same `metric_type` raises --
    duplicates would silently shadow each other otherwise.
    """
    if not cls.metric_type:
        raise ValueError(
            f"{cls.__name__}: subclasses of ShapePrimitive must set a "
            f"non-empty `metric_type` class attribute"
        )
    if cls.metric_type in SHAPE_PRIMITIVES:
        raise ValueError(
            f"shape primitive {cls.metric_type!r} already registered "
            f"(by {type(SHAPE_PRIMITIVES[cls.metric_type]).__name__})"
        )
    SHAPE_PRIMITIVES[cls.metric_type] = cls()
    return cls
