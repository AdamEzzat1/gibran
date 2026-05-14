"""Protocol-contract tests for the ShapePrimitive registry.

These tests are intentionally separate from test_shape_primitives.py
(which pins per-primitive behavior). The four tests here pin the
ShapePrimitive base class + register_shape_primitive decorator
contract, so a future user-declared primitive lands cleanly.
"""
from __future__ import annotations

import pytest

from gibran.dsl.errors import IntentValidationError
from gibran.dsl.shape_primitives import (
    SHAPE_PRIMITIVES,
    ShapePrimitive,
    register_shape_primitive,
)
from gibran.dsl.types import QueryIntent


class TestRegistryShape:
    def test_concrete_primitives_registered(self) -> None:
        # Shape primitives registered at import time via
        # @register_shape_primitive in dsl.compile. The explicit list
        # keeps the registry surface visible -- a future addition
        # must update this assertion.
        assert set(SHAPE_PRIMITIVES.keys()) == {
            "cohort_retention", "funnel", "multi_stage_filter",
            # Phase 3 addition: filter entities by sub-query intersection.
            "cohort_filter",
        }
        # Each entry is an INSTANCE (the decorator instantiates), not
        # a class -- so callers can invoke .build / .validate_intent
        # without an extra constructor call.
        for primitive in SHAPE_PRIMITIVES.values():
            assert isinstance(primitive, ShapePrimitive)


class TestRegisterDecorator:
    def test_register_subclass_with_empty_metric_type_raises(self) -> None:
        # A subclass that forgets to set metric_type would silently
        # shadow the base "" entry on registration -- the decorator
        # blocks this at definition time.
        class Unnamed(ShapePrimitive):
            pass  # metric_type defaults to "" from the base class

        with pytest.raises(ValueError, match="non-empty `metric_type`"):
            register_shape_primitive(Unnamed)

    def test_register_duplicate_metric_type_raises(self) -> None:
        # Re-registering a metric_type would silently shadow the previous
        # entry (which could be a different class entirely). The
        # decorator detects collisions and reports who the existing
        # registrant is.
        class FakeCohort(ShapePrimitive):
            metric_type = "cohort_retention"

        with pytest.raises(ValueError, match="already registered"):
            register_shape_primitive(FakeCohort)


class TestDefaultValidateIntent:
    """Pins the default validate_intent invariants on the base class.

    Uses the cohort_retention primitive (registered) since the default
    method body is what we're testing, not any per-primitive override.
    A future primitive that overrides validate_intent gets its own
    coverage in its own test file.
    """

    @pytest.fixture
    def primitive(self) -> ShapePrimitive:
        return SHAPE_PRIMITIVES["cohort_retention"]

    @pytest.mark.parametrize(
        "kwargs, expected_fragment",
        [
            # Multiple metrics -- the primitive must be alone.
            (
                {"metrics": ["a", "b"]},
                "must be the only metric",
            ),
            # A dimension -- the primitive emits its own dims.
            (
                {
                    "metrics": ["a"],
                    "dimensions": [{"id": "orders.region"}],
                },
                "cannot be combined with intent.dimensions",
            ),
            # A filter / having / order_by -- the primitive emits a
            # whole query, leaving nowhere to plug those in.
            (
                {
                    "metrics": ["a"],
                    "filters": [
                        {"op": "eq", "column": "status", "value": "paid"},
                    ],
                },
                "cannot be combined with intent.filters",
            ),
        ],
    )
    def test_default_validate_intent_rejects_violations(
        self,
        primitive: ShapePrimitive,
        kwargs: dict,
        expected_fragment: str,
    ) -> None:
        intent = QueryIntent(source="s", **kwargs)
        with pytest.raises(IntentValidationError, match=expected_fragment):
            primitive.validate_intent(intent, "a")
