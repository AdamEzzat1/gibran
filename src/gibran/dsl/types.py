"""Pydantic models for the Gibran DSL query intent.

Field-level constraints (types, enums, bounds, value-shape per op) live
here. Cross-entity checks (does this metric exist in AllowedSchema? does
the having.metric appear in intent.metrics?) live in dsl.validate."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


Grain = Literal["year", "quarter", "month", "week", "day", "hour"]
Direction = Literal["asc", "desc"]
HavingOp = Literal["eq", "neq", "lt", "lte", "gt", "gte", "in", "not_in"]


class DimensionRef(_Strict):
    id: str
    grain: Grain | None = None


class OrderBy(_Strict):
    key: str
    direction: Direction = "asc"


class HavingClause(_Strict):
    """A post-aggregation filter on a metric value.

    Unlike `filters` (which reference columns and apply pre-aggregation
    in WHERE), `having` references a *metric_id* in the intent's projection
    and applies post-aggregation via SQL HAVING.

    The shape of `value` depends on `op`:
      - comparison ops (eq/neq/lt/lte/gt/gte): scalar
      - in / not_in: list of scalars"""
    op: HavingOp
    metric: str
    value: Any  # validated by _check_value_shape

    @model_validator(mode="after")
    def _check_value_shape(self) -> "HavingClause":
        if self.op in ("in", "not_in"):
            if not isinstance(self.value, list) or not self.value:
                raise ValueError(
                    f"having.value must be a non-empty list for op={self.op!r}"
                )
            for v in self.value:
                _require_scalar(v, f"having.value[] for op={self.op!r}")
        else:
            _require_scalar(self.value, f"having.value for op={self.op!r}")
        return self


def _require_scalar(v: Any, where: str) -> None:
    if v is None:
        return
    if isinstance(v, (str, int, float, bool)):
        return
    raise ValueError(
        f"{where}: expected scalar (str/int/float/bool/null), "
        f"got {type(v).__name__}"
    )


class QueryIntent(_Strict):
    source: str
    metrics: list[str] = Field(default_factory=list)
    dimensions: list[DimensionRef] = Field(default_factory=list)
    filters: list[dict[str, Any]] = Field(default_factory=list)
    having: list[HavingClause] = Field(default_factory=list)
    order_by: list[OrderBy] = Field(default_factory=list)
    limit: int = Field(default=1000, ge=1, le=10000)

    @field_validator("dimensions", mode="before")
    @classmethod
    def _coerce_string_dims(cls, v: Any) -> Any:
        """Accept either `"orders.region"` (shorthand) or `{"id": "...", "grain": "..."}`."""
        if not isinstance(v, list):
            return v
        return [{"id": item} if isinstance(item, str) else item for item in v]

    @model_validator(mode="after")
    def _check_projection_non_empty(self) -> "QueryIntent":
        if not self.metrics and not self.dimensions:
            raise ValueError(
                "intent must have at least one metric or one dimension "
                "(SELECT must produce columns)"
            )
        return self
