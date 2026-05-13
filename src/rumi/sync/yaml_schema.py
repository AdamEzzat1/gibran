"""Pydantic schema for rumi.yaml -- the source-of-truth config.

Every field maps to a column in the metadata DB. Field-local validation
lives here; cross-entity validation (FKs, DAG cycle detection,
row_filter AST validation) is in sync.loader because Pydantic validators
are field-local by design.
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ColumnConfig(_Strict):
    name: str
    type: str
    sensitivity: str = "unclassified"
    description: str | None = None
    expose_examples: bool | None = None


class DimensionConfig(_Strict):
    id: str
    column: str
    display_name: str
    type: Literal["categorical", "temporal", "numeric_bin"]
    description: str | None = None


class SourceConfig(_Strict):
    id: str
    display_name: str
    type: Literal["parquet", "csv", "duckdb_table", "sql_view"]
    uri: str
    primary_grain: str | None = None
    columns: list[ColumnConfig] = Field(default_factory=list)
    dimensions: list[DimensionConfig] = Field(default_factory=list)


MetricType = Literal[
    "count", "sum", "avg", "min", "max", "ratio", "expression",
    "percentile", "rolling_window",
    "period_over_period",
    # cohort_retention, funnel: deferred (require multi-stage SQL / CTE
    # infrastructure)
]

RollingAggregate = Literal["sum", "avg", "min", "max", "count"]
PeriodUnit = Literal["year", "quarter", "month", "week", "day"]
PeriodComparison = Literal["delta", "ratio", "pct_change"]


class MetricConfig(_Strict):
    id: str
    source: str
    display_name: str
    type: MetricType
    expression: str | None = None
    numerator: str | None = None
    denominator: str | None = None
    filter: str | None = None
    unit: str | None = None
    description: str | None = None
    owner: str | None = None

    # percentile-specific
    column: str | None = None
    p: float | None = None

    # rolling_window-specific
    aggregate: RollingAggregate | None = None
    window: str | None = None             # DuckDB INTERVAL string, e.g. "28 days"
    order_by_column: str | None = None
    partition_by: list[str] | None = None

    # period_over_period-specific
    base_metric: str | None = None
    period_dim: str | None = None
    period_unit: PeriodUnit | None = None
    comparison: PeriodComparison | None = None

    @model_validator(mode="after")
    def _check_shape(self) -> "MetricConfig":
        if self.type == "ratio":
            if not self.numerator or not self.denominator:
                raise ValueError(
                    f"metric {self.id!r}: ratio requires 'numerator' and 'denominator'"
                )
            if self.expression is not None:
                raise ValueError(
                    f"metric {self.id!r}: ratio cannot have 'expression'"
                )
        elif self.type == "expression":
            if not self.expression:
                raise ValueError(
                    f"metric {self.id!r}: type 'expression' requires 'expression' field"
                )
            if self.numerator or self.denominator:
                raise ValueError(
                    f"metric {self.id!r}: expression metric cannot have numerator/denominator"
                )
        elif self.type == "count":
            if self.expression or self.numerator or self.denominator:
                raise ValueError(
                    f"metric {self.id!r}: count cannot have expression/numerator/denominator"
                )
        elif self.type == "percentile":
            if self.column is None or self.p is None:
                raise ValueError(
                    f"metric {self.id!r}: percentile requires 'column' and 'p'"
                )
            if not (0 < self.p < 1):
                raise ValueError(
                    f"metric {self.id!r}: percentile 'p' must be in (0, 1), got {self.p}"
                )
            if self.expression or self.numerator or self.denominator:
                raise ValueError(
                    f"metric {self.id!r}: percentile cannot have expression/numerator/denominator"
                )
        elif self.type == "rolling_window":
            missing = [
                f for f in ("column", "aggregate", "window", "order_by_column")
                if getattr(self, f) is None
            ]
            if missing:
                raise ValueError(
                    f"metric {self.id!r}: rolling_window requires {missing}"
                )
            if self.expression or self.numerator or self.denominator:
                raise ValueError(
                    f"metric {self.id!r}: rolling_window cannot have expression/numerator/denominator"
                )
        elif self.type == "period_over_period":
            missing = [
                f for f in ("base_metric", "period_dim", "period_unit", "comparison")
                if getattr(self, f) is None
            ]
            if missing:
                raise ValueError(
                    f"metric {self.id!r}: period_over_period requires {missing}"
                )
            if self.expression or self.numerator or self.denominator:
                raise ValueError(
                    f"metric {self.id!r}: period_over_period cannot have "
                    f"expression/numerator/denominator (use base_metric instead)"
                )
        else:  # sum, avg, min, max
            if not self.expression:
                raise ValueError(
                    f"metric {self.id!r}: type {self.type!r} requires 'expression'"
                )
            if self.numerator or self.denominator:
                raise ValueError(
                    f"metric {self.id!r}: type {self.type!r} cannot have numerator/denominator"
                )
        return self


class RoleConfig(_Strict):
    id: str
    display_name: str
    attributes: dict[str, str] = Field(default_factory=dict)


class PolicyConfig(_Strict):
    id: str
    role: str
    source: str
    default_column_mode: Literal["allow", "deny"] = "deny"
    column_overrides: dict[str, Literal["allow", "deny"]] = Field(default_factory=dict)
    row_filter: dict[str, Any] | None = None


class QualityRuleConfig(_Strict):
    id: str
    source: str
    type: Literal["not_null", "unique", "range", "custom_sql"]
    config: dict[str, Any]
    severity: Literal["warn", "block"]
    cost_class: Literal["cheap", "expensive"] | None = None
    staleness_seconds: int | None = None
    enabled: bool = True

    @model_validator(mode="after")
    def _check_config_shape(self) -> "QualityRuleConfig":
        if self.type == "not_null":
            if "column" not in self.config:
                raise ValueError(
                    f"quality_rule {self.id!r}: not_null requires config.column"
                )
        elif self.type == "unique":
            if "column" not in self.config:
                raise ValueError(
                    f"quality_rule {self.id!r}: unique requires config.column"
                )
        elif self.type == "range":
            if "column" not in self.config:
                raise ValueError(
                    f"quality_rule {self.id!r}: range requires config.column"
                )
            if "min" not in self.config and "max" not in self.config:
                raise ValueError(
                    f"quality_rule {self.id!r}: range requires config.min and/or config.max"
                )
        elif self.type == "custom_sql":
            if "sql" not in self.config:
                raise ValueError(
                    f"quality_rule {self.id!r}: custom_sql requires config.sql"
                )
        return self


class FreshnessRuleConfig(_Strict):
    id: str
    source: str
    watermark_column: str
    max_age_seconds: int
    severity: Literal["warn", "block"]
    staleness_seconds: int | None = None


class RumiConfig(_Strict):
    sources: list[SourceConfig] = Field(default_factory=list)
    metrics: list[MetricConfig] = Field(default_factory=list)
    roles: list[RoleConfig] = Field(default_factory=list)
    policies: list[PolicyConfig] = Field(default_factory=list)
    quality_rules: list[QualityRuleConfig] = Field(default_factory=list)
    freshness_rules: list[FreshnessRuleConfig] = Field(default_factory=list)
