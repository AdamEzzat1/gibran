"""Pydantic schema for gibran.yaml -- the source-of-truth config.

Every field maps to a column in the metadata DB. Field-local validation
lives here; cross-entity validation (FKs, DAG cycle detection,
row_filter AST validation) is in sync.loader because Pydantic validators
are field-local by design.
"""
from __future__ import annotations

from datetime import datetime
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
    "cohort_retention", "funnel",
    "multi_stage_filter",
    # Aggregate primitives (Tier 2 Item 5):
    "weighted_avg", "stddev_samp", "stddev_pop",
    "count_distinct", "count_distinct_approx", "mode",
    # Phase 1 Task 1.10 additions:
    "variance", "first_value", "last_value", "median",
    # Phase 3 shape primitive: filter entities by sub-query intersection.
    "cohort_filter",
    # Phase 3 shape primitive: query detected anomalies for a rule_id.
    "anomaly_query",
]

RollingAggregate = Literal["sum", "avg", "min", "max", "count"]
PeriodUnit = Literal["year", "quarter", "month", "week", "day"]
PeriodComparison = Literal["delta", "ratio", "pct_change"]
CohortGrain = Literal["year", "quarter", "month", "week", "day"]


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
    # Materialization: list of dimension_ids to pre-aggregate at.
    # When set, `gibran sync` creates a `gibran_mat_<metric_id>` table
    # populated with (dim_cols..., metric_value). Queries that match
    # the materialized shape (single metric + exact dim list) route to
    # the pre-aggregated table instead of re-scanning the source.
    # NULL/omitted = no materialization. Empty list = scalar
    # materialization (the metric value over the whole source, no
    # grouping). Validated below + by loader.
    materialized: list[str] | None = None
    # Materialization strategy (Phase 2C):
    #   "full"        -- rebuild the gibran_mat_<id> table on every sync.
    #                    Simple, correct, but O(source rows) per sync.
    #   "incremental" -- DELETE + INSERT only rows where watermark_column
    #                    is newer than the last refresh's watermark. Requires
    #                    watermark_column (typically a TIMESTAMP).
    # Default "full" matches pre-Phase-2C behavior; opt-in for incremental.
    materialized_strategy: Literal["full", "incremental"] | None = None
    watermark_column: str | None = None
    # Grace window for incremental refresh: rows where
    # watermark_column > (last_refresh_watermark - late_arrival_grace_seconds)
    # are re-evaluated each pass. Defends against rows arriving with a
    # backdated watermark (e.g. an order whose order_date is yesterday but
    # was inserted today). Default 0 = no grace, exact-match watermark.
    late_arrival_grace_seconds: int | None = None

    @model_validator(mode="after")
    def _check_materialized_compat(self) -> "MetricConfig":
        if self.materialized is None:
            if (
                self.materialized_strategy is not None
                or self.watermark_column is not None
                or self.late_arrival_grace_seconds is not None
            ):
                raise ValueError(
                    f"metric {self.id!r}: materialized_strategy / "
                    f"watermark_column / late_arrival_grace_seconds require "
                    f"`materialized` to be set"
                )
            return self
        # V1 restriction: only simple aggregates can be materialized.
        # Shape primitives (cohort/funnel/multi_stage_filter/cohort_filter)
        # have multi-column or CTE-based outputs that don't fit the
        # (dim_cols, value) shape. ratio / expression have template
        # references that we'd need to resolve at sync time -- deferrable.
        incompatible = {
            "cohort_retention", "funnel", "multi_stage_filter", "cohort_filter",
            "anomaly_query",
            "ratio", "expression", "rolling_window", "period_over_period",
        }
        if self.type in incompatible:
            raise ValueError(
                f"metric {self.id!r}: type {self.type!r} cannot be "
                f"materialized in V1 (only direct aggregates: count / "
                f"sum / avg / min / max / percentile / count_distinct / "
                f"count_distinct_approx / stddev_samp / stddev_pop / mode "
                f"/ weighted_avg / variance / first_value / last_value / median)"
            )
        if self.materialized_strategy == "incremental":
            if not self.watermark_column:
                raise ValueError(
                    f"metric {self.id!r}: materialized_strategy=incremental "
                    f"requires `watermark_column`"
                )
            if not self.materialized:
                raise ValueError(
                    f"metric {self.id!r}: scalar materialization "
                    f"(`materialized: []`) is incompatible with "
                    f"materialized_strategy=incremental -- a single scalar "
                    f"value has nothing to incrementally update; use "
                    f"materialized_strategy=full or specify dimensions"
                )
            if (
                self.late_arrival_grace_seconds is not None
                and self.late_arrival_grace_seconds < 0
            ):
                raise ValueError(
                    f"metric {self.id!r}: late_arrival_grace_seconds must "
                    f"be >= 0, got {self.late_arrival_grace_seconds}"
                )
        elif self.watermark_column is not None:
            # watermark_column without strategy=incremental is meaningless;
            # call it out so users don't think they've enabled incremental.
            raise ValueError(
                f"metric {self.id!r}: watermark_column is only meaningful "
                f"with materialized_strategy=incremental"
            )
        return self

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

    # cohort_retention-specific. The output is a fixed-shape table:
    #   (cohort_start, periods_since_cohort, retained_count, cohort_size, retention_rate)
    # When the intent's metrics include a cohort_retention metric, the
    # compiler short-circuits the normal SELECT-list builder and emits
    # a 3-CTE query whose columns are the above. Intent.dimensions must
    # be empty and the cohort metric must be the only entry in
    # intent.metrics (enforced by dsl/validate.py).
    entity_column: str | None = None      # e.g. customer_id -- the entity being cohorted
    event_column: str | None = None       # e.g. order_date  -- when each entity "shows up"
    cohort_grain: CohortGrain | None = None
    retention_grain: CohortGrain | None = None
    max_periods: int | None = None        # cap the periods_since_cohort dimension

    # funnel-specific. The output is also a fixed-shape table:
    #   (step_name, entity_count, conversion_from_previous, conversion_from_first)
    # `steps` is an ordered list of `(name, condition_sql)` pairs that
    # define each funnel stage. Same single-metric / no-dimensions
    # constraint as cohort_retention.
    funnel_entity_column: str | None = None    # the entity (user_id, customer_id, ...)
    funnel_event_order_column: str | None = None  # event timestamp for sequencing
    funnel_steps: list[dict[str, str]] | None = None  # [{name, condition}, ...]

    # multi_stage_filter-specific. The primitive answers questions like
    # "top decile by 90-day spend, then churn rate". Two-stage:
    #   1. rank each entity by `ranking_expression` (a raw SQL aggregate)
    #   2. compute `result_expression` over the filtered subset
    # Output is a single row: (entity_count, result_value). Pick exactly
    # ONE of (top_n, top_percentile) -- top_percentile uses PERCENT_RANK,
    # top_n uses ROW_NUMBER + LIMIT N.
    msf_entity_column: str | None = None
    msf_ranking_expression: str | None = None    # raw SQL aggregate; e.g. "SUM(amount)"
    msf_result_expression: str | None = None     # raw SQL aggregate; e.g. "COUNT(*)"
    top_n: int | None = None
    top_percentile: float | None = None          # 0 < p <= 1; "top decile" = 0.1

    # aggregate-primitive-specific (Tier 2 Item 5):
    #   weighted_avg requires weight_column (alongside expression for the value)
    #   mode reuses `column` (the value to find the mode of)
    weight_column: str | None = None

    # cohort_filter-specific (Phase 3 shape primitive). Output is one
    # scalar row: the count of distinct entities matching BOTH the
    # cohort_condition AND the result_condition.
    # entity_column reuses the shared field (also used by cohort_retention).
    # Conditions are raw SQL WHERE-clause fragments referencing the
    # source's columns -- same trust model as funnel_steps[].condition.
    cohort_condition: str | None = None
    result_condition: str | None = None

    # anomaly_query-specific (Phase 3 shape primitive). Queries
    # gibran_quality_runs for failed runs of the named rule_id. The
    # rule_id must reference an existing rule_type='anomaly' rule; the
    # cross-entity check happens at sync time (loader), not in this
    # field-local validator.
    rule_id: str | None = None

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
        elif self.type == "cohort_retention":
            missing = [
                f for f in (
                    "entity_column", "event_column",
                    "cohort_grain", "retention_grain",
                )
                if getattr(self, f) is None
            ]
            if missing:
                raise ValueError(
                    f"metric {self.id!r}: cohort_retention requires {missing}"
                )
            if self.max_periods is not None and self.max_periods <= 0:
                raise ValueError(
                    f"metric {self.id!r}: cohort_retention max_periods must be > 0"
                )
            if self.expression or self.numerator or self.denominator:
                raise ValueError(
                    f"metric {self.id!r}: cohort_retention cannot have "
                    f"expression/numerator/denominator"
                )
        elif self.type == "funnel":
            missing = [
                f for f in (
                    "funnel_entity_column", "funnel_event_order_column", "funnel_steps",
                )
                if getattr(self, f) is None
            ]
            if missing:
                raise ValueError(
                    f"metric {self.id!r}: funnel requires {missing}"
                )
            assert self.funnel_steps is not None
            if len(self.funnel_steps) < 2:
                raise ValueError(
                    f"metric {self.id!r}: funnel requires at least 2 steps"
                )
            seen_names: set[str] = set()
            for i, step in enumerate(self.funnel_steps):
                if not isinstance(step, dict):
                    raise ValueError(
                        f"metric {self.id!r}: funnel step {i} must be a dict, "
                        f"got {type(step).__name__}"
                    )
                if "name" not in step or "condition" not in step:
                    raise ValueError(
                        f"metric {self.id!r}: funnel step {i} requires "
                        f"`name` and `condition` keys"
                    )
                if step["name"] in seen_names:
                    raise ValueError(
                        f"metric {self.id!r}: funnel step name {step['name']!r} "
                        f"appears more than once"
                    )
                seen_names.add(step["name"])
            if self.expression or self.numerator or self.denominator:
                raise ValueError(
                    f"metric {self.id!r}: funnel cannot have "
                    f"expression/numerator/denominator"
                )
        elif self.type == "multi_stage_filter":
            missing = [
                f for f in (
                    "msf_entity_column", "msf_ranking_expression",
                    "msf_result_expression",
                )
                if getattr(self, f) is None
            ]
            if missing:
                raise ValueError(
                    f"metric {self.id!r}: multi_stage_filter requires {missing}"
                )
            has_n = self.top_n is not None
            has_p = self.top_percentile is not None
            if has_n == has_p:
                raise ValueError(
                    f"metric {self.id!r}: multi_stage_filter requires exactly "
                    f"ONE of `top_n` or `top_percentile`"
                )
            if has_n and self.top_n <= 0:
                raise ValueError(
                    f"metric {self.id!r}: multi_stage_filter top_n must be > 0"
                )
            if has_p and not (0 < self.top_percentile <= 1):
                raise ValueError(
                    f"metric {self.id!r}: multi_stage_filter top_percentile "
                    f"must be in (0, 1], got {self.top_percentile}"
                )
            if self.expression or self.numerator or self.denominator:
                raise ValueError(
                    f"metric {self.id!r}: multi_stage_filter cannot have "
                    f"expression/numerator/denominator"
                )
        elif self.type == "cohort_filter":
            missing = [
                f for f in ("entity_column", "cohort_condition", "result_condition")
                if getattr(self, f) is None
            ]
            if missing:
                raise ValueError(
                    f"metric {self.id!r}: cohort_filter requires {missing}"
                )
            if self.expression or self.numerator or self.denominator:
                raise ValueError(
                    f"metric {self.id!r}: cohort_filter cannot have "
                    f"expression/numerator/denominator"
                )
        elif self.type == "anomaly_query":
            if self.rule_id is None:
                raise ValueError(
                    f"metric {self.id!r}: anomaly_query requires `rule_id` "
                    f"(pointing to a rule_type='anomaly' rule in "
                    f"quality_rules)"
                )
            if self.expression or self.numerator or self.denominator:
                raise ValueError(
                    f"metric {self.id!r}: anomaly_query cannot have "
                    f"expression/numerator/denominator"
                )
        elif self.type == "weighted_avg":
            if self.expression is None or self.weight_column is None:
                raise ValueError(
                    f"metric {self.id!r}: weighted_avg requires `expression` "
                    f"(the value) and `weight_column`"
                )
            if self.numerator or self.denominator:
                raise ValueError(
                    f"metric {self.id!r}: weighted_avg cannot have numerator/denominator"
                )
        elif self.type in ("stddev_samp", "stddev_pop", "variance"):
            if not self.expression:
                raise ValueError(
                    f"metric {self.id!r}: {self.type} requires `expression`"
                )
            if self.numerator or self.denominator:
                raise ValueError(
                    f"metric {self.id!r}: {self.type} cannot have numerator/denominator"
                )
        elif self.type in (
            "count_distinct", "count_distinct_approx", "mode",
            "first_value", "last_value", "median",
        ):
            if self.column is None:
                raise ValueError(
                    f"metric {self.id!r}: {self.type} requires `column`"
                )
            if self.expression or self.numerator or self.denominator:
                raise ValueError(
                    f"metric {self.id!r}: {self.type} cannot have "
                    f"expression/numerator/denominator"
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
    # Break-glass: elevated-access role whose every use produces a marked
    # audit row (gibran_query_log.is_break_glass = TRUE). Default-deny
    # remains the right setting for normal roles; break-glass is the
    # explicit "I know I'm doing something high-privilege" toggle.
    is_break_glass: bool = False


class PolicyConfig(_Strict):
    id: str
    role: str
    source: str
    default_column_mode: Literal["allow", "deny"] = "deny"
    column_overrides: dict[str, Literal["allow", "deny"]] = Field(default_factory=dict)
    row_filter: dict[str, Any] | None = None
    # Time-bound grants (contractors, consultants, temporary credentials).
    # NULL/omitted = "never expires". Compared against DuckDB's
    # CURRENT_TIMESTAMP at evaluate-time; the applier normalizes tz-aware
    # values to naive UTC so round-trip equality holds on re-sync.
    valid_until: datetime | None = None


class QualityRuleConfig(_Strict):
    id: str
    source: str
    type: Literal["not_null", "unique", "range", "custom_sql", "anomaly"]
    config: dict[str, Any]
    severity: Literal["warn", "block"]
    cost_class: Literal["cheap", "expensive"] | None = None
    staleness_seconds: int | None = None
    enabled: bool = True
    # Optional webhook URL: POSTed a BlockingFailure JSON when this rule
    # fails with severity='block'. NULL = no webhook (the runner will
    # never make an outbound network call for rules without one).
    alert_webhook: str | None = None

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
        elif self.type == "anomaly":
            # Anomaly rules compute a numeric observation from `sql`, store
            # it in gibran_quality_runs.observed_value, and flag failures
            # when the new value falls outside +/- n_sigma * stddev of the
            # trailing trailing_periods observations. Bootstrapping: with
            # fewer than `min_observations` history, the rule never fails.
            if "sql" not in self.config:
                raise ValueError(
                    f"quality_rule {self.id!r}: anomaly requires config.sql"
                )
            if "n_sigma" not in self.config or self.config["n_sigma"] <= 0:
                raise ValueError(
                    f"quality_rule {self.id!r}: anomaly requires "
                    f"config.n_sigma > 0"
                )
            if "trailing_periods" not in self.config or self.config["trailing_periods"] < 2:
                raise ValueError(
                    f"quality_rule {self.id!r}: anomaly requires "
                    f"config.trailing_periods >= 2"
                )
        return self


class FreshnessRuleConfig(_Strict):
    id: str
    source: str
    watermark_column: str
    max_age_seconds: int
    severity: Literal["warn", "block"]
    staleness_seconds: int | None = None


class GibranConfig(_Strict):
    sources: list[SourceConfig] = Field(default_factory=list)
    metrics: list[MetricConfig] = Field(default_factory=list)
    roles: list[RoleConfig] = Field(default_factory=list)
    policies: list[PolicyConfig] = Field(default_factory=list)
    quality_rules: list[QualityRuleConfig] = Field(default_factory=list)
    freshness_rules: list[FreshnessRuleConfig] = Field(default_factory=list)
