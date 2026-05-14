"""Load gibran.yaml from disk; run cross-entity validation.

Cross-entity validation is what Pydantic can't do at the field level:
foreign-key checks (metric.source -> sources.id, dimension.column ->
columns.name, policy.role -> roles.id, etc.), uniqueness within scope,
same-source check for ratios, DAG cycle detection for metric
dependencies, and row_filter AST validation against source columns
(via governance.ast.validate).
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml
from pydantic import ValidationError

from gibran.governance.ast import ASTValidationError
from gibran.governance.ast import validate_policy_ast as validate_ast
from gibran.sync.yaml_schema import (
    PolicyConfig,
    QualityRuleConfig,
    GibranConfig,
)


class ConfigValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ValidatedConfig:
    config: GibranConfig
    metric_dependencies: dict[str, frozenset[str]]


def load(path: Path) -> ValidatedConfig:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ConfigValidationError(f"{path}: top-level must be a mapping")
    try:
        config = GibranConfig.model_validate(raw)
    except ValidationError as e:
        raise ConfigValidationError(f"{path}: {e}") from e
    deps = _validate_cross_entity(config)
    return ValidatedConfig(config=config, metric_dependencies=deps)


def _validate_cross_entity(cfg: GibranConfig) -> dict[str, frozenset[str]]:
    source_ids = _collect_unique_ids(cfg.sources, "source")
    metric_ids = _collect_unique_ids(cfg.metrics, "metric")
    role_ids = _collect_unique_ids(cfg.roles, "role")
    _ = _collect_unique_ids(cfg.policies, "policy")
    _ = _collect_unique_ids(cfg.quality_rules, "quality_rule")
    _ = _collect_unique_ids(cfg.freshness_rules, "freshness_rule")

    columns_by_source: dict[str, frozenset[str]] = {}
    for s in cfg.sources:
        cols: set[str] = set()
        for c in s.columns:
            if c.name in cols:
                raise ConfigValidationError(
                    f"source {s.id!r}: duplicate column {c.name!r}"
                )
            cols.add(c.name)
        columns_by_source[s.id] = frozenset(cols)

        dim_ids: set[str] = set()
        for d in s.dimensions:
            if d.id in dim_ids:
                raise ConfigValidationError(
                    f"source {s.id!r}: duplicate dimension id {d.id!r}"
                )
            dim_ids.add(d.id)
            if d.column not in cols:
                raise ConfigValidationError(
                    f"source {s.id!r}: dimension {d.id!r} references unknown column {d.column!r}"
                )

    metric_by_id = {m.id: m for m in cfg.metrics}
    # Build a lookup of (source_id -> {dimension_id -> DimensionConfig}) so
    # period_over_period can verify its `period_dim` exists on the same
    # source and is temporal.
    dims_by_source: dict[str, dict[str, "object"]] = {}
    for s in cfg.sources:
        dims_by_source[s.id] = {d.id: d for d in s.dimensions}

    deps: dict[str, set[str]] = {m.id: set() for m in cfg.metrics}
    for m in cfg.metrics:
        if m.source not in source_ids:
            raise ConfigValidationError(
                f"metric {m.id!r}: unknown source {m.source!r}"
            )
        if m.type == "ratio":
            for ref_field, ref_id in (
                ("numerator", m.numerator),
                ("denominator", m.denominator),
            ):
                assert ref_id is not None
                if ref_id not in metric_by_id:
                    raise ConfigValidationError(
                        f"metric {m.id!r}: {ref_field} {ref_id!r} not defined"
                    )
                if metric_by_id[ref_id].source != m.source:
                    raise ConfigValidationError(
                        f"metric {m.id!r}: ratio component {ref_id!r} is from "
                        f"source {metric_by_id[ref_id].source!r}; ratio metrics "
                        f"must be same-source as {m.source!r} (cross-source deferred to V2)"
                    )
                deps[m.id].add(ref_id)
        elif m.type == "period_over_period":
            assert m.base_metric is not None and m.period_dim is not None
            if m.base_metric not in metric_by_id:
                raise ConfigValidationError(
                    f"metric {m.id!r}: base_metric {m.base_metric!r} not defined"
                )
            if metric_by_id[m.base_metric].source != m.source:
                raise ConfigValidationError(
                    f"metric {m.id!r}: base_metric {m.base_metric!r} is from "
                    f"source {metric_by_id[m.base_metric].source!r}; "
                    f"period_over_period must be same-source as {m.source!r}"
                )
            src_dims = dims_by_source.get(m.source, {})
            period_dim = src_dims.get(m.period_dim)
            if period_dim is None:
                raise ConfigValidationError(
                    f"metric {m.id!r}: period_dim {m.period_dim!r} not defined "
                    f"on source {m.source!r}"
                )
            if getattr(period_dim, "type", None) != "temporal":
                raise ConfigValidationError(
                    f"metric {m.id!r}: period_dim {m.period_dim!r} must be a "
                    f"temporal dimension (got {getattr(period_dim, 'type', None)!r})"
                )
            deps[m.id].add(m.base_metric)
        elif m.type == "cohort_retention":
            cols = columns_by_source.get(m.source, frozenset())
            for col_field, col_value in (
                ("entity_column", m.entity_column),
                ("event_column", m.event_column),
            ):
                assert col_value is not None
                if col_value not in cols:
                    raise ConfigValidationError(
                        f"metric {m.id!r}: {col_field} {col_value!r} not "
                        f"defined on source {m.source!r}"
                    )
        elif m.type == "funnel":
            cols = columns_by_source.get(m.source, frozenset())
            for col_field, col_value in (
                ("funnel_entity_column", m.funnel_entity_column),
                ("funnel_event_order_column", m.funnel_event_order_column),
            ):
                assert col_value is not None
                if col_value not in cols:
                    raise ConfigValidationError(
                        f"metric {m.id!r}: {col_field} {col_value!r} not "
                        f"defined on source {m.source!r}"
                    )
        elif m.type == "weighted_avg":
            if m.weight_column is not None:
                cols = columns_by_source.get(m.source, frozenset())
                if m.weight_column not in cols:
                    raise ConfigValidationError(
                        f"metric {m.id!r}: weight_column {m.weight_column!r} "
                        f"not defined on source {m.source!r}"
                    )
        elif m.type in ("count_distinct", "count_distinct_approx", "mode"):
            if m.column is not None:
                cols = columns_by_source.get(m.source, frozenset())
                if m.column not in cols:
                    raise ConfigValidationError(
                        f"metric {m.id!r}: column {m.column!r} not defined "
                        f"on source {m.source!r}"
                    )
    _detect_cycles(deps)

    for p in cfg.policies:
        _validate_policy(p, role_ids, source_ids, columns_by_source)

    for q in cfg.quality_rules:
        _validate_quality_rule(q, source_ids, columns_by_source)

    for f in cfg.freshness_rules:
        if f.source not in source_ids:
            raise ConfigValidationError(
                f"freshness_rule {f.id!r}: unknown source {f.source!r}"
            )
        if f.watermark_column not in columns_by_source.get(f.source, frozenset()):
            raise ConfigValidationError(
                f"freshness_rule {f.id!r}: watermark_column {f.watermark_column!r} "
                f"not defined on source {f.source!r}"
            )

    return {k: frozenset(v) for k, v in deps.items()}


def _collect_unique_ids(items: list, kind: str) -> set[str]:
    seen: set[str] = set()
    for item in items:
        if item.id in seen:
            raise ConfigValidationError(f"duplicate {kind} id: {item.id!r}")
        seen.add(item.id)
    return seen


def _validate_policy(
    p: PolicyConfig,
    role_ids: set[str],
    source_ids: set[str],
    columns_by_source: dict[str, frozenset[str]],
) -> None:
    if p.role not in role_ids:
        raise ConfigValidationError(f"policy {p.id!r}: unknown role {p.role!r}")
    if p.source not in source_ids:
        raise ConfigValidationError(f"policy {p.id!r}: unknown source {p.source!r}")
    cols = columns_by_source.get(p.source, frozenset())
    for col in p.column_overrides:
        if col not in cols:
            raise ConfigValidationError(
                f"policy {p.id!r}: column_override references unknown column "
                f"{col!r} on source {p.source!r}"
            )
    if p.row_filter is not None:
        try:
            validate_ast(p.row_filter, cols)
        except ASTValidationError as e:
            raise ConfigValidationError(
                f"policy {p.id!r}: row_filter AST invalid: {e}"
            ) from e


def _validate_quality_rule(
    q: QualityRuleConfig,
    source_ids: set[str],
    columns_by_source: dict[str, frozenset[str]],
) -> None:
    if q.source not in source_ids:
        raise ConfigValidationError(
            f"quality_rule {q.id!r}: unknown source {q.source!r}"
        )
    cols = columns_by_source.get(q.source, frozenset())
    if q.type in ("not_null", "unique", "range"):
        col = q.config.get("column")
        if col not in cols:
            raise ConfigValidationError(
                f"quality_rule {q.id!r}: config.column {col!r} not defined on "
                f"source {q.source!r}"
            )


def _detect_cycles(deps: dict[str, set[str]]) -> None:
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = {k: WHITE for k in deps}

    def dfs(node: str, stack: list[str]) -> None:
        color[node] = GRAY
        for child in deps.get(node, ()):
            if color[child] == GRAY:
                idx = stack.index(child) if child in stack else 0
                cycle = stack[idx:] + [child]
                raise ConfigValidationError(
                    f"metric dependency cycle: {' -> '.join(cycle)}"
                )
            if color[child] == WHITE:
                dfs(child, stack + [child])
        color[node] = BLACK

    for k in deps:
        if color[k] == WHITE:
            dfs(k, [k])


def resolve_cost_class(rule: QualityRuleConfig) -> str:
    """Default cost_class derived from rule type (PM revision):
    not_null/unique -> cheap, range/custom_sql -> expensive.
    Explicit YAML value overrides."""
    if rule.cost_class is not None:
        return rule.cost_class
    return "cheap" if rule.type in ("not_null", "unique") else "expensive"
