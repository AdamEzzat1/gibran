"""Apply ValidatedConfig to a DuckDB connection. Single transaction;
idempotent additive upsert.

V1 limitation: removing an entity from YAML does not delete it from the DB
(except quality/freshness rules which are wholesale-replaced -- see below).
A future `rumi sync --force` will perform destructive sync for catalog +
governance entities."""
from __future__ import annotations

import json

import duckdb

from rumi.sync.loader import ValidatedConfig, resolve_cost_class
from rumi.sync.yaml_schema import (
    ColumnConfig,
    DimensionConfig,
    FreshnessRuleConfig,
    MetricConfig,
    PolicyConfig,
    QualityRuleConfig,
    RoleConfig,
    SourceConfig,
)


def apply(con: duckdb.DuckDBPyConnection, validated: ValidatedConfig) -> dict[str, int]:
    """Apply validated config in a single transaction. Returns counts."""
    cfg = validated.config
    counts = {
        "sources": 0, "columns": 0, "dimensions": 0, "metrics": 0,
        "roles": 0, "policies": 0, "quality_rules": 0, "freshness_rules": 0,
    }
    con.execute("BEGIN")
    try:
        for s in cfg.sources:
            _upsert_source(con, s)
            counts["sources"] += 1
            _replace_columns(con, s.id, s.columns)
            counts["columns"] += len(s.columns)
            _replace_dimensions(con, s.id, s.dimensions)
            counts["dimensions"] += len(s.dimensions)

        for m in cfg.metrics:
            _upsert_metric(con, m)
            _ensure_metric_version(con, m)
            counts["metrics"] += 1
        _replace_metric_dependencies(con, validated.metric_dependencies)

        for r in cfg.roles:
            _upsert_role(con, r)
            counts["roles"] += 1

        for p in cfg.policies:
            _upsert_policy(con, p)
            counts["policies"] += 1

        # Quality + freshness rules are wholesale-replaced. Their primary
        # consumers (rumi_quality_runs) keep rule_id as a plain string with
        # no FK, so deleting/recreating rules does not orphan run history.
        _replace_all_quality_rules(con, cfg.quality_rules)
        counts["quality_rules"] = len(cfg.quality_rules)
        _replace_all_freshness_rules(con, cfg.freshness_rules)
        counts["freshness_rules"] = len(cfg.freshness_rules)

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return counts


def _upsert_source(con: duckdb.DuckDBPyConnection, s: SourceConfig) -> None:
    existing = con.execute(
        "SELECT display_name, source_type, uri, primary_grain "
        "FROM rumi_sources WHERE source_id = ?",
        [s.id],
    ).fetchone()
    if existing is None:
        con.execute(
            "INSERT INTO rumi_sources "
            "(source_id, display_name, source_type, uri, primary_grain) "
            "VALUES (?, ?, ?, ?, ?)",
            [s.id, s.display_name, s.type, s.uri, s.primary_grain],
        )
        return
    if existing == (s.display_name, s.type, s.uri, s.primary_grain):
        return
    con.execute(
        "UPDATE rumi_sources SET "
        "display_name = ?, source_type = ?, uri = ?, "
        "primary_grain = ?, updated_at = now() "
        "WHERE source_id = ?",
        [s.display_name, s.type, s.uri, s.primary_grain, s.id],
    )


def _replace_columns(
    con: duckdb.DuckDBPyConnection, source_id: str, columns: list[ColumnConfig]
) -> None:
    con.execute("DELETE FROM rumi_columns WHERE source_id = ?", [source_id])
    for c in columns:
        con.execute(
            "INSERT INTO rumi_columns "
            "(source_id, column_name, data_type, sensitivity, description, expose_examples) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [source_id, c.name, c.type, c.sensitivity, c.description, c.expose_examples],
        )


def _replace_dimensions(
    con: duckdb.DuckDBPyConnection, source_id: str, dimensions: list[DimensionConfig]
) -> None:
    con.execute("DELETE FROM rumi_dimensions WHERE source_id = ?", [source_id])
    for d in dimensions:
        con.execute(
            "INSERT INTO rumi_dimensions "
            "(dimension_id, source_id, column_name, display_name, dim_type, description) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [d.id, source_id, d.column, d.display_name, d.type, d.description],
        )


def _upsert_metric(con: duckdb.DuckDBPyConnection, m: MetricConfig) -> None:
    existing = con.execute(
        "SELECT source_id, display_name, metric_type, unit, description, owner "
        "FROM rumi_metrics WHERE metric_id = ?",
        [m.id],
    ).fetchone()
    if existing is None:
        con.execute(
            "INSERT INTO rumi_metrics "
            "(metric_id, source_id, display_name, metric_type, unit, description, owner) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [m.id, m.source, m.display_name, m.type, m.unit, m.description, m.owner],
        )
        return
    new_values = (m.source, m.display_name, m.type, m.unit, m.description, m.owner)
    if existing == new_values:
        return
    con.execute(
        "UPDATE rumi_metrics SET "
        "source_id = ?, display_name = ?, metric_type = ?, "
        "unit = ?, description = ?, owner = ? "
        "WHERE metric_id = ?",
        [*new_values, m.id],
    )


def _ensure_metric_version(con: duckdb.DuckDBPyConnection, m: MetricConfig) -> None:
    expression = _render_expression(m)
    existing = con.execute(
        "SELECT version, expression, filter_sql FROM rumi_metric_versions "
        "WHERE metric_id = ? AND effective_to IS NULL",
        [m.id],
    ).fetchone()
    if existing is None:
        con.execute(
            "INSERT INTO rumi_metric_versions "
            "(metric_id, version, expression, filter_sql) "
            "VALUES (?, 1, ?, ?)",
            [m.id, expression, m.filter],
        )
        return
    if existing[1] == expression and existing[2] == m.filter:
        return
    new_version = existing[0] + 1
    con.execute(
        "UPDATE rumi_metric_versions SET effective_to = now() "
        "WHERE metric_id = ? AND version = ?",
        [m.id, existing[0]],
    )
    con.execute(
        "INSERT INTO rumi_metric_versions "
        "(metric_id, version, expression, filter_sql) "
        "VALUES (?, ?, ?, ?)",
        [m.id, new_version, expression, m.filter],
    )
    con.execute(
        "UPDATE rumi_metrics SET current_version = ? WHERE metric_id = ?",
        [new_version, m.id],
    )


def _render_expression(m: MetricConfig) -> str:
    if m.type == "count":
        return "COUNT(*)"
    if m.type == "ratio":
        return f"{{{m.numerator}}}/{{{m.denominator}}}"
    if m.type == "expression":
        assert m.expression is not None
        return m.expression
    if m.type == "percentile":
        # DuckDB's QUANTILE_CONT(column, p) -- a regular aggregate. Slots into
        # FILTER + GROUP BY just like SUM/AVG/etc.
        assert m.column is not None and m.p is not None
        return f"QUANTILE_CONT({m.column}, {m.p})"
    if m.type == "rolling_window":
        # SQL grammar for window-aggregate-with-filter requires:
        #   aggregate(args) [FILTER (WHERE cond)] OVER (window_spec)
        # FILTER must come BEFORE OVER, not after. We render it inline here so
        # the compiler can use the stored expression verbatim (and skip the
        # default "attach FILTER after the expression" step for this type).
        # V1 constraint: rolling_window metrics are not compatible with intent
        # dimensions; enforced by dsl.validate.validate_intent.
        assert (
            m.column is not None and m.aggregate is not None
            and m.window is not None and m.order_by_column is not None
        )
        filter_clause = f" FILTER (WHERE {m.filter})" if m.filter else ""
        partition_clause = ""
        if m.partition_by:
            partition_clause = f"PARTITION BY {', '.join(m.partition_by)} "
        return (
            f"{m.aggregate.upper()}({m.column}){filter_clause} OVER ("
            f"{partition_clause}ORDER BY {m.order_by_column} "
            f"RANGE BETWEEN INTERVAL '{m.window}' PRECEDING AND CURRENT ROW)"
        )
    assert m.expression is not None
    return f"{m.type.upper()}({m.expression})"


def _replace_metric_dependencies(
    con: duckdb.DuckDBPyConnection, deps: dict[str, frozenset[str]]
) -> None:
    con.execute("DELETE FROM rumi_metric_dependencies")
    for metric_id, dep_ids in deps.items():
        for dep_id in dep_ids:
            con.execute(
                "INSERT INTO rumi_metric_dependencies (metric_id, depends_on_id) "
                "VALUES (?, ?)",
                [metric_id, dep_id],
            )


def _upsert_role(con: duckdb.DuckDBPyConnection, r: RoleConfig) -> None:
    existing = con.execute(
        "SELECT display_name FROM rumi_roles WHERE role_id = ?", [r.id]
    ).fetchone()
    if existing is None:
        con.execute(
            "INSERT INTO rumi_roles (role_id, display_name) VALUES (?, ?)",
            [r.id, r.display_name],
        )
    elif existing != (r.display_name,):
        con.execute(
            "UPDATE rumi_roles SET display_name = ? WHERE role_id = ?",
            [r.display_name, r.id],
        )
    con.execute("DELETE FROM rumi_role_attributes WHERE role_id = ?", [r.id])
    for k, v in r.attributes.items():
        con.execute(
            "INSERT INTO rumi_role_attributes "
            "(role_id, attribute_key, attribute_value) VALUES (?, ?, ?)",
            [r.id, k, v],
        )


def _upsert_policy(con: duckdb.DuckDBPyConnection, p: PolicyConfig) -> None:
    row_filter_json = json.dumps(p.row_filter) if p.row_filter is not None else None
    existing = con.execute(
        "SELECT role_id, source_id, row_filter_ast, default_column_mode "
        "FROM rumi_policies WHERE policy_id = ?",
        [p.id],
    ).fetchone()
    if existing is None:
        con.execute(
            "INSERT INTO rumi_policies "
            "(policy_id, role_id, source_id, row_filter_ast, default_column_mode) "
            "VALUES (?, ?, ?, ?, ?)",
            [p.id, p.role, p.source, row_filter_json, p.default_column_mode],
        )
    else:
        new_values = (p.role, p.source, row_filter_json, p.default_column_mode)
        if existing != new_values:
            con.execute(
                "UPDATE rumi_policies SET "
                "role_id = ?, source_id = ?, row_filter_ast = ?, default_column_mode = ? "
                "WHERE policy_id = ?",
                [*new_values, p.id],
            )
    con.execute("DELETE FROM rumi_policy_columns WHERE policy_id = ?", [p.id])
    for col, mode in p.column_overrides.items():
        con.execute(
            "INSERT INTO rumi_policy_columns (policy_id, column_name, granted) "
            "VALUES (?, ?, ?)",
            [p.id, col, mode == "allow"],
        )


def _replace_all_quality_rules(
    con: duckdb.DuckDBPyConnection, rules: list[QualityRuleConfig]
) -> None:
    con.execute("DELETE FROM rumi_quality_rules")
    for q in rules:
        con.execute(
            "INSERT INTO rumi_quality_rules "
            "(rule_id, source_id, rule_type, rule_config, cost_class, severity, "
            "staleness_seconds, enabled) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                q.id,
                q.source,
                q.type,
                json.dumps(q.config),
                resolve_cost_class(q),
                q.severity,
                q.staleness_seconds,
                q.enabled,
            ],
        )


def _replace_all_freshness_rules(
    con: duckdb.DuckDBPyConnection, rules: list[FreshnessRuleConfig]
) -> None:
    con.execute("DELETE FROM rumi_freshness_rules")
    for f in rules:
        con.execute(
            "INSERT INTO rumi_freshness_rules "
            "(rule_id, source_id, watermark_column, max_age_seconds, severity, "
            "staleness_seconds) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                f.id, f.source, f.watermark_column, f.max_age_seconds,
                f.severity, f.staleness_seconds,
            ],
        )
