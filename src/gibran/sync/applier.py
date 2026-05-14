"""Apply ValidatedConfig to a DuckDB connection. Single transaction;
idempotent additive upsert.

V1 limitation: removing an entity from YAML does not delete it from the DB
(except quality/freshness rules which are wholesale-replaced -- see below).
A future `gibran sync --force` will perform destructive sync for catalog +
governance entities."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import duckdb

from gibran.sync.loader import ValidatedConfig, resolve_cost_class
from gibran.sync.yaml_schema import (
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
        # consumers (gibran_quality_runs) keep rule_id as a plain string with
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
        "FROM gibran_sources WHERE source_id = ?",
        [s.id],
    ).fetchone()
    if existing is None:
        con.execute(
            "INSERT INTO gibran_sources "
            "(source_id, display_name, source_type, uri, primary_grain) "
            "VALUES (?, ?, ?, ?, ?)",
            [s.id, s.display_name, s.type, s.uri, s.primary_grain],
        )
        return
    if existing == (s.display_name, s.type, s.uri, s.primary_grain):
        return
    con.execute(
        "UPDATE gibran_sources SET "
        "display_name = ?, source_type = ?, uri = ?, "
        "primary_grain = ?, updated_at = now() "
        "WHERE source_id = ?",
        [s.display_name, s.type, s.uri, s.primary_grain, s.id],
    )


def _replace_columns(
    con: duckdb.DuckDBPyConnection, source_id: str, columns: list[ColumnConfig]
) -> None:
    con.execute("DELETE FROM gibran_columns WHERE source_id = ?", [source_id])
    for c in columns:
        con.execute(
            "INSERT INTO gibran_columns "
            "(source_id, column_name, data_type, sensitivity, description, expose_examples) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [source_id, c.name, c.type, c.sensitivity, c.description, c.expose_examples],
        )


def _replace_dimensions(
    con: duckdb.DuckDBPyConnection, source_id: str, dimensions: list[DimensionConfig]
) -> None:
    con.execute("DELETE FROM gibran_dimensions WHERE source_id = ?", [source_id])
    for d in dimensions:
        con.execute(
            "INSERT INTO gibran_dimensions "
            "(dimension_id, source_id, column_name, display_name, dim_type, description) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [d.id, source_id, d.column, d.display_name, d.type, d.description],
        )


def _upsert_metric(con: duckdb.DuckDBPyConnection, m: MetricConfig) -> None:
    existing = con.execute(
        "SELECT source_id, display_name, metric_type, unit, description, owner "
        "FROM gibran_metrics WHERE metric_id = ?",
        [m.id],
    ).fetchone()
    if existing is None:
        con.execute(
            "INSERT INTO gibran_metrics "
            "(metric_id, source_id, display_name, metric_type, unit, description, owner) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [m.id, m.source, m.display_name, m.type, m.unit, m.description, m.owner],
        )
        return
    new_values = (m.source, m.display_name, m.type, m.unit, m.description, m.owner)
    if existing == new_values:
        return
    con.execute(
        "UPDATE gibran_metrics SET "
        "source_id = ?, display_name = ?, metric_type = ?, "
        "unit = ?, description = ?, owner = ? "
        "WHERE metric_id = ?",
        [*new_values, m.id],
    )


def _ensure_metric_version(con: duckdb.DuckDBPyConnection, m: MetricConfig) -> None:
    expression = _render_expression(m)
    metric_config_json = _render_metric_config(m)
    existing = con.execute(
        "SELECT version, expression, filter_sql, metric_config "
        "FROM gibran_metric_versions "
        "WHERE metric_id = ? AND effective_to IS NULL",
        [m.id],
    ).fetchone()
    if existing is None:
        con.execute(
            "INSERT INTO gibran_metric_versions "
            "(metric_id, version, expression, filter_sql, metric_config) "
            "VALUES (?, 1, ?, ?, ?)",
            [m.id, expression, m.filter, metric_config_json],
        )
        return
    if (
        existing[1] == expression
        and existing[2] == m.filter
        and existing[3] == metric_config_json
    ):
        return
    new_version = existing[0] + 1
    con.execute(
        "UPDATE gibran_metric_versions SET effective_to = now() "
        "WHERE metric_id = ? AND version = ?",
        [m.id, existing[0]],
    )
    con.execute(
        "INSERT INTO gibran_metric_versions "
        "(metric_id, version, expression, filter_sql, metric_config) "
        "VALUES (?, ?, ?, ?, ?)",
        [m.id, new_version, expression, m.filter, metric_config_json],
    )
    con.execute(
        "UPDATE gibran_metrics SET current_version = ? WHERE metric_id = ?",
        [new_version, m.id],
    )


def _render_metric_config(m: MetricConfig) -> str | None:
    """Pack primitive-specific config into a JSON blob, or None for shapes
    that need no extra config beyond `expression`."""
    if m.type == "period_over_period":
        return json.dumps({
            "base_metric": m.base_metric,
            "period_dim": m.period_dim,
            "period_unit": m.period_unit,
            "comparison": m.comparison,
        })
    if m.type == "cohort_retention":
        return json.dumps({
            "entity_column": m.entity_column,
            "event_column": m.event_column,
            "cohort_grain": m.cohort_grain,
            "retention_grain": m.retention_grain,
            "max_periods": m.max_periods,
        })
    if m.type == "funnel":
        return json.dumps({
            "entity_column": m.funnel_entity_column,
            "event_order_column": m.funnel_event_order_column,
            "steps": m.funnel_steps,
        })
    return None


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
    if m.type == "period_over_period":
        # The actual SQL is dynamic (it composes the base metric's
        # expression and the period_dim's column at compile time) so we
        # store a marker; the compiler reads metric_config and ignores
        # this string.
        return f"period_over_period[{m.base_metric}@{m.period_unit}/{m.comparison}]"
    if m.type == "cohort_retention":
        # The whole-query CTE shape is built by the compiler from
        # metric_config. Marker only.
        return (
            f"cohort_retention[{m.entity_column}/{m.event_column}/"
            f"{m.cohort_grain}->{m.retention_grain}]"
        )
    if m.type == "funnel":
        # Same: CTE chain built by the compiler from metric_config.
        return f"funnel[{m.funnel_entity_column}/{len(m.funnel_steps or [])} steps]"
    if m.type == "weighted_avg":
        # SUM(value * weight) / NULLIF(SUM(weight), 0). Single-pass aggregate.
        assert m.expression is not None and m.weight_column is not None
        return (
            f"SUM(({m.expression}) * {m.weight_column}) "
            f"/ NULLIF(SUM({m.weight_column}), 0)"
        )
    if m.type == "stddev_samp":
        assert m.expression is not None
        return f"STDDEV_SAMP({m.expression})"
    if m.type == "stddev_pop":
        assert m.expression is not None
        return f"STDDEV_POP({m.expression})"
    if m.type == "count_distinct":
        assert m.column is not None
        return f"COUNT(DISTINCT {m.column})"
    if m.type == "count_distinct_approx":
        # DuckDB's HyperLogLog-based approximate distinct count.
        assert m.column is not None
        return f"APPROX_COUNT_DISTINCT({m.column})"
    if m.type == "mode":
        # DuckDB returns the most common value of `column`. SQL standard
        # spells it `MODE() WITHIN GROUP (ORDER BY col)` but DuckDB also
        # accepts the function form, which is concise and composes with
        # FILTER + GROUP BY uniformly.
        assert m.column is not None
        return f"MODE({m.column})"
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
    con.execute("DELETE FROM gibran_metric_dependencies")
    for metric_id, dep_ids in deps.items():
        for dep_id in dep_ids:
            con.execute(
                "INSERT INTO gibran_metric_dependencies (metric_id, depends_on_id) "
                "VALUES (?, ?)",
                [metric_id, dep_id],
            )


def _upsert_role(con: duckdb.DuckDBPyConnection, r: RoleConfig) -> None:
    existing = con.execute(
        "SELECT display_name FROM gibran_roles WHERE role_id = ?", [r.id]
    ).fetchone()
    if existing is None:
        con.execute(
            "INSERT INTO gibran_roles (role_id, display_name) VALUES (?, ?)",
            [r.id, r.display_name],
        )
    elif existing != (r.display_name,):
        con.execute(
            "UPDATE gibran_roles SET display_name = ? WHERE role_id = ?",
            [r.display_name, r.id],
        )
    con.execute("DELETE FROM gibran_role_attributes WHERE role_id = ?", [r.id])
    for k, v in r.attributes.items():
        con.execute(
            "INSERT INTO gibran_role_attributes "
            "(role_id, attribute_key, attribute_value) VALUES (?, ?, ?)",
            [r.id, k, v],
        )


def _upsert_policy(con: duckdb.DuckDBPyConnection, p: PolicyConfig) -> None:
    row_filter_json = json.dumps(p.row_filter) if p.row_filter is not None else None
    valid_until = _normalize_valid_until(p.valid_until)
    existing = con.execute(
        "SELECT role_id, source_id, row_filter_ast, default_column_mode, valid_until "
        "FROM gibran_policies WHERE policy_id = ?",
        [p.id],
    ).fetchone()
    if existing is None:
        con.execute(
            "INSERT INTO gibran_policies "
            "(policy_id, role_id, source_id, row_filter_ast, default_column_mode, valid_until) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [p.id, p.role, p.source, row_filter_json, p.default_column_mode, valid_until],
        )
    else:
        new_values = (p.role, p.source, row_filter_json, p.default_column_mode, valid_until)
        if existing != new_values:
            con.execute(
                "UPDATE gibran_policies SET "
                "role_id = ?, source_id = ?, row_filter_ast = ?, default_column_mode = ?, "
                "valid_until = ? "
                "WHERE policy_id = ?",
                [*new_values, p.id],
            )
    con.execute("DELETE FROM gibran_policy_columns WHERE policy_id = ?", [p.id])
    for col, mode in p.column_overrides.items():
        con.execute(
            "INSERT INTO gibran_policy_columns (policy_id, column_name, granted) "
            "VALUES (?, ?, ?)",
            [p.id, col, mode == "allow"],
        )


def _normalize_valid_until(dt: datetime | None) -> datetime | None:
    # DuckDB's TIMESTAMP is naive (no tzinfo). If the YAML supplies a tz-aware
    # datetime, convert to UTC and strip the tz so the value round-trips back
    # equal on re-sync (otherwise the change-detection tuple would mismatch
    # every time and force an UPDATE).
    if dt is None or dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _replace_all_quality_rules(
    con: duckdb.DuckDBPyConnection, rules: list[QualityRuleConfig]
) -> None:
    con.execute("DELETE FROM gibran_quality_rules")
    for q in rules:
        con.execute(
            "INSERT INTO gibran_quality_rules "
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
    con.execute("DELETE FROM gibran_freshness_rules")
    for f in rules:
        con.execute(
            "INSERT INTO gibran_freshness_rules "
            "(rule_id, source_id, watermark_column, max_age_seconds, severity, "
            "staleness_seconds) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                f.id, f.source, f.watermark_column, f.max_age_seconds,
                f.severity, f.staleness_seconds,
            ],
        )
