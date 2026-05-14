"""DSL -> SQL compilation.

Given a validated intent and a Catalog (read-only DB access for metric
expressions and dimension columns), produce a SQL string. The output
flows through execution.run_sql_query, which re-parses, re-evaluates
through governance, ANDs in the policy row filter, executes, and writes
the audit log. This means the DSL compiler does NOT apply governance
itself -- separation of concerns.

Metric rendering uses SQL-standard `FILTER (WHERE ...)` clauses for
per-metric filters. This means:
  - Two metrics with different filters in the same query don't contaminate
    each other (each FILTER scopes only its own aggregate).
  - Groups with no matching rows show up as 0 (or NULL for non-SUM aggs),
    not as missing groups.

Ratio and expression metrics support {metric_id} template references in
their stored expression; the compiler recursively expands them. Cycles
are detected and rejected (defensive; the loader already catches them
for ratio metrics, but expression-metric dependencies aren't tracked
in gibran_metric_dependencies in V1)."""
from __future__ import annotations

import re
from dataclasses import dataclass

import duckdb

from gibran._source_dispatch import SourceDispatchError, from_clause_for_source
from gibran._sql import qident, render_literal
from gibran.dsl.types import DimensionRef, QueryIntent
from gibran.governance.ast import compile_intent_to_sql


class CompileError(ValueError):
    pass


# ---------------------------------------------------------------------------
# Compiled-query shape
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CTE:
    """A single common-table-expression in a compiled query.

    `depends_on` lists the names of OTHER CTEs this one references.
    The compiler is expected to provide CTEs in dependency-resolved
    order (parent before child) -- the renderer does not topologically
    re-sort. This keeps the contract obvious: the list IS the WITH
    clause's emission order.
    """
    name: str
    sql: str
    depends_on: tuple[str, ...] = ()


@dataclass(frozen=True)
class CompiledQuery:
    """Output of `compile_intent`.

    A compiled query has a (possibly empty) sequence of CTEs and a
    main SELECT that may reference those CTE names. For V1 primitives
    that fit in a single SELECT (count / sum / ratio / percentile /
    rolling_window / period_over_period / expression), `ctes` is empty
    and `main_sql` carries the whole query. New CTE-based primitives
    (cohort_retention, funnel) populate `ctes`.

    Use `.render()` to assemble the final `WITH ... SELECT ...` string
    suitable for handing to the execution layer.
    """
    ctes: tuple[CTE, ...]
    main_sql: str

    def render(self) -> str:
        """Assemble `WITH cte1 AS (...), cte2 AS (...) <main_sql>`.

        With no CTEs the result is just `main_sql` -- the single-SELECT
        primitives compile to the same SQL string they always did, so
        nothing in execution / governance / audit-log shape changes.
        """
        if not self.ctes:
            return self.main_sql
        cte_defs = ",\n".join(
            f"{cte.name} AS (\n  {_indent_body(cte.sql)}\n)"
            for cte in self.ctes
        )
        return f"WITH {cte_defs}\n{self.main_sql}"


def _indent_body(sql: str) -> str:
    """Indent each line by 2 spaces so the CTE body reads cleanly inside
    its parentheses. The first line is already indented by the caller."""
    return sql.replace("\n", "\n  ")


_TEMPLATE_REF_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")
_RATIO_TEMPLATE_RE = re.compile(r"^\{([a-zA-Z_][a-zA-Z0-9_]*)\}/\{([a-zA-Z_][a-zA-Z0-9_]*)\}$")


@dataclass(frozen=True)
class _MetricMeta:
    metric_id: str
    metric_type: str
    expression: str
    filter_sql: str | None
    metric_config: dict | None  # JSON blob from gibran_metric_versions; None for
                                # primitives that don't need extra config


@dataclass(frozen=True)
class _DimensionMeta:
    dimension_id: str
    column_name: str
    dim_type: str


class Catalog:
    """Read-only catalog access used by the DSL compiler."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def get_source_uri(self, source_id: str) -> str:
        row = self.con.execute(
            "SELECT uri FROM gibran_sources WHERE source_id = ?", [source_id],
        ).fetchone()
        if row is None:
            raise CompileError(f"unknown source: {source_id!r}")
        return row[0]

    def get_metric(self, metric_id: str) -> _MetricMeta:
        row = self.con.execute(
            "SELECT m.metric_type, mv.expression, mv.filter_sql, mv.metric_config "
            "FROM gibran_metrics m "
            "JOIN gibran_metric_versions mv "
            "  ON mv.metric_id = m.metric_id AND mv.effective_to IS NULL "
            "WHERE m.metric_id = ?",
            [metric_id],
        ).fetchone()
        if row is None:
            raise CompileError(f"unknown metric: {metric_id!r}")
        metric_config = None
        if row[3] is not None:
            import json as _json

            metric_config = _json.loads(row[3])
        return _MetricMeta(
            metric_id=metric_id, metric_type=row[0],
            expression=row[1], filter_sql=row[2],
            metric_config=metric_config,
        )

    def get_dimension(self, dimension_id: str) -> _DimensionMeta:
        row = self.con.execute(
            "SELECT column_name, dim_type FROM gibran_dimensions WHERE dimension_id = ?",
            [dimension_id],
        ).fetchone()
        if row is None:
            raise CompileError(f"unknown dimension: {dimension_id!r}")
        return _DimensionMeta(
            dimension_id=dimension_id, column_name=row[0], dim_type=row[1],
        )


_GRAIN_TO_TRUNC = {
    "year": "year", "quarter": "quarter", "month": "month",
    "week": "week", "day": "day", "hour": "hour",
}

_HAVING_BINOPS = {
    "eq": "=", "neq": "<>", "lt": "<", "lte": "<=", "gt": ">", "gte": ">=",
}


def compile_intent(intent: QueryIntent, catalog: Catalog) -> CompiledQuery:
    """Compile a validated intent to a CompiledQuery.

    For V1's single-SELECT primitives the returned CompiledQuery has
    empty `ctes` and `main_sql` holds the entire query. Future
    CTE-based primitives (cohort_retention, funnel) will populate
    `ctes`. Callers that need a flat SQL string call `.render()`.

    Pre-conditions (caller MUST have run):
      1. QueryIntent.model_validate succeeded (Pydantic structural check)
      2. dsl.validate.validate_intent passed (semantic check vs AllowedSchema)"""
    catalog.get_source_uri(intent.source)  # existence check; raises if unknown

    try:
        from_relation = from_clause_for_source(catalog.con, intent.source)
    except SourceDispatchError as e:
        raise CompileError(str(e)) from e

    metric_metas = [catalog.get_metric(m) for m in intent.metrics]
    dim_metas = [
        (dim, catalog.get_dimension(dim.id)) for dim in intent.dimensions
    ]

    select_parts: list[str] = []
    group_by_positions: list[int] = []
    for i, (dim, meta) in enumerate(dim_metas, start=1):
        select_parts.append(_render_dim_select(dim, meta))
        group_by_positions.append(i)
    for meta in metric_metas:
        select_parts.append(_render_metric_select(meta, catalog))

    # Filters from intent.filters go to WHERE -- they scope the universe.
    # Metric-level filters live INSIDE the aggregate via FILTER (WHERE ...),
    # so they don't appear here.
    where_parts: list[str] = [
        compile_intent_to_sql(filter_ast) for filter_ast in intent.filters
    ]

    having_parts = [_render_having(h) for h in intent.having]

    select_clause = "SELECT\n  " + ",\n  ".join(select_parts)
    # For relational sources (duckdb_table / sql_view) the dispatcher returns
    # a quoted identifier and we use it bare. For file-scan sources
    # (read_parquet / read_csv) we attach an alias matching intent.source so
    # the execution-layer parser still sees a "table name" (via alias_or_name)
    # and governance can locate the source.
    if from_relation.startswith(("read_parquet(", "read_csv(")):
        from_clause = f"FROM {from_relation} AS {qident(intent.source)}"
    else:
        from_clause = f"FROM {from_relation}"
    where_clause = (
        "\nWHERE " + "\n  AND ".join(where_parts) if where_parts else ""
    )
    group_by_clause = (
        "\nGROUP BY " + ", ".join(str(p) for p in group_by_positions)
        if group_by_positions else ""
    )
    having_clause = (
        "\nHAVING " + "\n  AND ".join(having_parts) if having_parts else ""
    )
    order_by_clause = (
        "\nORDER BY " + ", ".join(
            f"{qident(ob.key)} {ob.direction.upper()}" for ob in intent.order_by
        )
        if intent.order_by else ""
    )
    limit_clause = f"\nLIMIT {intent.limit}"

    main_sql = (
        select_clause
        + "\n" + from_clause
        + where_clause
        + group_by_clause
        + having_clause
        + order_by_clause
        + limit_clause
    )
    # V1 primitives are all single-SELECT shapes; no CTEs to emit.
    # cohort_retention / funnel (Tier 3) will populate the ctes tuple.
    return CompiledQuery(ctes=(), main_sql=main_sql)


# ---------------------------------------------------------------------------
# Metric expression rendering
# ---------------------------------------------------------------------------

def _render_metric_select(meta: _MetricMeta, catalog: Catalog) -> str:
    alias = qident(meta.metric_id)
    expr = _render_metric_expression(meta, catalog)
    return f"{expr} AS {alias}"


def _render_metric_expression(
    meta: _MetricMeta,
    catalog: Catalog,
    _seen: frozenset[str] = frozenset(),
) -> str:
    """Render a metric's full SQL expression with FILTER + template resolution.

    `_seen` carries the set of metric_ids on the recursion stack; if we
    encounter one already in `_seen`, we have a cycle and raise."""
    if meta.metric_id in _seen:
        raise CompileError(
            f"metric dependency cycle detected at {meta.metric_id!r} "
            f"(stack: {sorted(_seen)})"
        )
    seen = _seen | {meta.metric_id}

    # sync.applier._render_expression stores the FULL aggregate expression in
    # gibran_metric_versions.expression for count/sum/avg/min/max/percentile
    # (e.g. "SUM(amount)", "QUANTILE_CONT(amount, 0.95)"). Use it directly --
    # wrapping in another aggregate would double-aggregate.
    if meta.metric_type in (
        "count", "sum", "avg", "min", "max", "percentile"
    ):
        base = meta.expression
    elif meta.metric_type == "rolling_window":
        # rolling_window's stored expression already includes the FILTER (WHERE)
        # clause in the correct grammatical position (before OVER). Return as-is
        # -- DO NOT attach another FILTER below.
        return meta.expression
    elif meta.metric_type == "period_over_period":
        if meta.filter_sql:
            raise CompileError(
                f"period_over_period metric {meta.metric_id!r}: filter_sql "
                f"is not supported (apply filters on the base_metric instead)"
            )
        return _render_period_over_period(meta, catalog, seen)
    elif meta.metric_type == "ratio":
        if meta.filter_sql:
            raise CompileError(
                f"ratio metric {meta.metric_id!r}: filter_sql is not supported "
                f"on ratio metrics in V1 (filters go on the component metrics)"
            )
        return _render_ratio(meta, catalog, seen)
    elif meta.metric_type == "expression":
        if meta.filter_sql:
            raise CompileError(
                f"expression metric {meta.metric_id!r}: filter_sql is not "
                f"supported on expression metrics in V1 (embed filters in "
                f"the referenced metrics)"
            )
        return _resolve_template(meta.expression, catalog, seen)
    else:
        raise CompileError(f"unsupported metric_type: {meta.metric_type!r}")

    if meta.filter_sql:
        return f"{base} FILTER (WHERE {meta.filter_sql})"
    return base


_PERIOD_TO_TRUNC = {
    "year": "year", "quarter": "quarter", "month": "month",
    "week": "week", "day": "day",
}


def _render_period_over_period(
    meta: _MetricMeta, catalog: Catalog, seen: frozenset[str]
) -> str:
    """Render a period_over_period metric.

    Composes:
      - the base metric's fully-rendered SQL expression (parallel to ratio)
      - LAG() over DATE_TRUNC(period_unit, "period_dim_column")

    Emits one of (depending on comparison):
      delta:      (BASE) - LAG((BASE)) OVER (ORDER BY DATE_TRUNC('PERIOD', "col"))
      ratio:      (BASE) / NULLIF(LAG((BASE)) OVER (...), 0)
      pct_change: ((BASE) - LAG((BASE)) OVER (...)) / NULLIF(LAG((BASE)) OVER (...), 0)
    """
    cfg = meta.metric_config
    if not cfg:
        raise CompileError(
            f"period_over_period metric {meta.metric_id!r} is missing "
            f"metric_config (was the sync re-run after migration 0006?)"
        )

    base_id = cfg["base_metric"]
    period_dim_id = cfg["period_dim"]
    period_unit = cfg["period_unit"]
    comparison = cfg["comparison"]

    try:
        base_meta = catalog.get_metric(base_id)
    except CompileError as e:
        raise CompileError(
            f"period_over_period metric {meta.metric_id!r}: "
            f"base_metric {base_id!r} not found ({e})"
        ) from e
    try:
        period_dim_meta = catalog.get_dimension(period_dim_id)
    except CompileError as e:
        raise CompileError(
            f"period_over_period metric {meta.metric_id!r}: "
            f"period_dim {period_dim_id!r} not found ({e})"
        ) from e

    base_sql = _render_metric_expression(base_meta, catalog, seen)
    trunc = _PERIOD_TO_TRUNC[period_unit]
    period_col = qident(period_dim_meta.column_name)
    over_clause = f"OVER (ORDER BY DATE_TRUNC('{trunc}', {period_col}))"
    base_expr = f"({base_sql})"
    lag_expr = f"LAG({base_expr}) {over_clause}"

    if comparison == "delta":
        return f"{base_expr} - {lag_expr}"
    if comparison == "ratio":
        return f"{base_expr} / NULLIF({lag_expr}, 0)"
    if comparison == "pct_change":
        return f"({base_expr} - {lag_expr}) / NULLIF({lag_expr}, 0)"
    raise CompileError(
        f"period_over_period metric {meta.metric_id!r}: "
        f"unknown comparison {comparison!r}"
    )


def _render_ratio(
    meta: _MetricMeta, catalog: Catalog, seen: frozenset[str]
) -> str:
    """Render a ratio metric as `(num) / NULLIF((denom), 0)`.

    The NULLIF guards against division-by-zero -- DuckDB raises on int/0
    but returns NULL via NULLIF. Analytics queries prefer NULL to crash."""
    match = _RATIO_TEMPLATE_RE.match(meta.expression)
    if match is None:
        raise CompileError(
            f"ratio metric {meta.metric_id!r}: expression "
            f"{meta.expression!r} not in {{a}}/{{b}} form"
        )
    num_id, denom_id = match.group(1), match.group(2)
    num_sql = _render_metric_expression(catalog.get_metric(num_id), catalog, seen)
    denom_sql = _render_metric_expression(catalog.get_metric(denom_id), catalog, seen)
    return f"({num_sql}) / NULLIF(({denom_sql}), 0)"


def _resolve_template(
    template: str, catalog: Catalog, seen: frozenset[str]
) -> str:
    """Substitute {metric_id} placeholders in an expression metric's template
    with the referenced metric's fully-rendered SQL (recursively)."""
    def sub(match: re.Match[str]) -> str:
        dep_id = match.group(1)
        try:
            dep_meta = catalog.get_metric(dep_id)
        except CompileError:
            raise CompileError(
                f"expression metric template references unknown metric: {dep_id!r}"
            )
        return f"({_render_metric_expression(dep_meta, catalog, seen)})"

    return _TEMPLATE_REF_RE.sub(sub, template)


# ---------------------------------------------------------------------------
# Dimension rendering
# ---------------------------------------------------------------------------

def _render_dim_select(dim: DimensionRef, meta: _DimensionMeta) -> str:
    col = qident(meta.column_name)
    alias = qident(dim.id)
    if dim.grain is None:
        return f"{col} AS {alias}"
    trunc = _GRAIN_TO_TRUNC[dim.grain]
    return f"DATE_TRUNC('{trunc}', {col}) AS {alias}"


# ---------------------------------------------------------------------------
# HAVING rendering
# ---------------------------------------------------------------------------

def _render_having(h) -> str:
    """Render a HavingClause to SQL using the metric's SELECT alias.

    DuckDB supports referring to SELECT aliases in HAVING (extension to
    standard SQL, which requires repeating the expression). Since DuckDB
    is the V1 backend, the alias form is concise and unambiguous."""
    alias = qident(h.metric)
    if h.op in _HAVING_BINOPS:
        return f"({alias} {_HAVING_BINOPS[h.op]} {render_literal(h.value)})"
    if h.op == "in":
        rendered = ", ".join(render_literal(v) for v in h.value)
        return f"({alias} IN ({rendered}))"
    if h.op == "not_in":
        rendered = ", ".join(render_literal(v) for v in h.value)
        return f"({alias} NOT IN ({rendered}))"
    raise CompileError(f"unhandled having op: {h.op!r}")  # pragma: no cover
