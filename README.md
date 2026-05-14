# Rumi

Governed analytics over DuckDB. YAML-defined sources, metrics, dimensions,
roles, and policies; every query — raw SQL or a typed JSON DSL — flows
through an identity-aware governance layer that rewrites and audits it.

Same product category as Cube, dbt MetricFlow, Malloy, LookML. NOT a new
database engine — storage and execution belong to DuckDB. The wedge is
*governed analytics*: declarative semantics + identity-aware enforcement
+ structured audit, in a single pip-installable artifact with no server.

## Install

```
pip install gibran
```

The PyPI distribution is named `gibran` because the `rumi` name on PyPI
is held by another project. The CLI command and Python import path are
both `rumi` — `pip install gibran` gives you `import rumi` and a `rumi`
console script. (Same pattern as `pip install pyyaml` → `import yaml`.)

Requires Python 3.11+.

## Quickstart

```
rumi init --sample
rumi sync
rumi check
rumi query --role analyst_west --attr region=west \
  --dsl '{"source":"orders","metrics":["order_count","gross_revenue"]}'
```

The `analyst_west` role's policy auto-injects `WHERE region = 'west'`;
the query returns only west-region rows. A different role sees a
different view of the same underlying data, with a different audit row
recorded for each attempt.

## What you can do with it

| Capability | How |
|---|---|
| Declare metrics declaratively | `metrics:` block in `rumi.yaml`; 10 primitives (`count` / `sum` / `avg` / `min` / `max` / `ratio` / `expression` / `percentile` / `rolling_window` / `period_over_period`). |
| Compose metrics | `ratio` references two metrics; `expression` templates with `{metric_id}`; cycle detection at sync time via a dependency DAG. |
| Govern access by role | YAML-declared policies with row-filter ASTs (operator whitelist) and column allow/deny. |
| Hide PII | `sensitivity: pii` / `restricted` on columns; default-deny per-policy; explicit grants required. |
| Bound access in time | `valid_until` on policies for contractors / consultants / temporary credentials. Evaluation denies past the timestamp without re-sync. |
| Audit every query | `rumi_query_log` table; for each allow/deny/error, the rewritten SQL + structured deny reason are recorded. Literals adjacent to sensitive columns are redacted before persistence. |
| Detect data-quality issues | `quality_rules` (`not_null` / `unique` / `range` / `custom_sql`) + `freshness_rules`. A failing `severity: block` rule denies subsequent queries until the data is healthy. |
| Read from anywhere DuckDB can | Parquet, CSV, DuckDB table, or SQL view — a source-type dispatcher resolves the FROM clause. No manual `CREATE VIEW` needed for file-backed sources. |
| Introspect what's available | `rumi describe <source>`, `rumi catalog`, `rumi explain --dsl '...'` (parse + validate + compile without executing). |
| Export results | `rumi query --output csv|json|parquet [path]`. |

## What's proven (334 tests)

Every test runs in-process against an in-memory DuckDB; the whole suite
completes in under a minute.

| Test file | Count | What it covers |
|---|---:|---|
| `test_dsl.py` | 54 | DSL Pydantic validation, semantic validation against AllowedSchema, compiler SQL emission shape for every primitive, end-to-end execution. |
| `test_sync.py` | 34 | YAML loader/applier round-trip, idempotency, cross-entity validation, dependency-DAG cycle rejection, `valid_until` round-trip + resync stability. |
| `test_redaction.py` | 31 | Pure-function SQL + JSON redactors (eq / in / between / like / nested and-or-not / public columns unaffected / unparseable input fail-open) and end-to-end audit-row inspection. |
| `test_governance.py` | 29 | `preview_schema` + `evaluate` across every `DenyReason`, role-attribute substitution, observability-aware denial ordering, time-bound expiry. |
| `test_execution_sql.py` | 25 | Parse → govern → rewrite → execute pipeline; unsupported-feature rejection (joins, subqueries, CTEs, SELECT *). |
| `test_ast_validation.py` | 20 | Filter-AST operator whitelist; rejects `like`, `regex`, function calls, attribute refs in DSL context. |
| `test_ast_compile.py` | 20 | Policy + intent AST → SQL emission; identity-attribute substitution; literal rendering. |
| `test_observability_runner.py` | 20 | Quality + freshness rule evaluation; severity routing; staleness windows. |
| `test_jwt_resolver.py` | 18 | RS256/HS256, expiry, audience, issuer, tampered-signature rejection. |
| `test_period_over_period.py` | 17 | All three comparisons (`delta`/`ratio`/`pct_change`); validates against hand-computed expected output. |
| `test_cli_introspection.py` | 16 | `describe`, `catalog`, `explain` output shapes. |
| `test_observability.py` | 15 | Source-health cache reads; `record_run` semantics. |
| `test_migrations.py` | 10 | All 7 migrations apply clean + idempotent; pinned schema invariants. |
| `test_source_dispatch.py` | 9 | Parquet / CSV / DuckDB table / SQL view dispatcher; FROM-clause shape per type. |
| `test_ast_intent.py` | 8 | Intent-AST trust boundary — rejects `{"$attr":...}` substitution in DSL context. |
| `test_init_sample.py` | 5 | `rumi init --sample` round-trip with a synthetic project. |
| `test_imports.py` | 3 | All modules importable; no circular imports. |

## YAML syntax

### Sources

```yaml
sources:
  - id: orders
    display_name: Orders
    type: parquet                     # parquet | csv | duckdb_table | sql_view
    uri: data/orders.parquet
    primary_grain: order_id
    columns:
      - name: order_id
        type: VARCHAR
        sensitivity: public           # public | internal | pii | restricted | unclassified
      - name: amount
        type: DECIMAL(18,2)
        sensitivity: public
      - name: customer_email
        type: VARCHAR
        sensitivity: pii              # opt-in: governance must explicitly grant access
    dimensions:
      - id: orders.region
        column: region
        display_name: Region
        type: categorical             # categorical | temporal | numeric_bin
      - id: orders.order_date
        column: order_date
        display_name: Order Date
        type: temporal
```

### Metrics

```yaml
metrics:
  - id: order_count
    source: orders
    display_name: Order Count
    type: count
  - id: gross_revenue
    source: orders
    display_name: Gross Revenue
    type: sum
    expression: amount
    filter: "status = 'paid'"
    unit: USD
  - id: avg_order_value
    source: orders
    display_name: Average Order Value
    type: ratio
    numerator: gross_revenue          # composes existing metrics
    denominator: order_count
  - id: p95_amount
    source: orders
    display_name: P95 Order Amount
    type: percentile
    column: amount
    p: 0.95
  - id: revenue_7d_rolling
    source: orders
    display_name: 7-Day Rolling Revenue
    type: rolling_window
    aggregate: sum
    column: amount
    window: "7 days"                  # DuckDB INTERVAL
    order_by_column: order_date
    filter: "status = 'paid'"
  - id: revenue_mom
    source: orders
    display_name: Revenue MoM
    type: period_over_period          # composes a base metric with LAG()
    base_metric: gross_revenue
    period_dim: orders.order_date
    period_unit: month                # year | quarter | month | week | day
    comparison: delta                 # delta | ratio | pct_change
```

### Roles + policies

```yaml
roles:
  - id: analyst_west
    display_name: West Region Analyst
    attributes:
      region: west                    # surfaces as {"$attr":"region"} in row filters

policies:
  - id: analyst_west_orders
    role: analyst_west
    source: orders
    default_column_mode: allow        # or deny; column_overrides flip per-column
    valid_until: "2027-01-01T00:00:00"  # optional; NULL = never expires
    row_filter:                       # AST — operator whitelist enforced
      op: eq
      column: region
      value: { $attr: region }        # resolved from identity at query time

  - id: external_partner_orders
    role: external_partner
    source: orders
    default_column_mode: deny         # nothing visible by default
    column_overrides:
      order_id: allow
      amount: allow
      order_date: allow               # customer_email stays denied
    row_filter:
      op: eq
      column: region
      value: west                     # plain literal (no $attr ref)
```

### Quality + freshness

```yaml
quality_rules:
  - id: orders_amount_not_null
    source: orders
    type: not_null
    config: { column: amount }
    severity: block                   # block | warn

  - id: orders_amount_range
    source: orders
    type: range
    config: { column: amount, min: 0, max: 1000000 }
    severity: warn

  - id: orders_status_in_allowlist
    source: orders
    type: custom_sql
    config:
      sql: "SELECT COUNT(*) FROM orders WHERE status NOT IN ('paid','pending','refunded')"
    severity: warn

freshness_rules:
  - id: orders_freshness_24h
    source: orders
    watermark_column: order_date
    max_age_seconds: 86400
    severity: block
```

## DSL syntax — queries

```json
{
  "source": "orders",
  "metrics": ["order_count", "gross_revenue"],
  "dimensions": [{"id": "orders.order_date", "grain": "month"}],
  "filters": [
    {"op": "gte", "column": "amount", "value": 10},
    {"op": "in", "column": "region", "value": ["west", "central"]}
  ],
  "having": [{"op": "gt", "metric": "gross_revenue", "value": 100}],
  "order_by": [{"key": "orders.order_date", "direction": "asc"}],
  "limit": 100
}
```

The DSL is the user surface. There is **no LLM in the emission path** —
by design. Classical NLP / pattern templates / embedding retrieval are
in-scope for a future NL layer; constrained-LLM emission is permanently
out of scope because it can hallucinate references not in the schema.

### Filter AST operators

`and / or / not / eq / neq / lt / lte / gt / gte / in / not_in / is_null / is_not_null / between`

Notably absent: `like`, `regex`, function calls. A future "approved
functions" registry can extend, but the whitelist closes a class of
SQL-injection-via-policy-author bugs by construction.

## Worked example: same query, two roles, two outcomes

```bash
# analyst_west: gets the filtered view
$ rumi query --role analyst_west --attr region=west \
    --dsl '{"source":"orders","metrics":["order_count"]}'
# rewritten SQL: SELECT COUNT(*) FROM "orders" WHERE ("region" = 'west')
# row count: 1

# external_partner: column access denied
$ rumi query --role external_partner \
    --sql "SELECT customer_email FROM orders"
# status=denied, deny_reason=policy:no_column_access:customer_email
# (exit code 2)
```

Both attempts write an audit row. The denied query's `generated_sql`
column has the literal `customer_email` value in the SQL replaced with
`<redacted>` — the audit log itself cannot become a side channel for
the data it protects.

## Worked example: time-bound contractor access

```yaml
policies:
  - id: contractor_analytics
    role: contractor_analyst
    source: orders
    default_column_mode: allow
    valid_until: "2026-12-31T23:59:59"
```

```bash
# Before 2027-01-01: allowed
$ rumi query --role contractor_analyst --sql "SELECT COUNT(*) FROM orders"
# 12340

# After: denied automatically; no re-sync required
$ rumi query --role contractor_analyst --sql "SELECT COUNT(*) FROM orders"
# status=denied, deny_reason=policy:expired:valid_until=2026-12-31T23:59:59
```

The expiry comparison happens inside DuckDB (`CURRENT_TIMESTAMP`) at
each query, not in Python — eliminating a class of UTC-drift bugs.

## Worked example: composed metrics with audit trail

```yaml
metrics:
  - id: order_count
    type: count
  - id: gross_revenue
    type: sum
    expression: amount
    filter: "status = 'paid'"
  - id: avg_order_value
    type: ratio
    numerator: gross_revenue
    denominator: order_count
```

```bash
$ rumi query --role analyst_west --attr region=west \
    --dsl '{
      "source": "orders",
      "metrics": ["avg_order_value"],
      "dimensions": [{"id": "orders.order_date", "grain": "month"}]
    }'
```

The compiler resolves `avg_order_value` to
`(SUM(amount) FILTER (WHERE status='paid')) / NULLIF(COUNT(*), 0)`,
injects `WHERE region='west'`, groups by month, and runs. The
`rumi_query_log` row records `metric_versions=[("avg_order_value", 1),
("gross_revenue", 1), ("order_count", 1)]` for reproducibility — you
know exactly which metric definitions answered this question.

## CLI reference

| Command | What it does |
|---|---|
| `rumi init [--sample]` | Apply migrations; with `--sample`, drop a starter `rumi.yaml` + seed data. |
| `rumi sync` | Validate `rumi.yaml` and write to the catalog + governance tables. |
| `rumi check [--source <id>]` | Run quality + freshness rules; refresh source-health cache. |
| `rumi query --role <r> [--attr k=v]... ` `"<sql>"` ` \| --dsl '{...}'` | Execute a governed query. `--output tsv\|csv\|json\|parquet [file]`. Structured exit codes: 0=ok, 1=failed-rule, 2=denied, 3=error. |
| `rumi explain --role <r> --dsl '{...}'` | Compile without executing; print SQL + applied governance. |
| `rumi describe <source> --role <r>` | Show AllowedSchema (columns / dimensions / metrics / row filter) for an identity. |
| `rumi catalog --role <r>` | List sources the identity can see, with column/dim/metric counts. |
| `rumi register` | Generate a sample JWT for local dev. |

## Project layout

```
src/rumi/
  catalog/            # docstrings — schema is in migrations/
  governance/         # identity, policies, ast, evaluate, redaction
  observability/      # quality/freshness types + runner
  dsl/                # QueryIntent, validate, compile, run
  execution/          # parse → govern → rewrite → execute
  sync/               # YAML schema, loader, applier, migration runner
  cli/                # typer entrypoint
  _sql.py             # qident, render_literal
  _source_dispatch.py # source_type -> FROM-clause snippet
migrations/           # 0001 catalog -> 0007 time_bound_policies
tests/                # 334 tests across 17 files
prompts/
  architect_layer.md  # refined architect prompt with fixed constraints
HANDOFF.md            # forward-looking priority list
STATUS.md             # current per-layer state
```

## Run the suite

```
python -m pytest tests
```

## Intentionally out of scope (V1)

- **LLM in any emission path.** Any approach where the system can
  invent a metric or column name not in `AllowedSchema` is out.
  Pattern templates and local-embedding retrieval are in-scope for a
  future NL layer; constrained-LLM emission is not.
- **Cross-source metrics.** Composition is single-source in V1; the
  dependency DAG is structured so V2 can relax this without a migration.
- **Multi-process / server mode.** DuckDB is single-writer per file.
- **Cohort + funnel + multi-stage CTE primitives.** Tier 3 work; needs
  the compiler to emit `WITH a AS (...), b AS (...) SELECT ...` shape.

See `HANDOFF.md` for the full prioritized roadmap.

## License

MIT — see [LICENSE](LICENSE).
