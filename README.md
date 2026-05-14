# Gibran

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

Requires Python 3.11+.

## Quickstart

```
gibran init --sample
gibran sync
gibran check
gibran query --role analyst_west --attr region=west \
  --dsl '{"source":"orders","metrics":["order_count","gross_revenue"]}'
```

The `analyst_west` role's policy auto-injects `WHERE region = 'west'`;
the query returns only west-region rows. A different role sees a
different view of the same underlying data, with a different audit row
recorded for each attempt.

## What you can do with it

| Capability | How |
|---|---|
| Declare metrics declaratively | `metrics:` block in `gibran.yaml`; **23 primitives** (`count` / `sum` / `avg` / `min` / `max` / `ratio` / `expression` / `percentile` / `rolling_window` / `period_over_period` / `cohort_retention` / `funnel` / `multi_stage_filter` / `weighted_avg` / `stddev_samp` / `stddev_pop` / `count_distinct` / `count_distinct_approx` / `mode` / `variance` / `first_value` / `last_value` / `median`). |
| **Ask in plain English (NEW)** | `gibran ask "show me revenue by region"`. Pattern-template NL layer; **no LLM, no hallucination** — slot resolution requires real metric/dim names on the role's AllowedSchema. Returns "I don't know how to answer that" rather than invent. |
| **Cohort retention + funnels** | Declare `type: cohort_retention` or `type: funnel`; the engine emits multi-CTE queries (cohort assignment → period join → aggregate; or one CTE per funnel step with `LAG()` conversion ratios). |
| **Multi-stage filtering** | `type: multi_stage_filter` — "of the top decile by 90-day spend, what's their churn rate?" as a single declarative metric. |
| Compose metrics | `ratio` references two metrics; `expression` templates with `{metric_id}`; cycle detection at sync time via a dependency DAG. |
| Govern access by role | YAML-declared policies with row-filter ASTs (operator whitelist) + column allow/deny + identity-aware compilation. |
| **Time-bound + break-glass + rate-limited** | `valid_until` for contractor grants; `is_break_glass: true` for elevated-access roles (mirrored onto every audit row); optional per-process token-bucket rate limiter. |
| **Hide PII, with audit-log redaction** | `sensitivity: pii` / `restricted`; literal values are redacted in `gibran_query_log.generated_sql` AND `nl_prompt` before persistence. The audit log itself cannot become a side channel. |
| Audit every query | `gibran_query_log` records every allow/deny/error attempt with rewritten SQL, deny reason, identity, duration, break-glass flag. |
| **Detect data-quality issues — incl. anomalies** | 5 rule types (`not_null` / `unique` / `range` / `custom_sql` / **`anomaly`** — N-sigma vs trailing window) + freshness rules. Block-severity failures fire `alert_webhook`. |
| **Access-pattern anomaly detection** | `gibran detect-access-anomalies` flags users whose query volume today is > N sigma above their trailing baseline. |
| **Approval workflow** | High-sensitivity changes can be queued for out-of-band review (`gibran approve <id> --by <name>`). |
| **Schema-drift detection** | `gibran sync` probes each source's actual schema; warns on `missing_in_db` / `missing_in_yaml` / `type_mismatch`. |
| **In-process scheduler** | `gibran check --watch --interval N` for local-dev / small-deployment scheduling. |
| Read from anywhere DuckDB can | Parquet, CSV, DuckDB table, or SQL view — a source-type dispatcher resolves the FROM clause. |
| **Plan + result caching** | Catalog-generation token invalidates on `gibran sync`; source-health generation invalidates on `gibran check`. Audit-log row still written on cache hits. |
| **Materialized metrics** | `materialized: [dim_id, ...]` on a metric → `gibran sync` creates a pre-aggregated table; compile routes matching intents. |
| Introspect what's available | `gibran describe <source>`, `gibran catalog`, `gibran explain --dsl '...'`. |
| Export results | `gibran query --output csv|json|parquet [path]`. |

## What's proven (456 tests)

Every test runs in-process against an in-memory DuckDB; the whole suite
completes in under a minute.

| Test file | Count | What it covers |
|---|---:|---|
| `test_dsl.py` | 54 | DSL Pydantic validation, semantic validation against AllowedSchema, compiler SQL emission shape for every primitive, end-to-end execution. |
| `test_sync.py` | 34 | YAML loader/applier round-trip, idempotency, cross-entity validation, dependency-DAG cycle rejection, `valid_until` round-trip + resync stability. |
| `test_redaction.py` | 31 | Pure-function SQL + JSON redactors and end-to-end audit-row inspection (both `generated_sql` and `nl_prompt` redacted). |
| `test_governance.py` | 29 | `preview_schema` + `evaluate` across every `DenyReason`, role-attribute substitution, observability-aware denial ordering, time-bound expiry. |
| `test_execution_sql.py` | 25 | Parse → govern → rewrite → execute pipeline; unsupported-feature rejection (subqueries, SELECT *). |
| `test_ast_validation.py` | 20 | Filter-AST operator whitelist; rejects `like`, `regex`, function calls, attribute refs in DSL context. |
| `test_ast_compile.py` | 20 | Policy + intent AST → SQL emission; identity-attribute substitution; literal rendering. |
| `test_observability_runner.py` | 20 | Quality + freshness rule evaluation; severity routing; staleness windows. |
| `test_shape_primitives.py` | 20 | cohort_retention + funnel: Pydantic validation, applier persistence, compiler 3-CTE shape, end-to-end retention + funnel execution, governance walks CTE bodies. |
| `test_jwt_resolver.py` | 18 | RS256/HS256, expiry, audience, issuer, tampered-signature rejection. |
| `test_nl_patterns.py` | 18 | 6 NL patterns (top_n, by_grain, by_dim, count_of, filtered_by_value, single_metric) + Tier-5 no-invention safety + end-to-end run_nl_query. |
| `test_tier4_governance.py` | 17 | multi_stage_filter; anomaly rule; break-glass audit flag; webhook alerting; rate limiter; access-pattern anomaly; approval workflow; query timeout. |
| `test_period_over_period.py` | 17 | All three comparisons (`delta`/`ratio`/`pct_change`); validates against hand-computed expected output. |
| `test_cli_introspection.py` | 16 | `describe`, `catalog`, `explain` output shapes. |
| `test_observability.py` | 15 | Source-health cache reads; `record_run` semantics. |
| `test_drift.py` | 15 | Schema-drift detection: `missing_in_db` / `missing_in_yaml` / `type_mismatch` + unreachable-source handling + CLI integration. |
| `test_cte_infra.py` | 15 | CompiledQuery/CTE dataclasses; CTE-aware parser; column walk through CTE bodies; multi-source-via-CTE rejection. |
| `test_aggregate_primitives.py` | 15 | weighted_avg / stddev_samp / stddev_pop / count_distinct / count_distinct_approx / mode: validation + applier + end-to-end. |
| `test_perf_caches.py` | 12 | PlanCache / ResultCache hit-miss-eviction + catalog/health generation invalidation + materialized-metric routing. |
| `test_migrations.py` | 11 | All 9 migrations apply clean + idempotent; pinned schema invariants per migration. |
| `test_source_dispatch.py` | 9 | Parquet / CSV / DuckDB table / SQL view dispatcher; FROM-clause shape per type. |
| `test_ast_intent.py` | 8 | Intent-AST trust boundary — rejects `{"$attr":...}` substitution in DSL context. |
| `test_example_values.py` | 7 | Low-cardinality sampling + sensitivity gate + opt-out + CLI integration. |
| `test_init_sample.py` | 5 | `gibran init --sample` round-trip with a synthetic project. |
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

## Primitive reference: declarative YAML → compiled SQL

Each metric primitive in `gibran.yaml` compiles to a specific DuckDB
SQL shape. The user surface stays declarative; the SQL underneath is
non-trivial — window functions, recursive composition, governance-
injected WHERE clauses. This section pairs each primitive's YAML
declaration with the SQL it generates.

### `rolling_window` — sliding-window aggregate

```yaml
metrics:
  - id: revenue_7d_rolling
    type: rolling_window
    aggregate: sum                  # sum | avg | min | max | count
    column: amount
    window: "7 days"                # DuckDB INTERVAL
    order_by_column: order_date
    filter: "status = 'paid'"       # optional FILTER (WHERE …)
    partition_by: [region]          # optional PARTITION BY
```

Compiles to:

```sql
SUM(amount) FILTER (WHERE status = 'paid')
  OVER (PARTITION BY region
        ORDER BY order_date
        RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW)
```

Per-row trailing-window aggregate. The result has one row per input
row, not one per group. V1 constraint: cannot be combined with
intent-level `dimensions` — the window is row-level, not group-level.

### `period_over_period` — month-over-month / quarter-over-quarter / YoY

```yaml
metrics:
  - id: revenue_mom
    type: period_over_period
    base_metric: gross_revenue       # composes an existing metric
    period_dim: orders.order_date
    period_unit: month               # year | quarter | month | week | day
    comparison: delta                # delta | ratio | pct_change
```

Compiles to (for `comparison: delta`):

```sql
(SUM(amount) FILTER (WHERE status = 'paid'))
- LAG((SUM(amount) FILTER (WHERE status = 'paid')))
    OVER (ORDER BY DATE_TRUNC('month', order_date))
```

For `comparison: ratio` the divisor is wrapped in `NULLIF(LAG(...) OVER (...), 0)`.
For `comparison: pct_change` the whole shape is `(BASE - LAG(BASE)) / NULLIF(LAG(BASE), 0)`.

Change-over-time analytics. Requires the intent's `dimensions` to
include the `period_dim` at matching grain — the DSL validator enforces
this at compile time.

### `percentile` — quantile-as-aggregate

```yaml
metrics:
  - id: p95_amount
    type: percentile
    column: amount
    p: 0.95                          # 0 < p < 1
```

Compiles to:

```sql
QUANTILE_CONT(amount, 0.95)
```

Standard aggregate; composes inside `GROUP BY` just like `SUM` / `AVG`.

### `ratio` — composes two metrics

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
    numerator: gross_revenue         # references other metrics by id
    denominator: order_count
```

Compiles to:

```sql
(SUM(amount) FILTER (WHERE status = 'paid')) / NULLIF(COUNT(*), 0)
```

Any rate / per-X metric. The compiler resolves the numerator and
denominator expressions recursively; cycle detection prevents
`metric_a / metric_b / metric_a` loops at sync time.

### `expression` — templated metric reference

```yaml
metrics:
  - id: revenue_per_paid_order
    type: expression
    expression: "{gross_revenue} / NULLIF({order_count}, 0)"
```

Compiles to:

```sql
(SUM(amount) FILTER (WHERE status = 'paid')) / NULLIF(COUNT(*), 0)
```

Ad-hoc compositions that don't fit the `ratio` shape (e.g.
`{a} - {b}`, `{a} * 100`, multi-term arithmetic). `{metric_id}`
placeholders are resolved recursively with the same cycle detection as
`ratio`.

### Identity-aware row filtering — policy AST → injected WHERE clause

```yaml
roles:
  - id: analyst_west
    attributes:
      region: west

policies:
  - id: analyst_west_orders
    role: analyst_west
    source: orders
    row_filter:                      # AST — operator whitelist enforced
      op: eq
      column: region
      value: { $attr: region }       # resolved from identity at evaluate time
```

Given a query `SELECT amount FROM orders` from `analyst_west`, the
governance layer compiles `row_filter` with the identity's attributes
and rewrites the query via sqlglot:

```sql
SELECT amount FROM orders WHERE ("region" = 'west')
```

Same query from `analyst_east` (with `attributes: { region: east }`)
would inject `WHERE region = 'east'` instead — same SQL the user
wrote, two different row sets, no application-level branching. The
`{ $attr: <key> }` substitution is policy-only; DSL filters reject it
by function signature in [src/gibran/governance/ast.py](src/gibran/governance/ast.py).

## Worked example: same query, two roles, two outcomes

```bash
# analyst_west: gets the filtered view
$ gibran query --role analyst_west --attr region=west \
    --dsl '{"source":"orders","metrics":["order_count"]}'
# rewritten SQL: SELECT COUNT(*) FROM "orders" WHERE ("region" = 'west')
# row count: 1

# external_partner: column access denied
$ gibran query --role external_partner \
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
$ gibran query --role contractor_analyst --sql "SELECT COUNT(*) FROM orders"
# 12340

# After: denied automatically; no re-sync required
$ gibran query --role contractor_analyst --sql "SELECT COUNT(*) FROM orders"
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
$ gibran query --role analyst_west --attr region=west \
    --dsl '{
      "source": "orders",
      "metrics": ["avg_order_value"],
      "dimensions": [{"id": "orders.order_date", "grain": "month"}]
    }'
```

The compiler resolves `avg_order_value` to
`(SUM(amount) FILTER (WHERE status='paid')) / NULLIF(COUNT(*), 0)`,
injects `WHERE region='west'`, groups by month, and runs. The
`gibran_query_log` row records `metric_versions=[("avg_order_value", 1),
("gross_revenue", 1), ("order_count", 1)]` for reproducibility — you
know exactly which metric definitions answered this question.

## Ask in plain English (NL layer, no LLM)

`gibran ask "<question>"` routes natural-language questions through a
fixed pattern matcher (6 templates) that resolves every metric /
dimension / column name against the role's `AllowedSchema`. **No LLM,
no hallucination** — if a slot can't resolve to a real reference, the
matcher returns "I don't know how to answer that" rather than invent.

### One-line questions, non-trivial SQL

```bash
# 1. Cohort retention from a single phrase
$ gibran ask "show me customer retention" --source orders --role admin
# -> matches single_metric, resolves to `customer_retention` (cohort_retention type)
# -> emits a 3-CTE query:
#    WITH cohorts AS (...),
#         retention AS (...),
#         cohort_sizes AS (...)
#    SELECT cohort_start, periods_since_cohort, retained_count,
#           cohort_size, retention_rate
#    FROM retention r JOIN cohort_sizes sc ON r.cohort_start = sc.cohort_start
#    GROUP BY ... ORDER BY ...

# 2. Funnel conversion
$ gibran ask "show me paid funnel" --source orders --role admin
# -> matches single_metric, resolves to `paid_funnel` (funnel type)
# -> emits one CTE per step + step_counts aggregator + LAG/FIRST_VALUE
#    conversion ratios

# 3. Month-over-month delta
$ gibran ask "show me revenue mom" --source orders --role admin
# -> matches single_metric, resolves to `revenue_mom` (period_over_period)
# -> emits LAG window function over DATE_TRUNC('month', order_date)

# 4. Time-grained aggregation
$ gibran ask "gross revenue by month" --source orders --role admin
# -> matches metric_by_grain
# -> emits DATE_TRUNC('month', order_date) + GROUP BY 1

# 5. Top-N with ordering
$ gibran ask "top 5 region by gross revenue" --source orders --role admin
# -> matches top_n_by_metric
# -> emits ORDER BY gross_revenue DESC LIMIT 5

# 6. Filter inferred from sampled example values
$ gibran ask "gross revenue for west" --source orders --role admin
# -> matches metric_filtered_by_value; "west" found in region.example_values
# -> emits WHERE region = 'west'
```

### Same question, two roles, two results — governance applies after NL

```bash
# analyst_west's policy auto-injects WHERE region = 'west'
$ gibran ask "show me gross revenue by region" --role analyst_west --attr region=west --source orders
# -> Returns one row (west only). Same NL input, same DSL intent, but
#    different injected WHERE clause depending on the role's policy.
```

### The "I don't know" case (the contract you can't get from LLMs)

```bash
$ gibran ask "why did revenue drop last week" --source orders --role admin
I don't know how to answer that.
(The NL layer matches a fixed set of patterns; rephrase or use
 `gibran query --dsl` directly.)
# (exit code 4)
```

This is a *good* failure. An LLM-based layer would happily produce
something — possibly correct, possibly subtly wrong. The pattern
matcher reports honestly when it doesn't recognize a shape, and
cannot fabricate a metric that doesn't exist.

### What patterns are wired

| Pattern | Example input | Routes to |
|---|---|---|
| `top_n_by_metric` | "top 5 region by gross revenue" | DSL with ORDER BY + LIMIT |
| `metric_by_grain` | "revenue by month" / "by quarter" / "by year" | DSL with grain on temporal dim |
| `metric_by_dim` | "revenue by region" | DSL with one dimension |
| `count_of_thing` | "count of orders" / "how many" / "total" | First `count` metric on the source |
| `metric_filtered_by_value` | "revenue for west" | Equality filter on the column whose `example_values` contains the literal |
| `single_metric` | "show me revenue" / "what's the p95 amount" | Bare metric selection |

Adding patterns is mechanical (decorator + builder). The architecture
supports ~30 cleanly per the architecture estimate.

## CLI reference

| Command | What it does |
|---|---|
| `gibran init [--sample]` | Apply migrations; with `--sample`, drop a starter `gibran.yaml` + seed data. |
| `gibran sync` | Validate `gibran.yaml` and write to the catalog + governance tables. |
| `gibran check [--source <id>]` | Run quality + freshness rules; refresh source-health cache. |
| `gibran query --role <r> [--attr k=v]... ` `"<sql>"` ` \| --dsl '{...}'` | Execute a governed query. `--output tsv\|csv\|json\|parquet [file]`. Structured exit codes: 0=ok, 1=failed-rule, 2=denied, 3=error. |
| `gibran explain --role <r> --dsl '{...}'` | Compile without executing; print SQL + applied governance. |
| `gibran describe <source> --role <r>` | Show AllowedSchema (columns / dimensions / metrics / row filter) for an identity. |
| `gibran catalog --role <r>` | List sources the identity can see, with column/dim/metric counts. |
| `gibran register` | Generate a sample JWT for local dev. |
| **`gibran ask "<question>" --source <s> --role <r>`** | Natural-language NL layer (no LLM). Pattern-template matching with slot resolution against AllowedSchema. Exit code 4 when no pattern matches — distinct from 2=denied, 3=error so scripts can branch on "didn't understand". |
| **`gibran approve <change_id> --by <name>`** | Apply a pending change from the approval queue. |
| **`gibran detect-access-anomalies`** | Scan `gibran_query_log` for users whose query volume today is > N sigma above their trailing baseline. |
| **`gibran check --watch --interval N`** | In-process scheduler: loops on N-second intervals. Local-dev / small-deployment shape only — production should use cron / systemd / k8s CronJob. |
| **`gibran touch <source_id>`** | Bump a source's data-version token so the result cache invalidates cached rows. Useful after writing to a `duckdb_table` source externally. For `parquet` / `csv` the cache picks up file mtime automatically — no touch needed. |

## Project layout

```
src/gibran/
  catalog/            # docstrings — schema is in migrations/
  governance/         # identity, policies, ast, evaluate, redaction, rate_limit
  observability/      # quality/freshness types + runner + access_anomaly
  dsl/                # QueryIntent, validate, compile, run, plan_cache
  execution/          # parse → govern → rewrite → execute, result_cache
  sync/               # YAML schema, loader, applier, migrations, drift,
                      # example_values, approval
  cli/                # typer entrypoint (incl. `gibran ask`)
  nl/                 # pattern-template NL layer (no LLM)
  _sql.py             # qident, render_literal
  _source_dispatch.py # source_type -> FROM-clause snippet
migrations/           # 0001 catalog -> 0009 tier4_governance
tests/                # 456 tests across 23 files (+ benchmarks/)
prompts/
  architect_layer.md  # refined architect prompt with fixed constraints
STATUS.md             # current per-layer state
```

## Run the suite

```
python -m pytest tests
```

## Intentionally out of scope (V1)

- **LLM in any emission path.** Any approach where the system can
  invent a metric or column name not in `AllowedSchema` is out.
  Pattern templates (shipped) and local-embedding retrieval (planned)
  are in-scope for the NL layer; constrained-LLM emission is not.
- **Cross-source metrics.** Composition is single-source in V1; the
  dependency DAG is structured so V2 can relax this without a migration.
- **Multi-process / server mode.** DuckDB is single-writer per file.
  Rate limiter is per-process accordingly; cross-process needs
  Redis-or-equivalent.
- **Multi-tenancy** — V2 architectural pass; `tenant_id` would need to
  propagate through every governance table.

See `ROADMAP.md` for the V0.1 phased roadmap.

## License

MIT — see [LICENSE](LICENSE).
