# Rumi

Governed analytics over DuckDB. YAML-defined sources, metrics, dimensions,
roles, and policies; queries flow through an identity-aware governance
layer that rewrites and audits every attempt.

Same product category as Cube, dbt MetricFlow, Malloy, LookML. NOT a new
database engine -- storage and execution belong to DuckDB.

## What you get

- A **semantic layer**: declare metrics (sum / avg / ratio / percentile /
  rolling_window / period_over_period / expression) and dimensions in
  YAML. Reference them from a typed JSON **DSL** -- no hand-written SQL.
- **Governance**: role + policy bindings rewrite every query with a row
  filter; column-level allow/deny; PII opt-in via explicit policy.
- **Audit log**: every query attempt -- allow, deny, error -- is recorded
  with the rewritten SQL and the structured deny reason.
- **Observability**: declarative quality + freshness rules. A failing
  block-severity rule denies subsequent queries with a structured
  `quality:rule_failed` reason until the data is healthy again.
- **Pluggable sources**: parquet, csv, duckdb table, or sql view --
  registered by source_type so the runner builds the right FROM clause.

## Install + quickstart

```
pip install rumi
rumi init --sample      # creates rumi.duckdb + a starter rumi.yaml + sample data
rumi sync               # applies rumi.yaml -> catalog + governance tables
rumi check              # runs quality rules; refreshes the source-health cache
rumi query --role analyst_west --attr region=west \
  --dsl '{"source":"orders","metrics":["order_count","gross_revenue"]}'
```

That's it -- you now have a governed query path with audit logging. Reads
return only the `west` region (per policy); a different role sees a
different view.

## CLI surface

| Command | What it does |
|---|---|
| `rumi init [--sample]` | Apply migrations; with `--sample`, drop a starter config and seed data. |
| `rumi sync` | Validate `rumi.yaml` and write to the catalog / governance tables. |
| `rumi check [--source <id>]` | Run quality + freshness rules; refresh source-health cache. |
| `rumi query --role <r> [--attr k=v]... "<sql>" \| --dsl '{...}'` | Execute a governed query. Output: `--output tsv\|csv\|json\|parquet`. |
| `rumi explain --role <r> --dsl '{...}'` | Compile a DSL intent without executing; print SQL + governance decision. |
| `rumi describe <source> --role <r>` | Show the AllowedSchema (columns, dimensions, metrics, row filter) for an identity. |
| `rumi catalog --role <r>` | List sources the identity can see, with column/dimension/metric counts. |

## DSL shape

```json
{
  "source": "orders",
  "metrics": ["order_count", "gross_revenue"],
  "dimensions": [{"id": "orders.order_date", "grain": "month"}],
  "filters": [{"op": "gte", "column": "amount", "value": 10}],
  "having": [{"op": "gt", "metric": "gross_revenue", "value": 100}],
  "order_by": [{"key": "orders.order_date", "direction": "asc"}],
  "limit": 100
}
```

The DSL is the user surface. There is no LLM in the emission path -- by
design. (See `prompts/architect_layer.md` for the fixed constraints.)

## Metric primitives

`count, sum, avg, min, max` -- standard aggregates.

`ratio` -- composes two metrics: `(numerator) / NULLIF(denominator, 0)`.

`expression` -- a `{metric_id}` template; the compiler resolves
references recursively with cycle detection.

`percentile` -- `QUANTILE_CONT(column, p)`; composes with GROUP BY.

`rolling_window` -- `AGG(col) FILTER (WHERE ...) OVER (ORDER BY ts RANGE
INTERVAL '<window>' PRECEDING)`; per-row, no GROUP BY.

`period_over_period` -- composes a base metric with `LAG()` over
`DATE_TRUNC(period_unit, period_dim_col)`. Three comparisons:

| Comparison | SQL shape |
|---|---|
| `delta` | `(BASE) - LAG((BASE)) OVER (...)` |
| `ratio` | `(BASE) / NULLIF(LAG((BASE)) OVER (...), 0)` |
| `pct_change` | `((BASE) - LAG((BASE)) OVER (...)) / NULLIF(LAG(...) OVER (...), 0)` |

## Project layout

```
src/rumi/
  catalog/, semantic/, governance/, observability/, dsl/, execution/, sync/, cli/
  _sql.py              # shared SQL utilities (qident, render_literal)
  _source_dispatch.py  # source_type -> FROM-clause snippet
migrations/            # 0001 catalog -> 0006 metric_config
tests/                 # 289 tests across 14 files
prompts/
  architect_layer.md   # refined architect prompt + fixed constraints
HANDOFF.md             # forward-looking priority list
STATUS.md              # current per-layer state
```

## Run tests

```
python -m pytest tests
```

## License

TBD.
