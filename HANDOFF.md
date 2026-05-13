# Rumi V1 Handoff

This document is the entry point for whoever picks up Rumi next — whether
that's you in a month, a teammate, or a future Claude session. It pairs
with `STATUS.md` (current-state snapshot) but is forward-looking: what to
build next, why, and in what order.

Commit checkpoint: `c77f911` — Initial commit: Rumi V1.

---

## 1. What Rumi is, in one paragraph

Rumi is a **governed semantic / metric layer** packaged as a Python
library + CLI, embedded over DuckDB. Same product category as Cube, dbt
MetricFlow, Malloy, LookML. NOT a new database engine — storage and
execution belong to DuckDB. The wedge is *governed analytics*: YAML config
defines sources, metrics, dimensions, roles, policies, and data-quality
rules; queries (either raw SQL or a structured DSL) flow through an
identity-aware governance layer that rewrites and audits every attempt.
246 automated tests pass; wheel installs cleanly into a fresh venv.

## 2. Current state, briefly

| Surface | Works today |
|---|---|
| `rumi init` / `rumi sync` / `rumi check` / `rumi query` (raw SQL + `--dsl`) | All four commands wired end-to-end, exit codes structured for CI. |
| Metric vocabulary | `count, sum, avg, min, max, ratio, expression, percentile, rolling_window` |
| DSL features | source / metrics / dimensions (with time grain) / filters (AST) / order_by / limit / having |
| Governance | JWT identity, row + column policies, AST validator + compiler, attribute substitution, structured deny reasons |
| Observability | Quality + freshness rules with cache-first reads (`rumi_source_health`), V1.5 SQL-aggregation fallback, `rumi check` runner |
| Audit | Every attempt — allow, deny, error — records a `rumi_query_log` row with deny_reason and (for DSL) the original intent JSON |
| Distribution | `python -m build --wheel` produces a ~44 KB pip-installable artifact |

See `STATUS.md` for the per-layer breakdown and the deferred-with-rationale list.

## 3. Known immediate next work (from prior planning)

### 3.1 `period_over_period` metric primitive — ~1 turn

**Goal**: support metrics like "month-over-month revenue change."

**What it needs**:
- New `MetricType` value + Pydantic config: `{base_metric, period_dim, period_unit, comparison}` where comparison ∈ `{delta, ratio, pct_change}`.
- The compiler resolves the base metric's expression (same pattern as ratio) and the period dimension's column, then emits a `LAG()` window over `DATE_TRUNC(period_unit, col)`.
- DSL validation: the intent's dimensions must include the period_dim (with matching grain) — the window function references the grouping expression.

**Why it fits in one turn**: the compiler primitive composes one existing metric and one existing dimension; no new compiler shape needed (still single `SELECT ... FROM ... GROUP BY`).

**Reference**: the relevant rendering goes in `src/rumi/dsl/compile.py:_render_metric_expression`, parallel to the existing `_render_ratio` helper.

### 3.2 CTE compiler infrastructure — ~1–2 turns, **prerequisite for cohort + funnel**

**Goal**: let the DSL compiler emit `WITH a AS (...), b AS (...) SELECT ...` shape, not just a single `SELECT`.

**Why it matters**: cohort retention and funnel analytics inherently require multi-stage SQL — cohort assignment is one CTE, retention join is another, aggregate is a third. Trying to inline these as a single-SELECT is unreadable and error-prone.

**Implementation shape**:
- A new `CTE` dataclass: `{name, sql, depends_on: list[str]}`.
- `compile_intent(intent, catalog)` returns `CompiledQuery(ctes: list[CTE], main_sql: str)` instead of a bare string.
- Final SQL = `WITH cte_1 AS (...), cte_2 AS (...) SELECT ...`.
- The execution layer needs to be aware: governance.evaluate runs against the *final* SELECT, but column extraction now needs to look through CTE definitions for source-column references.

**The harder part**: governance column-check. If a CTE selects `customer_email` internally and the main SELECT uses only `cohort_size`, sqlglot's `find_all(exp.Column)` walks the entire tree — including CTEs — and flags `customer_email` for governance. That's actually correct: even reading a column "internally" within a CTE is data access that policy must allow. The CTE infrastructure surfaces this naturally; we don't need to change the governance logic.

**After this lands**: `cohort_retention` and `funnel` become tractable as standard primitives. Each is ~150 lines on top of the CTE infrastructure.

---

## 4. Additional functionality opportunities — Stack Role Group

Asked each role to nominate one or two pieces of functionality that
would extend what Rumi can already do, scoped to what's plausibly
useful in the next few iterations.

### Database Systems Architect

- **Compiled-DSL plan cache.** Right now every `rumi query --dsl '{...}'`
  invocation re-parses Pydantic, re-validates against AllowedSchema,
  re-compiles to SQL, and re-feeds to sqlglot for governance column
  extraction. For a dashboard firing the same intent every 30 seconds,
  that's 100% redundant work. A cache keyed by `hash(intent_json, role_id,
  policy_schema_version)` → compiled SQL string would cut the hot path
  meaningfully. ~80 lines + tests.
- **Source-type dispatch in the runner.** `rumi check` currently assumes
  `source_id` is the DuckDB relation name. For real Parquet/CSV sources,
  users have to manually `CREATE VIEW orders AS SELECT * FROM 'path.parquet'`.
  A dispatcher in `observability/runner.py` that reads `rumi_sources.source_type`
  and constructs the right FROM clause (e.g., `read_parquet('uri')`) would
  remove the manual step. Real adoption blocker. ~50 lines.

### Analytics Engineer

- **Discrete aggregate primitives**: `weighted_avg`, `stddev_samp`, `stddev_pop`,
  `count_distinct`, `count_distinct_approx`. All compile as a single
  function call (`<FUNC>(args)`) — same shape as the existing `sum/avg/min/max`.
  ~30 lines of code for the lot. Materially expands what analysts can
  express without writing `expression`-type metrics.
- **Reusable filter sets.** Right now `status = 'paid'` appears in
  `gross_revenue.filter`, would also appear in `avg_paid_order_value.filter`,
  and so on. A `filter_sets` YAML section that defines `{name, sql}` (or
  `{name, ast}`) and lets metrics reference by name would deduplicate.

### Data Quality Engineer

- **Anomaly detection rules.** A new quality `rule_type='anomaly'` that
  flags a metric value > N standard deviations from its trailing
  K-period mean. Reuses metric_versions + query_log infrastructure
  (the rule executes its own DSL query, compares to historical results,
  records pass/fail). Lands as a 6th `quality_rules.rule_type`.
- **Schema-drift detection in `rumi sync`.** When sync notices that a
  source's actual DuckDB schema diverges from `rumi_columns` (e.g., a
  column was dropped, a type changed, a new column appeared), surface
  a warning *before* applying YAML. Currently sync trusts the YAML.

### Security / Governance Architect

- **Time-bound policies.** Add `valid_until TIMESTAMP` to `rumi_policies`;
  `governance.evaluate` checks this before allowing. Critical for
  contractor / consultant / time-limited access patterns. ~20 lines of
  governance + a migration.
- **Audit-log redaction of SQL literals.** `rumi_query_log.generated_sql`
  currently includes literal values from policy row filters
  (e.g., `region = 'west'`). For high-sensitivity attributes
  (e.g., a literal email or SSN), this *re-leaks* the value via the audit
  log itself. A redactor that rewrites literals for `sensitivity=pii`
  columns before persisting would prevent the side channel.

### Performance Engineer

- **DSL result caching.** Same intent + same identity + same source-health
  generation → return cached rows without re-executing. TTL or invalidation
  on `rumi check` of any source the intent touches. Reduces dashboard
  load on the underlying DuckDB queries.
- **Materialized metrics.** A `materialized: true` flag on a metric
  definition; `rumi sync` creates a DuckDB materialized view that
  pre-aggregates by the metric's most common dimensions. Queries against
  the metric route to the view. Big speedup for frequently-asked metrics.

### Product Manager

- **`rumi describe <source>` and `rumi catalog`.** Let users (especially
  new ones) see what they have access to without writing a query. Output:
  source(s) the role can see, columns (with sensitivity), dimensions,
  metrics, the row filter that gets applied to them. This is the
  *exploration* step in any analytics workflow; without it, new users
  are flying blind. Probably the highest user-visible-value item on the
  list. ~150 lines + tests.
- **Result export formats.** `rumi query --output csv` / `--output json` /
  `--output parquet`. Right now output is tab-separated to stdout —
  fine for `grep`, useless for spreadsheets and notebooks. Every analyst
  pipes results somewhere; without export, Rumi feels like a toy.

### ML/NLP Engineer (mostly idle in V1, but worth a seat)

- **`rumi explain --dsl '{...}'`.** Given an intent, return what data it
  accesses, what governance applied, and what the compiled SQL is —
  *without* executing. Useful for sandbox / preview / impact analysis
  before commit. Lays groundwork for any future NL layer that might
  want to show "I'm about to run this — is that what you meant?" UX.

---

## 5. Verification loop — PM challenges the priority list

The PM read the panel and pushed back on three points.

**Challenge 1: "Half of this is gold-plating. 246 tests, working CLI,
no users. What's the SMALLEST change that materially expands real-world
coverage?"**

Fair. The DB-architect items (plan cache) and Perf items (result caching,
materialized metrics) are *optimizations* — they make existing functionality
faster, but they don't unlock new use cases. They belong AFTER we have
actual users with measured bottlenecks.

The user-visible gaps are:
- **Source-type dispatch** (DB Architect's #2) — without it, every new
  installer has to learn the manual `CREATE VIEW` workaround. Pure
  adoption friction.
- **`rumi describe`** (PM's #1) — without it, new users can't discover
  what's available without reading YAML. First-five-minutes UX.
- **Result export** (PM's #2) — without it, Rumi is a CLI demo, not an
  analyst tool.

These three should be the near-term tier.

**Challenge 2: "Adding 5 more aggregate types is *count* of features,
not *depth*. Would you rather have 12 metric primitives or 1 robust
dashboard story?"**

Depth. The dashboard story needs `rumi describe` + result export + maybe
a `rumi init --sample` that drops a runnable fixture. The 5 aggregate
primitives can land as a single small follow-up after the dashboard
story works.

**Challenge 3: "What's the right ordering of period_over_period vs CTE
infra vs cohort/funnel?"**

- `period_over_period` is small AND demos well (MoM, QoQ, YoY are the
  most-requested analytics in the entire space). Do this first.
- CTE infra is *investment* — no user-visible feature on its own, but
  unblocks the next two.
- Cohort + funnel land on the CTE infra. Each is ~1 turn after the infra.

---

## 6. Synthesized priority list (post-verification)

### Near-term — high user value per unit of work

1. **Source-type dispatch in the runner** (DB Architect's #2)
2. **`rumi describe` / `rumi catalog`** (PM's #1)
3. **Result export `--output csv|json|parquet`** (PM's #2)

### Mid-term — substantive feature pass

4. **`period_over_period` metric primitive** (already on the roadmap)
5. **`rumi explain --dsl '{...}'`** (ML/NLP's only proposal — small, useful)
6. **Discrete aggregate primitives** (`weighted_avg`, `stddev`, `count_distinct`)

### Architectural — investment, unblocks the next round

7. **CTE compiler infrastructure**
8. **`cohort_retention` metric primitive** (on top of CTE infra)
9. **`funnel` metric primitive** (on top of CTE infra)

### Strategic — each its own design pass

10. **Time-bound policies** + **audit-log redaction** (Sec/Gov)
11. **Anomaly detection rules** + **schema-drift detection** (DQ)
12. **DSL result caching + plan cache + materialized metrics** (Perf,
    once we have measured bottlenecks)
13. **Rate limiting** (architectural; needs deployment shape decided)
14. **Multi-process safety / server mode** (V2+; effectively a different
    product)

### Strategic — DEFERRED indefinitely per user decision

- **NL layer above the DSL** — no LLM, no classical NLP. The DSL is the
  user surface. If revisited later, the architect prompt at
  `prompts/architect_layer.md` already encodes the trust boundary.

---

## 7. Onboarding for the next contributor

### Repo layout

```
src/rumi/
  _sql.py            # shared SQL utilities (qident, render_literal)
  catalog/           # docstring only; metadata schema is in migrations/
  governance/
    types.py         # Protocols, dataclasses, DenyReason enum
    ast.py           # validate_policy_ast + compile_policy_to_sql (with identity)
                     # validate_intent_ast + compile_intent_to_sql (no identity)
    identity.py      # JWTResolver / EnvResolver / CLIResolver
    default.py       # DefaultGovernance
  observability/
    types.py         # BlockingFailure, ObservabilityAPI, staleness defaults
    default.py       # DefaultObservability (cache-first reads + refresh_health)
    runner.py        # run_checks + per-rule-type evaluators
  dsl/
    types.py         # QueryIntent, DimensionRef, OrderBy, HavingClause
    validate.py      # validate_intent against AllowedSchema
    compile.py       # compile_intent + Catalog helper
    run.py           # run_dsl_query orchestrator
  execution/
    sql.py           # run_sql_query, parser, alias-aware column extraction
  sync/
    yaml_schema.py   # Pydantic models for rumi.yaml
    loader.py        # cross-entity validation, DAG cycle detection
    applier.py       # idempotent catalog upsert
    migrations.py    # migration runner
  cli/
    main.py          # typer subcommands: init / sync / check / query / register
migrations/          # 0001 catalog -> 0005 metric_primitives
tests/
  fixtures/rumi.yaml # canonical test config with 6 metrics, 2 roles, etc.
  test_*.py          # 246 tests across 10 files
prompts/
  architect_layer.md # the refined architect prompt with FIXED CONSTRAINTS
STATUS.md            # per-layer state, deferred-with-rationale list
HANDOFF.md           # this file
```

### How to run things

| What | How |
|---|---|
| Run all tests | `python -m pytest tests` (from repo root; uses `conftest.py` to add src/ to path) |
| Build the wheel | `python -m build --wheel` → `dist/rumi-0.0.1-py3-none-any.whl` |
| Install editable | `pip install -e .` |
| Smoke-test from a clean venv | `python -m venv X; X/Scripts/pip install dist/*.whl; X/Scripts/rumi --help` |
| Run an end-to-end demo | See "Smoke transcripts" in `STATUS.md` |

### How to add a new metric primitive (cookbook)

1. Extend `MetricType` Literal in `src/rumi/sync/yaml_schema.py:MetricConfig`.
2. Add type-specific fields (e.g., `column`, `p`, `window`).
3. Add a per-type branch in `MetricConfig._check_shape` validating required fields.
4. Add the SQL rendering in `src/rumi/sync/applier.py:_render_expression`.
5. If the type's stored expression is self-contained: add it to the
   "use directly" branch in `src/rumi/dsl/compile.py:_render_metric_expression`.
6. If it composes other metrics: write a `_render_<type>` helper that
   recurses via `_render_metric_expression(..., seen)` for cycle detection.
7. If it has compatibility constraints (like rolling_window forbidding
   dimensions), add validation in `src/rumi/dsl/validate.py:validate_intent`.
8. Add to the fixture `tests/fixtures/rumi.yaml` and update affected
   metric-count assertions in `tests/test_sync.py`, `tests/test_governance.py`.
9. Add primitive-specific tests in `tests/test_dsl.py` (Pydantic validation,
   SQL emission shape, **executes correctly against DuckDB** — don't skip
   the execution test, it catches grammar bugs the string-shape tests
   miss).

### How to add a new CLI command (cookbook)

1. Add a `@app.command()` function in `src/rumi/cli/main.py`.
2. Use `typer.Option` / `typer.Argument` for args.
3. Inside, instantiate `DefaultObservability(con)` + `DefaultGovernance(con, observability=obs)`.
4. Structured exit codes: 0=ok, 1=denied/failed-rule, 2=denied-by-policy, 3=error.

### How to extend a migration

1. Create `migrations/NNNN_description.sql`. The runner picks them up
   lexically.
2. DuckDB *does not* support ALTER on CHECK constraints — if you need to
   widen a CHECK enum, recreate the table (see migration 0005 for the
   pattern). Or drop the CHECK and rely on Pydantic Literal.
3. DuckDB FK enforcement during UPDATE has quirks (see comments in
   migration 0001 above `rumi_metric_versions`). Inbound FKs to a table
   you'll later UPDATE on non-PK columns will block; remove the FK and
   document.
4. Add tests in `tests/test_migrations.py` that pin the new schema invariant.

### Working with Claude on this project

The Rumi memory files at
`C:\Users\adame\.claude\projects\C--Users-adame-Rumi\memory\` contain
fixed-constraint decisions and a preferred interaction pattern (panel +
verification loop). Future Claude sessions read these automatically.
Update them when locking in a new decision.

The refined architect prompt at `prompts/architect_layer.md` is the
canonical prompt for designing a new layer. Run it once per layer;
treat its FIXED CONSTRAINTS block as the source of truth for decisions
already made.

---

## 8. Open architectural decisions still on the table

These came up during V1 and were deliberately punted. They'll need
answers when the system grows past single-user single-process usage.

1. **Multi-process safety / server mode.** DuckDB is single-writer per
   file. Multi-process Rumi effectively means a server with its own DB
   handle. Decision-deferred until deployment shape is known.
2. **Where does the `rumi check` scheduler run?** Today it's manual or
   cron. A real product needs a scheduler (eventually). Could be a
   sidecar process, a systemd timer, or an in-Rumi scheduler thread.
3. **Rate limiting in a multi-process world.** Per-process token buckets
   are false security. Cross-process needs Redis-or-equivalent. Decision
   deferred.
4. **Audit log retention + archival.** `rumi_query_log` grows unbounded.
   Need a policy + a `rumi prune` command. Not urgent.
5. **Migration story for live deployments.** Migrations are append-only
   DDL today. Once anyone is in production, real data migrations
   (renames, sensitivity reclassification, type changes) need helpers.
6. **Source-type dispatch (Parquet/CSV).** Recommended as near-term work
   above. Decision: where in the codebase does the dispatcher live —
   inside `observability/runner.py` only, or as a shared helper that
   `dsl/compile.py` could also use to render the FROM clause?
7. **Compiled DSL output: bare SQL vs typed query plan?** Right now
   `compile_intent` returns a SQL string. A typed `CompiledQuery` object
   (carrying CTEs, parameter bindings, source references) is more
   structured and would help when CTE infrastructure lands. Worth
   considering during the CTE pass.

---

## 9. Verification loop on this handoff itself

To pressure-test the priority list, I asked PM-as-critic to read it
once more and flag what's missing or wrong. Two adjustments came back:

**"#3 result export is below `rumi describe` but feels equally critical.
Why is one near-term-1 and the other near-term-3?"**

Fair. They're peers. Both can be done in the same iteration (~1 turn each,
or together in a single turn since they share the introspection-output
plumbing). Treat them as a pair.

**"You said `rumi explain` is small + useful but listed it mid-term.
If it's small, why isn't it near-term?"**

Also fair. `rumi explain` is essentially "the runner without the execute
step." It's ~40 lines. Move it into near-term alongside `rumi describe`
and result export — they're all "introspection" features and ship well
together.

**Revised near-term tier**:

1. Source-type dispatch in `rumi check` (DB Architect's #2)
2. **Introspection bundle**: `rumi describe`, `rumi catalog`, `rumi explain --dsl`,
   `rumi query --output csv|json|parquet`
3. `period_over_period` metric primitive

After this bundle: CTE infrastructure → cohort → funnel → strategic items.

The strategic + DEFERRED items below #9 in §6 are unchanged.

---

*End of handoff. Update this file when the priority list shifts —
it's the document that lets the next contributor (or the next session)
start work without re-reading the entire repo.*
