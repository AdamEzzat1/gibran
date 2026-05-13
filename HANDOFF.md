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

Each role's full roadmap of "still has potential" items, drawn from the
grading panel. Items are sized roughly (S/M/L) and ordered by user-visible
value within each role.

### Database Systems Architect — current grade B

- **Source-type dispatch in the runner** (S). `rumi check` and the DSL
  compiler currently assume `source_id` is the DuckDB relation name. For
  real Parquet/CSV sources, users have to manually `CREATE VIEW orders
  AS SELECT * FROM 'path.parquet'`. A dispatcher in a shared helper that
  reads `rumi_sources.source_type` and constructs the right FROM clause
  (e.g., `read_parquet('uri')` for parquet) would remove the manual step.
  Real adoption blocker. ~50 lines.
- **Compiled-DSL plan cache** (M). Every `rumi query --dsl '{...}'` re-parses
  Pydantic, re-validates against AllowedSchema, re-compiles to SQL, re-feeds
  to sqlglot for governance column extraction. A cache keyed by
  `hash(intent_json, role_id, policy_schema_version)` → compiled SQL would
  cut the hot path. ~80 lines.
- **Query-plan introspection** (S). Expose DuckDB's EXPLAIN through
  `rumi query --explain` or `rumi explain --dsl '{...}'`. Closes a real
  visibility gap.

To move to A: ship source-type dispatch and introspection.

### Analytics Engineer — current grade B

- **`period_over_period` metric primitive** (M). MoM/QoQ/YoY are the most-asked
  analytics in this entire space. Composes a base metric + LAG over a
  time dimension. Spec in §3.1.
- **Discrete aggregate primitives** (S). `weighted_avg`, `stddev_samp`,
  `stddev_pop`, `count_distinct`, `count_distinct_approx`, `mode`, `top_k`.
  All compile as a single function call — same shape as existing
  `sum/avg/min/max`. One-line each in the applier. ~30 lines for the lot.
- **CTE compiler infrastructure** (M-L). Standalone refactor; prerequisite
  for cohort + funnel. Compiler learns to emit `WITH a AS (...), b AS (...)
  SELECT ...` shape. Spec in §3.2.
- **`cohort_retention` metric primitive** (M, after CTE infra). Flagship
  analytical pattern. CTE chain: cohort assignment → period join →
  aggregate.
- **`funnel` metric primitive** (M, after CTE infra). Sequential step
  matching. Same CTE infrastructure.
- **Dimension hierarchies** (M). Country > state > city is a standard
  analytical pattern. `parent_dimension: orders.country` on a dimension
  declaration would let the compiler emit drill-down queries.
- **Reusable filter sets** (S). Right now `status = 'paid'` appears
  verbatim in every paid-revenue metric. A `filter_sets` YAML section
  that defines `{name, ast}` and lets metrics reference by name would
  deduplicate.
- **Time grain at intent level** (S). Currently grain is only on dimensions.
  An intent-level `time_grain: month` would auto-apply to any temporal
  dimension in the projection.

To move to A: ship period_over_period, then CTE infrastructure, then
cohort + funnel. Add the missing aggregate primitives.

### Data Quality / Observability Engineer — current grade B+

- **Anomaly-detection rule type** (M). New `quality_rules.rule_type='anomaly'`:
  flag a metric value > N standard deviations from its trailing K-period
  mean. Reuses `rumi_query_log` history. Lands as a 6th rule_type.
  Table-stakes for modern data products.
- **`rumi check` scheduler** (M). Today it's manual or cron. An in-product
  scheduler — either a `rumi check --watch` mode or a separate `rumi
  scheduler` daemon — would close the loop without external infrastructure.
- **Alerting integration** (M). Webhooks / Slack / PagerDuty when a
  `severity='block'` rule fails or a source flips to status='block'.
  At minimum, a webhook POST with the BlockingFailure JSON.
- **Rule dependencies** (S). "Freshness must pass before quality runs"
  can't be expressed. A `depends_on: [rule_id, ...]` field on
  `rumi_quality_rules` + dependency-aware ordering in `run_checks`.
- **Schema-drift detection in `rumi sync`** (S, high-leverage). When sync
  notices that a source's actual DuckDB schema diverges from `rumi_columns`
  (column dropped, type changed, new column appeared), surface a warning
  before applying YAML.
- **Custom_sql rule template library** (S). Pre-baked templates for
  "uniqueness within group," "referential integrity," "value distribution
  matches expected." Users reference by name; sync expands.

To move to A: anomaly rule type + scheduler + at least one alerting
integration. The schema-drift detector is a small addition that punches
above its weight.

### Security / Governance Architect — current grade B+

- **Time-bound policies** (S). `valid_until TIMESTAMP` column on
  `rumi_policies`; `governance.evaluate` checks before allowing.
  Contractor / consultant / temporary access. ~20 lines + migration.
- **Audit-log SQL redaction** (S). `rumi_query_log.generated_sql` includes
  literal values from filters (e.g., `email = 'alice@x.com'`). For
  `sensitivity in (pii, restricted)` columns, the literal value
  re-leaks via the audit log itself. A redactor that rewrites literals
  before persisting closes the side channel.
- **Approval workflow** (M). High-sensitivity policy changes (e.g., any
  change touching a `restricted` column) require sign-off. Could be
  enforced via a `rumi sync --require-approval` mode that writes pending
  changes to a separate table awaiting approval.
- **Access-pattern anomaly detection** (M). "User X just queried 100x
  their typical daily volume" → alert. Reuses `rumi_query_log`.
- **Multi-tenancy primitives** (L). `tenant_id` column on every governance
  table; tenant-scoped queries; isolation guarantees. Real architectural
  pass — likely V2.
- **Break-glass / emergency-access pattern** (M). A special role with
  elevated access, every use of which produces a high-priority audit row
  + alert. Industry-standard for compliance-regulated deployments.

To move to A: time-bound policies + audit-log redaction. Both are small
additions with outsized security value.

### ML/NLP Engineer — current grade C+

**NL constraint update (2026-05-13)**: an NL layer is now in-scope IF
it's non-LLM. Classical NLP (pattern templates, slot-filling with
spaCy/regex) or embedding retrieval with local sentence-transformers
are acceptable. LLM-emission paths (even with constrained decoding)
remain out of scope because they can hallucinate AllowedSchema
references. Rule of thumb: any approach that can FAIL to parse but
CANNOT invent is in-scope.

- **`rumi explain --dsl '{...}'`** (S). Given an intent, return what
  data it accesses, what governance applied, and what the compiled SQL
  is — *without* executing. The smallest useful introspection. Useful
  for sandbox / preview / impact analysis. Also the obvious first
  building block for any NL layer to debug its emissions.
- **Pattern-template NL layer** (M). Hand-written regex/glob patterns
  for ~20–30 common question shapes ("show X by Y", "top K X by Y",
  "X over time"). Match → fill DSL slots → emit. Deterministic. Limited
  coverage but covers the most-asked questions.
- **Embedding-retrieval NL layer** (M-L). Local sentence-transformers
  embeds metric/dimension descriptions; user query gets embedded;
  nearest-neighbor matches drive DSL slot-filling. Higher coverage than
  patterns; still deterministic given fixed model + corpus.
- **Populate `example_values`** (S). `AllowedSchema.ColumnView.example_values`
  is wired through the contract but never populated (deferred). For
  low-cardinality public columns, the applier should sample distinct
  values and store them on `rumi_columns`. Required for NL prompt
  construction and useful for `rumi describe`.
- **Schema search via embeddings** (M). "Find metrics related to 'revenue'"
  → embedding search over metric descriptions. Same local-model
  infrastructure as the NL layer.

To move to A: ship `rumi explain` (V1-week work), then choose one of
the NL approaches (pattern or embedding) and build it.

### Performance Engineer — current grade C+

- **Benchmarks** (S). `tests/benchmarks/` directory with at least three
  representative measurements (small / medium / large source). Until
  these exist, all perf engineering is fiction.
- **DSL result caching** (M). Same intent + same identity + same
  source-health generation → cached rows. TTL or invalidation on
  `rumi check`. Reduces dashboard load.
- **Compiled-DSL plan cache** (M, also under DB Architect). Avoid
  recompilation on repeated intents.
- **Materialized metrics** (L). `materialized: true` flag → `rumi sync`
  creates a DuckDB materialized view pre-aggregating by common dimensions.
- **Query timeout enforcement** (S). Per-query deadline on the executor.

To move to A: benchmarks first, then plan + result caching.

### Product Manager — current grade B-

- **Introspection bundle** (M, high user-visible value):
  - `rumi describe <source>` — show AllowedSchema for the identity:
    columns (with sensitivity), dimensions, metrics, the row filter
    that gets applied to them.
  - `rumi catalog` — list all sources the identity can see.
  - `rumi explain --dsl '{...}'` — parse + validate + compile WITHOUT
    executing.
  - `rumi query --output csv|json|parquet` — result export. Without it,
    Rumi is a CLI demo, not an analyst tool.
- **README + quickstart** (S). A top-level `README.md` aimed at first-time
  visitors. `rumi init --sample` that drops a runnable fixture +
  three-line "now run these three commands" demo.
- **One real user** (no estimate). The grade does not move past B- until
  someone outside the dev environment uses Rumi for a real workflow.
- **Web UI for catalog browsing** (L, V2). The `rumi catalog` data
  surfaced via a small HTTP service + static HTML.
- **Pricing/packaging model** (no engineering). Distribution decision:
  open-source library? Hosted SaaS? Enterprise on-prem?

To move to A: introspection bundle + README + a quickstart + at least
one shipping user.

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

## 6. Synthesized priority list (post-verification, with expanded panel)

The panel's full list is much larger than the original near-term tier.
PM critic was asked to re-prioritize given the new items — three things
changed: (a) `rumi explain` moves into the introspection bundle (small +
high-leverage); (b) benchmarks become a prerequisite for any perf work;
(c) NL is no longer DEFERRED — it's now CONDITIONAL (in-scope if non-LLM).

### Tier 1 — near-term, high user-visible value

1. **Source-type dispatch** — closes adoption friction (`rumi check`
   currently breaks on parquet sources unless users CREATE VIEW manually).
2. **Introspection bundle** — `rumi describe`, `rumi catalog`,
   `rumi explain --dsl`, `rumi query --output csv|json|parquet`. Four
   sub-features that ship well together (they share introspection
   plumbing). Without these, Rumi is a CLI demo, not an analyst tool.
3. **`period_over_period` metric primitive** — MoM/QoQ/YoY are the most-asked
   analytics in this space.
4. **README + `rumi init --sample`** — the project doesn't have a
   first-time-visitor entry document.

### Tier 2 — substantive feature pass

5. **Discrete aggregate primitives** — `weighted_avg`, `stddev_samp`,
   `stddev_pop`, `count_distinct`, `count_distinct_approx`, `mode`,
   `top_k`. ~30 lines for the lot.
6. **Time-bound policies + audit-log SQL redaction** (Sec/Gov A-move).
7. **Schema-drift detection in `rumi sync`** (DQ small-but-high-leverage).
8. **Populate `example_values`** for low-cardinality public columns
   (prerequisite for `rumi describe` to be useful + any future NL layer).
9. **Benchmarks** — `tests/benchmarks/` with small/medium/large
   measurements. Prerequisite for any perf work.

### Tier 3 — architectural, unlocks the next round

10. **CTE compiler infrastructure** — standalone refactor. Compiler learns
    to emit `WITH a AS (...), b AS (...) SELECT ...`. Spec in §3.2.
11. **`cohort_retention` metric primitive** (on top of CTE infra).
12. **`funnel` metric primitive** (on top of CTE infra).
13. **Dimension hierarchies + reusable filter sets + intent-level time grain**
    (Analytics depth).

### Tier 4 — strategic, each its own design pass

14. **Anomaly-detection rule type + `rumi check` scheduler + alerting
    integration** (DQ A-move).
15. **DSL result caching + compiled-DSL plan cache + materialized metrics**
    (Perf — after benchmarks).
16. **Access-pattern anomaly detection + approval workflow + break-glass
    pattern** (Sec/Gov advanced).
17. **Rate limiting** (needs deployment shape decided).
18. **Multi-process safety / server mode** (V2+; effectively a different
    product).

### Tier 5 — NL layer (CONDITIONAL on non-LLM approach)

The user constraint changed 2026-05-13: an NL layer is in-scope IF it's
NOT LLM-based. Hallucination is the disqualifier, not ML.

19. **Pattern-template NL layer** — regex/glob patterns for ~20–30 common
    question shapes. Deterministic; narrow coverage but covers the most-
    asked questions. Tier-5 because it lands AFTER the DSL surface is
    polished (introspection, exports, etc.) — there's no point putting NL
    in front of a tool whose DSL surface itself isn't great.
20. **Embedding-retrieval NL layer** — local sentence-transformers
    (no API calls). Embed metric/dimension descriptions; match user
    query; drive DSL slot-filling. Higher coverage than patterns;
    still deterministic given fixed model + corpus.
21. **Schema search via embeddings** — "find metrics related to X"
    uses the same local-model infrastructure.

### Permanently out of scope (per user decision)

- **LLM-based NL emission** — even with constrained decoding. Reason:
  LLMs can hallucinate references. The DSL is the contract; what sits
  above the DSL must respect the same "no fabrication" rule.

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

## 9. Verification loop — PM critic challenges the expanded list

After the panel produced its full roadmap (~30 items across 7 roles),
PM read it and pushed back on three things.

**Challenge 1: "The expanded list has 30+ items. What's missing if I
just do Tier 1?"**

If you only ship Tier 1 (source-type dispatch + introspection bundle +
period_over_period + README): you have a real product. Tiers 2–4 are
*depth* on top of *real product*. Tier 1 is what makes the difference
between "demo" and "tool."

**Challenge 2: "Tier 5 NL — is the constraint clear enough that someone
won't waste two weeks building a constrained-LLM thing?"**

The constraint is now in three places: `prompts/architect_layer.md`
FIXED CONSTRAINTS, `memory/rumi_constraints.md`, and §4 ML/NLP role
above. Anyone who reads any of those finds: *no LLM in the emission
path; classical / pattern / embedding are in-scope*. The trigger rule
is "can this approach invent a reference that doesn't exist in
AllowedSchema?" If yes (LLMs), out. If no (patterns, retrieval), in.

**Challenge 3: "Tier 4 anomaly detection — Sec/Gov has access-pattern
anomaly detection AND DQ has metric-value anomaly detection. Same
infrastructure?"**

Mostly. Both compute "is this observation N sigma from a trailing
mean?" against `rumi_query_log` (access patterns) or
`rumi_metric_versions` history (metric values). They could share a
`rumi anomaly` runner or be implemented as two `rule_type` variants.
Worth designing together when either is implemented.

### Revised stop-doing list

PM noted that the original handoff implied "more aggregate primitives
should come before CTE infra." That's wrong — the *aggregate primitives*
(stddev, count_distinct, etc.) are commodity work; analysts can use
`expression`-type metrics today for any of them. The *real depth gap*
is CTE-based primitives (cohort, funnel). Order Tier 2 aggregates AFTER
Tier 3 CTE infra IF time-constrained; deliver them only as a
"completeness pass" once cohort + funnel work.

### Final tier ordering (binding)

Tier 1 → Tier 2 (selectively, items 6+7+8) → Tier 3 (CTE infra → cohort
→ funnel) → fill in Tier 2 remainder → Tier 4/5 as priorities shift.

---

*End of handoff. Update this file when the priority list shifts —
it's the document that lets the next contributor (or the next session)
start work without re-reading the entire repo.*
