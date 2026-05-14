# Gibran Status — 2026-05-13 (post-window-primitives: percentile + rolling_window)

Snapshot of what works, what's deferred, and what's still being decided.
Refresh after each major design pass or implementation milestone.

## What Gibran is

Python library + CLI that embeds DuckDB. Categorically a **governed
semantic / metric layer** (Cube / dbt MetricFlow / Malloy / LookML
category). NOT a database. Single artifact: `gibran.duckdb`, a DuckDB
file with extra `gibran_*` metadata tables.

## End-to-end vertical (works today)

```
gibran init                                       -> applies migrations
gibran sync                                       -> YAML -> catalog + governance tables
gibran query --role <r> --attr k=v "<sql>"        -> raw SQL path: govern + rewrite + execute + audit
gibran query --role <r> --attr k=v --dsl '{...}'  -> DSL path:    parse + validate + compile + share execution + audit
```

Two end-to-end smokes pass in a fresh temp dir:

**Raw SQL path**:
```
('alice',         'analyst_west',     'ok',     None,                                          2)
('partner_acme',  'external_partner', 'denied', 'policy:no_column_access:customer_email',      None)
```

**DSL path** (intent JSON in nl_prompt; compiled SQL in generated_sql):
```
user=alice         role=analyst_west     status=ok    sql=SELECT DATE_TRUNC('MONTH',"order_date") AS "orders.order_date"...
user=partner_acme  role=external_partner status=ok    sql=SELECT COUNT(*) AS "order_count" FROM "orders" WHERE ("region"='west')
user=dev           role=analyst_west     status=error reason=intent_invalid: metric 'ghost_metric' not in AllowedSchema
```

**DSL features smoke** (FILTER, expression metrics, HAVING, ratio expansion all working):
```
Multi-metric isolation:    order_count=2, gross_revenue=100  (different FILTER scopes, no contamination)
Expression metric:         revenue_per_paid_order=50.0       (template {a}/{b} resolved at compile time)
Ratio metric:              avg_order_value=50.0              (was broken with literal {a}/{b}; now fixed)
HAVING > 50:               1 row (west, 100)                  (post-aggregation filter applied)
HAVING > 1000:             0 rows                              (correctly drops all)
```

**Metric primitives smoke** (percentile + rolling_window):
```
Percentile (no dims):          p95_amount = 97.50          (west rows: 100, 50 -> p95 ~= 97.5)
Percentile + region dim:       west=97.50                   (regular aggregate, GROUP BY works)
Rolling 7-day (no dims):       2 per-row results            (window function emits per row)
Rolling + region dim:          ERROR exit=3                 (V1 validation: window functions don't compose with GROUP BY)
```

**`gibran check` smoke** (cache-driven block/unblock loop):
```
gibran check    -> 3/3 PASS    exit=0   (clean data)
gibran query    -> ALLOWED              (cache: healthy -> proceed)
[inject NULL row that breaks not_null]
gibran check    -> not_null FAIL exit=1
gibran query    -> DENIED       exit=2  (cache: blocked -> deny with structured reason)
```

**Wheel build**: `python -m build --wheel` produces `dist/gibran-0.0.1-py3-none-any.whl` (~44 KB). Hatchling builds cleanly via isolated environment; the wheel installs into any Python 3.11+ env with the four dep pins (duckdb, pyyaml, typer, pyjwt[crypto], pydantic, sqlglot).

## Per-layer status

| Layer | State | Notes |
|---|---|---|
| `catalog` | Implemented | sources, columns, dimensions, metrics, metric_versions, metric_dependencies (DAG with cycle detection). Populated via sync. |
| `governance` | V1 implemented | `DefaultGovernance` with `preview_schema`, `evaluate`, `validate_alternatives`. AST validate + compile_to_sql with attribute substitution + SQL escaping. |
| `semantic` | Stub | Module skeleton only. Metric expression rendering currently happens in sync.applier (`SUM(amount)`, `{n}/{d}`, etc.) -- will move here when complex expressions land. |
| `observability` | V2 implemented | `DefaultObservability` with cache-first `latest_blocking_failures` (reads `gibran_source_health` O(1); falls back to V1.5 SQL aggregation when cache empty) + `record_run` + `refresh_health`. Per-rule `staleness_seconds`. Stale checks fail closed. Wired into `DefaultGovernance.evaluate`. `observability.runner.run_checks` evaluates all enabled rules and updates the cache. |
| `dsl` | V1 expanded | `QueryIntent` / `DimensionRef` / `OrderBy` / `HavingClause` Pydantic models. `validate_intent(intent, schema)` semantic checker. `compile_intent(intent, catalog)` -> SQL string. `run_dsl_query(con, gov, identity, raw_intent)` orchestrator. CLI: `gibran query --dsl '<json>'` and `--dsl-file`. NO LLM, NO NLP — users type the DSL directly. Features: per-metric `FILTER (WHERE ...)` clauses, ratio metric template expansion with NULLIF guard, expression metric template resolution with cycle detection, HAVING post-aggregation filters, **percentile** (`QUANTILE_CONT`) and **rolling_window** (window function with `FILTER ... OVER`) metric primitives. |
| `nl_to_sql` | Removed from V1 scope | User clarified no LLM/agent. DSL is the user surface (see `dsl` row). NL bridge layers (templates / embeddings / LLM) deferred indefinitely; DSL is forward-compatible. |
| `perf` | Not started | Phase 4. |
| `sync` | Implemented | Full YAML schema for sources/columns/dimensions/metrics/roles/policies/quality_rules/freshness_rules. AST validation via governance.ast.validate. Cross-entity validation. Metric DAG cycle detection. |
| `cli` | Wired | `gibran init`, `gibran sync`, `gibran query` (raw SQL or `--dsl`), `gibran check`. `gibran register` is stubbed (use `gibran sync` instead). |
| `execution` | V1 implemented | `run_sql_query` parses with sqlglot, calls governance.evaluate, ANDs injected filter, executes, writes audit log. Single-source SELECT only. |

## Tests

246 tests, all passing. Breakdown:

| File | Tests | Coverage |
|---|---|---|
| `test_imports.py` | 3 | Module import smoke + governance public API + identity resolver signatures |
| `test_migrations.py` | 7 | Apply (3 migrations), idempotency, expected tables, sensitivity seed, deny-by-default invariants, staleness_seconds columns |
| `test_ast_validation.py` | 20 | All 14 ops + every rejection case (unknown op, unknown col, malformed attr ref, etc.) |
| `test_ast_compile.py` | 20 | All ops produce expected SQL + attr substitution + SQL escaping + round-trip-via-DuckDB |
| `test_sync.py` | 33 | Loader, applier, idempotency, version bumping, governance entities (roles/policies/quality/freshness), cost-class resolution, 9 governance loader rejections + 6 catalog rejections |
| `test_governance.py` | 24 | DefaultGovernance.preview_schema (5 cases), evaluate (9 cases including ATTRIBUTE_MISSING and cross-source NotImplementedError), validate_alternatives, evaluate-with-observability (6 cases including QUALITY_BLOCK / FRESHNESS_BLOCK / never_run / no-policy-beats-quality / no-obs-skip) |
| `test_execution_sql.py` | 25 | Parser unit (11), runner allowed (5), denied (3), errors (2), audit log (4) |
| `test_observability.py` | 15 | Staleness defaults, blocking-failure classification (never_run/rule_failed/stale_check), warn-severity excluded, disabled excluded, per-rule override, record_run + latest-wins |
| `test_observability_runner.py` | 18 | Per-rule evaluators for not_null / unique / range / custom_sql / freshness (10 cases including edge cases). run_checks records runs + refreshes health (5 cases). Cache fallback when no health row exists (2 cases). |
| `test_jwt_resolver.py` | 21 | Constructor contract (3), claim projection (8), bearer header / dict / object adapter (4), signature / audience / issuer / exp validation (4), attribute coercion (3) including non-scalar drops and invalid `attrs` rejection. |
| `test_ast_intent.py` | 8 | Intent variant rejects $attr; policy variant still accepts it; compile_intent_to_sql refuses $attr; signature has no `identity` parameter |
| `test_dsl.py` | 54 | Pydantic model parsing (7), semantic validate_intent against AllowedSchema (8), compile_intent SQL output (5 + 2 ratio-executes), end-to-end run_dsl_query (8), FILTER-aggregate isolation (3), expression metric template + cycle detection (3), HAVING validation + compilation + runner (7), **percentile primitive (6: Pydantic validation, QUANTILE_CONT emission, execution, GROUP-BY composition, runner)**, **rolling_window primitive (5: Pydantic validation, OVER+FILTER emission, per-row execution, dimension-incompatibility error, runner)**. Covers ratio bug fix, FILTER syntax migration, alias-exclusion fix for HAVING aliases, and the FILTER-before-OVER SQL grammar fix discovered while writing rolling_window. |

End-to-end CLI smoke produces audit rows; not in pytest yet (runs against a real `gibran.duckdb` file in a temp dir).

## Fixed constraints (locked, do not re-litigate)

- **Storage / execution:** DuckDB embedded for Phase 1.
- **Language:** Python 3.11+.
- **Identity:** JWT via pluggable `IdentityResolver`. Gibran never owns user table. Resolvers: JWT (prod), Env (dev only), CLI (operator/CI).
- **Metric scope V1:** ratios + same-source expressions. Cross-source deferred to V2. DAG + cycle detection live now.
- **Policy authoring:** YAML in git is source of truth. `gibran sync` validates AST + applies in transaction.
- **Sensitivity:** configurable `gibran_sensitivity_levels` table. Auto-inferred columns get `unclassified`, never `public`.
- **Operator whitelist for `row_filter_ast`:** see `src/gibran/governance/types.py:ALLOWED_AST_OPS`. No `like`, `regex`, `custom_sql`, function calls.
- **NL approach:** deterministic NL -> DSL -> SQL. The LLM never emits SQL. Advanced SQL techniques (window functions, cohort retention, funnels) live as metric type primitives compiled by the engine.
- **`validate_alternatives`, not `suggest`:** governance validates NL-generated candidates; never generates them. O(1) per candidate after the first (amortization deferred but contracted).
- **DuckDB FK quirk:** inbound FKs to `gibran_metrics` deliberately omitted (DuckDB enforces FKs during UPDATE as delete-then-insert; integrity enforced by `sync.loader`).

## Deferred (with stubs and tests pinning the contract)

- **NL layer above the DSL** — any of templates / embeddings / LLM, in that preference order. Deferred indefinitely per user decision; the DSL is forward-compatible.
- **Rate limiting** — architectural; cross-process rate limiting needs a shared store (Redis or similar). Per-process limiters give false security in deployments with multiple workers. Worth its own design pass once deployment shape is settled.
- **Multi-process safety** — DuckDB is single-writer-per-DB-file by design. Multi-process operation effectively requires a server mode, which is V2+. Documented architectural choice.
- **`period_over_period` metric primitive** — composes another metric + `LAG()` window + dimension grain. Needs runtime config resolution (the base metric's expression must be looked up at compile time, like `ratio`). Probably 1 dedicated turn.
- **`cohort_retention` and `funnel` metric primitives** — require **CTE chains**: a multi-stage SQL compiler (one CTE for cohort assignment, another for retention join / step matching, then an aggregate). The current compiler emits a single `SELECT ... FROM ... GROUP BY` shape; CTE composition is an architecture pass, not just a primitive addition. 2 turns: one for CTE infrastructure, one for cohort + funnel semantics on top.
- **Source-type dispatch in `gibran check`** — V1 assumes `source_id` is the DuckDB relation name. For real parquet/csv sources, users must register them as views before checking. Future: dispatch on `source_type` in the runner.
- **`applied_constraints`** in `GovernanceDecision` is `()` — V2 walks the AST into `Constraint[]`. Not blocking anything.
- **`example_values`** never populated — pinned by `test_example_values_never_populated_in_v1`. Plugs in when NL prompt builder needs them.
- **`validate_alternatives` amortization** — naive per-candidate eval. Plug in shared compiled-policy + AllowedSchema cache when profiling shows the need.
- **Metric expansion column check** — currently only the *requested* columns are checked against the policy allowlist. A metric whose internal expression touches a denied column would still execute. To do when nl_to_sql lands.
- **Cross-source queries** — `evaluate(source_ids: frozenset[str], ...)` accepts the V2 shape but enforces `len == 1`.
- **Multi-file YAML config** — single `gibran.yaml` only.
- **Destructive sync** — `gibran sync` is additive for catalog/governance entities (quality + freshness rules are wholesale-replaced). `gibran sync --force` for destructive behavior is a future flag.
- **`gibran register`** — placeholder. Use `gibran sync` to register sources via YAML.
- **JSON `applied_constraints` in audit log** — currently audit log only records SQL string. Structured constraints would help impact analysis.
- **`compile_to_sql` plan-cache thrash** — every query inlines literal user attributes. DuckDB plan cache misses. Real concern once query throughput rises; not blocking.
- **Result-set masking** — column-level redaction without full denial. Not in V1; explicit deny is the contract.

## Open architectural questions (deferred decisions)

1. **Where does column `display_name` live?** Currently `gibran_columns` has no display_name column; `ColumnView.display_name = name`. Should the YAML allow a per-column display name?
2. **Should `metric_dependencies` track cross-source deps when V2 lands?** Current schema allows it (no source FK on dependency rows). The loader rejects it; relaxing in V2 needs a same-source-only flag or removal of the check.
3. **Caching strategy for governance evaluate.** In-process LRU vs shared (Redis-backed) for multi-process. Profiling-driven, not premature.
4. **Service mode.** All current usage is in-process (CLI or library). Long-term: HTTP server with a wire protocol? If so, governance.evaluate becomes a network call -- caching gets more interesting.
5. **Schema migration for live deployments.** Migrations are append-only DDL today. Once anyone is in production, we need data migration helpers (renaming columns, sensitivity reclassification, etc.).
6. **Time-grain as a first-class metric attribute** vs a runtime parameter. Affects DSL design.
7. **Compiled-DSL caching keyed by NL prompt embedding** -- "we've answered this question before, here's the cached DSL." Phase 2/3.

## What "done with V1" means

- Catalog + governance + execution wired end-to-end (DONE)
- Audit log captures every query attempt (DONE)
- AST validator + compiler covers all whitelisted ops (DONE)
- Quality / freshness consultation in evaluate (DONE: V2 cache-first reads)
- `gibran check` evaluates all rules and refreshes the cache (DONE)
- DSL user surface: types + validate + compile + run + CLI (DONE)
- `pip install gibran` produces a working CLI (DONE: wheel builds via hatchling)
- Real JWT resolver (DONE: PyJWT + JWKS, static-key fallback)

**V1 production is feature-complete as of this turn.** Remaining gaps (rate limiting, multi-process safety, cohort/funnel primitives) are documented as deferred with rationale.
