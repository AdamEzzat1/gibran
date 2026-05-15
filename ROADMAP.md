# Gibran V0.1 Roadmap

Forward-looking from `c940eb7` (v0.0.2 shipped on PyPI + GitHub).
This document covers the next ~3 months of work in **4 sequential
phases**, sized to be achievable individually. Later phases assume
earlier ones land; phases inside a tier can be parallelized where
noted.

Each phase follows the stack-role-group panel pattern:
**panel → synthesized direction → tasks → adversarial verification.**

## Executive summary

| Phase | Focus | Time | What lands |
|---|---|---|---|
| 1 | NL pattern expansion + easy primitives | 1-2 wk | 6 → ~15 patterns, synonyms dict, 4 new scalar primitives. Mechanical work; biggest cheap win. |
| 2A | Shape-primitive refactor | 1-2 wk | Branch in `compile_intent` becomes a `ShapePrimitive` protocol + registry. Cohort/funnel/multi_stage as registered classes. |
| 2B | Result cache data-version tracking | 1 wk | File mtime for parquet/csv; `gibran_table_versions` for duckdb tables; `gibran touch <source>` CLI. Closes the stale-cache hole. |
| 2C | Materialized metric incremental refresh | 1-2 wk | `materialized_strategy: incremental` with watermark; `gibran_mat_state` track table; `gibran materialize` CLI. Sync stays interactive at 1M+ rows. |
| 3 | NL deepening toward 30 | 2-3 wk | Entity recognizer (no LLM); time-phrase parser; `comparison` + `relative_time_filter` + `cohort_filter` primitives; 12-15 new patterns across 5 categories. |
| 4 | `gibran ui` command | 4-6 wk | FastAPI backend + React frontend bundled in the wheel. Catalog browser + query box + audit log viewer. 5x audience expansion. |

Sequential total: **~12-18 weeks**. Parallelizable across Phase 2 (~10-15 weeks) if multiple contributors.

### How the role-group pattern shows up

Each phase has the same shape (matching the lock in `feedback_panel_pattern.md`):

- **Panel** — 2-3 most-relevant roles, each with one pro/con line
- **Synthesized direction** — what's decided + the rationale
- **Tasks** — sized, file-pointed, ordered
- **Adversarial** — one critic role pushes back; answers either defend the call or revise it

E.g. for Phase 2A (shape-primitive refactor):

- DB Architect notes the refactor is mechanical (`CompiledQuery` already supports the shape)
- Analytics Engineer flags the unlock (user-declared shape primitives in YAML)
- PM critic challenges the user-visible value
- Answer defends it as enabling Phase 3 without doubling the work

### What's NOT in the roadmap (deliberately)

| Item | Why deferred |
|---|---|
| Multi-tenancy primitives | V2; needs deployment-shape decision first |
| Embedding-retrieval NL (Tier 5 Item 20) | After Phase 3 patterns hit the ceiling AND user feedback says "I don't know" rate is unacceptable |
| Server mode + cross-process rate limit | Tied to multi-tenancy |
| dbt / Cube integration | Different roadmap entirely; revisit after Phase 4 |

### The stop-doing list (explicit non-goals)

These four lines are the clearest contribution of the roadmap:

- **DO NOT** add an LLM to the NL emission path.
- **DO NOT** ship a new shape primitive on top of the current branch hack (do Phase 2A first).
- **DO NOT** generalize the cache before Phase 2B's data-version tracking lands.
- **DO NOT** add new dependencies that aren't already in the wheel during Phase 4.

The first one is the locked Tier-5 constraint. The other three are anti-patterns a future contributor would drift into if they're just "shipping things" without re-reading the design rationale — calling them out by name is the cheapest insurance.

---

## Context

V0.0.2 ships a structurally-complete governed metric layer with a
non-LLM NL surface. The honest gaps that this roadmap exists to
close:

1. **Shape primitives are a special-case branch in `compile_intent`.**
   Works for 3 primitives; doesn't scale to 6+ and blocks user-
   declared shape primitives.
2. **Result cache invalidation only sees catalog + health changes.**
   A parquet file getting rewritten between syncs serves stale rows.
3. **Materialized metrics fully rebuild on every sync.** Fine at 10k
   rows; P0 issue at any real scale.
4. **NL coverage is 6 patterns.** Architecture supports ~30; mechanical
   work to close the gap. Per the locked Tier-5 constraint, no LLM in any emission path.
5. **No UI.** The audience that can use Gibran today is the audience
   that reads docs. A `gibran ui` command would 5x that.

## Phase ordering (binding)

```
Phase 1 (1-2 weeks) — mechanical NL pattern expansion + easy primitives
  ↓
Phase 2 (2-4 weeks) — foundation hardening: 3 engineering shortcuts
  ├─ 2A: shape primitive refactor (Subquery abstraction)
  ├─ 2B: result cache data-version tracking
  └─ 2C: materialized metric incremental refresh
  ↓
Phase 3 (2-3 weeks) — NL deepening: toward 30 patterns + supporting primitives
  ↓
Phase 4 (4-6 weeks) — `gibran ui` command + local React app
```

Phases 2A/2B/2C can run in parallel if multiple contributors. Phase 1
is the cheapest user-visible win and should ship first regardless.

---

## Phase 1 — NL pattern expansion + easy metric primitives (1-2 weeks)

**Goal:** 2-3x the perceived intelligence of the NL layer by
mechanically expanding pattern coverage. No architectural changes.

### Panel

- **ML/NLP Engineer:** Most "I don't know" responses today come from
  missing pattern *shapes*, not missing metric primitives. A user asking
  "revenue last 30 days" fails because we don't parse time phrases, not
  because we don't have the metric.
- **Analytics Engineer:** Adding metric primitives is mechanical (one
  branch in `_render_expression` + a Pydantic validator). New
  primitives are only useful if at least one NL pattern can address
  them — pair every new primitive with a pattern.
- **PM (critic):** Don't over-engineer this phase. 10 well-chosen
  patterns covering 80% of asked questions beats 30 patterns that
  all match the same trivial shape. Coverage > depth here.

### Synthesized direction

Expand patterns from **6 → ~15**, focused on shapes that real
questions actually take (time phrases, conditions, synonyms). Add
**3-5 new scalar primitives** (variance, first_value, last_value,
median) that fit the existing aggregate code path. Build a synonym
dictionary so "biggest" / "trend" / "average" route correctly.

### Tasks

| # | Task | Sizing | Where |
|---|---|---|---|
| 1.1 | `metric_in_period` pattern: "revenue in 2026", "in January" | S | [src/gibran/nl/patterns.py](src/gibran/nl/patterns.py) |
| 1.2 | `metric_last_n_period` pattern: "revenue last 30 days", "last 3 months" | S | same |
| 1.3 | `metric_over_time` synonym pattern: auto-picks temporal dim + month grain | S | same |
| 1.4 | `count_with_condition` pattern: "count of paid orders" | M (needs filter inference like `metric_filtered_by_value`) | same |
| 1.5 | `top_n_by_metric` improvements: accept "biggest N <dim>" | S (regex) | same |
| 1.6 | `bottom_n` pattern: "bottom 5 region by revenue" | S | same |
| 1.7 | `metric_excluding_value` pattern: "revenue excluding refunded orders" | M | same |
| 1.8 | `metric_distribution`: routes to a percentile primitive | M (needs new primitive) | same |
| 1.9 | Synonym dictionary: "biggest"/"largest"→top_n, "trend"→by_month, "average"→avg | S | new `gibran/nl/synonyms.py` |
| 1.10 | New primitives: `variance`, `first_value`, `last_value`, `median` (percentile p=0.5 wrapper) | M | [src/gibran/sync/applier.py:_render_expression](src/gibran/sync/applier.py) |
| 1.11 | Tests: ~3 per new pattern + 1 per new primitive | M | tests/test_nl_patterns.py, tests/test_aggregate_primitives.py |

**Estimated**: 1-2 weeks for one contributor.

### Adversarial verification — PM critic

> *Challenge 1: 10 new patterns is still narrow vs the universe of real
> questions. Why not skip ahead to embedding retrieval (Tier 5 Item 20)?*

**Answer:** Pattern templates are zero-cost to deploy and have hard
correctness guarantees. Embedding retrieval adds:
- a ~100MB sentence-transformers dependency
- per-query inference time (~50-200ms)
- a "matched well enough?" subjective threshold that's hard to test

Patterns are the right *floor*. Embeddings (Tier 5 Item 20) are the
right *ceiling*, after patterns prove there's user demand for richer
NL. Skipping the floor risks shipping a "smarter" layer that's
slower AND has weaker correctness guarantees.

> *Challenge 2: How do you know which 10 patterns matter most?*

**Answer:** We don't. We pick shapes that exhibit obvious linguistic
structure ("last N days", "in <period>", "excluding <value>") and
ship. The next 5 patterns come from user feedback after Phase 4
(the UI) gets eyeballs on it. Avoid optimizing for hypothetical
question shapes.

---

## Phase 2 — Foundation hardening: the 3 engineering shortcuts (2-4 weeks)

Three sub-phases that can run in parallel. Each addresses a known
shortcut that will become a P0 bug at real-world scale.

### Phase 2A — Shape primitive refactor (1-2 weeks)

**Current state:** `cohort_retention`, `funnel`, and
`multi_stage_filter` each special-case `compile_intent` with their
own whole-query emission. The pattern works for 3 primitives but
doesn't scale to ~6+ and blocks user-declared shape primitives in
YAML.

#### Panel

- **DB Architect:** `CompiledQuery` already has
  `ctes: tuple[CTE, ...]`. The refactor is mechanical: each shape
  primitive becomes a `ShapePrimitive` class that builds its CTE
  list. `compile_intent` routes to the registered primitive instead
  of branching on `metric_type`.
- **Analytics Engineer:** After this, users could define their own
  shape primitives in YAML (specifying CTE chains declaratively).
  That's the unlock — composable analytics patterns without writing
  Python.
- **PM (critic):** This is invisible to users today. Is it worth doing
  before shipping more user-visible features (UI, more patterns)?
  **Counter:** Yes, because the current branch will become a 6-way
  `elif` chain by Phase 3 if we don't fix it now, AND fixing it
  later means reworking each new shape primitive twice.

#### Synthesized direction

Define a `ShapePrimitive` protocol; convert the 3 existing primitives
to instances; route via registry rather than branch.

```python
# src/gibran/dsl/shape_primitives/types.py (new)
class ShapePrimitive(Protocol):
    metric_type: str  # e.g. "cohort_retention"
    def build(
        self, meta: _MetricMeta, intent: QueryIntent,
        from_clause: str
    ) -> CompiledQuery: ...
    def validate_intent(self, intent: QueryIntent) -> None:
        """Raise IntentValidationError if intent shape is incompatible.
        Default: enforce single-metric + no dimensions/filters/having/order_by."""
```

#### Tasks

| # | Task |
|---|---|
| 2A.1 | Define `ShapePrimitive` protocol + `register_shape_primitive` decorator |
| 2A.2 | Move `_build_cohort_retention` into `CohortRetention(ShapePrimitive)` class |
| 2A.3 | Move `_build_funnel` into `Funnel(ShapePrimitive)` class |
| 2A.4 | Move `_build_multi_stage_filter` into `MultiStageFilter(ShapePrimitive)` class |
| 2A.5 | Replace the special-case branch in `compile_intent` with registry lookup |
| 2A.6 | Move per-primitive intent validation from `dsl/validate.py` into each `ShapePrimitive.validate_intent` |
| 2A.7 | Tests: existing ~52 shape-primitive tests still pass; add 4 for the protocol contract |
| 2A.8 | Stretch: YAML support for user-declared shape primitives (CTE chain in `shape_steps:` field). Likely defer to V0.2. |

#### Adversarial — DB Architect critic

> *Challenge: Doesn't a registry just rename the existing branch?*

It does, structurally — but the registry forces the "what does each
primitive need to validate?" question into per-class methods instead
of a growing if/elif chain in `dsl/validate.py`. The user-visible
payoff is small; the next-contributor payoff is "I added a new shape
primitive without touching 4 files."

### Phase 2B — Result cache data-version tracking (1 week)

**Current state:** Result cache invalidates on `catalog_generation`
(bumped by `sync`) + `source_health_generation` (bumped by `check`).
Misses: source data changing between sync/check calls. A parquet
file rewritten externally still serves the old cached row.

#### Panel

- **DB Architect:** For file-backed sources (parquet/csv), file mtime
  is reliable and cheap to probe. For `duckdb_table` sources, we'd
  need a per-table version counter or a CHECKSUM probe; DuckDB has
  `MAX(rowid)` as a poor proxy but no stable version metadata. SQL
  views are derived — version follows the underlying tables.
- **Perf Engineer:** The cost we want to avoid is the cache lookup
  triggering an expensive probe. mtime is `os.stat` — sub-millisecond.
  Acceptable cost on the hot path.
- **Sec/Gov Architect:** If a user can manipulate file mtime
  (touching a parquet), they can poison the cache to either serve
  stale data OR force a re-execute on every query. Neither is a
  *governance* break (the data they see is still allowed by policy),
  but the latter is a DoS-y class of bug. Document.

#### Synthesized direction

Add a `data_version` generation token computed per-source at lookup
time. For each source-type:
- `parquet` / `csv`: `os.stat(uri).st_mtime_ns`
- `duckdb_table`: query a `gibran_table_versions` map populated by
  manual operations or a `gibran touch <source>` command
- `sql_view`: union of underlying tables' versions (recursive)

Cache key adds `data_version` for each source touched by the query.

#### Tasks

| # | Task |
|---|---|
| 2B.1 | New helper `source_data_version(con, source_id) -> str`. Per-type dispatch in `_source_dispatch.py`. |
| 2B.2 | Modify `result_cache.cache_key` to include data versions for the source. |
| 2B.3 | New CLI `gibran touch <source>` for manual invalidation of `duckdb_table` sources. |
| 2B.4 | Migration 0010: `gibran_table_versions` table for `duckdb_table` versioning. |
| 2B.5 | Tests: parquet mtime change invalidates; touch invalidates; sql_view recursively invalidates. |
| 2B.6 | Doc: cache invalidation contract in README + result_cache.py module docstring. |

#### Adversarial — Sec/Gov critic

> *Challenge: Per-query stat() of every source file is a side channel
> for "does this file exist." Is that a leak?*

Negligible. The user already has access to the source (it's in their
schema) and the file path is in their YAML. mtime is a side-channel
for "when did this change," which is in-scope information.

### Phase 2C — Materialized metric incremental refresh (1-2 weeks)

**Current state:** `gibran sync` runs `CREATE OR REPLACE TABLE
gibran_mat_<metric_id>` for every materialized metric, every time.
At 100k+ rows this becomes the slowest part of sync; at 1M+ rows
it's untenable.

#### Panel

- **DB Architect:** Standard pattern: watermark column + UPSERT. For
  time-bound metrics, the watermark is the temporal dimension. For
  non-temporal, use a row hash or just full-rebuild.
- **Perf Engineer:** At what cardinality does incremental beat full
  rebuild? Probably ~100k rows depending on aggregation cost.
  Below that threshold, full rebuild is simpler. The materialized
  metric YAML should let users opt into incremental.
- **Analytics Engineer:** Watermarks shift the correctness boundary.
  Late-arriving data (an order whose `order_date` is yesterday but
  written today) gets missed. Document.

#### Synthesized direction

Add `materialized_strategy: full | incremental` field on the metric
config; default `full` (current behavior). For `incremental`, require
a `watermark_column` and emit:

```sql
DELETE FROM gibran_mat_<metric_id>
 WHERE <dim_cols> IN (
   SELECT DISTINCT <dim_cols> FROM <source>
    WHERE <watermark_column> > <last_refresh_watermark>
 );
INSERT INTO gibran_mat_<metric_id>
SELECT <dim_cols>, <metric_expr>
  FROM <source>
 WHERE <watermark_column> > <last_refresh_watermark>
 GROUP BY <dim_cols>;
```

Track `<last_refresh_watermark>` in a new `gibran_mat_state` table.

#### Tasks

| # | Task |
|---|---|
| 2C.1 | New YAML fields: `materialized_strategy`, `watermark_column`, `late_arrival_grace_seconds` |
| 2C.2 | Migration 0011: `gibran_mat_state(metric_id, last_refresh_watermark, last_refresh_at)` |
| 2C.3 | Implement incremental refresh in `applier._materialize_metrics` |
| 2C.4 | New CLI `gibran materialize [--metric <id>] [--full]` for ad-hoc refresh |
| 2C.5 | Wire into `gibran sync` and `gibran check --watch` schedulers |
| 2C.6 | Tests: incremental matches full rebuild on synthetic data; late-arrival handling |
| 2C.7 | Bench: full vs incremental at 100k / 1M / 10M synthetic rows |

#### Adversarial — Analytics Engineer critic

> *Challenge: Late-arriving data is a real problem. A `late_arrival_grace_seconds:
> 3600` covers most cases but not all.*

True. The contract: incremental refresh is opt-in and the user takes
responsibility for the watermark covering their data-arrival pattern.
If a source has unbounded late arrivals (e.g. timestamps backfilled
from a separate system), they should use `materialized_strategy: full`.
Document loudly.

> *Challenge: Is this user-visible value or just a perf win?*

User-visible: with full-rebuild, `gibran sync` takes 30s on a 1M-row
source. With incremental, it takes ~1s for typical daily new rows.
Sync becomes interactive instead of "go get coffee."

---

## Phase 3 — NL deepening: toward 30 patterns + richer primitives (2-3 weeks)

**Goal:** Hit the architecture's pattern ceiling (~30) and add the
metric primitives needed to make new patterns route to interesting
SQL. Still no LLM.

### Panel

- **ML/NLP Engineer:** Past ~15 patterns we start hitting diminishing
  returns from raw regex. Need a small NER pass: identify entities
  (metric names, dim values, time expressions, numbers) BEFORE
  pattern dispatch, so patterns work on entity-typed slots instead
  of raw strings.
- **Analytics Engineer:** Several missing primitives unblock multiple
  patterns at once. E.g. a `comparison` primitive (compose two
  metrics with delta/ratio) unblocks "X vs Y", "compare X to Y",
  "X relative to Y" — 3+ patterns from 1 primitive.
- **PM (critic):** What's the user-visible test? "Could an analyst
  with no SQL knowledge get their top 10 questions answered?" Not
  "could we match 30 regex patterns." Pick the 30 by working
  backwards from real questions you'd want answered.

### Synthesized direction

Add a thin entity-recognition pass (still deterministic, still no
LLM): identify metric phrases, dim phrases, time phrases, numbers,
and literal values in a pre-processing step. Then patterns operate on
typed slots: `<METRIC> over <TIME_PHRASE>` matches more naturally
than 5 separate regex variants.

Pair each new pattern category with one new metric primitive that
exposes new capability.

### Pattern categories (each unlocks 3-5 new patterns)

| Category | Example patterns | New primitive needed |
|---|---|---|
| **Time phrases** | "last 30 days", "this quarter", "year to date", "month over month" | `relative_time_filter` (deterministic time-window resolver) |
| **Comparisons** | "X vs Y", "compare A to B", "X relative to Y" | `comparison` (compose two metric expressions with delta/ratio/pct_change) |
| **Anomalies** | "show me anomalies in revenue", "outliers in <metric>" | Routes to existing `anomaly` quality rule — but as a query, not a check |
| **Compound filters** | "revenue for west and paid orders", "revenue excluding test" | Multi-filter pattern; routes to existing DSL filters[] array |
| **Nested questions** | "customers who ordered last month and returned this month" | `cohort_filter` (filter to entities matching a sub-query) |

### Tasks

| # | Task | Sizing |
|---|---|---|
| 3.1 | Entity recognizer: `gibran/nl/entities.py`. Pure-function `extract_entities(text, schema) -> EntitySet`. | L |
| 3.2 | Time-phrase parser: relative (`last 30 days`), absolute (`in 2026`), explicit (`Q1 2026`). | M |
| 3.3 | Refactor pattern matchers to work on entity-typed slots. | M |
| 3.4 | New `comparison` metric primitive: compose two metric IDs with delta/ratio/pct_change. | M |
| 3.5 | New `relative_time_filter` primitive: emits `WHERE col >= now() - INTERVAL '...'`. | M |
| 3.6 | New `cohort_filter` shape primitive (rides on Phase 2A refactor). | L |
| 3.7 | 12-15 new patterns covering the 5 categories above. | M |
| 3.8 | "Show example questions" CLI: `gibran ask --examples` lists patterns + sample questions. | S |
| 3.9 | Tests: ~3 per pattern + 1 per primitive + entity-recognizer coverage. | L |

**Estimated**: 2-3 weeks for one contributor.

### Adversarial — ML/NLP critic

> *Challenge: Entity recognition without ML feels limiting. Real users
> say "biggest spenders by Q2 amount" — that requires understanding
> "biggest spenders" as a comparative + entity reference. Doable
> without ML?*

For canonical phrasings, yes — synonyms table + entity matching
covers "biggest"→top-N, "spenders"→customer entity. For arbitrary
paraphrasing, no — that's the LLM/embedding territory. The
honest contract: we cover the 60-70% of questions that match a
documented pattern. The other 30-40% become "I don't know" until
Tier 5 Item 20 (embedding retrieval) lands.

> *Challenge: 30 patterns + entity recognition might be more
> implementation surface than a small fine-tuned model.*

Possibly. But the contract differs: ML gives "probabilistic
correctness," patterns give "deterministic coverage." The latter
matches Gibran's broader design philosophy (no fabrication, ever).

---

## Phase 4 — `gibran ui` command + local React app (4-6 weeks)

**Goal:** 5x the audience that can use Gibran by giving the people
who won't read docs something to click around in.

### Panel

- **PM:** This is the biggest single audience-expansion lever in the
  entire roadmap. Internal analytics tools without a UI lose every
  non-technical adopter at "what's a DSL?"
- **DB Architect:** Don't introduce stateful complications. The UI is
  a thin client over the existing CLI surface; all writes (sync,
  approve, etc.) still go through the CLI. The UI is read-only +
  query execution.
- **Perf Engineer:** Latency budget matters. For "feels like a real
  product" interactivity, query results need to render under 2s
  end-to-end. Stream results where possible; don't block on full
  execution.
- **Sec/Gov Architect:** The UI introduces a new entry point. Every
  request still goes through `DefaultGovernance.evaluate`; the UI's
  identity comes from whichever resolver the user configures
  (JWT in prod, CLI for dev). No new trust boundary.

### Synthesized direction

A `gibran ui` command starts a local HTTP server (FastAPI) on a free
port and opens the user's browser to it. The server is a thin RPC
layer over the existing `gibran` commands. Frontend is React (Vite
build, pre-bundled into the wheel so no Node toolchain at install
time). Local-only — no `--host 0.0.0.0`, no auth (it's running on
the user's machine for the user's session).

Three core views:
1. **Catalog browser**: tree of sources → columns / dimensions / metrics
   (uses `gibran catalog` + `gibran describe` data).
2. **Query box**: NL input (`gibran ask`) with a compile-preview
   showing the matched pattern + emitted DSL + SQL. Run button
   executes and shows the result table.
3. **Audit log viewer**: filterable view of `gibran_query_log` so
   admins can review who-queried-what.

### Tasks (sequenced)

#### 4A: HTTP backend (1-1.5 weeks)

| # | Task |
|---|---|
| 4A.1 | New module `gibran/ui/server.py`: FastAPI app with endpoints `/catalog`, `/describe/<source>`, `/ask`, `/explain`, `/query`, `/log`. |
| 4A.2 | Identity resolution from request: env-var token (dev) or JWT header (prod). |
| 4A.3 | Each endpoint maps to an existing CLI command's underlying function — DON'T duplicate logic. |
| 4A.4 | Static file serving for the bundled React build. |
| 4A.5 | `gibran ui` CLI command: starts the server, opens browser, exits on Ctrl-C. |
| 4A.6 | Optional dependency group `[ui]` so headless installs don't pull FastAPI/Uvicorn. |

#### 4B: React frontend (2-3 weeks)

| # | Task |
|---|---|
| 4B.1 | Vite + React + TypeScript project under `frontend/`. |
| 4B.2 | Build pipeline that outputs to `src/gibran/ui/static/` — bundled into the wheel. |
| 4B.3 | Catalog browser component (read-only tree). |
| 4B.4 | Query box: NL input → preview (pattern + DSL + SQL) → execute button. |
| 4B.5 | Result table component with column-type-aware rendering. |
| 4B.6 | Audit-log viewer with filters by user / role / status / time. |
| 4B.7 | Empty / no-pattern-matched state with example questions from `gibran ask --examples`. |

#### 4C: Polish (~1 week)

| # | Task |
|---|---|
| 4C.1 | Keyboard shortcuts (cmd-enter to run, etc.) |
| 4C.2 | Dark mode |
| 4C.3 | Export-as-CSV button on results |
| 4C.4 | Documentation: README screenshots + GIF of `gibran ui` flow |
| 4C.5 | Tests: backend endpoints (~15 tests); frontend component tests (~10 tests); end-to-end (~3 tests with a headless browser) |

**Estimated**: 4-6 weeks for one contributor with both backend + frontend
chops. Could parallelize: 1.5 weeks backend + 3 weeks frontend if
parallel; otherwise sequential.

### Adversarial — PM critic

> *Challenge 1: You're building a UI before you have users. Aren't
> you optimizing for the wrong end?*

Counter: the UI IS how you get users. Every developer-tool category
shows the same pattern — `psql` < `pgcli` < `pgadmin` < `dbeaver` in
terms of adoption. The audience that can use a CLI is small. The
audience that can click around in a UI is 10x larger. Without the
UI, Gibran caps at "tools for the kind of engineer who'll write
YAML." With the UI, it can reach analysts.

> *Challenge 2: Bundling React into a Python wheel is unusual. Real
> dependency cost?*

The Vite production build is ~200-400KB gzipped. Bundled into the
wheel via Hatch's static-file handling, total install size goes from
44KB to ~500KB. Negligible compared to the dependencies already
present (sqlglot is 4MB, pydantic+sqlglot+duckdb together are ~50MB).

> *Challenge 3: Why not just use Streamlit / Gradio / etc.?*

Both ship a UI in 50 lines of Python but lock you into their
component model and don't compose well with a deeper application
shell. For a one-day prototype, Streamlit. For a "this is the
product surface," a real React app with a real API boundary is
worth the upfront cost.

---

## Cross-phase concerns

### What this roadmap deliberately doesn't include

| Item | Why deferred |
|---|---|
| **Multi-tenancy primitives** | V2 architectural pass; adding `tenant_id` to every governance table is a migration cascade that touches every code path. Needs a deployment-shape decision first (per-tenant DB vs. shared with RLS). |
| **Embedding-retrieval NL (Tier 5 Item 20)** | After Phase 3 patterns hit the ceiling AND we have user-feedback signal that the residual "I don't know" rate is unacceptable. Premature to add a model dep before patterns are exhausted. |
| **Server mode + cross-process rate limiting** | Tied to multi-tenancy. Deployment shape decision gates both. |
| **Cohort/funnel as user-declared shape primitives in YAML** | Phase 2A enables this technically; punt until Phase 4 surfaces actual user requests for new shape types. |
| **dbt / Cube integration** | Different roadmap entirely. Decide after Phase 4 whether Gibran is "standalone" or "complement to existing stack." |

### Effort budget summary

```
Phase 1:        1-2 weeks    — 1 contributor, mostly mechanical
Phase 2A:       1-2 weeks    — 1 contributor, internal refactor
Phase 2B:       1 week       — 1 contributor, additive
Phase 2C:       1-2 weeks    — 1 contributor, additive
Phase 3:        2-3 weeks    — 1 contributor, NL focus
Phase 4:        4-6 weeks    — 1-2 contributors (backend + frontend)
                ---
Sequential:     ~12-18 weeks total
Parallel (P2):  ~10-15 weeks
```

### Stop-doing list (from the v0.0.2 review)

These are *bad* paths the roadmap explicitly avoids:

- **DO NOT** add an LLM to the NL emission path. The structural
  no-hallucination guarantee is Gibran's strongest differentiator;
  giving it up would erase the post's thesis.
- **DO NOT** ship a new shape primitive on top of the current branch
  hack. Either do Phase 2A first or add it as a special-case and
  promise to refactor (worst pattern; resist).
- **DO NOT** generalize the cache before Phase 2B's data-version
  tracking lands. Today's cache CAN return stale rows; making it
  smarter without fixing that makes the bug class harder to find.
- **DO NOT** add new dependencies that aren't already in the wheel
  during Phase 4. The UI bundles its own assets; no `npm install`
  on user machines.

---

## Verification — overall

**Q: Is this roadmap honest about effort?**

Yes. Sized at the granularity that a single experienced contributor
could plausibly hit. The error bars are real — Phase 4 in
particular could double if the React tooling has surprises.

**Q: Is the sequencing right?**

Phase 1 first is the cheapest user-visible win and builds momentum.
Phase 2 is foundation work that pays for itself by Phase 3 (which
adds shape primitives Phase 2A makes tractable). Phase 4 is the
biggest-impact item but is gated on having enough functionality to
make the UI worth building.

**Q: What kills this roadmap?**

A real user who needs something not in any of these phases (e.g.,
"we need multi-tenancy by next quarter"). At that point, the
roadmap pauses while the requirement gets evaluated; the deferred
items in "What this roadmap deliberately doesn't include" become
candidates for priority promotion.

---

*End of roadmap. Update this document when a phase completes or
when external requirements force re-prioritization.*
