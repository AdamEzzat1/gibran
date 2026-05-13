# Refined Architect Prompt — single layer at a time

This is the prompt used when designing one layer of Rumi. Run it once
per layer. Sequential adversarial pairs (architect -> critic -> architect),
not parallel multi-persona panels. Concrete artifacts only.

---

```
ROLE: senior architect designing ONE layer of Rumi —
a governed analytics + NL-to-SQL system embedded over DuckDB.

FIXED CONSTRAINTS (do not re-derive, treat as given):
- Storage/execution: DuckDB embedded for Phase 1. Pluggable later.
- Language: Python 3.11+ (3.12 preferred)
- Target user: data analyst at a 50-person SaaS company,
  comfortable with SQL but not data engineering
- MVP surface: Python library + CLI (typer)
- Wedge: governed metric layer with NL-to-SQL.
  NOT a new storage engine. NOT a BI frontend.
- Identity: JWT via pluggable IdentityResolver. Rumi never owns user table.
- Metric scope: ratios + same-source expressions in V1.
  Cross-source deferred. DAG + cycle detection live now.
- Policy authoring: YAML in git is source of truth.
  `rumi sync` validates AST + applies in transaction.
- Sensitivity: configurable rumi_sensitivity_levels table.
  Auto-inferred columns get 'unclassified', not 'public'.
- Operator whitelist for row_filter_ast: see rumi/governance/types.py.
- Conversational front-end: NONE in V1. The DSL is the user surface.
  WHEN/IF an NL layer is later added, the constraint is: NO LLM in the
  emission path. Classical NLP (pattern templates, slot-filling with
  spaCy/regex), or embedding retrieval with local sentence-transformers
  are acceptable. The rule is "no hallucination", not "no ML": any
  approach that can FAIL to parse a question but CANNOT invent metrics
  or columns is in-scope. LLM emission (even with constrained decoding)
  can fabricate AllowedSchema references and is therefore out-of-scope.
  Same product model as Cube's CubeQL / MetricFlow's `mf query`: the
  query language is the user surface; NL is a thin convenience layer
  above it, not a replacement.
- Advanced SQL techniques (window functions, period-over-period, cohort
  retention, funnels, percentiles) live as metric type primitives compiled
  by the engine -- not as user-authored SQL fragments.
- Rumi DSL grammar: source + metrics[] + dimensions[] (with optional grain)
  + filters[] (AST nodes from the operator whitelist) + order_by[] + limit
  + optional unanswerable_reason. Filter values are pure scalars only; the
  DSL does NOT permit {"$attr":...} (attribute references are policy-only).
- AST trust boundary is encoded in function signatures:
  validate_policy_ast / compile_policy_to_sql -- accepts identity, allows
  {"$attr":...}. validate_intent_ast / compile_intent_to_sql -- no identity
  argument, no attribute references. Same shape, different trust context;
  separate functions prevent accidental cross-use.
- NL system prompt is a versioned constant (rumi/nl/prompt.py:SYSTEM_PROMPT)
  with a unit test asserting required substrings. Tampering = test failure.
- Observability consultation in governance.evaluate is V1.5 (direct SQL
  aggregation per call, no cache table). V2 introduces rumi_source_health
  cache table refreshed by `rumi check`. Both behind ObservabilityAPI.
- Closest peers to study (not copy): Cube, dbt MetricFlow, Malloy,
  LookML; Calcite for planner ideas; ClickHouse for observability.

LAYER TO DESIGN (pick exactly one):
catalog | semantic_layer | governance | nl_to_sql
| observability | performance_advisor | execution_glue | sync

PROCESS (sequential, not parallel):

STEP 1 -- Architect proposes V1. Produce:
  a) Responsibilities: 3-5 bullets, what this layer owns and what it does NOT.
  b) Schema/types: concrete DDL (DuckDB dialect) or typed structs.
     No prose. No "consider adding..." -- just the schema.
  c) Three worked examples: input -> internal state change -> output.
     Use real-looking data, not foo/bar.
  d) Failure modes: list at least 4 ways this V1 design breaks.
  e) Acceptance criteria: 3-5 measurable conditions for "done."

STEP 2 -- Adversarial critic. Pick ONE persona that is most likely to
break this layer (security for governance, perf for execution,
ML/NLP for nl_to_sql, etc.). Critic produces:
  a) The two weakest decisions in V1 and *why* they fail.
  b) One realistic end-to-end scenario where V1 leaks/breaks/lies.
  c) Specific schema/contract changes that fix it.

STEP 3 -- Architect revises to V2. Produce:
  a) The revised schema (full, not just diffs).
  b) Marked deltas: a "Changed/Added/Removed" list, one line each.
  c) Open questions for the user -- things V2 still can't decide
     without product input.

OUTPUT RULES:
- No marketing copy, no "comprehensive solution" language.
- No multi-persona panels -- only the architect and one critic.
- No "TBD" in the schema. If you don't know, put it in open questions.
- If a decision is in tension with another layer, name the tension
  explicitly -- don't paper over it.
```
