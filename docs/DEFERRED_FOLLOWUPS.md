# Deferred follow-ups (Phase 4 + Phase 5 close-out)

This drop closes the structural gaps from `PHASE_4_UI_HANDOFF.md` and
the Phase 5 multi-DB foundation. What follows is the **non-shipped**
backlog -- items that are well-understood enough to scope but were
deliberately deferred to keep the drop reviewable.

## Phase 4B — UI views

Five views shipped: Query Workbench, Catalog Browser, Audit Log,
Source Health, Policy Visualizer. Plus the in-Workbench Examples
panel + ErrorBoundary + polished styling.

**Three more views were planned**; the FastAPI endpoints exist for
most of them, so each is a UI-only follow-up:

| View | Backend ready | Estimate |
|---|---|---|
| Source Health Dashboard | ✅ shipped | — |
| Policy Visualizer | ✅ shipped (with /api/roles for picker) | — |
| Examples Panel | ✅ shipped (with /api/examples backend) | — |
| Materialization Status | ⚠️ needs `/api/materialize/list` endpoint | ~3 days |
| Cache Inspector | ⚠️ needs `/api/cache/stats` endpoint | ~2 days |
| Approval Queue | ✅ `/api/approvals/*` | ~2 days |
| Access Anomaly Alerts | ✅ `/api/anomalies/access` | ~2 days |
| Onboarding tour | (frontend-only) | ~2 days |

Total: ~9 days of frontend work to hit the original 11-view plan.

## Phase 4C — verification deferred

The handoff lists a 12-step verification loop. We can't run the
visual parts (screenshots, Lighthouse, axe-core scan) without
human-run browser instances. **Shipped**: deployment doc, backend
suite (29 endpoint tests), **vitest suite (21 frontend tests)**,
JSON-placeholder fallback when no SPA is built.
**Deferred**:

- Screenshots + demo GIF for README
- Lighthouse runs (mobile + desktop)
- axe-core a11y scan for all views
- Playwright E2E tests (~10-15 specs from the handoff plan)
  -- the vitest suite covers unit + component logic; Playwright is
  the cross-view, real-browser, real-server tier still missing.

## Phase 5 — multi-DB beyond what's shipped

Shipped: Dialect enum + Engine protocol + 4 adapters (DuckDB,
Postgres, Snowflake, BigQuery) + Postgres migrations + `gibran init
--engine`. Deferred:

### 5A.1c — connection-to-engine migration (DONE)

Shipped: `result_cache.lookup`, `catalog_generation`, `source_health_generation`,
`bump_*_generation` all accept either a raw connection or an engine.
The DuckDB-only guard around the cache call in `run_sql_query` is
gone. Postgres now participates in caching end-to-end.

Verified by `tests/test_postgres_engine_integration.py` (8 tests
that auto-spin Postgres via `pgserver`, no external service needed)
and the standalone smoke at `tools/postgres_e2e_smoke.py`.

### Snowflake / BigQuery verification

Both adapters ship as **code only**. End-to-end verification needs:
- A paid Snowflake account (with credits to run `SELECT 1`)
- A GCP project with BigQuery billing enabled

The integration test files skip gracefully when `GIBRAN_SNOWFLAKE_URL`
/ `GIBRAN_BIGQUERY_PROJECT` aren't set, so the default test suite
runs free. Once credentials are wired into a CI environment, the
integration tests run automatically.

### 5D follow-up — migrate hardcoded time SQL

The `dsl/dialect_emit.py` registry exists but the existing callers
(`sync/applier.py`'s rolling_window emission, `observability/default.py`
and `observability/runner.py`'s DATE_DIFF calls) still use hardcoded
DuckDB syntax. Migration: replace the literal SQL fragments with
calls to `emitter_for(active_dialect()).date_diff_seconds(...)` etc.
~10 sites; covered by the existing dialect-emit unit tests once
plumbed.

### 5B — multi-tenancy

Not started. Greenfield 6-10 weeks of work; sized in
`PHASE_4_UI_HANDOFF.md` Phase 5B.

### 5C, 5E — charts + dashboards

Not started. Gated on Phase 4B's 8 missing views landing first.

## Phase 5A.5 — known unverified

The 8 Postgres migration translations were mechanical (`JSON` →
`JSONB`, `DEFAULT '[]'::jsonb` casts). They've **not been run against
a real Postgres in CI** -- the integration test stub
(`tests/test_migrations_engine.py::test_apply_all_for_engine_postgres`)
skips without `GIBRAN_POSTGRES_URL`. To verify:

```bash
docker run --rm -d --name pg -e POSTGRES_PASSWORD=t -p 5432:5432 postgres:16
pip install -e .[dev,postgres]
GIBRAN_POSTGRES_URL=postgresql://postgres:t@localhost:5432/postgres \
  pytest tests/test_migrations_engine.py -v
docker stop pg
```

That's a 30-second local verification and should run cleanly. CI
will need a Postgres service container to make it automatic.
