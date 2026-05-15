# Phase 4 Handoff — Gibran UI

Forward-looking from the close of Phases 1-3 (PR #1, ~16 commits, 603 tests
passing). Phase 4 introduces a **local HTTP server + bundled React UI**
that exposes 11 views — 4 for analysts, 7 for operators — over the
existing Python pipeline. **No new SQL/governance logic is introduced**;
every view is a thin presentation layer over existing CLI commands and
core APIs.

Each phase below follows the stack-role-group panel pattern:
**panel → synthesized direction → tasks → adversarial verification.**

## Context: what this is not

The roadmap originally sized Phase 4 at **4-6 weeks** for 3 views
(catalog browser, query box, audit log viewer). The scope here expands
to **11 views**, which is closer to **8-12 weeks** of single-contributor
work. Two reasonable cuts to V0.1 if needed:

- **Ship the analyst views first** (Workbench, Catalog Browser, Examples,
  Export). Operator views become a Phase 4.5 PR.
- **Or ship analyst + audit + source-health** (~7 views). Defer
  Materialization / Cache / Approval / Access Anomaly to V0.2.

Recommendation: **ship analyst + audit + source-health + policy
visualizer** (7 views) as Phase 4. The other 4 are operator-polish that
matter once there are users but don't drive adoption.

## Locked constraints (binding)

From the roadmap's stop-doing list and Phase 4 panel:

- **DO NOT** add new dependencies that aren't already in the wheel.
  FastAPI/uvicorn go in an optional `[ui]` extras group.
- **DO NOT** ship the React build expecting users to `npm install`.
  Vite output is bundled into `src/gibran/ui/static/` and packed into
  the wheel via Hatch's `force-include`.
- **DO NOT** bind to `0.0.0.0`. Local-only (127.0.0.1) by default.
- **DO NOT** add a new trust boundary. Identity comes from the existing
  resolver (env-var token for dev, JWT for prod) — same as the CLI.
- **DO NOT** introduce a generative-AI surface. The UI exposes gibran's
  existing pattern-template NL; no LLM proxy, no embedding retrieval.
  Tier-5 constraint is binding.

---

## Phase 4A — HTTP Backend (1-1.5 weeks)

### Panel

- **DB Architect:** Every endpoint is a thin wrapper around an existing
  CLI command's underlying function. `/catalog` calls
  `gov.preview_schema()`; `/ask` calls `run_nl_query()`; `/query` calls
  `run_dsl_query()`; `/log` queries `gibran_query_log` directly. No new
  governance logic. The endpoint surface is small and well-typed.
- **Sec/Gov Architect:** The only new attack surface is HTTP. Identity
  must come from the request (env-token or JWT header) and be passed
  unchanged into `IdentityContext` — never trust path/body parameters
  for identity. Local-only bind plus no auth-bypass paths means dev-mode
  security is "the user's machine"; prod hardening (TLS, JWT validation,
  rate limiting) is the operator's responsibility behind a reverse proxy.
- **SRE/Ops:** DuckDB is in-process and single-writer. Concurrent reads
  are fine; concurrent writes (sync, touch, materialize) need
  serialization. FastAPI's default async pool will handle reads; writes
  go through a lock or `asyncio.Lock`.

### Synthesized direction

A FastAPI app at `src/gibran/ui/server.py` exposing ~12 endpoints. Each
endpoint resolves identity from the request, calls into existing
gibran functions, and returns JSON. Static-file serving for the React
build. A new `gibran ui` CLI command starts the server and opens the
browser. Optional `[ui]` extras group so headless installs don't pull
FastAPI/uvicorn.

```python
# src/gibran/ui/server.py (sketch)
from fastapi import FastAPI, Depends, HTTPException
from gibran.governance.types import IdentityContext

app = FastAPI(title="Gibran UI", docs_url="/api/docs")

def current_identity(request: Request) -> IdentityContext:
    """Resolve identity from env-var token (dev) or JWT header (prod).
    Returns 401 if neither is valid."""
    ...

@app.get("/api/catalog")
async def get_catalog(identity: IdentityContext = Depends(current_identity)):
    return {"sources": [...]}  # AllowedSchema per source

@app.post("/api/ask")
async def ask(body: AskBody, identity: IdentityContext = Depends(current_identity)):
    # ... routes to gibran.nl.runner.run_nl_query
```

### Tasks

| # | Task | File |
|---|---|---|
| 4A.1 | New module `gibran/ui/__init__.py` + `gibran/ui/server.py` with FastAPI app skeleton. | `src/gibran/ui/server.py` |
| 4A.2 | Identity resolver dependency: env-var token (dev), JWT header (prod). Returns 401 if neither resolves. | `src/gibran/ui/auth.py` |
| 4A.3 | Endpoint `/api/catalog` -> list sources visible to identity, each with AllowedSchema. | `src/gibran/ui/endpoints/catalog.py` |
| 4A.4 | Endpoint `/api/describe/{source_id}` -> single AllowedSchema with full column/dim/metric detail. | same |
| 4A.5 | Endpoint `POST /api/ask` -> NL prompt -> run_nl_query result OR "no pattern matched" with closest-pattern hint. | `src/gibran/ui/endpoints/ask.py` |
| 4A.6 | Endpoint `POST /api/explain` -> intent (DSL JSON) -> compiled SQL + applied governance (no execute). | `src/gibran/ui/endpoints/query.py` |
| 4A.7 | Endpoint `POST /api/query` -> intent -> run_dsl_query result, with pagination via `limit`/`offset`. | same |
| 4A.8 | Endpoint `GET /api/log` -> paginated audit log entries with filtering (user, role, status, source, time range). Admin-only. | `src/gibran/ui/endpoints/log.py` |
| 4A.9 | Endpoint `GET /api/health/{source_id}` -> latest quality + freshness rule outcomes. | `src/gibran/ui/endpoints/health.py` |
| 4A.10 | Endpoint `GET /api/policy/{role_id}` -> AllowedSchema as the named role would see it. Admin-only. | `src/gibran/ui/endpoints/policy.py` |
| 4A.11 | Endpoint `POST /api/materialize/{metric_id}` (admin) -> calls `_materialize_metrics`. | `src/gibran/ui/endpoints/materialize.py` |
| 4A.12 | Endpoint `POST /api/touch/{source_id}` (admin) -> calls `touch_source`. | `src/gibran/ui/endpoints/cache.py` |
| 4A.13 | Endpoint `GET /api/approvals/pending` + `POST /api/approvals/{change_id}/approve` (admin). | `src/gibran/ui/endpoints/approval.py` |
| 4A.14 | Endpoint `GET /api/anomalies/access` -> output of `detect_access_anomalies` (admin). | `src/gibran/ui/endpoints/anomalies.py` |
| 4A.15 | Endpoint `GET /api/anomalies/{source_id}` -> failed runs of source's anomaly rules (via `anomaly_query` if declared, or direct query). | same |
| 4A.16 | Static file serving for `src/gibran/ui/static/*` -> `/`. | `src/gibran/ui/server.py` |
| 4A.17 | `gibran ui` CLI command: start uvicorn on free port, open browser, handle Ctrl-C. | `src/gibran/cli/main.py` |
| 4A.18 | Optional dependency group `[ui]` in pyproject.toml (fastapi, uvicorn). | `pyproject.toml` |
| 4A.19 | Pydantic models for request/response bodies (AskBody, IntentBody, LogFilter, etc.). | `src/gibran/ui/models.py` |
| 4A.20 | Pagination contract: `cursor`-based for audit log (timestamps), `limit`/`offset` for catalog (small lists). | each endpoint module |

### Adversarial — Sec/Gov critic

> *Challenge 1: Identity comes from an env-var token in dev mode. Doesn't
> that mean anyone with terminal access can impersonate any role?*

Yes — by design. The dev mode runs on the user's own machine and reads
the user's own env. The threat model is "developer running locally,"
not "shared multi-tenant server." Production identity comes through
the JWT path (already exists, unchanged from the CLI). Document the
mode toggle clearly so operators don't accidentally ship dev mode.

> *Challenge 2: The `/api/policy/{role_id}` endpoint lets admins see
> what any role can see, including PII-allowing roles. Doesn't that
> leak which roles have elevated access?*

It surfaces information already in `gibran_policies` (which admins can
read directly). Restricting the endpoint to admin identities only is
the safeguard. Add an audit log row for each policy-visualizer access
so abuse is traceable — same pattern as `gibran query`.

> *Challenge 3: Concurrent writes to DuckDB?*

DuckDB serializes writes internally. The risk is two `gibran touch`
or `gibran materialize` calls racing through different HTTP requests.
Mitigation: wrap write-endpoint handlers in `asyncio.Lock`, scoped to
the FastAPI app. Reads are unrestricted.

---

## Phase 4B — React Frontend (3-5 weeks)

### Panel

- **Frontend Architect:** Vite + React + TypeScript per roadmap. State
  via React hooks + Tanstack Query (server state) — no Redux. Styling
  via Tailwind (no Material UI, no component library — too much bloat
  for 11 views). Code-splitting per view so the initial bundle is small;
  catalog browser shouldn't ship the audit-log filter UI it doesn't use.
- **Analytics Engineer:** The Query Workbench is the leverage view. Its
  three-pane shape (NL input -> compiled preview -> result table) is
  what makes gibran's no-fabrication transparency *visible*. Every other
  view either feeds into this or surfaces what happens around it.
- **A11y Specialist:** Tables with millions of rows need virtualization
  (Tanstack Virtual). All form inputs need labels. Color choices for
  sensitivity badges (PII red, public green) need contrast ≥ 4.5:1.
  Keyboard navigation: cmd+enter to run, esc to cancel, tab through
  results. Don't ship without axe-core passing on every view.

### Synthesized direction

Vite + React 18 + TypeScript scaffold under `frontend/`, building into
`src/gibran/ui/static/`. Three layouts: analyst (sidebar with catalog
+ history, main pane is workbench), admin (sidebar with operator views,
main pane is the selected dashboard), and shared (login/error states).
Per-view code splitting via React.lazy.

```typescript
// frontend/src/views/QueryWorkbench/QueryWorkbench.tsx (sketch)
import { useQuery, useMutation } from '@tanstack/react-query'
import { askApi, queryApi } from '@/api'

export function QueryWorkbench() {
  const [mode, setMode] = useState<'nl' | 'dsl' | 'sql'>('nl')
  const [input, setInput] = useState('')
  const askMutation = useMutation({ mutationFn: askApi })
  // Live preview: debounced call to /api/ask as user types
  // Run button: call /api/query with the compiled intent
  // Result table: virtualized, column-type-aware formatters
  return <ThreePaneLayout
    top={<QueryInput mode={mode} value={input} onChange={setInput} />}
    middle={<CompilePreview result={askMutation.data} />}
    bottom={<ResultTable rows={...} />}
  />
}
```

### Tasks

| # | Task | File |
|---|---|---|
| 4B.1 | Vite + React 18 + TypeScript project scaffold. Tailwind config. ESLint + Prettier. | `frontend/` |
| 4B.2 | Build pipeline outputs to `src/gibran/ui/static/`. Hatch `force-include` packs into wheel. | `frontend/vite.config.ts`, `pyproject.toml` |
| 4B.3 | API client with type-safe wrappers around all backend endpoints. Generated from OpenAPI if FastAPI exposes one, hand-written otherwise. | `frontend/src/api/` |
| 4B.4 | Layout components: `AppShell`, `AnalystLayout`, `AdminLayout`. Sidebar with role-aware view list. | `frontend/src/components/layout/` |
| 4B.5 | **Query Workbench**: 3-pane (input / compiled preview / result table). Tabs for NL / DSL / SQL. Live pattern matching via debounced `/api/ask`. History sidebar with one-click re-run. Cmd+enter to run. | `frontend/src/views/QueryWorkbench/` |
| 4B.6 | **Result Table**: virtualized via Tanstack Virtual. Column-type-aware formatting (numbers right-aligned, dates as ISO, currency unit suffixes). Click cell to copy. | `frontend/src/components/ResultTable/` |
| 4B.7 | **Catalog Browser**: tree of sources -> columns/dimensions/metrics. Sensitivity badges. Click metric -> definition card with depends_on graph (use Reactflow or a hand-rolled SVG for the small case). | `frontend/src/views/CatalogBrowser/` |
| 4B.8 | **"What can I ask?" panel**: auto-generated example questions per pattern, populated from the user's catalog. Click -> fills query box. | `frontend/src/views/QueryWorkbench/ExamplesPanel.tsx` |
| 4B.9 | **Export menu** on Result Table: CSV / JSON / Parquet. Calls API with format param. | `frontend/src/components/ResultTable/ExportMenu.tsx` |
| 4B.10 | **Share URL**: encode intent JSON in URL fragment; on load, decode and populate query workbench. Shared URLs are reproducible (governance still applies on whoever opens it). | `frontend/src/views/QueryWorkbench/useShareLink.ts` |
| 4B.11 | **Audit Log Viewer**: filterable table (user, role, status, source, time range). Click row to expand: full SQL, governance decision, redacted prompt. Break-glass rows visually distinct (red border). Pagination via cursor. | `frontend/src/views/AuditLog/` |
| 4B.12 | **Source Health Dashboard**: per-source status cards. Click -> recent rule runs, observed_value sparkline for anomaly rules. "Source is blocked" badge with reason. | `frontend/src/views/SourceHealth/` |
| 4B.13 | **Policy Visualizer**: role picker -> AllowedSchema per source. Highlight denied columns. Render row filter AST as readable predicate ("region = your assigned region"). "Simulate as this role" button switches workbench identity (admin-only). | `frontend/src/views/PolicyVisualizer/` |
| 4B.14 | **Materialization Status**: list of materialized metrics with strategy / last refresh / row count / stale flag. "Refresh now" button. | `frontend/src/views/Materialization/` |
| 4B.15 | **Cache Inspector**: hit/miss counts, per-source data_version timestamps, "Invalidate source" button. | `frontend/src/views/CacheInspector/` |
| 4B.16 | **Approval Queue**: pending changes table -> click -> diff view + approve button. | `frontend/src/views/ApprovalQueue/` |
| 4B.17 | **Access Anomaly Alerts**: user volume vs baseline chart, "Investigate" button -> Audit Log filtered to that user. | `frontend/src/views/AccessAnomalies/` |
| 4B.18 | **Empty state + error states** for each view. "No pattern matched" with closest-pattern hint and example. Clear denial messaging ("denied: column X is PII, your role doesn't allow PII"). | per-view |
| 4B.19 | **Loading skeletons** (not spinners) for every async view. | `frontend/src/components/Skeleton.tsx` |
| 4B.20 | **Keyboard shortcuts**: cmd+k to focus query input, cmd+enter to run, cmd+/ for shortcuts help. | `frontend/src/hooks/useShortcuts.ts` |
| 4B.21 | **Dark mode** toggle, persisted in localStorage. Tailwind dark variants. | `frontend/src/hooks/useTheme.ts` |
| 4B.22 | **Routing** via Tanstack Router or React Router. URL reflects current view + filter state for shareable links. | `frontend/src/router.tsx` |

### Adversarial — Frontend Architect critic

> *Challenge 1: 11 views is a lot. Bundle size?*

Per-view code splitting via `React.lazy` + dynamic imports keeps the
initial bundle under 200KB gzipped. Each operator view loads on
demand. Critical path (workbench + catalog) is what users actually
see first. Verify with `vite-bundle-visualizer` after build.

> *Challenge 2: Tanstack Query + React hooks instead of Redux — won't
> we regret it once 11 views need to share state?*

The views are mostly independent. The shared state is small:
identity, current source filter, theme. Hooks + Context handle this.
Tanstack Query handles server state (caching, refetching, optimistic
updates). Redux would add ~3 weeks of boilerplate for marginal gain.

> *Challenge 3: Tailwind for styling — won't we regret not picking a
> component library?*

A component library would lock us into its design system. Tailwind +
~15 reusable primitive components (Button, Card, Input, Badge, Table,
Modal, etc.) gives full control without lock-in. Build the primitives
once, use everywhere. Roughly 1 day of upfront work.

---

## Phase 4C — Polish + Documentation (~1 week)

### Panel

- **PM:** README needs screenshots and a GIF showing `gibran ui` in
  action. Without those, the README still reads "CLI tool" and the
  audience-expansion goal misses.
- **A11y Specialist:** axe-core has to pass on every view. Keyboard
  navigation has to work end-to-end. Don't ship without it.
- **SRE/Ops:** Production deployment guide. How to put gibran ui
  behind nginx with TLS, JWT auth, and rate limiting.

### Tasks

| # | Task | File |
|---|---|---|
| 4C.1 | README screenshots (catalog browser, query workbench, audit log). | `README.md`, `docs/screenshots/` |
| 4C.2 | README GIF of `gibran ui` flow (ask question -> see compiled SQL -> run -> export). | `docs/screenshots/ui-demo.gif` |
| 4C.3 | Production deployment guide: nginx config, JWT auth, rate limiting, TLS. | `docs/UI_DEPLOYMENT.md` |
| 4C.4 | Onboarding flow: first-time `gibran ui` opens with a tour overlay. Skippable. | `frontend/src/components/Onboarding/` |
| 4C.5 | Error-boundary at the AppShell level; surface backend errors readably. | `frontend/src/components/ErrorBoundary.tsx` |
| 4C.6 | Toast notifications (success / error / info) using a small library or hand-rolled. | `frontend/src/components/Toast.tsx` |

---

## Testing Requirements (comprehensive)

Per gibran's existing standard: every feature has tests, every PR is
green before merge. Phase 4 expands the test surface significantly
since we now have a JavaScript runtime + browser-side code.

### Backend tests (Python, in `tests/`)

| Category | Tool | Scope |
|---|---|---|
| **Endpoint unit tests** | `pytest` + FastAPI `TestClient` | Each endpoint: happy path, denied path, invalid input, identity-missing. ~5-10 tests per endpoint × ~15 endpoints = 75-150 tests. |
| **Integration tests** | `pytest` + real DuckDB | End-to-end: HTTP request -> governance -> compile -> execute -> response. One per analyst view + one per admin view. ~15 tests. |
| **Concurrency tests** | `pytest` + `httpx.AsyncClient` | Two concurrent writes (`POST /api/touch` x2) serialized correctly via the asyncio.Lock. ~3 tests. |
| **Pagination tests** | `pytest` | Cursor stability under inserts; offset bounds; max page size enforcement. ~5 tests. |

### Frontend tests (`frontend/tests/`)

| Category | Tool | Scope |
|---|---|---|
| **Component unit tests** | Vitest + React Testing Library | Each reusable component: render, props, events, accessibility (toHaveNoViolations). ~3-5 tests per component × ~20 components = 60-100 tests. |
| **View integration tests** | Vitest + MSW (Mock Service Worker) | Each view: rendering with mock API responses, user interactions, error states. ~5-10 per view × 11 views = 55-110 tests. |
| **End-to-end browser tests** | Playwright | Critical user flows: ask a question, see result; browse catalog, click metric; view audit log, expand a row. ~10-15 tests. |
| **Visual regression** *(optional)* | Playwright screenshots | Catch unintended visual changes. Skip in CI, run locally before release. |
| **Accessibility** | axe-core via Playwright | Every view must pass. Fails CI on violations. ~11 tests. |

### Security tests (the critical block)

| Test category | What it pins | Example |
|---|---|---|
| **Auth bypass** | No endpoint serves data without a valid identity. Missing/invalid token -> 401. | `test_catalog_requires_identity`, `test_query_requires_identity` for each endpoint |
| **Identity propagation** | Endpoints use the request's identity, never trust body/path params. POST `/api/query` with `body.role_id="admin"` while authenticated as `analyst_west` -> request uses analyst_west, NOT admin. | `test_body_identity_ignored` |
| **Admin-only endpoints** | `/api/log`, `/api/policy/{}`, `/api/anomalies/*`, `/api/materialize/*`, `/api/touch/*`, `/api/approvals/*` reject non-admin identities. | `test_log_denies_non_admin` per admin endpoint |
| **XSS prevention** | Column values, error messages, audit-log prompts rendered as text via React (not `dangerouslySetInnerHTML`). User input containing `<script>` shows as literal text. | `test_xss_in_metric_name`, `test_xss_in_log_prompt` |
| **CSRF** | State-changing endpoints (POST /api/touch, /api/materialize, /api/approvals) require either a same-origin check or a CSRF token. Cross-origin POST -> 403. | `test_csrf_blocks_cross_origin` |
| **SQL injection** | User input cannot inject SQL. Already prevented by gibran's pipeline (Pydantic + AST), but verify end-to-end at the HTTP boundary. POST `/api/query` with `intent.filters[0].value="x'; DROP TABLE orders; --"` executes safely. | `test_sql_injection_in_intent` |
| **Identity spoofing via headers** | Multiple `X-User-Id` headers / case variations resolve to ONE identity deterministically (the first valid one). | `test_duplicate_identity_headers` |
| **Rate limiting** | If implemented: N requests / minute / identity. 401 returns count separately from successful. | `test_rate_limit_per_identity` |
| **Local-only bind** | `gibran ui` defaults to 127.0.0.1. `--host 0.0.0.0` requires explicit override + warning. | `test_default_bind_is_localhost` |
| **CORS** | Default allow-list is empty (UI served same-origin). Cross-origin requests get rejected. | `test_cors_blocks_cross_origin` |
| **Audit-log access control** | Non-admin identities see only their own queries in `/api/log`. Admin sees all. | `test_log_filters_to_own_queries_for_non_admin` |
| **Result-set size limit** | Endpoints enforce `intent.limit <= MAX_PAGE_SIZE` (10k per existing Pydantic). Bigger requests rejected with 400. | `test_oversized_limit_rejected` |
| **Path traversal** | `/api/describe/{source_id}` with `../etc/passwd` doesn't escape the source ID. | `test_path_traversal_in_source_id` |
| **Open redirect** | `/` page doesn't honor a `?next=` parameter (no auth flow exists; just guard against future temptation). | `test_no_open_redirect` |
| **Secrets in error messages** | Backend errors don't leak SQL, env-var names, file paths. Production error responses are user-safe; details go to logs. | `test_error_response_redacts_internals` |

### Performance tests

| Test | Tool | Threshold |
|---|---|---|
| First contentful paint < 1.5s | Lighthouse CI | Initial bundle < 200KB gzipped |
| Query workbench keystroke -> compile preview < 200ms | Manual + Playwright trace | Debounce at 150ms |
| Audit log scroll past 1000 rows smooth | Manual | Virtualized; no jank |
| `/api/query` p95 < 500ms for small queries | pytest-benchmark | Baseline = CLI `gibran query` time |
| `gibran ui` cold start < 3s | Manual | Measure from CLI invocation to browser-ready |

### Test count targets

- **Backend:** 90-160 new tests (~150 mid-estimate)
- **Frontend unit/integration:** 115-210 new tests (~160 mid-estimate)
- **End-to-end (Playwright):** 10-15 tests
- **Security (subset of backend):** ~20 tests called out in the security section above
- **A11y:** 11 tests (one per view via axe-core)

**Total estimate: ~300-400 new tests.** Suite size: 603 -> 900-1000.
Wall-clock should stay under 10 minutes for backend + unit; Playwright
end-to-end is a separate ~3-5 minute job.

---

## Security Hardening Specifics

### Identity flow

```
Request comes in
  -> middleware: extract identity from
       (a) X-Gibran-Token header (dev mode, matched against env var)
       (b) Authorization: Bearer <jwt> (prod mode, validated against JWKS)
  -> resolve to IdentityContext (user_id, role_id, attributes)
  -> if neither resolves: 401 Unauthorized
  -> attach to request.state.identity
  -> endpoint reads via Depends(current_identity)
  -> NEVER reads identity from request body or path
```

The CLI already does this via `gibran.cli.main` — the UI's identity
middleware should call the same `resolve_identity()` function for
consistency. **Do not** reimplement; reuse.

### Admin-only endpoints

Admin identity is determined by the role's `is_break_glass` flag OR
a configured admin role allowlist. Endpoints check this via:

```python
def require_admin(identity: IdentityContext = Depends(current_identity)) -> IdentityContext:
    if not is_admin(identity):
        raise HTTPException(403, "admin role required")
    return identity
```

Admin endpoints depend on `require_admin` instead of `current_identity`.
Tests verify each admin endpoint rejects non-admin.

### Audit logging

**Every UI request that reads data is logged to `gibran_query_log`** —
same row format as the CLI. The audit log itself is the source of
truth for "who saw what." The Audit Log Viewer reads from this table;
the same table records each Viewer access (recursive but bounded —
no infinite expansion since reads don't re-write).

Policy visualizer access is logged too. Pattern:

```python
@app.get("/api/policy/{role_id}")
async def get_policy(role_id: str, identity = Depends(require_admin)):
    log_admin_action(identity, "policy_view", {"target_role": role_id})
    return ...
```

### Local-only by default

```python
def main():
    uvicorn.run(app, host="127.0.0.1", port=find_free_port())
```

Override via `gibran ui --host 0.0.0.0` requires:
- A warning printed to stderr
- A confirmation prompt unless `--yes` is passed
- A check that JWT auth is configured (env-var dev mode rejected with `--host` not localhost)

### Static file CSP

Bundled HTML includes a strict Content-Security-Policy:

```html
<meta http-equiv="Content-Security-Policy"
      content="default-src 'self';
               script-src 'self';
               style-src 'self' 'unsafe-inline';
               connect-src 'self';
               img-src 'self' data:;">
```

Test: a stub view that tries to `eval()` should fail in browsers.

### Dependency audit

Before merge:
- `npm audit --production` clean (or each finding has a documented suppression with reasoning)
- `pip-audit` clean for the `[ui]` extras group

---

## File-system plan

```
src/gibran/ui/
  __init__.py
  server.py              # FastAPI app
  auth.py                # identity middleware
  models.py              # Pydantic request/response models
  endpoints/
    catalog.py
    ask.py               # /api/ask, /api/explain
    query.py             # /api/query, with pagination
    log.py               # /api/log (admin)
    health.py            # /api/health/{source}
    policy.py            # /api/policy/{role} (admin)
    materialize.py       # /api/materialize/* (admin)
    cache.py             # /api/touch/* (admin)
    approval.py          # /api/approvals/* (admin)
    anomalies.py         # /api/anomalies/* (admin)
  static/                # bundled Vite output (built artifact, not tracked)
    index.html
    assets/...

frontend/
  package.json
  package-lock.json
  vite.config.ts
  tsconfig.json
  tailwind.config.ts
  postcss.config.js
  index.html
  .eslintrc.cjs
  .prettierrc
  src/
    main.tsx             # React entry
    App.tsx              # Root with routing
    api/
      client.ts          # Typed fetch wrapper
      catalog.ts
      ask.ts
      query.ts
      log.ts
      health.ts
      policy.ts
    components/
      layout/
        AppShell.tsx
        Sidebar.tsx
      primitives/        # Tailwind-based base components
        Button.tsx
        Card.tsx
        Input.tsx
        Badge.tsx
        Modal.tsx
        Table.tsx
      ResultTable/
      Skeleton.tsx
      Toast.tsx
      ErrorBoundary.tsx
      Onboarding/
    views/
      QueryWorkbench/
        QueryWorkbench.tsx
        ExamplesPanel.tsx
        useShareLink.ts
      CatalogBrowser/
      AuditLog/
      SourceHealth/
      PolicyVisualizer/
      Materialization/
      CacheInspector/
      ApprovalQueue/
      AccessAnomalies/
    hooks/
      useIdentity.ts
      useShortcuts.ts
      useTheme.ts
    types/
      api.ts             # Generated from OpenAPI or hand-written
    router.tsx
  tests/
    unit/                # Vitest component tests
    integration/         # Vitest view tests with MSW
    e2e/                 # Playwright
      smoke.spec.ts
      query-workbench.spec.ts
      catalog-browser.spec.ts
      audit-log.spec.ts
      a11y.spec.ts

tests/                   # Existing pytest tests, extended:
  test_ui_endpoints.py   # Backend endpoint tests
  test_ui_security.py    # Security tests (auth bypass, XSS, CSRF, etc.)
  test_ui_concurrency.py # asyncio.Lock + concurrent write tests
```

---

## Sequencing recommendation

**Three PRs**, each independently mergeable:

1. **PR: Phase 4A — backend** (1-1.5 weeks of work, ~150 tests)
   - All endpoints + auth + tests + `gibran ui` CLI command
   - Serves a placeholder HTML page (no React yet)
   - Frontend developers can start mocking against this immediately

2. **PR: Phase 4B — frontend** (3-5 weeks, ~200 tests)
   - Full React app under `frontend/`
   - Vite build wired into Hatch
   - All 11 views (or the 7-view MVP cut)
   - Vitest + Playwright tests

3. **PR: Phase 4C — polish + docs** (~1 week)
   - README screenshots + GIF
   - Production deployment guide
   - Onboarding tour, error boundaries, toasts

---

## What's NOT in Phase 4 (explicit non-goals)

| Item | Why deferred |
|---|---|
| Multi-tenancy | V2. Phase 4 UI is single-tenant local-only. |
| Real-time dashboards / WebSockets | The Source Health view refreshes on user action, not push. WebSocket layer is V0.2. |
| User-defined dashboards / saved layouts | "Pin this query" is the V0.1 story. Free-form dashboard composition is V0.2. |
| Permission UI for editing policies | View-only in V0.1. Edits still go through YAML + `gibran sync` + `gibran approve`. |
| Generative AI (LLM proxies, embedding retrieval) | Locked Tier-5 constraint. Pattern templates are the NL layer. |
| Mobile responsive | Desktop-first. The audience is "analyst at their workstation." Mobile lands in V0.2 if there's demand. |
| Server-side rendering / Next.js | Local-only HTTP server doesn't need SSR. Static SPA is the right shape. |
| Internationalization | English only for V0.1. i18n string extraction can land later without rework if components are written cleanly. |

---

## Verification loop (before declaring Phase 4 done)

Run this checklist sequentially. Each step gates the next.

1. **Backend suite green:** `pytest tests/test_ui_*.py` — all 150+ pass.
2. **Frontend unit suite green:** `cd frontend && npm test` — all 200+ pass.
3. **Frontend a11y suite green:** Playwright + axe-core — zero violations across all 11 views.
4. **End-to-end smoke:** Playwright runs query-workbench.spec.ts on a fresh `gibran init --sample` setup. Asks "top 5 region by gross revenue", sees result, exports CSV, downloads file.
5. **Security suite green:** every test in `tests/test_ui_security.py` passes. No XSS, no CSRF bypass, no identity spoof.
6. **`gibran ui` cold start:** time from CLI invocation to browser displaying QueryWorkbench < 3 seconds on a fresh repo.
7. **Bundle size:** `vite-bundle-visualizer` -> initial bundle < 200KB gzipped.
8. **Lighthouse:** mobile + desktop scores > 90 for Performance, Accessibility, Best Practices.
9. **Deps clean:** `npm audit --production` and `pip-audit` both clean (or suppressions documented).
10. **README updated:** screenshots + GIF + new `gibran ui` row in the CLI reference table.
11. **No regressions:** full Python suite (Phases 1-3 + new Phase 4 backend) green. 900+ tests total.
12. **Manual smoke on real data:** use the UI for 30 minutes on actual production-shaped data. Note any frictions in a follow-up issue list.

---

## Quick-start for the next session

```bash
# 1. Update local from PR #1's merge (assumes it's merged)
git fetch origin
git checkout master
git pull

# 2. Start Phase 4A on a fresh branch
git checkout -b phase-4a-ui-backend

# 3. Scaffold the FastAPI app
mkdir -p src/gibran/ui/endpoints
# ... create server.py, auth.py, etc. per the task list above

# 4. Add the optional deps group
# Edit pyproject.toml: add [project.optional-dependencies] ui = ["fastapi>=0.110", "uvicorn>=0.27"]

# 5. Install dev + ui deps
pip install -e ".[dev,ui]"

# 6. First endpoint to land: GET /api/catalog
#    Write the test first (tests/test_ui_endpoints.py::TestCatalogEndpoint)
#    Then implement the endpoint
#    Iterate
```

The first PR should be small enough to review in one sitting. Start
with the `/api/catalog` and `/api/ask` endpoints + identity middleware
+ their tests. Land that. Then the rest of the endpoints in a follow-up.

Good luck — this is the audience-expansion phase, and getting it right
is what turns gibran from "tool for engineers who write YAML" into
"tool an analyst can use."
