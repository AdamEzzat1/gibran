# Gibran UI — Frontend

A Vite + React + TypeScript SPA that talks to the gibran HTTP backend
(`src/gibran/ui/server.py`). Built artifacts land in
`../src/gibran/ui/static/`, which the FastAPI server serves at `/` and
which Hatch packs into the Python wheel.

## Build

```bash
cd frontend
npm install
npm run build     # outputs to ../src/gibran/ui/static/
```

After building, `gibran ui` serves the SPA at the root path.

## Dev

```bash
cd frontend
npm install
npm run dev       # vite dev server on :5173, proxies /api/* to :8000
```

In a separate terminal, run the backend with `--port 8000`:

```bash
gibran ui --port 8000 --no-open
```

Then open <http://localhost:5173> -- changes hot-reload, API calls are
proxied to the live FastAPI server.

## Scope of the V0.1 frontend

Three views ship today:

1. **Query Workbench** — NL prompt input, compiled SQL preview, result
   table. Cmd/Ctrl+Enter to run. CSV export on results.
2. **Catalog Browser** — Tree of sources → metrics / dimensions /
   columns with sensitivity badges (PII, restricted, etc.).
3. **Audit Log** — Filterable view of `gibran_query_log`. Non-admin
   identities see only own rows; admins see all. Break-glass rows are
   red-bordered. Cursor pagination.

## Deferred from the original 11-view plan

The handoff in `PHASE_4_UI_HANDOFF.md` planned 11 views; we ship the 3
load-bearing ones. The other 8 are documented follow-ups:

- Source Health Dashboard
- Policy Visualizer (backend endpoint exists; needs UI)
- Materialization Status
- Cache Inspector
- Approval Queue (backend endpoint exists; needs UI)
- Access Anomaly Alerts (backend endpoint exists; needs UI)
- Examples Panel (auto-generated example questions)
- Onboarding tour

These are additive on the foundation here. The API client (`src/api/client.ts`)
already wraps the corresponding backend endpoints, so adding a view is
mostly view-shell + chart rendering.

## What this frontend is NOT (verified)

- **Not visually tested.** This source code compiles to a working SPA
  but the screenshots / GIFs / Lighthouse runs in the handoff require
  a person to actually run `npm install && npm run build && gibran ui`
  and exercise the UI in a browser. No screenshots are committed in
  this drop.
- **Not Tanstack-Query-backed.** Server state lives in component-local
  useState/useEffect. For three views with no shared data this is
  fine; add `@tanstack/react-query` when the view count or
  cross-view caching needs grow.
- **Not Tailwind-styled.** Plain CSS in `src/styles.css` keeps the dep
  count to react + react-dom (+ Vite tooling). A Tailwind drop-in
  would be ~2 hours of work.
- **Vitest unit tests (21) ship** covering the api client (12 tests
  including header injection, error decoding, JSON body serialization),
  IdentitySetup (4 tests), and QueryWorkbench (5 tests for run flow,
  no-match, denial, and network errors). Run via `npm test`.
- **No Playwright E2E tests yet.** The Python side has 29 endpoint
  tests covering every API surface; full-browser smoke (mouse +
  keyboard through the SPA) is the obvious next addition.
