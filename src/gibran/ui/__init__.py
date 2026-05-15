"""Local HTTP UI for gibran (Phase 4).

A small FastAPI app that exposes read + admin operations from gibran's
CLI / core APIs over JSON-RPC-style endpoints. Designed to be served
locally (127.0.0.1) alongside a bundled React SPA that lives in
`gibran/ui/static/`.

Trust boundary: identity comes from the same `IdentityContext` resolver
the CLI uses -- env-var token for dev, JWT bearer for prod. The UI
adds no new auth or governance logic.

This package is OPTIONAL: `pip install gibran[ui]` installs FastAPI +
uvicorn. Without the extras, `from gibran.ui.server import app` raises
ImportError with a clear install hint.
"""
