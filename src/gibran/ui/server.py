"""FastAPI app for `gibran ui` (Phase 4A).

Endpoint surface:

  Reads (any authenticated identity):
    GET  /api/catalog                 -> list sources visible to identity
    GET  /api/describe/{source_id}    -> single AllowedSchema with detail
    POST /api/ask                     -> NL prompt -> result OR no-match hint
    POST /api/explain                 -> intent -> compiled SQL (no execute)
    POST /api/query                   -> intent -> executed result
    GET  /api/health/{source_id}      -> latest quality/freshness outcomes

  Admin-only (role.is_break_glass = TRUE):
    GET  /api/log                     -> paginated audit log
    GET  /api/policy/{role_id}        -> AllowedSchema as that role
    POST /api/materialize/{metric_id} -> trigger metric rebuild
    POST /api/touch/{source_id}       -> bump source data_version
    GET  /api/approvals/pending       -> pending changes queue
    POST /api/approvals/{change_id}/approve -> approve a change
    GET  /api/anomalies/access        -> detect_access_anomalies output
    GET  /api/anomalies/{source_id}   -> failed anomaly rules for source

  Static:
    GET  /                            -> bundled React index.html
    GET  /assets/*                    -> bundled React assets

Identity comes from `auth.current_identity` (dev: headers; prod: JWT).
DB path comes from `app.state.db_path`, settable at construction. A
fresh DuckDB connection is opened per request.
"""
from __future__ import annotations

import os
from pathlib import Path

try:
    from fastapi import Depends, FastAPI, HTTPException, Request
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import FileResponse, JSONResponse
    from fastapi.staticfiles import StaticFiles
except ImportError as e:
    raise ImportError(
        "gibran.ui.server requires fastapi + uvicorn. "
        "Install via `pip install gibran[ui]`."
    ) from e

import duckdb

from gibran.ui.auth import current_identity, require_admin
from gibran.ui.models import (
    AskBody,
    HealthResponse,
    IntentBody,
)


STATIC_DIR = Path(__file__).parent / "static"


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app(db_path: str | Path | None = None) -> FastAPI:
    """Construct the FastAPI app, wire up endpoints, mount static files.

    `db_path` defaults to `./gibran.duckdb` (matching the CLI). The path
    is stored on `app.state.db_path` so endpoint dependencies can open
    short-lived connections per request.
    """
    app = FastAPI(
        title="Gibran UI",
        version="0.1",
        description=(
            "Local HTTP UI for gibran -- governed analytics + non-LLM "
            "NL-to-SQL. Run via `gibran ui`."
        ),
        docs_url="/api/docs",
        redoc_url="/api/redoc",
    )

    resolved_path = Path(db_path) if db_path else Path.cwd() / "gibran.duckdb"
    app.state.db_path = resolved_path

    # CORS: deny by default; the SPA is served same-origin so cross-
    # origin requests are an attack surface, not a feature. Operators
    # who DO need cross-origin (e.g. embedding the UI in another app)
    # can set GIBRAN_UI_CORS_ORIGINS=https://example.com,...
    cors_origins = [
        o.strip()
        for o in os.environ.get("GIBRAN_UI_CORS_ORIGINS", "").split(",")
        if o.strip()
    ]
    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["GET", "POST"],
            allow_headers=["*"],
        )

    _wire_endpoints(app)
    _mount_static(app)
    return app


# ---------------------------------------------------------------------------
# Per-request DB dependency
# ---------------------------------------------------------------------------


def db_con(request: Request):
    """Open a fresh DuckDB connection for this request, close on exit.

    Used as a FastAPI Depends. DuckDB is in-process and per-connection
    state is cheap; concurrent reads are fine. Concurrent writes (touch,
    materialize) are serialized via a separate asyncio.Lock around the
    write endpoints (see admin endpoints).
    """
    db_path = request.app.state.db_path
    if not Path(db_path).exists():
        raise HTTPException(
            status_code=503,
            detail=(
                f"no gibran DB at {db_path}; run `gibran init` first. "
                f"Override DB path via GIBRAN_DB_PATH env var or "
                f"`gibran ui --db <path>`."
            ),
        )
    con = duckdb.connect(str(db_path), read_only=False)
    try:
        yield con
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Endpoint wiring
# ---------------------------------------------------------------------------


def _wire_endpoints(app: FastAPI) -> None:
    """Attach all endpoint functions to the app.

    Endpoints are defined in `gibran.ui.endpoints.*` modules and
    registered here -- one source of truth for the URL map."""
    from gibran.ui.endpoints import (
        admin as admin_ep,
        ask as ask_ep,
        catalog as catalog_ep,
        examples as examples_ep,
        health as health_ep,
        log as log_ep,
        policy as policy_ep,
        query as query_ep,
    )

    catalog_ep.register(app)
    ask_ep.register(app)
    query_ep.register(app)
    log_ep.register(app)
    health_ep.register(app)
    policy_ep.register(app)
    examples_ep.register(app)
    admin_ep.register(app)


# ---------------------------------------------------------------------------
# Static file serving
# ---------------------------------------------------------------------------


def _mount_static(app: FastAPI) -> None:
    """Serve the bundled React SPA at `/`, with `/assets/*` for chunks.

    If `static/` doesn't exist (e.g. running from source without a
    frontend build), serves a placeholder HTML explaining the situation.
    """
    if (STATIC_DIR / "index.html").exists():
        # Real build present: mount assets/ AND a catch-all that returns
        # index.html for SPA routing.
        app.mount(
            "/assets",
            StaticFiles(directory=STATIC_DIR / "assets"),
            name="assets",
        )

        @app.get("/", include_in_schema=False)
        async def serve_index():
            return FileResponse(STATIC_DIR / "index.html")

        @app.get("/{full_path:path}", include_in_schema=False)
        async def spa_fallback(full_path: str):
            # /api/* paths are handled by other endpoints (FastAPI's
            # routing tries those first). Anything else is an SPA route.
            if full_path.startswith("api/"):
                raise HTTPException(status_code=404)
            return FileResponse(STATIC_DIR / "index.html")
    else:
        @app.get("/", include_in_schema=False)
        async def serve_placeholder():
            return JSONResponse(
                content={
                    "message": (
                        "Frontend bundle not present (build the React "
                        "app or use the API directly at /api/docs)."
                    ),
                    "api_docs": "/api/docs",
                },
            )


# Default app instance for `uvicorn gibran.ui.server:app`.
app = create_app()
