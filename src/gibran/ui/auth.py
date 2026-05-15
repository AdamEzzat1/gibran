"""Identity resolution for the UI's FastAPI app.

Two resolvers, matched to gibran's existing identity model:

  - Dev mode: reads `X-Gibran-User`, `X-Gibran-Role`, `X-Gibran-Attrs`
    request headers. The headers are sufficient because the UI runs
    locally for the user's own session; there's no real auth boundary
    on `127.0.0.1`. `X-Gibran-Attrs` is a comma-separated `k=v` list,
    matching the EnvResolver format.

  - Prod mode: validates a Bearer JWT from the `Authorization` header.
    Reuses `gibran.governance.identity.JWTResolver` with operator-
    provided JWKS URL or static key (configured at server start).

Mode toggle: env-var `GIBRAN_UI_AUTH_MODE` with values `dev` (default)
or `jwt`. Mismatched config (e.g. JWT mode without JWKS URL) raises at
startup, not on first request.

Admin check: an identity is admin if the role's `is_break_glass` flag
is TRUE. `require_admin` is a FastAPI dependency that wraps
`current_identity` and 403s when the role isn't break-glass.
"""
from __future__ import annotations

import os
from typing import Any

try:
    from fastapi import Depends, Header, HTTPException, Request
except ImportError as e:
    raise ImportError(
        "gibran.ui requires fastapi + uvicorn. "
        "Install via `pip install gibran[ui]`."
    ) from e

from gibran.governance.identity import JWTResolver
from gibran.governance.types import IdentityContext


def _resolve_dev_identity(
    user: str | None,
    role: str | None,
    attrs_raw: str | None,
) -> IdentityContext:
    """Build an IdentityContext from dev-mode request headers.

    Headers:
      X-Gibran-User:  required, the user_id
      X-Gibran-Role:  required, the role_id
      X-Gibran-Attrs: optional, "k1=v1,k2=v2" attribute string
    """
    if not user or not role:
        raise HTTPException(
            status_code=401,
            detail=(
                "dev-mode identity requires X-Gibran-User and "
                "X-Gibran-Role headers"
            ),
        )
    attrs: dict[str, str] = {}
    if attrs_raw:
        for pair in attrs_raw.split(","):
            pair = pair.strip()
            if not pair:
                continue
            if "=" not in pair:
                raise HTTPException(
                    status_code=401,
                    detail=f"X-Gibran-Attrs pair must be k=v, got {pair!r}",
                )
            k, v = pair.split("=", 1)
            attrs[k.strip()] = v.strip()
    return IdentityContext(
        user_id=user, role_id=role, attributes=attrs, source="ui-dev",
    )


# Module-level JWT resolver, lazily constructed from env at first use.
_jwt_resolver: JWTResolver | None = None


def _get_jwt_resolver() -> JWTResolver:
    """Build (and memoize) a JWT resolver from env vars.

    Env vars consulted:
      GIBRAN_UI_JWT_JWKS_URL  -- production: validates against rotating keys
      GIBRAN_UI_JWT_STATIC_KEY -- testing / HS256
      GIBRAN_UI_JWT_AUDIENCE  -- optional
      GIBRAN_UI_JWT_ISSUER    -- optional
    """
    global _jwt_resolver
    if _jwt_resolver is not None:
        return _jwt_resolver
    jwks_url = os.environ.get("GIBRAN_UI_JWT_JWKS_URL")
    static_key = os.environ.get("GIBRAN_UI_JWT_STATIC_KEY")
    if not (jwks_url or static_key):
        raise HTTPException(
            status_code=500,
            detail=(
                "JWT auth requested but neither GIBRAN_UI_JWT_JWKS_URL "
                "nor GIBRAN_UI_JWT_STATIC_KEY is set"
            ),
        )
    _jwt_resolver = JWTResolver(
        jwks_url=jwks_url,
        static_key=static_key,
        audience=os.environ.get("GIBRAN_UI_JWT_AUDIENCE"),
        issuer=os.environ.get("GIBRAN_UI_JWT_ISSUER"),
    )
    return _jwt_resolver


def current_identity(
    request: Request,
    x_gibran_user: str | None = Header(default=None),
    x_gibran_role: str | None = Header(default=None),
    x_gibran_attrs: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
) -> IdentityContext:
    """FastAPI dependency that resolves the request's IdentityContext.

    Mode is picked by `GIBRAN_UI_AUTH_MODE` env var:
      "dev" (default) -- X-Gibran-* headers
      "jwt"           -- Authorization: Bearer <token>

    Returns 401 if the relevant credentials are missing or invalid.
    """
    mode = os.environ.get("GIBRAN_UI_AUTH_MODE", "dev").lower()
    if mode == "dev":
        return _resolve_dev_identity(x_gibran_user, x_gibran_role, x_gibran_attrs)
    if mode == "jwt":
        if not authorization:
            raise HTTPException(
                status_code=401,
                detail="missing Authorization header",
            )
        try:
            return _get_jwt_resolver().resolve(authorization)
        except Exception as e:
            raise HTTPException(status_code=401, detail=f"invalid JWT: {e}") from e
    raise HTTPException(
        status_code=500,
        detail=f"unrecognized GIBRAN_UI_AUTH_MODE={mode!r}; expected 'dev' or 'jwt'",
    )


def require_admin(
    identity: IdentityContext = Depends(current_identity),
    request: Request = None,  # type: ignore[assignment]
) -> IdentityContext:
    """FastAPI dependency that wraps `current_identity` and 403s when
    the role isn't break-glass.

    The admin check requires a DB connection -- we read it from
    `request.app.state.db_path` and open a short-lived connection.
    Connection is closed before returning."""
    import duckdb

    db_path = getattr(request.app.state, "db_path", None) if request else None
    if db_path is None:
        # No DB configured (e.g. in tests using monkeypatched state).
        # Default-deny admin to avoid surprising privilege escalation.
        raise HTTPException(
            status_code=403,
            detail="admin check requires a DB connection",
        )
    con = duckdb.connect(str(db_path))
    try:
        row = con.execute(
            "SELECT is_break_glass FROM gibran_roles WHERE role_id = ?",
            [identity.role_id],
        ).fetchone()
    finally:
        con.close()
    is_admin = bool(row[0]) if row is not None else False
    if not is_admin:
        raise HTTPException(
            status_code=403,
            detail=f"admin role required; {identity.role_id!r} is not break-glass",
        )
    return identity
