"""IdentityResolver implementations.

Three resolvers, each scoped to a different deployment context:

- JWTResolver: production. Validates a bearer JWT (RS256 + JWKS, or HS256
  with a static key), projects claims into IdentityContext.
- EnvResolver: local development. Reads RUMI_DEV_USER / RUMI_DEV_ROLE /
  RUMI_DEV_ATTRS. Refuses to operate when RUMI_ENV != 'dev' to prevent
  accidental prod use.
- CLIResolver: operator / CI scripts. Accepts explicit user/role/attributes
  via constructor; gated behind --allow-unauth at the CLI layer."""
from __future__ import annotations

import os
from typing import Any

import jwt
from jwt import PyJWKClient

from rumi.governance.types import IdentityContext


class JWTResolver:
    """Validate a bearer JWT and project its claims into an IdentityContext.

    Two modes:
      - JWKS-backed (production): pass jwks_url; JWKS is fetched and cached
        by PyJWKClient. Verifies signature against the rotating set of keys.
      - Static-key (testing / HS256): pass static_key + algorithms.

    Claim mapping:
      sub                -> user_id        (required)
      role  (custom)     -> role_id        (defaults to 'default' if absent)
      attrs (custom dict)-> attributes     (non-scalar values are dropped)
    """

    def __init__(
        self,
        *,
        jwks_url: str | None = None,
        static_key: str | None = None,
        algorithms: list[str] | None = None,
        audience: str | None = None,
        issuer: str | None = None,
    ) -> None:
        if (jwks_url is None) == (static_key is None):
            raise ValueError(
                "JWTResolver: exactly one of jwks_url or static_key must be provided"
            )
        self.jwks_url = jwks_url
        self.static_key = static_key
        self.audience = audience
        self.issuer = issuer
        if algorithms is None:
            algorithms = ["RS256"] if jwks_url else ["HS256"]
        self.algorithms = algorithms
        self._jwks_client = PyJWKClient(jwks_url) if jwks_url else None

    def resolve(self, request_context: object) -> IdentityContext:
        """Extract + validate the JWT from request_context, return IdentityContext.

        `request_context` may be:
          - a str: the raw token, optionally prefixed with 'Bearer '
          - a dict with 'token' or 'authorization' key
          - any object with a `.token` attribute
        """
        token = self._extract_token(request_context)
        key = self._signing_key_for(token)

        decode_kwargs: dict[str, Any] = {"algorithms": self.algorithms}
        if self.audience is not None:
            decode_kwargs["audience"] = self.audience
        if self.issuer is not None:
            decode_kwargs["issuer"] = self.issuer

        decoded = jwt.decode(token, key, **decode_kwargs)

        if "sub" not in decoded:
            raise ValueError("JWT missing required 'sub' claim")

        raw_attrs = decoded.get("attrs", {}) or {}
        if not isinstance(raw_attrs, dict):
            raise ValueError("JWT 'attrs' claim must be an object")
        # Coerce scalars to str; drop non-scalar values (lists, nested dicts)
        # to keep the attributes mapping flat -- policy AST expects string values.
        attrs: dict[str, str] = {}
        for k, v in raw_attrs.items():
            if isinstance(v, (str, int, float, bool)):
                attrs[str(k)] = str(v)

        return IdentityContext(
            user_id=str(decoded["sub"]),
            role_id=str(decoded.get("role", "default")),
            attributes=attrs,
            source="jwt",
        )

    def _signing_key_for(self, token: str) -> Any:
        if self.static_key is not None:
            return self.static_key
        assert self._jwks_client is not None  # narrowed by constructor invariant
        return self._jwks_client.get_signing_key_from_jwt(token).key

    @staticmethod
    def _extract_token(request_context: object) -> str:
        if isinstance(request_context, str):
            return _strip_bearer(request_context)
        if isinstance(request_context, dict):
            if "token" in request_context:
                return _strip_bearer(str(request_context["token"]))
            if "authorization" in request_context:
                return _strip_bearer(str(request_context["authorization"]))
        token_attr = getattr(request_context, "token", None)
        if token_attr is not None:
            return _strip_bearer(str(token_attr))
        raise ValueError(
            "JWTResolver.resolve: could not extract token from request_context; "
            "expected str, dict with 'token'/'authorization', or object with .token"
        )


def _strip_bearer(s: str) -> str:
    s = s.strip()
    if s.lower().startswith("bearer "):
        return s[len("bearer "):].strip()
    return s


class EnvResolver:
    """Local-dev resolver. Reads RUMI_DEV_USER, RUMI_DEV_ROLE, RUMI_DEV_ATTRS.

    Attributes are encoded as comma-separated key=value pairs:
      RUMI_DEV_ATTRS='region=west,partner_id=acme'

    Refuses to operate when RUMI_ENV != 'dev' (prevents accidental prod use)."""

    def resolve(self, request_context: object) -> IdentityContext:
        if os.environ.get("RUMI_ENV") != "dev":
            raise RuntimeError("EnvResolver requires RUMI_ENV=dev")
        user = os.environ.get("RUMI_DEV_USER")
        role = os.environ.get("RUMI_DEV_ROLE")
        if not user or not role:
            raise RuntimeError(
                "EnvResolver requires RUMI_DEV_USER and RUMI_DEV_ROLE"
            )
        attrs: dict[str, str] = {}
        raw_attrs = os.environ.get("RUMI_DEV_ATTRS", "")
        for pair in raw_attrs.split(","):
            pair = pair.strip()
            if not pair:
                continue
            if "=" not in pair:
                raise RuntimeError(
                    f"RUMI_DEV_ATTRS pair must be key=value, got {pair!r}"
                )
            k, v = pair.split("=", 1)
            attrs[k.strip()] = v.strip()
        return IdentityContext(
            user_id=user, role_id=role, attributes=attrs, source="env",
        )


class CLIResolver:
    """Operator/CI resolver. Accepts explicit user/role/attributes.
    Gated behind --allow-unauth at the CLI layer."""

    def __init__(
        self,
        user_id: str,
        role_id: str,
        attributes: dict[str, str] | None = None,
    ) -> None:
        self._ident = IdentityContext(
            user_id=user_id, role_id=role_id,
            attributes=dict(attributes or {}), source="cli",
        )

    def resolve(self, request_context: object) -> IdentityContext:
        return self._ident
