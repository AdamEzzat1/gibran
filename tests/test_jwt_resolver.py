"""JWTResolver tests.

We test against the HS256 static-key path -- it's the same `jwt.decode`
code path as the JWKS production path, just with a different way of
producing the key. JWKS-against-real-endpoints is integration-test
territory."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import jwt
import pytest

from gibran.governance.identity import JWTResolver


SECRET = "test-secret-do-not-use-in-prod"


def _encode(payload: dict, *, secret: str = SECRET) -> str:
    return jwt.encode(payload, secret, algorithm="HS256")


class TestConstructorContract:
    def test_requires_one_of_jwks_or_static_key(self) -> None:
        with pytest.raises(ValueError, match="exactly one"):
            JWTResolver()  # type: ignore[call-arg]
        with pytest.raises(ValueError, match="exactly one"):
            JWTResolver(jwks_url="x", static_key="y")

    def test_static_key_defaults_to_hs256(self) -> None:
        r = JWTResolver(static_key=SECRET)
        assert r.algorithms == ["HS256"]

    def test_jwks_url_defaults_to_rs256(self) -> None:
        # Doesn't fetch JWKS until resolve() is called
        r = JWTResolver(jwks_url="https://idp.example/jwks.json")
        assert r.algorithms == ["RS256"]


class TestResolve:
    def test_happy_path_projects_claims(self) -> None:
        token = _encode({
            "sub": "alice", "role": "analyst_west",
            "attrs": {"region": "west", "tier": "gold"},
            "iat": int(datetime.now(timezone.utc).timestamp()),
        })
        r = JWTResolver(static_key=SECRET)
        ident = r.resolve(token)
        assert ident.user_id == "alice"
        assert ident.role_id == "analyst_west"
        assert ident.attributes == {"region": "west", "tier": "gold"}
        assert ident.source == "jwt"

    def test_missing_role_defaults_to_default(self) -> None:
        token = _encode({"sub": "alice"})
        r = JWTResolver(static_key=SECRET)
        ident = r.resolve(token)
        assert ident.role_id == "default"

    def test_missing_sub_rejected(self) -> None:
        token = _encode({"role": "x"})
        r = JWTResolver(static_key=SECRET)
        with pytest.raises(ValueError, match="missing required 'sub'"):
            r.resolve(token)

    def test_bearer_prefix_stripped(self) -> None:
        token = _encode({"sub": "alice"})
        r = JWTResolver(static_key=SECRET)
        ident = r.resolve(f"Bearer {token}")
        assert ident.user_id == "alice"

    def test_dict_with_authorization_header(self) -> None:
        token = _encode({"sub": "alice"})
        r = JWTResolver(static_key=SECRET)
        ident = r.resolve({"authorization": f"Bearer {token}"})
        assert ident.user_id == "alice"

    def test_dict_with_token_key(self) -> None:
        token = _encode({"sub": "alice"})
        r = JWTResolver(static_key=SECRET)
        ident = r.resolve({"token": token})
        assert ident.user_id == "alice"

    def test_object_with_token_attr(self) -> None:
        token = _encode({"sub": "alice"})

        class Req:
            def __init__(self, t): self.token = t

        r = JWTResolver(static_key=SECRET)
        ident = r.resolve(Req(token))
        assert ident.user_id == "alice"

    def test_signature_validation_rejects_tampered_token(self) -> None:
        token = _encode({"sub": "alice"}, secret="different-secret")
        r = JWTResolver(static_key=SECRET)
        with pytest.raises(jwt.InvalidSignatureError):
            r.resolve(token)

    def test_audience_enforced(self) -> None:
        token = _encode({"sub": "alice", "aud": "gibran"})
        r = JWTResolver(static_key=SECRET, audience="other_service")
        with pytest.raises(jwt.InvalidAudienceError):
            r.resolve(token)

    def test_audience_accepted_when_matching(self) -> None:
        token = _encode({"sub": "alice", "aud": "gibran"})
        r = JWTResolver(static_key=SECRET, audience="gibran")
        ident = r.resolve(token)
        assert ident.user_id == "alice"

    def test_issuer_enforced(self) -> None:
        token = _encode({"sub": "alice", "iss": "evil.example"})
        r = JWTResolver(static_key=SECRET, issuer="gibran.example")
        with pytest.raises(jwt.InvalidIssuerError):
            r.resolve(token)

    def test_expired_token_rejected(self) -> None:
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        token = _encode({"sub": "alice", "exp": int(past.timestamp())})
        r = JWTResolver(static_key=SECRET)
        with pytest.raises(jwt.ExpiredSignatureError):
            r.resolve(token)


class TestAttributeCoercion:
    def test_non_scalar_attrs_dropped(self) -> None:
        token = _encode({
            "sub": "alice",
            "attrs": {
                "region": "west",
                "groups": ["a", "b"],     # list -> dropped
                "nested": {"x": 1},        # dict -> dropped
                "tier": "gold",
            },
        })
        r = JWTResolver(static_key=SECRET)
        ident = r.resolve(token)
        assert ident.attributes == {"region": "west", "tier": "gold"}

    def test_numeric_attrs_coerced_to_string(self) -> None:
        token = _encode({
            "sub": "alice",
            "attrs": {"region": "west", "level": 5},
        })
        r = JWTResolver(static_key=SECRET)
        ident = r.resolve(token)
        assert ident.attributes["level"] == "5"

    def test_attrs_must_be_object(self) -> None:
        token = _encode({"sub": "alice", "attrs": "not-a-dict"})
        r = JWTResolver(static_key=SECRET)
        with pytest.raises(ValueError, match="'attrs' claim must be an object"):
            r.resolve(token)
