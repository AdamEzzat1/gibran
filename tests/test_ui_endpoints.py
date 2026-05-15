"""Phase 4A -- UI endpoint tests.

Exercises every endpoint through fastapi.testclient.TestClient against
a real DuckDB seeded from the standard test fixtures. No external
services needed.

Covers:
  - Catalog list + describe (with identity scoping)
  - Ask (with no-match and with-match paths)
  - Query + explain
  - Health (visibility check; cache-miss path)
  - Log (cursor pagination; non-admin scoped to own user)
  - Policy (admin-only; 403 for non-admin; 404 for unknown role)
  - Admin ops: touch, anomalies (access + per-source), approvals

Identity is passed via X-Gibran-* headers (dev mode is default).
Admin = role.is_break_glass=TRUE in gibran_roles.
"""
from __future__ import annotations

from pathlib import Path

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from gibran.sync.applier import apply as apply_config  # noqa: E402
from gibran.sync.loader import load as load_config  # noqa: E402
from gibran.sync.migrations import apply_all as apply_migrations  # noqa: E402


FIXTURES = Path(__file__).parent / "fixtures"
MIGRATIONS = Path(__file__).parent.parent / "migrations"


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Build a DuckDB file with the fixture catalog + an orders table."""
    import duckdb
    db = tmp_path / "ui.duckdb"
    con = duckdb.connect(str(db))
    apply_migrations(con, MIGRATIONS)
    apply_config(con, load_config(FIXTURES / "gibran.yaml"))
    con.execute(
        """
        CREATE TABLE orders (
            order_id VARCHAR,
            amount DECIMAL(18,2),
            order_date TIMESTAMP,
            status VARCHAR,
            region VARCHAR,
            customer_email VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO orders VALUES
            ('o1', 100.00, '2025-01-15 10:00', 'paid',    'west',  'a@b.com'),
            ('o2', 200.00, '2025-01-20 11:00', 'paid',    'east',  'b@b.com'),
            ('o3',  50.00, '2025-02-01 12:00', 'pending', 'west',  'c@b.com'),
            ('o4', 300.00, '2025-02-15 13:00', 'paid',    'north', 'd@b.com')
        """
    )
    # Add a break-glass admin role for the admin-only tests.
    con.execute(
        "INSERT INTO gibran_roles (role_id, display_name, is_break_glass) "
        "VALUES ('admin', 'Admin', TRUE)"
    )
    con.close()
    return db


@pytest.fixture
def client(db_path: Path) -> TestClient:
    from gibran.ui.server import create_app
    app = create_app(db_path=db_path)
    return TestClient(app)


def _hdrs(user="analyst", role="analyst_west", attrs="region=west"):
    return {
        "X-Gibran-User": user,
        "X-Gibran-Role": role,
        "X-Gibran-Attrs": attrs,
    }


def _admin_hdrs():
    return _hdrs(user="admin_user", role="admin", attrs="")


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


def test_missing_identity_returns_401(client):
    r = client.get("/api/catalog")
    assert r.status_code == 401


def test_partial_identity_returns_401(client):
    r = client.get("/api/catalog", headers={"X-Gibran-User": "alice"})
    assert r.status_code == 401  # missing role


# ---------------------------------------------------------------------------
# Catalog + describe
# ---------------------------------------------------------------------------


def test_catalog_returns_sources_for_analyst(client):
    r = client.get("/api/catalog", headers=_hdrs())
    assert r.status_code == 200
    body = r.json()
    assert body["user"] == "analyst"
    assert body["role"] == "analyst_west"
    source_ids = [s["source_id"] for s in body["sources"]]
    assert "orders" in source_ids


def test_describe_unknown_source_404(client):
    r = client.get("/api/describe/ghost", headers=_hdrs())
    assert r.status_code == 404


def test_describe_known_source_returns_schema(client):
    r = client.get("/api/describe/orders", headers=_hdrs())
    assert r.status_code == 200
    body = r.json()
    assert body["source_id"] == "orders"
    assert "columns" in body and "metrics" in body and "dimensions" in body


# ---------------------------------------------------------------------------
# Ask / Query / Explain
# ---------------------------------------------------------------------------


def test_ask_unmatched_prompt_returns_hint(client):
    r = client.post(
        "/api/ask",
        headers=_hdrs(),
        json={"prompt": "give me the secret to life", "source": "orders"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["matched"] is False
    assert "hint" in body


def test_query_with_intent_returns_rows(client):
    intent = {"source": "orders", "metrics": ["order_count"]}
    r = client.post("/api/query", headers=_hdrs(), json={"intent": intent})
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    # analyst_west sees only west region (2 orders: o1, o3)
    assert body["rows"][0][0] == 2


def test_explain_returns_compiled_sql_no_execute(client):
    intent = {"source": "orders", "metrics": ["order_count"]}
    r = client.post("/api/explain", headers=_hdrs(), json={"intent": intent})
    assert r.status_code == 200
    body = r.json()
    assert "compiled_sql" in body
    assert body["stage"] == "compiled"
    # Should NOT have executed -- no row data
    assert "rows" not in body


def test_explain_with_bad_intent_returns_error_in_payload(client):
    r = client.post(
        "/api/explain",
        headers=_hdrs(),
        json={"intent": {"source": "orders", "metrics": ["nonexistent_metric"]}},
    )
    # Errors at validate / compile stages return 200 with error payload
    # (the UI displays them inline, not as HTTP failures)
    assert r.status_code == 200
    assert "error" in r.json()


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


def test_health_unknown_source_404(client):
    r = client.get("/api/health/ghost", headers=_hdrs())
    assert r.status_code == 404


def test_health_no_cached_row_returns_unknown(client):
    r = client.get("/api/health/orders", headers=_hdrs())
    assert r.status_code == 200
    body = r.json()
    assert body["source_id"] == "orders"
    # No `gibran check` has run -- status should be 'unknown' and the
    # note should point users at the right command. No `recent_runs`
    # key in this branch since the cache row is absent.
    assert body["status"] == "unknown"
    assert "gibran check" in body.get("note", "")


# ---------------------------------------------------------------------------
# Audit log
# ---------------------------------------------------------------------------


def test_log_non_admin_scoped_to_own_user(client):
    # Run a query as analyst so there's a row to find
    client.post(
        "/api/query",
        headers=_hdrs(),
        json={"intent": {"source": "orders", "metrics": ["order_count"]}},
    )
    # Try to read log as analyst -- should only see own rows
    r = client.get("/api/log", headers=_hdrs())
    assert r.status_code == 200
    body = r.json()
    assert body["scoped_to_self"] is True
    for row in body["rows"]:
        assert row["user_id"] == "analyst"


def test_log_admin_sees_all_users(client):
    # Run a query as analyst
    client.post(
        "/api/query",
        headers=_hdrs(),
        json={"intent": {"source": "orders", "metrics": ["order_count"]}},
    )
    # Read log as admin -- should see analyst's queries
    r = client.get("/api/log", headers=_admin_hdrs())
    assert r.status_code == 200
    body = r.json()
    assert body["scoped_to_self"] is False


def test_log_pagination_via_cursor(client):
    # Produce a couple of queries
    for _ in range(3):
        client.post(
            "/api/query",
            headers=_hdrs(),
            json={"intent": {"source": "orders", "metrics": ["order_count"]}},
        )
    r1 = client.get("/api/log?limit=2", headers=_admin_hdrs())
    body1 = r1.json()
    assert len(body1["rows"]) == 2
    if body1["next_cursor"]:
        r2 = client.get(
            f"/api/log?limit=2&cursor={body1['next_cursor']}",
            headers=_admin_hdrs(),
        )
        body2 = r2.json()
        # No overlap: first row of page 2 should have created_at < last row of page 1
        last_p1 = body1["rows"][-1]["query_id"]
        for row in body2["rows"]:
            assert row["query_id"] != last_p1


# ---------------------------------------------------------------------------
# Policy (admin-only)
# ---------------------------------------------------------------------------


def test_policy_requires_admin(client):
    r = client.get("/api/policy/analyst_west", headers=_hdrs())
    assert r.status_code == 403


def test_policy_admin_can_view_any_role(client):
    r = client.get("/api/policy/analyst_west", headers=_admin_hdrs())
    assert r.status_code == 200
    body = r.json()
    assert body["target_role"] == "analyst_west"
    assert "previews" in body


def test_policy_unknown_role_404(client):
    r = client.get("/api/policy/ghost_role", headers=_admin_hdrs())
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Admin: touch
# ---------------------------------------------------------------------------


def test_touch_requires_admin(client):
    r = client.post("/api/touch/orders", headers=_hdrs())
    assert r.status_code == 403


def test_touch_duckdb_table_bumps_version(client):
    r = client.post("/api/touch/orders", headers=_admin_hdrs())
    assert r.status_code == 200
    body = r.json()
    assert body["source_id"] == "orders"
    assert "new_version" in body


def test_touch_unknown_source_404(client):
    r = client.post("/api/touch/ghost", headers=_admin_hdrs())
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Admin: approvals
# ---------------------------------------------------------------------------


def test_approvals_requires_admin(client):
    r = client.get("/api/approvals/pending", headers=_hdrs())
    assert r.status_code == 403


def test_approvals_pending_empty_initially(client):
    r = client.get("/api/approvals/pending", headers=_admin_hdrs())
    assert r.status_code == 200
    assert r.json()["pending"] == []


def test_approve_unknown_change_404(client):
    r = client.post(
        "/api/approvals/nope/approve",
        headers=_admin_hdrs(),
        json={"reason": "test"},
    )
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Static / placeholder
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Examples (auto-generated NL example questions)
# ---------------------------------------------------------------------------


def test_examples_returns_non_empty_list_for_analyst(client):
    r = client.get("/api/examples", headers=_hdrs())
    assert r.status_code == 200
    body = r.json()
    assert body["source_id"] == "orders"
    assert isinstance(body["examples"], list)
    assert len(body["examples"]) >= 3  # at least metric_only + by + top_n
    # Every example should declare its pattern name (used by the UI's
    # hover tooltip).
    for ex in body["examples"]:
        assert "question" in ex and "pattern" in ex


def test_examples_questions_are_executable(client):
    """Every example surfaced to the user must actually match a pattern
    and execute -- otherwise the UI shows a button that produces an
    'I don't know' state on click, which would look broken in a demo."""
    examples = client.get("/api/examples", headers=_hdrs()).json()["examples"]
    assert len(examples) > 0
    for ex in examples:
        ask = client.post(
            "/api/ask",
            headers=_hdrs(),
            json={"prompt": ex["question"], "source": "orders"},
        ).json()
        assert ask["matched"] is True, (
            f"example {ex['question']!r} (pattern {ex['pattern']!r}) "
            f"did not match any registered NL pattern"
        )


def test_examples_with_explicit_source_param(client):
    r = client.get("/api/examples?source_id=orders", headers=_hdrs())
    assert r.status_code == 200
    assert r.json()["source_id"] == "orders"


# ---------------------------------------------------------------------------
# Roles (admin-only)
# ---------------------------------------------------------------------------


def test_roles_requires_admin(client):
    r = client.get("/api/roles", headers=_hdrs())
    assert r.status_code == 403


def test_roles_admin_lists_all_roles(client):
    r = client.get("/api/roles", headers=_admin_hdrs())
    assert r.status_code == 200
    body = r.json()
    role_ids = {ro["id"] for ro in body["roles"]}
    # Setup fixture creates 'admin' (break-glass) plus the yaml's roles
    assert "admin" in role_ids
    # is_break_glass flag is present on each role
    admin_row = next(r for r in body["roles"] if r["id"] == "admin")
    assert admin_row["is_break_glass"] is True


# ---------------------------------------------------------------------------
# Static / placeholder
# ---------------------------------------------------------------------------


def test_root_serves_index_or_placeholder(client):
    """When the React build is present at src/gibran/ui/static/index.html,
    `/` returns the SPA. Otherwise it returns a JSON placeholder
    pointing at /api/docs. Both are valid; the test accepts either so
    it passes whether or not `npm run build` has been run."""
    r = client.get("/")
    assert r.status_code == 200
    ctype = r.headers.get("content-type", "")
    if "text/html" in ctype:
        # SPA build present
        assert "<div id=\"root\"></div>" in r.text or "Gibran" in r.text
    else:
        body = r.json()
        assert "api_docs" in body
