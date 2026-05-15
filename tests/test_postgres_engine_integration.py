"""Phase 5A.2 + 5A.1c -- PostgresEngine integration tests against real Postgres.

Two ways to enable:

  1. Set GIBRAN_POSTGRES_URL pointing at an existing Postgres
     (e.g. postgresql://user:pass@localhost:5432/gibran_test).

  2. Install pgserver (`pip install pgserver`). When the env var isn't
     set but pgserver IS installed, the module spins a fresh embedded
     Postgres in a temp dir per session -- fully self-contained, no
     external dependencies. This is what runs locally + in CI.

If neither path is available (no env var, no pgserver), the module
skips entirely.

Coverage:
  - PostgresEngine constructor + connection holding
  - file_scan_sql output runs against a real PG query
  - all 9 translated Postgres migrations apply cleanly
  - the 5A.1c-migrated cache helpers (catalog_generation,
    source_health_generation, lookup) work end-to-end through the engine
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

POSTGRES_URL = os.environ.get("GIBRAN_POSTGRES_URL")

# If no env-var Postgres is configured, try to spin pgserver. This
# section runs at module-import time so the test gate is "either path
# works or skip".
_pgserver_srv = None
if not POSTGRES_URL:
    try:
        import pgserver as _pgserver
        import tempfile
        _tmp = tempfile.mkdtemp(prefix="gibran_pg_pytest_")
        _pgserver_srv = _pgserver.get_server(_tmp)
        POSTGRES_URL = _pgserver_srv.get_uri()
    except ImportError:
        pass  # pgserver not installed; module will skip below
    except Exception:
        # pgserver install present but failed to start (port collision,
        # missing C runtime, etc.). Skip rather than crash collection.
        POSTGRES_URL = None

pytestmark = pytest.mark.skipif(
    not POSTGRES_URL,
    reason=(
        "No Postgres available -- set GIBRAN_POSTGRES_URL or "
        "`pip install pgserver` to enable Postgres integration tests."
    ),
)


MIGRATIONS = Path(__file__).parent.parent / "migrations"


def teardown_module(module):  # pylint: disable=unused-argument
    """Tear down the pgserver instance if we started one."""
    if _pgserver_srv is not None:
        try:
            _pgserver_srv.cleanup()
        except Exception:
            pass


@pytest.fixture
def pg_con():
    """Open a psycopg connection per test and close it after. Each test
    gets a fresh connection so test order can't cause state leaks
    (e.g. an uncommitted transaction from a previous test)."""
    con = psycopg.connect(POSTGRES_URL)
    try:
        yield con
    finally:
        con.close()


@pytest.fixture
def clean_pg(pg_con):
    """Drop any `gibran_*` tables before the test so each test starts
    from a clean schema. Used by tests that apply migrations."""
    with pg_con.cursor() as cur:
        cur.execute(
            "DO $$ DECLARE r RECORD; BEGIN "
            "FOR r IN (SELECT tablename FROM pg_tables "
            "WHERE schemaname = 'public' AND tablename LIKE 'gibran_%') LOOP "
            "EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE'; "
            "END LOOP; END $$;"
        )
        pg_con.commit()
    return pg_con


# ---------------------------------------------------------------------------
# Engine basics
# ---------------------------------------------------------------------------


def test_engine_holds_real_connection(pg_con):
    from gibran.execution.engines.postgres import PostgresEngine
    engine = PostgresEngine(con=pg_con)
    assert engine.con is pg_con


def test_file_scan_output_runs_against_real_postgres(pg_con):
    """The qident output for `duckdb_table` source_type must produce a
    SQL fragment that's valid in a real Postgres SELECT."""
    from gibran.execution.engines.postgres import PostgresEngine

    engine = PostgresEngine(con=pg_con)
    with pg_con.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS gibran_engine_test")
        cur.execute("CREATE TABLE gibran_engine_test (id INT, label TEXT)")
        cur.execute("INSERT INTO gibran_engine_test VALUES (1, 'a'), (2, 'b')")
        pg_con.commit()

        from_clause = engine.file_scan_sql("duckdb_table", "gibran_engine_test")
        cur.execute(f"SELECT id, label FROM {from_clause} ORDER BY id")
        rows = cur.fetchall()
        assert rows == [(1, "a"), (2, "b")]

        cur.execute("DROP TABLE gibran_engine_test")
        pg_con.commit()


def test_connect_helper_returns_usable_connection():
    from gibran.execution.engines.postgres import connect
    con = connect(POSTGRES_URL)
    try:
        with con.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)
    finally:
        con.close()


# ---------------------------------------------------------------------------
# 5A.1b -- execute/query/fetchone roundtrip
# ---------------------------------------------------------------------------


def test_execute_query_fetchone_roundtrip(pg_con):
    from gibran.execution.engines.postgres import PostgresEngine
    engine = PostgresEngine(con=pg_con)

    with pg_con.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS gibran_rt_test")
        pg_con.commit()

    engine.execute("CREATE TABLE gibran_rt_test (id INT, label TEXT)")
    engine.execute("INSERT INTO gibran_rt_test VALUES (?, ?)", [1, "alpha"])
    engine.execute("INSERT INTO gibran_rt_test VALUES (?, ?)", [2, "beta"])
    engine.commit()

    rows, cols = engine.query("SELECT id, label FROM gibran_rt_test ORDER BY id")
    assert cols == ["id", "label"]
    assert rows == [(1, "alpha"), (2, "beta")]

    one = engine.fetchone("SELECT label FROM gibran_rt_test WHERE id = ?", [2])
    assert one == ("beta",)

    engine.execute("DROP TABLE gibran_rt_test")
    engine.commit()


# ---------------------------------------------------------------------------
# 5A.5 -- all 9 migrations apply against real Postgres
# ---------------------------------------------------------------------------


def test_apply_all_migrations_against_postgres(clean_pg):
    from gibran.execution.engines.postgres import PostgresEngine
    from gibran.sync.migrations import apply_all_for_engine

    engine = PostgresEngine(con=clean_pg)
    applied = apply_all_for_engine(engine, MIGRATIONS)
    assert applied == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]

    # Verify a representative table from each migration exists
    rows, _ = engine.query(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name LIKE 'gibran_%' "
        "ORDER BY table_name"
    )
    names = [r[0] for r in rows]
    assert "gibran_sources" in names  # 0001
    assert "gibran_policies" in names  # 0002
    assert "gibran_source_health" in names  # 0004
    assert "gibran_pending_changes" in names  # 0009


# ---------------------------------------------------------------------------
# 5A.1c -- cache helpers work through the engine
# ---------------------------------------------------------------------------


def test_catalog_generation_through_engine(clean_pg):
    """5A.1c migration: catalog_generation accepts an engine. Verify
    against a real Postgres so we know the meta-table machinery
    actually works (not just on DuckDB)."""
    from gibran.dsl.plan_cache import bump_catalog_generation, catalog_generation
    from gibran.execution.engines.postgres import PostgresEngine
    from gibran.sync.migrations import apply_all_for_engine

    engine = PostgresEngine(con=clean_pg)
    apply_all_for_engine(engine, MIGRATIONS)

    gen0 = catalog_generation(engine)
    gen1 = bump_catalog_generation(engine)
    assert gen0 != gen1
    # And reading again returns the same value (was persisted, not memoized)
    gen2 = catalog_generation(engine)
    assert gen1 == gen2


def test_source_health_generation_through_engine(clean_pg):
    from gibran.execution.engines.postgres import PostgresEngine
    from gibran.execution.result_cache import (
        bump_source_health_generation,
        source_health_generation,
    )
    from gibran.sync.migrations import apply_all_for_engine

    engine = PostgresEngine(con=clean_pg)
    apply_all_for_engine(engine, MIGRATIONS)

    hg0 = source_health_generation(engine)
    hg1 = bump_source_health_generation(engine)
    assert hg0 != hg1


def test_result_cache_lookup_through_engine(clean_pg):
    from gibran.execution.engines.postgres import PostgresEngine
    from gibran.execution.result_cache import lookup
    from gibran.governance.types import IdentityContext
    from gibran.sync.migrations import apply_all_for_engine

    engine = PostgresEngine(con=clean_pg)
    apply_all_for_engine(engine, MIGRATIONS)

    ident = IdentityContext(
        user_id="adam", role_id="analyst", attributes={}, source="test",
    )
    key, cached = lookup(engine, "SELECT 1", ident)
    assert cached is None  # nothing in the cache on a cold start
    assert len(key) > 0  # well-formed key string
