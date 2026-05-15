"""Phase 5A.5 -- engine-aware migration runner.

Tests the new `apply_all_for_engine` + `resolve_migrations_dir` paths.
The legacy `apply_all(con, dir)` shape is covered by tests/test_migrations.py
and stays unchanged.

Postgres-side integration is gated on psycopg + GIBRAN_POSTGRES_URL the
same way as test_postgres_engine_integration.py.
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pytest

from gibran.execution.dialect import Dialect
from gibran.execution.engines.duckdb import DuckDBEngine
from gibran.sync.migrations import (
    apply_all,
    apply_all_for_engine,
    resolve_migrations_dir,
)


MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"


# ---------------------------------------------------------------------------
# resolve_migrations_dir
# ---------------------------------------------------------------------------


def test_resolve_picks_per_dialect_dir_when_present():
    """If `<base>/postgres/` exists with SQL files, resolve to that
    subdirectory (the canonical per-dialect layout)."""
    resolved = resolve_migrations_dir(MIGRATIONS_DIR, Dialect.POSTGRES)
    assert resolved == MIGRATIONS_DIR / "postgres"


def test_resolve_falls_back_to_flat_layout_for_duckdb():
    """For backward compatibility, DuckDB resolves to the flat layout
    if `<base>/duckdb/` doesn't exist. (Today the flat layout IS the
    DuckDB layout; a 5A.5 follow-up moves it to `migrations/duckdb/`.)
    """
    resolved = resolve_migrations_dir(MIGRATIONS_DIR, Dialect.DUCKDB)
    assert resolved == MIGRATIONS_DIR


def test_resolve_raises_for_unknown_dialect_without_dir(tmp_path):
    """A Postgres / Snowflake / BigQuery deployment without
    `<base>/<dialect>/` migrations is a config error -- fail loudly
    with the expected path in the message."""
    # Use an empty base dir so neither flat nor per-dialect exists.
    with pytest.raises(FileNotFoundError) as exc:
        resolve_migrations_dir(tmp_path, Dialect.POSTGRES)
    assert "postgres" in str(exc.value)
    assert str(tmp_path / "postgres") in str(exc.value)


def test_resolve_raises_for_duckdb_with_no_sql_files(tmp_path):
    """Even DuckDB needs SQL files somewhere -- an empty base dir with
    no `duckdb/` subdir is a hard error."""
    with pytest.raises(FileNotFoundError):
        resolve_migrations_dir(tmp_path, Dialect.DUCKDB)


# ---------------------------------------------------------------------------
# apply_all_for_engine (DuckDB)
# ---------------------------------------------------------------------------


def test_apply_all_for_engine_duckdb_uses_flat_layout():
    """DuckDBEngine + flat migrations layout = same 9 migrations as
    legacy apply_all(con, dir)."""
    con = duckdb.connect(":memory:")
    engine = DuckDBEngine(con)
    applied = apply_all_for_engine(engine, MIGRATIONS_DIR)
    assert applied == [1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_apply_all_for_engine_idempotent():
    con = duckdb.connect(":memory:")
    engine = DuckDBEngine(con)
    apply_all_for_engine(engine, MIGRATIONS_DIR)
    second = apply_all_for_engine(engine, MIGRATIONS_DIR)
    assert second == []


def test_engine_and_legacy_paths_produce_identical_schema():
    """Two DBs: one via `apply_all(con, dir)`, one via
    `apply_all_for_engine(engine, dir)`. The resulting set of tables
    must be identical -- this is the backward-compat invariant for
    5A.5."""
    con_legacy = duckdb.connect(":memory:")
    apply_all(con_legacy, MIGRATIONS_DIR)

    con_engine = duckdb.connect(":memory:")
    apply_all_for_engine(DuckDBEngine(con_engine), MIGRATIONS_DIR)

    tables_legacy = {
        r[0]
        for r in con_legacy.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    tables_engine = {
        r[0]
        for r in con_engine.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    assert tables_legacy == tables_engine


# ---------------------------------------------------------------------------
# Postgres integration -- skip when psycopg or GIBRAN_POSTGRES_URL missing
# ---------------------------------------------------------------------------

POSTGRES_URL = os.environ.get("GIBRAN_POSTGRES_URL")


@pytest.mark.skipif(
    not POSTGRES_URL,
    reason=(
        "GIBRAN_POSTGRES_URL not set -- skipping Postgres migration "
        "integration tests."
    ),
)
def test_apply_all_for_engine_postgres():
    """The translated 0001_metadata.sql applies cleanly through
    PostgresEngine. Drops + recreates everything afterwards so the test
    DB stays clean for re-runs."""
    psycopg = pytest.importorskip("psycopg")
    from gibran.execution.engines.postgres import PostgresEngine, connect

    con = connect(POSTGRES_URL)
    try:
        # Clean slate so the migration tracking table starts fresh.
        with con.cursor() as cur:
            cur.execute(
                """
                DO $$ DECLARE
                    r RECORD;
                BEGIN
                    FOR r IN (
                        SELECT tablename FROM pg_tables
                        WHERE schemaname = 'public'
                          AND tablename LIKE 'gibran_%'
                    ) LOOP
                        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                    END LOOP;
                END $$;
                """
            )

        engine = PostgresEngine(con=con)
        applied = apply_all_for_engine(engine, MIGRATIONS_DIR)
        # All 9 migrations translated to Postgres as of 5A.5 cleanup.
        assert applied == [1, 2, 3, 4, 5, 6, 7, 8, 9]

        # Verify the schema actually landed
        rows, _ = engine.query(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name LIKE 'gibran_%' "
            "ORDER BY table_name"
        )
        names = [r[0] for r in rows]
        assert "gibran_sources" in names
        assert "gibran_query_log" in names
    finally:
        con.close()
