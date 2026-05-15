"""Phase 5A.2 + 5A.1c end-to-end smoke against an embedded Postgres.

Spins up a fresh Postgres via pgserver, applies all 9 translated
migrations, seeds a tiny orders table, and exercises every engine
method plus the cache helpers that 5A.1c migrated.

Run: python tools/postgres_e2e_smoke.py

Requires: pip install gibran[postgres] && pip install pgserver
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import pgserver

from gibran.dsl.plan_cache import bump_catalog_generation, catalog_generation
from gibran.execution.engines.postgres import PostgresEngine, connect
from gibran.execution.result_cache import (
    bump_source_health_generation,
    lookup,
    source_health_generation,
)
from gibran.governance.types import IdentityContext
from gibran.sync.migrations import apply_all_for_engine

MIGRATIONS = Path(__file__).parent.parent / "migrations"


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="gibran_pg_e2e_")
    srv = pgserver.get_server(tmp)
    uri = srv.get_uri()
    print(f"[setup] Postgres URI: {uri}")

    con = connect(uri)
    engine = PostgresEngine(con=con)

    # 1. Migrations: all 9 translated files should apply cleanly
    applied = apply_all_for_engine(engine, MIGRATIONS)
    print(f"[migrate] applied versions: {applied}")
    assert applied == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], f"unexpected: {applied}"

    # 2. Seed a tiny dataset
    with con.cursor() as cur:
        cur.execute(
            "INSERT INTO gibran_sources (source_id, display_name, source_type, uri, primary_grain) "
            "VALUES ('orders', 'Orders', 'duckdb_table', 'orders', 'order_id')"
        )
        cur.execute(
            "CREATE TABLE orders (order_id TEXT, amount NUMERIC(18,2), region TEXT, status TEXT)"
        )
        cur.execute(
            "INSERT INTO orders VALUES "
            "('o1', 100.00, 'west',  'paid'), "
            "('o2', 200.00, 'east',  'paid'), "
            "('o3',  50.00, 'west',  'pending'), "
            "('o4', 300.00, 'north', 'paid')"
        )
    print("[seed] inserted 4 orders")

    # 3. Real query via PostgresEngine.query
    rows, cols = engine.query(
        "SELECT region, SUM(amount) AS revenue FROM orders GROUP BY region ORDER BY region"
    )
    print(f"[query] columns: {cols}")
    print(f"[query] rows:    {rows}")
    assert cols == ["region", "revenue"], f"unexpected cols: {cols}"
    assert len(rows) == 3, f"expected 3 region groups, got {len(rows)}"

    # 4. Meta-table machinery (5A.1c migration: takes the engine, not a con)
    gen0 = catalog_generation(engine)
    gen1 = bump_catalog_generation(engine)
    print(f"[meta] catalog_generation: {gen0!r} -> {gen1!r}")
    assert gen0 != gen1, "bump did not change the generation"

    hg0 = source_health_generation(engine)
    hg1 = bump_source_health_generation(engine)
    print(f"[meta] source_health_generation: {hg0!r} -> {hg1!r}")
    assert hg0 != hg1, "bump did not change the source-health generation"

    # 5. Result-cache lookup with the engine
    ident = IdentityContext(
        user_id="adam", role_id="analyst", attributes={}, source="test",
    )
    key, cached = lookup(engine, "SELECT 1", ident)
    assert cached is None, "unexpected cache hit on empty cache"
    print(f"[cache] lookup miss OK; key length={len(key)}")

    # 6. fetchone with a param
    row = engine.fetchone("SELECT SUM(amount) FROM orders WHERE region = ?", ["west"])
    print(f"[fetchone] west total: {row[0]}")
    assert float(row[0]) == 150.0, f"expected west=150, got {row[0]}"

    # 7. execute (DDL) + commit
    engine.execute("CREATE TABLE smoke_test (x INTEGER)")
    engine.execute("INSERT INTO smoke_test VALUES (?)", [42])
    engine.commit()
    row = engine.fetchone("SELECT x FROM smoke_test")
    assert row == (42,), f"expected (42,), got {row}"
    print("[execute] DDL + parameterized INSERT + fetchone roundtrip OK")

    con.close()
    srv.cleanup()
    print()
    print("===> POSTGRES E2E PASSED <===")


if __name__ == "__main__":
    main()
