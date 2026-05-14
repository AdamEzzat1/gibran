from pathlib import Path

import duckdb

from gibran.sync.migrations import apply_all


MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"


def test_migrations_apply_clean() -> None:
    con = duckdb.connect(":memory:")
    applied = apply_all(con, MIGRATIONS_DIR)
    assert applied == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


def test_metric_config_column_exists() -> None:
    con = duckdb.connect(":memory:")
    apply_all(con, MIGRATIONS_DIR)
    cols = {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'gibran_metric_versions'"
        ).fetchall()
    }
    assert "metric_config" in cols


def test_valid_until_column_exists_and_nullable() -> None:
    con = duckdb.connect(":memory:")
    apply_all(con, MIGRATIONS_DIR)
    row = con.execute(
        "SELECT data_type, is_nullable FROM information_schema.columns "
        "WHERE table_name = 'gibran_policies' AND column_name = 'valid_until'"
    ).fetchone()
    assert row is not None, "valid_until column should exist on gibran_policies"
    assert row[0].upper().startswith("TIMESTAMP")
    assert row[1] == "YES"  # nullable -- NULL means "never expires"


def test_example_values_column_exists_and_nullable() -> None:
    con = duckdb.connect(":memory:")
    apply_all(con, MIGRATIONS_DIR)
    row = con.execute(
        "SELECT data_type, is_nullable FROM information_schema.columns "
        "WHERE table_name = 'gibran_columns' AND column_name = 'example_values'"
    ).fetchone()
    assert row is not None
    # DuckDB renders JSON as "JSON"; column is nullable (NULL = not sampled).
    assert row[0].upper() in ("JSON", "VARCHAR", "TEXT")
    assert row[1] == "YES"


def test_source_health_table_exists() -> None:
    con = duckdb.connect(":memory:")
    apply_all(con, MIGRATIONS_DIR)
    tables = {
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()
    }
    assert "gibran_source_health" in tables


def test_staleness_seconds_columns_exist() -> None:
    con = duckdb.connect(":memory:")
    apply_all(con, MIGRATIONS_DIR)
    quality_cols = {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'gibran_quality_rules'"
        ).fetchall()
    }
    freshness_cols = {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'gibran_freshness_rules'"
        ).fetchall()
    }
    assert "staleness_seconds" in quality_cols
    assert "staleness_seconds" in freshness_cols


def test_migrations_idempotent() -> None:
    con = duckdb.connect(":memory:")
    apply_all(con, MIGRATIONS_DIR)
    second = apply_all(con, MIGRATIONS_DIR)
    assert second == []


def test_expected_tables_exist() -> None:
    con = duckdb.connect(":memory:")
    apply_all(con, MIGRATIONS_DIR)
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main'"
    ).fetchall()
    tables = {r[0] for r in rows}
    expected = {
        "gibran_schema_version",
        "gibran_sensitivity_levels",
        "gibran_sources",
        "gibran_columns",
        "gibran_dimensions",
        "gibran_metrics",
        "gibran_metric_versions",
        "gibran_metric_dependencies",
        "gibran_quality_rules",
        "gibran_freshness_rules",
        "gibran_quality_runs",
        "gibran_query_log",
        "gibran_query_metrics",
        "gibran_roles",
        "gibran_role_attributes",
        "gibran_user_attributes",
        "gibran_policies",
        "gibran_policy_columns",
    }
    missing = expected - tables
    assert not missing, f"missing: {missing}"


def test_seeded_sensitivity_levels_include_unclassified() -> None:
    con = duckdb.connect(":memory:")
    apply_all(con, MIGRATIONS_DIR)
    levels = {
        r[0]
        for r in con.execute(
            "SELECT level_id FROM gibran_sensitivity_levels"
        ).fetchall()
    }
    assert levels == {"public", "internal", "pii", "restricted", "unclassified"}


def test_unclassified_is_default_for_new_columns() -> None:
    con = duckdb.connect(":memory:")
    apply_all(con, MIGRATIONS_DIR)
    con.execute(
        "INSERT INTO gibran_sources (source_id, display_name, source_type, uri) "
        "VALUES ('orders', 'Orders', 'parquet', '/tmp/orders.parquet')"
    )
    con.execute(
        "INSERT INTO gibran_columns (source_id, column_name, data_type) "
        "VALUES ('orders', 'amount', 'DECIMAL(18,2)')"
    )
    sensitivity = con.execute(
        "SELECT sensitivity FROM gibran_columns "
        "WHERE source_id = 'orders' AND column_name = 'amount'"
    ).fetchone()[0]
    assert sensitivity == "unclassified"


def test_default_column_mode_is_deny() -> None:
    con = duckdb.connect(":memory:")
    apply_all(con, MIGRATIONS_DIR)
    con.execute(
        "INSERT INTO gibran_sources (source_id, display_name, source_type, uri) "
        "VALUES ('orders', 'Orders', 'parquet', '/tmp/orders.parquet')"
    )
    con.execute(
        "INSERT INTO gibran_roles (role_id, display_name) "
        "VALUES ('analyst', 'Analyst')"
    )
    con.execute(
        "INSERT INTO gibran_policies (policy_id, role_id, source_id) "
        "VALUES ('p1', 'analyst', 'orders')"
    )
    mode = con.execute(
        "SELECT default_column_mode FROM gibran_policies WHERE policy_id = 'p1'"
    ).fetchone()[0]
    assert mode == "deny"
