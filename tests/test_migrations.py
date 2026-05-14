from pathlib import Path

import duckdb

from rumi.sync.migrations import apply_all


MIGRATIONS_DIR = Path(__file__).parent.parent / "migrations"


def test_migrations_apply_clean() -> None:
    con = duckdb.connect(":memory:")
    applied = apply_all(con, MIGRATIONS_DIR)
    assert applied == [1, 2, 3, 4, 5, 6, 7]


def test_metric_config_column_exists() -> None:
    con = duckdb.connect(":memory:")
    apply_all(con, MIGRATIONS_DIR)
    cols = {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'rumi_metric_versions'"
        ).fetchall()
    }
    assert "metric_config" in cols


def test_valid_until_column_exists_and_nullable() -> None:
    con = duckdb.connect(":memory:")
    apply_all(con, MIGRATIONS_DIR)
    row = con.execute(
        "SELECT data_type, is_nullable FROM information_schema.columns "
        "WHERE table_name = 'rumi_policies' AND column_name = 'valid_until'"
    ).fetchone()
    assert row is not None, "valid_until column should exist on rumi_policies"
    assert row[0].upper().startswith("TIMESTAMP")
    assert row[1] == "YES"  # nullable -- NULL means "never expires"


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
    assert "rumi_source_health" in tables


def test_staleness_seconds_columns_exist() -> None:
    con = duckdb.connect(":memory:")
    apply_all(con, MIGRATIONS_DIR)
    quality_cols = {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'rumi_quality_rules'"
        ).fetchall()
    }
    freshness_cols = {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'rumi_freshness_rules'"
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
        "rumi_schema_version",
        "rumi_sensitivity_levels",
        "rumi_sources",
        "rumi_columns",
        "rumi_dimensions",
        "rumi_metrics",
        "rumi_metric_versions",
        "rumi_metric_dependencies",
        "rumi_quality_rules",
        "rumi_freshness_rules",
        "rumi_quality_runs",
        "rumi_query_log",
        "rumi_query_metrics",
        "rumi_roles",
        "rumi_role_attributes",
        "rumi_user_attributes",
        "rumi_policies",
        "rumi_policy_columns",
    }
    missing = expected - tables
    assert not missing, f"missing: {missing}"


def test_seeded_sensitivity_levels_include_unclassified() -> None:
    con = duckdb.connect(":memory:")
    apply_all(con, MIGRATIONS_DIR)
    levels = {
        r[0]
        for r in con.execute(
            "SELECT level_id FROM rumi_sensitivity_levels"
        ).fetchall()
    }
    assert levels == {"public", "internal", "pii", "restricted", "unclassified"}


def test_unclassified_is_default_for_new_columns() -> None:
    con = duckdb.connect(":memory:")
    apply_all(con, MIGRATIONS_DIR)
    con.execute(
        "INSERT INTO rumi_sources (source_id, display_name, source_type, uri) "
        "VALUES ('orders', 'Orders', 'parquet', '/tmp/orders.parquet')"
    )
    con.execute(
        "INSERT INTO rumi_columns (source_id, column_name, data_type) "
        "VALUES ('orders', 'amount', 'DECIMAL(18,2)')"
    )
    sensitivity = con.execute(
        "SELECT sensitivity FROM rumi_columns "
        "WHERE source_id = 'orders' AND column_name = 'amount'"
    ).fetchone()[0]
    assert sensitivity == "unclassified"


def test_default_column_mode_is_deny() -> None:
    con = duckdb.connect(":memory:")
    apply_all(con, MIGRATIONS_DIR)
    con.execute(
        "INSERT INTO rumi_sources (source_id, display_name, source_type, uri) "
        "VALUES ('orders', 'Orders', 'parquet', '/tmp/orders.parquet')"
    )
    con.execute(
        "INSERT INTO rumi_roles (role_id, display_name) "
        "VALUES ('analyst', 'Analyst')"
    )
    con.execute(
        "INSERT INTO rumi_policies (policy_id, role_id, source_id) "
        "VALUES ('p1', 'analyst', 'orders')"
    )
    mode = con.execute(
        "SELECT default_column_mode FROM rumi_policies WHERE policy_id = 'p1'"
    ).fetchone()[0]
    assert mode == "deny"
