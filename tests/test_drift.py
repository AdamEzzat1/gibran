"""Tests for schema-drift detection in `gibran sync`.

Three layers:
  1. `detect_drift` pure-function: per-source probe + event emission.
  2. `DriftEvent.as_warning`: human-readable string shape.
  3. CLI integration via `gibran sync` -- verifies the warning surfaces on
     stderr without aborting the apply.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from gibran.cli.main import app
from gibran.sync.drift import (
    DriftEvent,
    UnreachableSource,
    detect_drift,
)
from gibran.sync.migrations import apply_all as apply_migrations
from gibran.sync.yaml_schema import (
    ColumnConfig,
    GibranConfig,
    SourceConfig,
)


MIGRATIONS = Path(__file__).parent.parent / "migrations"


def _src(id: str, uri: str, **cols: str) -> SourceConfig:
    """Quick SourceConfig builder with `duckdb_table` defaults."""
    return SourceConfig(
        id=id,
        display_name=id.title(),
        type="duckdb_table",
        uri=uri,
        columns=[ColumnConfig(name=n, type=t) for n, t in cols.items()],
    )


def _cfg(*sources: SourceConfig) -> GibranConfig:
    return GibranConfig(sources=list(sources))


def _db_with(*table_ddl: str) -> duckdb.DuckDBPyConnection:
    """Migrated in-memory DB plus one or more CREATE TABLE statements."""
    con = duckdb.connect(":memory:")
    apply_migrations(con, MIGRATIONS)
    for ddl in table_ddl:
        con.execute(ddl)
    return con


# ---------------------------------------------------------------------------
# detect_drift -- pure function
# ---------------------------------------------------------------------------

class TestDetectDrift:
    def test_clean_match_produces_no_events(self) -> None:
        con = _db_with(
            "CREATE TABLE orders (order_id VARCHAR, amount DECIMAL(18,2))"
        )
        cfg = _cfg(_src("orders", "orders",
                        order_id="VARCHAR", amount="DECIMAL(18,2)"))
        drift, unreachable = detect_drift(con, cfg)
        assert drift == []
        assert unreachable == []

    def test_yaml_declares_column_db_does_not_have(self) -> None:
        con = _db_with("CREATE TABLE orders (order_id VARCHAR)")
        cfg = _cfg(_src("orders", "orders",
                        order_id="VARCHAR", amount="DECIMAL(18,2)"))
        drift, _ = detect_drift(con, cfg)
        assert len(drift) == 1
        [e] = drift
        assert e.source_id == "orders"
        assert e.column == "amount"
        assert e.kind == "missing_in_db"
        assert e.yaml_type == "DECIMAL(18,2)"
        assert e.actual_type is None

    def test_db_has_column_yaml_does_not_declare(self) -> None:
        con = _db_with(
            "CREATE TABLE orders (order_id VARCHAR, surprise VARCHAR)"
        )
        cfg = _cfg(_src("orders", "orders", order_id="VARCHAR"))
        drift, _ = detect_drift(con, cfg)
        assert len(drift) == 1
        [e] = drift
        assert e.column == "surprise"
        assert e.kind == "missing_in_yaml"
        assert e.yaml_type is None
        assert e.actual_type == "VARCHAR"

    def test_type_mismatch(self) -> None:
        con = _db_with("CREATE TABLE orders (order_id INTEGER)")
        cfg = _cfg(_src("orders", "orders", order_id="VARCHAR"))
        drift, _ = detect_drift(con, cfg)
        assert len(drift) == 1
        [e] = drift
        assert e.kind == "type_mismatch"
        assert e.yaml_type == "VARCHAR"
        assert e.actual_type == "INTEGER"

    def test_case_and_whitespace_normalization_does_not_drift(self) -> None:
        # YAML "decimal(18, 2)" vs DB "DECIMAL(18,2)" -- same type, different
        # casing and a stray space. Should NOT be reported as drift.
        con = _db_with("CREATE TABLE orders (amount DECIMAL(18,2))")
        cfg = _cfg(_src("orders", "orders", amount="decimal(18, 2)"))
        drift, _ = detect_drift(con, cfg)
        assert drift == []

    def test_unreachable_source_lands_in_unreachable_list(self) -> None:
        # No `ghost` table; probe should error and the source go into
        # the unreachable list, not the drift list.
        con = _db_with("CREATE TABLE other (col VARCHAR)")
        cfg = _cfg(_src("ghost", "ghost", col="VARCHAR"))
        drift, unreachable = detect_drift(con, cfg)
        assert drift == []
        assert len(unreachable) == 1
        assert unreachable[0].source_id == "ghost"
        assert isinstance(unreachable[0], UnreachableSource)

    def test_multiple_drift_kinds_in_one_source(self) -> None:
        con = _db_with(
            "CREATE TABLE orders (order_id INTEGER, surprise VARCHAR)"
        )
        cfg = _cfg(_src("orders", "orders",
                        order_id="VARCHAR", amount="DECIMAL(18,2)"))
        drift, _ = detect_drift(con, cfg)
        kinds = {e.kind for e in drift}
        assert kinds == {"type_mismatch", "missing_in_db", "missing_in_yaml"}

    def test_multiple_sources_isolate(self) -> None:
        con = _db_with(
            "CREATE TABLE clean_t (a VARCHAR)",
            "CREATE TABLE dirty_t (a INTEGER)",
        )
        cfg = _cfg(
            _src("clean_t", "clean_t", a="VARCHAR"),
            _src("dirty_t", "dirty_t", a="VARCHAR"),
        )
        drift, _ = detect_drift(con, cfg)
        source_ids_with_drift = {e.source_id for e in drift}
        assert source_ids_with_drift == {"dirty_t"}

    def test_no_sources_returns_empty(self) -> None:
        con = _db_with()
        cfg = _cfg()
        drift, unreachable = detect_drift(con, cfg)
        assert drift == []
        assert unreachable == []


# ---------------------------------------------------------------------------
# DriftEvent.as_warning shape
# ---------------------------------------------------------------------------

class TestDriftEventWarning:
    def test_missing_in_db_message(self) -> None:
        e = DriftEvent("orders", "amount", "missing_in_db", "DECIMAL(18,2)", None)
        msg = e.as_warning()
        assert msg.startswith("warning:")
        assert "orders.amount" in msg
        assert "DECIMAL(18,2)" in msg
        assert "missing" in msg.lower()

    def test_missing_in_yaml_message(self) -> None:
        e = DriftEvent("orders", "surprise", "missing_in_yaml", None, "VARCHAR")
        msg = e.as_warning()
        assert msg.startswith("warning:")
        assert "orders.surprise" in msg
        assert "VARCHAR" in msg
        assert "not declared" in msg.lower()

    def test_type_mismatch_message(self) -> None:
        e = DriftEvent("orders", "amount", "type_mismatch", "VARCHAR", "INTEGER")
        msg = e.as_warning()
        assert msg.startswith("warning:")
        assert "VARCHAR" in msg
        assert "INTEGER" in msg


# ---------------------------------------------------------------------------
# CLI integration: `gibran sync` emits warnings on drift
# ---------------------------------------------------------------------------

class TestSyncCliEmitsWarnings:
    def test_sync_emits_drift_warning_when_type_diverges(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The user's YAML says VARCHAR, the live source has INTEGER. `gibran
        sync` should print a warning on stderr but still apply successfully
        (drift is informational, not fatal)."""
        db = tmp_path / "gibran.duckdb"
        con = duckdb.connect(str(db))
        try:
            apply_migrations(con, MIGRATIONS)
            con.execute("CREATE TABLE widgets (id INTEGER, label VARCHAR)")
        finally:
            con.close()

        (tmp_path / "gibran.yaml").write_text(
            "sources:\n"
            "  - id: widgets\n"
            "    display_name: Widgets\n"
            "    type: duckdb_table\n"
            "    uri: widgets\n"
            "    columns:\n"
            "      - name: id\n"
            "        type: VARCHAR\n"        # mismatch -- DB has INTEGER
            "      - name: label\n"
            "        type: VARCHAR\n",
            encoding="utf-8",
        )

        monkeypatch.chdir(tmp_path)
        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(app, ["sync"])
        assert result.exit_code == 0, (result.stdout, result.stderr)
        # The mismatch warning lands on stderr.
        assert "widgets.id" in result.stderr
        assert "VARCHAR" in result.stderr
        assert "INTEGER" in result.stderr
        # And the apply still proceeded.
        assert "applied:" in result.stdout

    def test_sync_clean_emits_no_warnings(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        db = tmp_path / "gibran.duckdb"
        con = duckdb.connect(str(db))
        try:
            apply_migrations(con, MIGRATIONS)
            con.execute("CREATE TABLE widgets (id VARCHAR)")
        finally:
            con.close()

        (tmp_path / "gibran.yaml").write_text(
            "sources:\n"
            "  - id: widgets\n"
            "    display_name: Widgets\n"
            "    type: duckdb_table\n"
            "    uri: widgets\n"
            "    columns:\n"
            "      - name: id\n"
            "        type: VARCHAR\n",
            encoding="utf-8",
        )

        monkeypatch.chdir(tmp_path)
        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(app, ["sync"])
        assert result.exit_code == 0
        assert "warning:" not in result.stderr

    def test_sync_warns_when_source_unreachable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A YAML declaring a parquet source whose file doesn't exist should
        produce a 'schema not probable' note rather than a hard failure --
        drift detection is opportunistic."""
        db = tmp_path / "gibran.duckdb"
        con = duckdb.connect(str(db))
        try:
            apply_migrations(con, MIGRATIONS)
        finally:
            con.close()

        (tmp_path / "gibran.yaml").write_text(
            "sources:\n"
            "  - id: orders\n"
            "    display_name: Orders\n"
            "    type: parquet\n"
            "    uri: nonexistent.parquet\n"
            "    columns:\n"
            "      - name: order_id\n"
            "        type: VARCHAR\n",
            encoding="utf-8",
        )

        monkeypatch.chdir(tmp_path)
        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(app, ["sync"])
        assert result.exit_code == 0
        assert "orders" in result.stderr
        assert "not probable" in result.stderr
        assert "applied:" in result.stdout
