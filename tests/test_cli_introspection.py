"""Tests for the introspection CLI bundle: explain / describe / catalog
and the new `query --output csv|json|parquet` formats.

Each test:
  - builds a tmp_path-based gibran.duckdb (init + sync against fixtures/gibran.yaml)
  - seeds the `orders` table with a few rows
  - runs `gibran check` so source_health is populated
  - invokes the CLI subcommand via typer.testing.CliRunner
  - asserts the exit code + stdout shape
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from gibran.cli.main import app
from gibran.observability.default import DefaultObservability
from gibran.observability.runner import run_checks
from gibran.sync.applier import apply as apply_config
from gibran.sync.loader import load as load_config
from gibran.sync.migrations import apply_all as apply_migrations


FIXTURES = Path(__file__).parent / "fixtures"
MIGRATIONS = Path(__file__).parent.parent / "migrations"


def _bootstrap(tmp_path: Path) -> None:
    """Build a populated gibran.duckdb in tmp_path and run gibran check so the
    source_health cache is fresh."""
    db = tmp_path / "gibran.duckdb"
    con = duckdb.connect(str(db))
    try:
        apply_migrations(con, MIGRATIONS)
        apply_config(con, load_config(FIXTURES / "gibran.yaml"))
        con.execute(
            "CREATE TABLE orders ("
            "order_id VARCHAR, amount DECIMAL(18,2), order_date TIMESTAMP, "
            "status VARCHAR, region VARCHAR, customer_email VARCHAR)"
        )
        con.execute(
            "INSERT INTO orders VALUES "
            "('o1', 100.00, now() - INTERVAL '1 hour',  'paid',    'west', 'a@x'), "
            "('o2', 200.00, now() - INTERVAL '2 hours', 'paid',    'east', 'b@x'), "
            "('o3',  50.00, now() - INTERVAL '3 hours', 'pending', 'west', 'c@x')"
        )
        obs = DefaultObservability(con)
        run_checks(con, "orders", obs)
    finally:
        con.close()


@pytest.fixture
def cli_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Bootstrap a gibran.duckdb in tmp_path and chdir there so CLI commands
    that look at cwd find it."""
    _bootstrap(tmp_path)
    # The CLI helper functions use Path.cwd() to find the DB.
    monkeypatch.chdir(tmp_path)
    return tmp_path


# ---------------------------------------------------------------------------
# explain
# ---------------------------------------------------------------------------

class TestExplain:
    def test_explain_compiles_and_shows_sql_and_decision(self, cli_env: Path) -> None:
        runner = CliRunner()
        intent = {"source": "orders", "metrics": ["order_count"]}
        result = runner.invoke(app, [
            "explain", "--role", "analyst_west", "--attr", "region=west",
            "--dsl", json.dumps(intent),
        ])
        assert result.exit_code == 0, result.output
        assert "-- Compiled SQL --" in result.output
        assert "SELECT" in result.output
        assert "order_count" in result.output
        assert "-- Governance Decision --" in result.output
        assert "allowed:           True" in result.output

    def test_explain_does_not_write_audit_log(self, cli_env: Path) -> None:
        runner = CliRunner()
        intent = {"source": "orders", "metrics": ["order_count"]}
        result = runner.invoke(app, [
            "explain", "--role", "analyst_west", "--attr", "region=west",
            "--dsl", json.dumps(intent),
        ])
        assert result.exit_code == 0
        # Audit log should still be empty (no execution).
        db = cli_env / "gibran.duckdb"
        con = duckdb.connect(str(db))
        try:
            cnt = con.execute("SELECT COUNT(*) FROM gibran_query_log").fetchone()[0]
            assert cnt == 0
        finally:
            con.close()

    def test_explain_invalid_intent_exits_3(self, cli_env: Path) -> None:
        runner = CliRunner()
        intent = {"source": "orders", "metrics": ["ghost_metric"]}
        result = runner.invoke(app, [
            "explain", "--role", "analyst_west", "--attr", "region=west",
            "--dsl", json.dumps(intent),
        ])
        assert result.exit_code == 3
        assert "intent_invalid" in result.output


# ---------------------------------------------------------------------------
# describe
# ---------------------------------------------------------------------------

class TestDescribe:
    def test_describe_shows_columns_dimensions_metrics(self, cli_env: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(app, [
            "describe", "orders",
            "--role", "analyst_west", "--attr", "region=west",
        ])
        assert result.exit_code == 0, result.output
        assert "source: orders" in result.output
        assert "columns:" in result.output
        assert "order_id" in result.output
        assert "dimensions:" in result.output
        assert "orders.region" in result.output
        assert "metrics:" in result.output
        assert "order_count" in result.output

    def test_describe_shows_row_filter(self, cli_env: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(app, [
            "describe", "orders",
            "--role", "analyst_west", "--attr", "region=west",
        ])
        assert result.exit_code == 0, result.output
        assert "row_filter" in result.output
        # The fixture policy filters on region via $attr substitution.
        assert "region" in result.output

    def test_describe_partner_role_omits_pii_columns(self, cli_env: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(app, [
            "describe", "orders",
            "--role", "external_partner",
        ])
        assert result.exit_code == 0, result.output
        # The partner policy denies customer_email (default_mode=deny, no override)
        assert "customer_email" not in result.output

    def test_describe_unknown_source_errors(self, cli_env: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(app, [
            "describe", "ghost",
            "--role", "analyst_west", "--attr", "region=west",
        ])
        assert result.exit_code == 1
        assert "unknown source" in result.output


# ---------------------------------------------------------------------------
# catalog
# ---------------------------------------------------------------------------

class TestCatalog:
    def test_catalog_lists_accessible_sources(self, cli_env: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(app, [
            "catalog",
            "--role", "analyst_west", "--attr", "region=west",
        ])
        assert result.exit_code == 0, result.output
        assert "orders" in result.output
        assert "columns=" in result.output
        assert "metrics=" in result.output

    def test_catalog_no_policy_returns_message(self, cli_env: Path) -> None:
        runner = CliRunner()
        # `analyst_unknown` has no policies in the fixture.
        result = runner.invoke(app, [
            "catalog", "--role", "analyst_unknown",
        ])
        assert result.exit_code == 0
        assert "no sources accessible" in result.output


# ---------------------------------------------------------------------------
# query --output
# ---------------------------------------------------------------------------

class TestQueryOutput:
    INTENT = {"source": "orders", "metrics": ["order_count"]}

    def test_default_output_is_tsv(self, cli_env: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(app, [
            "query",
            "--role", "analyst_west", "--attr", "region=west",
            "--dsl", json.dumps(self.INTENT),
        ])
        assert result.exit_code == 0, result.output
        # tsv: header line then row line
        first_line = result.output.splitlines()[0]
        assert first_line == "order_count"
        # 2 of 3 rows are in west
        assert "2" in result.output

    def test_csv_output_to_stdout(self, cli_env: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(app, [
            "query",
            "--role", "analyst_west", "--attr", "region=west",
            "--dsl", json.dumps(self.INTENT),
            "--output", "csv",
        ])
        assert result.exit_code == 0, result.output
        # Should look like CSV: comma-separated, headers first
        lines = [l for l in result.output.splitlines() if not l.startswith("--")]
        assert lines[0] == "order_count"
        assert lines[1] == "2"

    def test_json_output_is_array_of_objects(self, cli_env: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(app, [
            "query",
            "--role", "analyst_west", "--attr", "region=west",
            "--dsl", json.dumps(self.INTENT),
            "--output", "json",
        ])
        assert result.exit_code == 0, result.output
        # Parse the JSON; the audit '--' line goes to stderr, so output is
        # almost all JSON. Strip trailing audit comments.
        body = result.output.split("\n--")[0]
        rows = json.loads(body)
        assert isinstance(rows, list)
        assert len(rows) == 1
        assert rows[0]["order_count"] == 2

    def test_csv_output_to_file(self, cli_env: Path) -> None:
        runner = CliRunner()
        out = cli_env / "out.csv"
        result = runner.invoke(app, [
            "query",
            "--role", "analyst_west", "--attr", "region=west",
            "--dsl", json.dumps(self.INTENT),
            "--output", "csv",
            "--output-file", str(out),
        ])
        assert result.exit_code == 0, result.output
        text = out.read_text(encoding="utf-8")
        assert text.splitlines()[0] == "order_count"
        assert text.splitlines()[1] == "2"

    def test_parquet_output_requires_output_file(self, cli_env: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(app, [
            "query",
            "--role", "analyst_west", "--attr", "region=west",
            "--dsl", json.dumps(self.INTENT),
            "--output", "parquet",
        ])
        assert result.exit_code == 1
        assert "requires --output-file" in result.output

    def test_parquet_output_writes_readable_file(self, cli_env: Path) -> None:
        runner = CliRunner()
        out = cli_env / "out.parquet"
        result = runner.invoke(app, [
            "query",
            "--role", "analyst_west", "--attr", "region=west",
            "--dsl", json.dumps(self.INTENT),
            "--output", "parquet",
            "--output-file", str(out),
        ])
        assert result.exit_code == 0, result.output
        assert out.exists()
        # Read it back via DuckDB.
        verify = duckdb.connect(":memory:")
        try:
            rows = verify.execute(
                f"SELECT * FROM read_parquet('{out.as_posix()}')"
            ).fetchall()
            assert rows == [(2,)]
        finally:
            verify.close()

    def test_invalid_output_format_rejected(self, cli_env: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(app, [
            "query",
            "--role", "analyst_west", "--attr", "region=west",
            "--dsl", json.dumps(self.INTENT),
            "--output", "xml",
        ])
        assert result.exit_code == 1
        assert "must be one of" in result.output
