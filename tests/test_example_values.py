"""Tests for `gibran/sync/example_values.py` -- sample distinct values
for low-cardinality public columns and surface them via ColumnView.

Covers:
  * the pure-function `populate_example_values` outcomes
  * the gates: sensitivity != 'public', expose_examples=False,
    high-cardinality, unreachable source
  * end-to-end: after sampling, `preview_schema` returns the values
    on ColumnView.example_values
  * CLI integration: `gibran sync` triggers sampling
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from gibran.cli.main import app
from gibran.governance.default import DefaultGovernance
from gibran.governance.types import IdentityContext
from gibran.sync.applier import apply as apply_config
from gibran.sync.example_values import populate_example_values
from gibran.sync.loader import load as load_config
from gibran.sync.migrations import apply_all as apply_migrations


FIXTURES = Path(__file__).parent / "fixtures"
MIGRATIONS = Path(__file__).parent.parent / "migrations"


def _populated_db_with_orders() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    apply_migrations(con, MIGRATIONS)
    apply_config(con, load_config(FIXTURES / "gibran.yaml"))
    con.execute(
        "CREATE TABLE orders ("
        "  order_id VARCHAR, amount DECIMAL(18,2), order_date TIMESTAMP,"
        "  status VARCHAR, region VARCHAR, customer_email VARCHAR"
        ")"
    )
    con.execute(
        "INSERT INTO orders VALUES "
        "('o1', 100, TIMESTAMP '2026-01-05', 'paid',    'west',    'a@x'),"
        "('o2', 200, TIMESTAMP '2026-01-10', 'pending', 'east',    'b@x'),"
        "('o3',  50, TIMESTAMP '2026-02-15', 'paid',    'central', 'c@x')"
    )
    return con


# ---------------------------------------------------------------------------
# populate_example_values: per-column outcomes
# ---------------------------------------------------------------------------

class TestPopulateExampleValues:
    def test_public_low_cardinality_column_sampled(self) -> None:
        con = _populated_db_with_orders()
        validated = load_config(FIXTURES / "gibran.yaml")
        results = populate_example_values(con, validated.config)

        # `region` has 3 distinct values across our 3 rows -- low-cardinality,
        # public, no opt-out -> sampled.
        region_result = next(
            r for r in results
            if r.source_id == "orders" and r.column_name == "region"
        )
        assert region_result.status == "sampled"
        assert region_result.value_count == 3

        stored = con.execute(
            "SELECT example_values FROM gibran_columns "
            "WHERE source_id = 'orders' AND column_name = 'region'"
        ).fetchone()[0]
        assert stored is not None
        assert set(json.loads(stored)) == {"west", "east", "central"}

    def test_pii_column_never_sampled(self) -> None:
        # `customer_email` is classified `pii` in the fixture; the gate
        # is hard -- no sampling regardless of cardinality or opt-out.
        con = _populated_db_with_orders()
        validated = load_config(FIXTURES / "gibran.yaml")
        results = populate_example_values(con, validated.config)

        email_result = next(
            r for r in results
            if r.source_id == "orders" and r.column_name == "customer_email"
        )
        assert email_result.status == "skipped_non_public"

        stored = con.execute(
            "SELECT example_values FROM gibran_columns "
            "WHERE source_id = 'orders' AND column_name = 'customer_email'"
        ).fetchone()[0]
        assert stored is None

    def test_high_cardinality_column_skipped(self) -> None:
        con = _populated_db_with_orders()
        validated = load_config(FIXTURES / "gibran.yaml")
        # Override the threshold to 2 -- with 3 distinct regions we'd
        # have 3 > 2, so the column is considered high-cardinality.
        results = populate_example_values(
            con, validated.config, low_cardinality_threshold=2,
        )
        region_result = next(
            r for r in results
            if r.source_id == "orders" and r.column_name == "region"
        )
        assert region_result.status == "skipped_high_cardinality"

    def test_opt_out_with_expose_examples_false(self, tmp_path: Path) -> None:
        # Author a YAML where a public column explicitly opts out via
        # `expose_examples: false`. The sampling pass should skip it.
        yaml_path = tmp_path / "gibran.yaml"
        text = (FIXTURES / "gibran.yaml").read_text(encoding="utf-8")
        text = text.replace(
            "      - name: status\n"
            "        type: VARCHAR\n"
            "        sensitivity: public\n",
            "      - name: status\n"
            "        type: VARCHAR\n"
            "        sensitivity: public\n"
            "        expose_examples: false\n",
        )
        yaml_path.write_text(text, encoding="utf-8")

        con = _populated_db_with_orders()
        # Re-apply with the opt-out so the catalog knows about it.
        apply_config(con, load_config(yaml_path))

        validated = load_config(yaml_path)
        results = populate_example_values(con, validated.config)
        status_result = next(
            r for r in results
            if r.source_id == "orders" and r.column_name == "status"
        )
        assert status_result.status == "skipped_opt_out"

    def test_unreachable_source_returns_unreachable_results(
        self, tmp_path: Path
    ) -> None:
        # Author a YAML pointing at a nonexistent parquet file. Sampling
        # should produce skipped_unreachable rows, not crash.
        yaml_path = tmp_path / "gibran.yaml"
        yaml_path.write_text(
            "sources:\n"
            "  - id: missing\n"
            "    display_name: Missing\n"
            "    type: parquet\n"
            "    uri: nonexistent.parquet\n"
            "    columns:\n"
            "      - name: x\n"
            "        type: VARCHAR\n"
            "        sensitivity: public\n",
            encoding="utf-8",
        )
        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        apply_config(con, load_config(yaml_path))

        validated = load_config(yaml_path)
        results = populate_example_values(con, validated.config)
        assert all(r.status == "skipped_unreachable" for r in results)


# ---------------------------------------------------------------------------
# preview_schema reads sampled values onto ColumnView
# ---------------------------------------------------------------------------

class TestColumnViewExampleValues:
    def test_after_sampling_column_view_carries_values(self) -> None:
        con = _populated_db_with_orders()
        validated = load_config(FIXTURES / "gibran.yaml")
        populate_example_values(con, validated.config)

        gov = DefaultGovernance(con)
        ident = IdentityContext(
            user_id="aw", role_id="analyst_west",
            attributes={"region": "west"}, source="test",
        )
        schema = gov.preview_schema(ident, "orders")
        region_col = next(c for c in schema.columns if c.name == "region")
        assert region_col.example_values is not None
        assert set(region_col.example_values) == {"west", "east", "central"}

        email_col = next(c for c in schema.columns if c.name == "customer_email")
        assert email_col.example_values is None  # pii -- never populated


# ---------------------------------------------------------------------------
# CLI integration: `gibran sync` triggers sampling
# ---------------------------------------------------------------------------

class TestSyncCliSamples:
    def test_sync_populates_example_values(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Bootstrap: copy the fixture YAML into tmp_path, build a fresh
        # gibran.duckdb with the orders table, then run `gibran sync`.
        db = tmp_path / "gibran.duckdb"
        con = duckdb.connect(str(db))
        try:
            apply_migrations(con, MIGRATIONS)
            con.execute(
                "CREATE TABLE orders ("
                "order_id VARCHAR, amount DECIMAL(18,2), order_date TIMESTAMP,"
                "status VARCHAR, region VARCHAR, customer_email VARCHAR)"
            )
            con.execute(
                "INSERT INTO orders VALUES "
                "('o1', 100, TIMESTAMP '2026-01-05', 'paid', 'west', 'a@x'),"
                "('o2', 200, TIMESTAMP '2026-01-10', 'paid', 'east', 'b@x')"
            )
        finally:
            con.close()

        (tmp_path / "gibran.yaml").write_text(
            (FIXTURES / "gibran.yaml").read_text(encoding="utf-8"),
            encoding="utf-8",
        )

        monkeypatch.chdir(tmp_path)
        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(app, ["sync"])
        assert result.exit_code == 0, (result.stdout, result.stderr)
        assert "sampled example_values" in result.stdout

        # Verify that region got sampled but customer_email (pii) did not.
        con = duckdb.connect(str(db))
        try:
            region_val = con.execute(
                "SELECT example_values FROM gibran_columns "
                "WHERE source_id = 'orders' AND column_name = 'region'"
            ).fetchone()[0]
            assert region_val is not None
            assert "west" in region_val

            email_val = con.execute(
                "SELECT example_values FROM gibran_columns "
                "WHERE source_id = 'orders' AND column_name = 'customer_email'"
            ).fetchone()[0]
            assert email_val is None
        finally:
            con.close()
