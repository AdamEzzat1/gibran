"""Tests for the source-type dispatcher and its integration into the
observability runner + DSL compiler.

Key invariants:
  - duckdb_table / sql_view sources produce a quoted identifier
  - parquet sources produce read_parquet('uri')
  - csv sources produce read_csv('uri')
  - the runner can execute quality + freshness rules against a parquet file
  - the DSL compiler emits a FROM-clause that lets execution.run_sql_query
    parse the source_id back out via alias_or_name
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from gibran._source_dispatch import (
    SourceDispatchError,
    from_clause_for_source,
)
from gibran.dsl.compile import Catalog, compile_intent
from gibran.dsl.run import run_dsl_query
from gibran.dsl.types import QueryIntent
from gibran.governance.default import DefaultGovernance
from gibran.governance.identity import CLIResolver
from gibran.observability.default import DefaultObservability
from gibran.observability.runner import (
    _evaluate_freshness_rule,
    _evaluate_quality_rule,
    run_checks,
)
from gibran.sync.applier import apply as apply_config
from gibran.sync.loader import load as load_config
from gibran.sync.migrations import apply_all as apply_migrations


MIGRATIONS = Path(__file__).parent.parent / "migrations"
FIXTURES = Path(__file__).parent / "fixtures"


def _register_source(
    con: duckdb.DuckDBPyConnection, source_id: str, source_type: str, uri: str
) -> None:
    con.execute(
        "INSERT INTO gibran_sources (source_id, display_name, source_type, uri) "
        "VALUES (?, ?, ?, ?)",
        [source_id, source_id, source_type, uri],
    )


class TestFromClauseForSource:
    def test_duckdb_table_returns_quoted_ident(self) -> None:
        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        _register_source(con, "orders", "duckdb_table", "orders")
        assert from_clause_for_source(con, "orders") == '"orders"'

    def test_sql_view_returns_quoted_ident(self) -> None:
        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        _register_source(con, "v_orders", "sql_view", "v_orders")
        assert from_clause_for_source(con, "v_orders") == '"v_orders"'

    def test_parquet_returns_read_parquet_call(self) -> None:
        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        _register_source(con, "orders", "parquet", "data/orders.parquet")
        assert (
            from_clause_for_source(con, "orders")
            == "read_parquet('data/orders.parquet')"
        )

    def test_csv_returns_read_csv_call(self) -> None:
        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        _register_source(con, "events", "csv", "data/events.csv")
        assert (
            from_clause_for_source(con, "events")
            == "read_csv('data/events.csv')"
        )

    def test_uri_with_single_quote_is_escaped(self) -> None:
        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        _register_source(con, "weird", "parquet", "data/o'reilly.parquet")
        # render_literal doubles the single quote
        assert (
            from_clause_for_source(con, "weird")
            == "read_parquet('data/o''reilly.parquet')"
        )

    def test_unknown_source_raises(self) -> None:
        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        with pytest.raises(SourceDispatchError, match="unknown source"):
            from_clause_for_source(con, "ghost")


class TestRunnerAgainstParquet:
    """End-to-end: register a parquet source, run quality + freshness rules
    against the actual parquet file, no manual CREATE VIEW required."""

    def test_quality_and_freshness_rules_execute_against_parquet(
        self, tmp_path: Path
    ) -> None:
        parquet_path = tmp_path / "orders.parquet"
        # Build the parquet file via DuckDB itself.
        seed = duckdb.connect(":memory:")
        seed.execute(
            "CREATE TABLE t AS SELECT * FROM (VALUES "
            "('o1', 100.0, TIMESTAMP '2026-05-13 10:00:00', 'paid', 'west'), "
            "('o2', 200.0, TIMESTAMP '2026-05-13 11:00:00', 'paid', 'east'), "
            "('o3',  50.0, TIMESTAMP '2026-05-13 12:00:00', 'pending', 'west')"
            ") AS t(order_id, amount, order_date, status, region)"
        )
        seed.execute(
            f"COPY t TO '{parquet_path.as_posix()}' (FORMAT PARQUET)"
        )
        seed.close()

        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        _register_source(con, "orders", "parquet", parquet_path.as_posix())

        # not_null on a non-null column
        passed, observed = _evaluate_quality_rule(
            con, "orders", "not_null", {"column": "amount"}
        )
        assert passed is True
        assert observed == {"null_count": 0}

        # unique on order_id
        passed, observed = _evaluate_quality_rule(
            con, "orders", "unique", {"column": "order_id"}
        )
        assert passed is True
        assert observed["duplicates"] == 0

        # range catches the row with amount=200 if max=150
        passed, observed = _evaluate_quality_rule(
            con, "orders", "range",
            {"column": "amount", "min": 0, "max": 150},
        )
        assert passed is False
        assert observed["out_of_range_count"] == 1

        # Freshness: data is from 2026-05-13; if we say max_age=1 second it must fail.
        passed, observed = _evaluate_freshness_rule(
            con, "orders", "order_date", 1
        )
        # The watermark is some real timestamp; we just assert the call
        # didn't blow up and returned a structured observation.
        assert "watermark" in observed
        assert observed["max_age_seconds"] == 1


class TestDSLCompileAgainstParquet:
    """DSL compile + run path against a parquet source -- no manual view."""

    def test_dsl_query_runs_against_parquet_source(self, tmp_path: Path) -> None:
        parquet_path = tmp_path / "orders.parquet"
        seed = duckdb.connect(":memory:")
        seed.execute(
            "CREATE TABLE t AS SELECT * FROM (VALUES "
            "('o1', 100.0, TIMESTAMP '2026-05-13 10:00:00', 'paid',    'west',  'a@x'), "
            "('o2', 200.0, TIMESTAMP '2026-05-13 11:00:00', 'paid',    'east',  'b@x'), "
            "('o3',  50.0, TIMESTAMP '2026-05-13 12:00:00', 'pending', 'west',  'c@x')"
            ") AS t(order_id, amount, order_date, status, region, customer_email)"
        )
        seed.execute(
            f"COPY t TO '{parquet_path.as_posix()}' (FORMAT PARQUET)"
        )
        seed.close()

        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        apply_config(con, load_config(FIXTURES / "gibran.yaml"))

        # Override the orders source to be a parquet pointing at our tmp file.
        con.execute(
            "UPDATE gibran_sources SET source_type = 'parquet', uri = ? "
            "WHERE source_id = 'orders'",
            [parquet_path.as_posix()],
        )

        # Disable the freshness rule -- our seeded data is from 2026-05-13,
        # not "now", so a 24-hour max would fail in real time.
        con.execute("DELETE FROM gibran_freshness_rules")

        identity = CLIResolver(
            user_id="alice", role_id="analyst_west", attributes={"region": "west"}
        ).resolve(None)
        obs = DefaultObservability(con)
        gov = DefaultGovernance(con, observability=obs)

        # Run quality checks first so source_health is populated (otherwise
        # governance.evaluate denies with QUALITY_BLOCK: no_run_recorded).
        # This also exercises the runner-on-parquet path end-to-end.
        run_checks(con, "orders", obs)

        sql = compile_intent(
            QueryIntent(source="orders", metrics=["order_count"]),
            Catalog(con),
        ).render()
        # The compiled SQL should reference read_parquet, with an alias of 'orders'.
        assert "read_parquet(" in sql
        assert 'AS "orders"' in sql

        result = run_dsl_query(
            con, gov, identity,
            {"source": "orders", "metrics": ["order_count"]},
        )
        # Pre-compile error path?
        assert result.pre_compile_error is None
        assert result.query_result is not None
        assert result.query_result.status == "ok", (
            f"denied={result.query_result.deny_reason} "
            f"detail={result.query_result.deny_detail} "
            f"err={result.query_result.error_message} "
            f"sql={result.query_result.rewritten_sql}"
        )
        # 2 of 3 rows are in west; the policy restricts to region=west.
        assert result.query_result.rows == ((2,),)


class TestDSLCompileEmitsRelationalFrom:
    """For duckdb_table / sql_view sources, the FROM clause stays a bare
    quoted identifier (no read_* function call)."""

    def test_duckdb_table_from_is_bare_ident(self) -> None:
        con = duckdb.connect(":memory:")
        apply_migrations(con, MIGRATIONS)
        apply_config(con, load_config(FIXTURES / "gibran.yaml"))
        # The fixture uses duckdb_table type already.
        sql = compile_intent(
            QueryIntent(source="orders", metrics=["order_count"]),
            Catalog(con),
        ).render()
        assert 'FROM "orders"' in sql
        assert "read_parquet" not in sql
        assert "read_csv" not in sql
