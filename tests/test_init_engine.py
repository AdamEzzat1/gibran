"""Phase 5A.6 -- `gibran init --engine` CLI tests.

Covers the new `--engine` flag on `gibran init`:
  - No flag -> existing behavior (DuckDB at ./gibran.duckdb)
  - `--engine duckdb:custom.db` -> DuckDB at the given path
  - `--engine postgres://...` is rejected when psycopg isn't installed,
    with a clear install hint
  - `--engine postgres://... --sample` is rejected (sample seeding is
    DuckDB-specific)
  - Unsupported scheme is rejected

Real Postgres integration (engine actually connecting) is exercised in
tests/test_postgres_engine_integration.py and tests/test_migrations_engine.py
under the `GIBRAN_POSTGRES_URL` skip gate. The CLI is just glue here.
"""
from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from gibran.cli.main import app


PROJECT_ROOT = Path(__file__).parent.parent


@pytest.fixture
def staged_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Copy migrations into a tmp dir and chdir there."""
    shutil.copytree(PROJECT_ROOT / "migrations", tmp_path / "migrations")
    monkeypatch.chdir(tmp_path)
    return tmp_path


# ---------------------------------------------------------------------------
# Default behavior (no --engine)
# ---------------------------------------------------------------------------


def test_init_with_no_engine_flag_still_uses_duckdb(staged_env: Path):
    runner = CliRunner()
    result = runner.invoke(app, ["init"])
    assert result.exit_code == 0, result.output
    assert (staged_env / "gibran.duckdb").exists()


# ---------------------------------------------------------------------------
# Explicit DuckDB engine URL
# ---------------------------------------------------------------------------


def test_init_with_explicit_duckdb_engine_default_path(staged_env: Path):
    """`--engine duckdb` (no path) falls back to default ./gibran.duckdb."""
    runner = CliRunner()
    result = runner.invoke(app, ["init", "--engine", "duckdb"])
    assert result.exit_code == 0, result.output
    assert (staged_env / "gibran.duckdb").exists()


def test_init_with_duckdb_engine_custom_path(staged_env: Path):
    """`--engine duckdb:custom.duckdb` creates the DB at the named path."""
    runner = CliRunner()
    custom_path = "custom.duckdb"
    result = runner.invoke(app, ["init", "--engine", f"duckdb:{custom_path}"])
    assert result.exit_code == 0, result.output
    assert (staged_env / custom_path).exists()
    # And NOT at the default path
    assert not (staged_env / "gibran.duckdb").exists()


def test_init_with_duckdb_engine_applies_all_migrations(staged_env: Path):
    """Same migration set as the legacy path."""
    runner = CliRunner()
    result = runner.invoke(app, ["init", "--engine", "duckdb"])
    assert result.exit_code == 0
    db = staged_env / "gibran.duckdb"
    con = duckdb.connect(str(db))
    try:
        versions = [
            r[0]
            for r in con.execute(
                "SELECT version FROM gibran_schema_version ORDER BY version"
            ).fetchall()
        ]
        assert versions == [1, 2, 3, 4, 5, 6, 7, 8, 9]
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Postgres rejections (no real Postgres needed)
# ---------------------------------------------------------------------------


def test_init_postgres_with_sample_is_rejected(staged_env: Path):
    """`--sample` is DuckDB-specific (seeds an in-DuckDB orders table).
    Combining it with `--engine postgres://...` must fail with a clear
    message rather than silently ignoring `--sample`."""
    runner = CliRunner()
    result = runner.invoke(
        app, ["init", "--engine", "postgres://nope/x", "--sample"]
    )
    assert result.exit_code != 0
    assert "sample" in result.output.lower()
    assert "duckdb" in result.output.lower()


def test_init_postgres_without_psycopg_gives_clear_hint(
    staged_env: Path, monkeypatch: pytest.MonkeyPatch
):
    """If psycopg isn't installed, the CLI must surface the install
    hint rather than crashing with a bare ImportError."""
    # Force psycopg import to fail
    import builtins
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "psycopg":
            raise ImportError("simulated missing psycopg")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    runner = CliRunner()
    result = runner.invoke(
        app, ["init", "--engine", "postgres://user:pass@host:5432/db"]
    )
    assert result.exit_code != 0
    # The install hint should mention psycopg or the extras group
    assert "psycopg" in result.output.lower() or "gibran[postgres]" in result.output


# ---------------------------------------------------------------------------
# Unsupported engine scheme
# ---------------------------------------------------------------------------


def test_init_with_unsupported_engine_scheme_is_rejected(staged_env: Path):
    runner = CliRunner()
    result = runner.invoke(app, ["init", "--engine", "snowflake://acct/db"])
    assert result.exit_code != 0
    assert "snowflake" in result.output.lower() or "unsupported" in result.output.lower()


def test_init_with_garbage_engine_is_rejected(staged_env: Path):
    runner = CliRunner()
    result = runner.invoke(app, ["init", "--engine", "not-a-url"])
    assert result.exit_code != 0
    assert "unsupported" in result.output.lower() or "not-a-url" in result.output
