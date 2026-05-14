"""Tests for `gibran init --sample`.

Verifies:
  - --sample creates gibran.yaml in CWD
  - --sample seeds a populated `orders` table
  - The full quickstart pipeline (sync + check + query) works against
    the seeded sample without any manual setup
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from gibran.cli.main import app


PROJECT_ROOT = Path(__file__).parent.parent


@pytest.fixture
def sample_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Stage a tmp dir with a migrations/ symlink (copy on Windows) so
    init can find it, and chdir there."""
    migrations_src = PROJECT_ROOT / "migrations"
    migrations_dst = tmp_path / "migrations"
    # Copy migrations rather than symlink -- Windows symlinks need
    # admin/dev-mode and we want this test to run anywhere.
    shutil.copytree(migrations_src, migrations_dst)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_init_sample_creates_yaml(sample_env: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["init", "--sample"])
    assert result.exit_code == 0, result.output
    cfg = sample_env / "gibran.yaml"
    assert cfg.exists()
    text = cfg.read_text(encoding="utf-8")
    assert "sources:" in text
    assert "id: orders" in text
    assert "type: duckdb_table" in text


def test_init_sample_seeds_orders_table(sample_env: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["init", "--sample"])
    assert result.exit_code == 0
    db = sample_env / "gibran.duckdb"
    assert db.exists()
    con = duckdb.connect(str(db))
    try:
        rows = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        assert rows == 4
    finally:
        con.close()


def test_init_sample_does_not_overwrite_existing_yaml(
    sample_env: Path,
) -> None:
    cfg = sample_env / "gibran.yaml"
    cfg.write_text("# existing user config\nsources: []\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(app, ["init", "--sample"])
    assert result.exit_code == 0
    # Should NOT clobber the existing file.
    assert "existing user config" in cfg.read_text(encoding="utf-8")


def test_full_quickstart_pipeline_runs(sample_env: Path) -> None:
    """sync + query against the sample seed work end-to-end."""
    runner = CliRunner()
    init_r = runner.invoke(app, ["init", "--sample"])
    assert init_r.exit_code == 0, init_r.output

    sync_r = runner.invoke(app, ["sync"])
    assert sync_r.exit_code == 0, sync_r.output

    query_r = runner.invoke(app, [
        "query",
        "--role", "analyst_west", "--attr", "region=west",
        "--dsl", json.dumps({"source": "orders", "metrics": ["order_count"]}),
    ])
    assert query_r.exit_code == 0, query_r.output
    # 2 of 4 rows are west.
    assert "2" in query_r.output


def test_init_without_sample_does_not_create_yaml(sample_env: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["init"])
    assert result.exit_code == 0
    assert not (sample_env / "gibran.yaml").exists()
