"""Schema-migration runner. Applies idempotent SQL files in lexical order.

Phase 5A.5: now accepts either a raw DuckDB connection (`apply_all`) or
an `ExecutionEngine` (`apply_all_for_engine`). The engine-aware variant
picks the right per-dialect migration directory:

  - `<base_dir>/duckdb/*.sql`  for DuckDBEngine (or legacy flat layout
                                where `<base_dir>/*.sql` is treated as
                                dialect-neutral / DuckDB)
  - `<base_dir>/postgres/*.sql` for PostgresEngine
  - (future) `<base_dir>/snowflake/*.sql`, `<base_dir>/bigquery/*.sql`

The legacy flat-layout fallback preserves backward compatibility for
the 20+ test files and the CLI that hardcode `repo_root/migrations` as
the migrations dir. Once those callers move to engine-aware code paths
(5A.5 follow-up), the fallback can be retired.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from gibran.execution.dialect import Dialect
from gibran.execution.engine import ExecutionEngine


SCHEMA_VERSION_DDL = """
CREATE TABLE IF NOT EXISTS gibran_schema_version (
    version    INTEGER PRIMARY KEY,
    filename   TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


def list_migrations(migrations_dir: Path) -> list[Path]:
    return sorted(migrations_dir.glob("[0-9]*_*.sql"))


def resolve_migrations_dir(base_dir: Path, dialect: Dialect) -> Path:
    """Return the directory containing migrations for `dialect`.

    Lookup order:
      1. `<base_dir>/<dialect.value>/`  -- the canonical per-dialect layout
      2. `<base_dir>/`                  -- legacy flat layout (DuckDB only)

    For DuckDB, the flat layout is the original gibran shape and remains
    the default until all callers migrate. For non-DuckDB dialects, the
    flat layout is rejected -- a Postgres deployment without
    `<base_dir>/postgres/` migrations is almost certainly a config error.
    """
    per_dialect = base_dir / dialect.value
    if per_dialect.is_dir() and list_migrations(per_dialect):
        return per_dialect
    if dialect == Dialect.DUCKDB and list_migrations(base_dir):
        return base_dir  # legacy flat layout
    raise FileNotFoundError(
        f"No migrations found for dialect={dialect.value!r} under "
        f"{base_dir}. Expected SQL files in {per_dialect}/."
    )


def apply_all(
    con: duckdb.DuckDBPyConnection,
    migrations_dir: Path,
) -> list[int]:
    """Apply unapplied migrations in lexical order. Idempotent.

    Legacy entry point: takes a raw DuckDB connection and a flat
    migrations directory. New code should prefer `apply_all_for_engine`,
    which picks the right per-dialect subdirectory.

    Returns versions applied this run. Each migration runs in its own
    transaction; a failure rolls back that migration only and re-raises.
    """
    return _apply_all_impl(_RawConExecutor(con), migrations_dir)


def apply_all_for_engine(
    engine: ExecutionEngine,
    base_dir: Path,
) -> list[int]:
    """Apply migrations through an engine, picking the right per-dialect
    directory under `base_dir`.

    For DuckDB engines, falls back to the flat `base_dir` layout if
    `base_dir/duckdb/` doesn't exist (preserves backward compat).
    For other dialects, requires `base_dir/<dialect>/` to exist.
    """
    migrations_dir = resolve_migrations_dir(base_dir, engine.dialect)
    return _apply_all_impl(_EngineExecutor(engine), migrations_dir)


# ---------------------------------------------------------------------------
# Shared implementation
# ---------------------------------------------------------------------------


class _RawConExecutor:
    """Adapter so the migration loop can speak the old con.execute(...)
    API without caring whether the underlying caller passed an engine
    or a connection. Used by `apply_all` (legacy)."""

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def execute(self, sql: str, params: tuple | list = ()) -> None:
        if params:
            self.con.execute(sql, list(params))
        else:
            self.con.execute(sql)

    def fetchall(self, sql: str) -> list[tuple]:
        return self.con.execute(sql).fetchall()


class _EngineExecutor:
    """Adapter that delegates to the engine's execute/query methods."""

    def __init__(self, engine: ExecutionEngine) -> None:
        self.engine = engine

    def execute(self, sql: str, params: tuple | list = ()) -> None:
        self.engine.execute(sql, params)

    def fetchall(self, sql: str) -> list[tuple]:
        rows, _ = self.engine.query(sql)
        return rows


def _apply_all_impl(executor, migrations_dir: Path) -> list[int]:
    executor.execute(SCHEMA_VERSION_DDL)
    applied = {
        row[0]
        for row in executor.fetchall("SELECT version FROM gibran_schema_version")
    }
    newly: list[int] = []
    for path in list_migrations(migrations_dir):
        version = int(path.name.split("_", 1)[0])
        if version in applied:
            continue
        sql = path.read_text(encoding="utf-8")
        # Per-migration transaction: roll back on failure so the schema
        # never ends up half-applied.
        executor.execute("BEGIN")
        try:
            executor.execute(sql)
            executor.execute(
                "INSERT INTO gibran_schema_version (version, filename) VALUES (?, ?)",
                [version, path.name],
            )
            executor.execute("COMMIT")
        except Exception:
            executor.execute("ROLLBACK")
            raise
        newly.append(version)
    return newly
