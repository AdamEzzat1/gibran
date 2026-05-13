from __future__ import annotations

from pathlib import Path

import duckdb


SCHEMA_VERSION_DDL = """
CREATE TABLE IF NOT EXISTS rumi_schema_version (
    version    INTEGER PRIMARY KEY,
    filename   TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


def list_migrations(migrations_dir: Path) -> list[Path]:
    return sorted(migrations_dir.glob("[0-9]*_*.sql"))


def apply_all(
    con: duckdb.DuckDBPyConnection,
    migrations_dir: Path,
) -> list[int]:
    """Apply unapplied migrations in lexical order. Idempotent.

    Returns versions applied this run. Each migration runs in its own
    transaction; a failure rolls back that migration only and re-raises.
    """
    con.execute(SCHEMA_VERSION_DDL)
    applied = {
        row[0]
        for row in con.execute(
            "SELECT version FROM rumi_schema_version"
        ).fetchall()
    }
    newly: list[int] = []
    for path in list_migrations(migrations_dir):
        version = int(path.name.split("_", 1)[0])
        if version in applied:
            continue
        sql = path.read_text(encoding="utf-8")
        con.execute("BEGIN")
        try:
            con.execute(sql)
            con.execute(
                "INSERT INTO rumi_schema_version (version, filename) VALUES (?, ?)",
                [version, path.name],
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        newly.append(version)
    return newly
