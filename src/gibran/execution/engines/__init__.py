"""Per-dialect execution engines.

Each module implements the `ExecutionEngine` protocol defined in
`gibran.execution.engine` for one target backend. Engines are imported
from this package, e.g.:

    from gibran.execution.engines.duckdb import DuckDBEngine
    from gibran.execution.engines.postgres import PostgresEngine

For backward-compatibility, `gibran.execution.engine` re-exports the
DuckDB and Postgres engines so older imports continue to work.
"""
