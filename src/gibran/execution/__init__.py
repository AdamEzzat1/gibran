"""Execution layer (the 'execution_glue' from the architect prompt).

Routes user queries through governance, rewrites SQL with the injected
filter, executes via DuckDB, writes the audit log row. V1 surface:
single SELECT, single source, no joins / subqueries / CTEs / SELECT *.

The DSL pipeline (Phase 2) will produce SQL that flows through this same
runner -- SQL path and DSL path share execution + audit; only the input
shape differs."""
from gibran.execution.sql import (
    QueryParseError,
    QueryResult,
    UnsupportedQueryError,
    run_sql_query,
)

__all__ = [
    "QueryParseError",
    "QueryResult",
    "UnsupportedQueryError",
    "run_sql_query",
]
