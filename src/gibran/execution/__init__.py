"""Execution layer (the 'execution_glue' from the architect prompt).

Routes user queries through governance, rewrites SQL with the injected
filter, executes via the active engine, writes the audit log row. V1
surface: single SELECT, single source, no joins / subqueries / CTEs /
SELECT *.

The DSL pipeline (Phase 2) produces SQL that flows through this same
runner -- SQL path and DSL path share execution + audit; only the input
shape differs.

This `__init__.py` is intentionally minimal. Eagerly importing
`execution.sql` here triggers a circular import chain through
governance/observability/_source_dispatch when `_source_dispatch`
imports engine classes. Callers should import names directly from
the submodule they need:

    from gibran.execution.sql import run_sql_query
    from gibran.execution.dialect import Dialect, active_dialect
    from gibran.execution.engine import DuckDBEngine, ExecutionEngine
"""
