"""Rumi: governed analytics + NL-to-SQL over DuckDB.

Layer split:
- catalog       sources, columns, dimensions, metrics
- governance    identity, policies, query rewriting (security-critical wedge)
- semantic      metric/dimension compiler
- observability quality, freshness, audit
- nl            natural-language to SQL pipeline
- perf          query plans, index recommendations
- sync          YAML-to-DB synchronization (rumi sync, migrations)
- cli           typer entrypoint

Every layer that consumes data reads governance.AllowedSchema and submits
requests through governance.evaluate(). The governance layer is the only
layer that may rewrite SQL; storage/execution stays DuckDB."""
__version__ = "0.0.1"
