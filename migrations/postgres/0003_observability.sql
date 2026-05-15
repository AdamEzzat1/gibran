-- Postgres translation of migrations/0003_observability.sql.
-- Per-rule staleness windows. Fully portable -- no DuckDB-specific syntax.

ALTER TABLE gibran_quality_rules   ADD COLUMN staleness_seconds INTEGER;
ALTER TABLE gibran_freshness_rules ADD COLUMN staleness_seconds INTEGER;
