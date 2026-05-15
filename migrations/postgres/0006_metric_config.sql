-- Postgres translation of migrations/0006_metric_config.sql.
-- JSON metric_config blob on gibran_metric_versions.
-- Difference from DuckDB: JSON -> JSONB.

ALTER TABLE gibran_metric_versions ADD COLUMN metric_config JSONB;
