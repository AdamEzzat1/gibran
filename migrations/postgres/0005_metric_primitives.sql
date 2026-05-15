-- Postgres translation of migrations/0005_metric_primitives.sql.
-- Widen metric_type vocabulary by recreate-and-copy.
--
-- Note: Postgres DOES support ALTER on CHECK constraints (DROP CONSTRAINT
-- + ADD CONSTRAINT), so a simpler in-place edit would work here. We
-- keep the recreate-and-copy pattern to stay parallel with the DuckDB
-- migration -- same shape, same final state, less divergence to reason
-- about during the per-dialect translation pass.

CREATE TABLE gibran_metrics_new (
  metric_id       TEXT PRIMARY KEY,
  source_id       TEXT NOT NULL REFERENCES gibran_sources,
  display_name    TEXT NOT NULL,
  metric_type     TEXT NOT NULL,   -- no CHECK; validated by Pydantic Literal in yaml_schema
  unit            TEXT,
  description     TEXT,
  owner           TEXT,
  current_version INTEGER NOT NULL DEFAULT 1
);

INSERT INTO gibran_metrics_new
  (metric_id, source_id, display_name, metric_type, unit, description, owner, current_version)
SELECT
  metric_id, source_id, display_name, metric_type, unit, description, owner, current_version
FROM gibran_metrics;

DROP TABLE gibran_metrics;
ALTER TABLE gibran_metrics_new RENAME TO gibran_metrics;
