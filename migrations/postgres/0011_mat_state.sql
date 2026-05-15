-- Postgres translation of migrations/0011_mat_state.sql.
-- Incremental refresh state for materialized metrics (Phase 2C).
-- Fully portable -- no DuckDB-specific syntax.

CREATE TABLE gibran_mat_state (
  metric_id              TEXT PRIMARY KEY REFERENCES gibran_metrics(metric_id),
  last_refresh_watermark TEXT,
  last_refresh_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
