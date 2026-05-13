-- Expand the metric vocabulary with window-function primitives.
--
-- Two new metric_type values added this migration:
--   percentile        -- QUANTILE_CONT(col, p), a regular aggregate
--   rolling_window    -- SUM(col) OVER (ORDER BY ... RANGE ... PRECEDING ...)
--
-- Three more primitives (period_over_period, cohort_retention, funnel) are
-- intentionally deferred -- they require multi-stage SQL (CTE chains, output
-- matrices, sequential step matching) that's a compiler-architecture pass,
-- not an enum extension.
--
-- DuckDB does not support ALTER on CHECK constraints, so we widen the
-- metric_type vocabulary by dropping the CHECK constraint entirely and
-- relying on Pydantic Literal-typed validation in rumi.sync.yaml_schema.
-- The DB-layer CHECK was always defense-in-depth here; the application
-- layer remains the source of truth.

CREATE TABLE rumi_metrics_new (
  metric_id       TEXT PRIMARY KEY,
  source_id       TEXT NOT NULL REFERENCES rumi_sources,
  display_name    TEXT NOT NULL,
  metric_type     TEXT NOT NULL,   -- no CHECK; validated by Pydantic Literal in yaml_schema
  unit            TEXT,
  description     TEXT,
  owner           TEXT,
  current_version INTEGER NOT NULL DEFAULT 1
);

INSERT INTO rumi_metrics_new
  (metric_id, source_id, display_name, metric_type, unit, description, owner, current_version)
SELECT
  metric_id, source_id, display_name, metric_type, unit, description, owner, current_version
FROM rumi_metrics;

DROP TABLE rumi_metrics;
ALTER TABLE rumi_metrics_new RENAME TO rumi_metrics;
