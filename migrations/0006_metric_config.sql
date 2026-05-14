-- Add a JSON `metric_config` blob to gibran_metric_versions for primitives
-- that need typed configuration beyond a single SQL expression string.
--
-- Why a JSON blob rather than typed columns:
--   - Each metric primitive's config shape differs (period_over_period has
--     base_metric/period_dim/period_unit/comparison; future cohort_retention
--     will have cohort_dim/retention_dim/etc). Per-primitive columns would
--     mean a migration per primitive plus N NULL columns per row.
--   - The Pydantic Literal in gibran.sync.yaml_schema.MetricConfig is the source
--     of truth for shape; the DB column is just structured storage.
--
-- For metric_types that don't need config (count/sum/avg/min/max/ratio/expression/
-- percentile/rolling_window) the column stays NULL.

ALTER TABLE gibran_metric_versions ADD COLUMN metric_config JSON;
