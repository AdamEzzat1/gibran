-- Phase 2C: incremental refresh state for materialized metrics.
--
-- Before this migration, `gibran sync` ran CREATE OR REPLACE TABLE
-- gibran_mat_<metric_id> for every materialized metric on every pass.
-- At 100k+ source rows this becomes the slowest part of sync; at 1M+
-- it's untenable.
--
-- This table records the per-metric refresh state for the incremental
-- strategy. On each pass:
--   1. Read last_refresh_watermark for the metric (NULL = first run).
--   2. Run DELETE + INSERT against the materialized table covering rows
--      where source.watermark_column > (last_refresh_watermark - grace).
--   3. UPDATE this row with the new MAX(watermark_column) seen.
--
-- The `last_refresh_watermark` is stored as TEXT because the watermark
-- column may be any type (TIMESTAMP, BIGINT, DATE). The applier
-- serializes via the source DB's native CAST(... AS VARCHAR) and parses
-- it back into the right context at refresh time.

CREATE TABLE gibran_mat_state (
  metric_id              TEXT PRIMARY KEY REFERENCES gibran_metrics(metric_id),
  last_refresh_watermark TEXT,                     -- NULL = never refreshed
  last_refresh_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
