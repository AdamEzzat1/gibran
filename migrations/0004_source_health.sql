-- Observability V2: denormalized per-source health cache.
--
-- Populated by `gibran check` (via DefaultObservability.refresh_health).
-- Read O(1) by governance.evaluate -> latest_blocking_failures.
--
-- When this row is missing for a source, the V1.5 fallback path in
-- DefaultObservability re-aggregates from gibran_quality_runs on the fly --
-- so this table is a performance optimization, not a correctness gate.

CREATE TABLE gibran_source_health (
  source_id          TEXT PRIMARY KEY,
  status             TEXT NOT NULL CHECK (status IN ('healthy','warn','block','unknown')),
  blocking_failures  JSON NOT NULL DEFAULT '[]',
  warnings           JSON NOT NULL DEFAULT '[]',
  refreshed_at       TIMESTAMP NOT NULL DEFAULT now()
);
