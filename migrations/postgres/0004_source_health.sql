-- Postgres translation of migrations/0004_source_health.sql.
-- Denormalized per-source health cache.
--
-- Differences from DuckDB original:
--   * JSON columns -> JSONB
--   * JSONB defaults need an explicit ::jsonb cast (Postgres treats the
--     bare '[]' literal as a TEXT, not a JSONB).
--   * `now()` is portable (Postgres has it; CURRENT_TIMESTAMP would also work).

CREATE TABLE gibran_source_health (
  source_id          TEXT PRIMARY KEY,
  status             TEXT NOT NULL CHECK (status IN ('healthy','warn','block','unknown')),
  blocking_failures  JSONB NOT NULL DEFAULT '[]'::jsonb,
  warnings           JSONB NOT NULL DEFAULT '[]'::jsonb,
  refreshed_at       TIMESTAMP NOT NULL DEFAULT now()
);
