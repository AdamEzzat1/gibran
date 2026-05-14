-- Tier 4 strategic primitives. Additive schema changes covering:
--   * Anomaly-detection rule type (numeric observations stored in
--     gibran_quality_runs.observed_value, evaluated against trailing
--     window of past runs for the same rule).
--   * Break-glass role pattern (is_break_glass flag on roles; mirrored
--     onto query-log rows for elevated-access auditing).
--   * Approval workflow (pending-changes queue distinct from the
--     applied catalog).
--   * Webhook alerting (per-rule alert_webhook URL).
--
-- DuckDB does NOT support `ALTER TABLE ADD COLUMN` with DEFAULT or NOT
-- NULL constraints in the same statement (parser error "Adding columns
-- with constraints not yet supported"). We work around this two ways:
--
--   1. For the CHECK-widening on quality_rules.rule_type, recreate the
--      whole table (same pattern as migration 0005).
--   2. For new boolean columns (is_break_glass on roles + query_log),
--      add them as nullable columns and immediately backfill existing
--      rows to FALSE. The application layer treats NULL as FALSE on
--      read, so the column behaves identically to a NOT NULL DEFAULT
--      FALSE column for all callers.

-- 1. Widen quality_rule.rule_type to accept 'anomaly' and add
--    alert_webhook column (recreate-and-copy because DuckDB cannot ALTER
--    a CHECK constraint).
CREATE TABLE gibran_quality_rules_new (
  rule_id            TEXT PRIMARY KEY,
  source_id          TEXT NOT NULL REFERENCES gibran_sources,
  rule_type          TEXT NOT NULL,  -- validated by Pydantic Literal
  rule_config        JSON NOT NULL,
  cost_class         TEXT NOT NULL DEFAULT 'expensive' CHECK (cost_class IN ('cheap','expensive')),
  severity           TEXT NOT NULL CHECK (severity IN ('warn','block')),
  staleness_seconds  INTEGER,
  enabled            BOOLEAN NOT NULL DEFAULT TRUE,
  alert_webhook      TEXT             -- optional URL; nullable
);

INSERT INTO gibran_quality_rules_new
  (rule_id, source_id, rule_type, rule_config, cost_class, severity,
   staleness_seconds, enabled)
SELECT
  rule_id, source_id, rule_type, rule_config, cost_class, severity,
  staleness_seconds, enabled
FROM gibran_quality_rules;

DROP TABLE gibran_quality_rules;
ALTER TABLE gibran_quality_rules_new RENAME TO gibran_quality_rules;

-- 2. Break-glass role flag. Added as nullable; existing rows backfilled
--    to FALSE. The applier writes explicit values going forward (TRUE
--    or FALSE), so the column is functionally NOT NULL despite the
--    nullable schema. Read paths treat NULL as FALSE.
ALTER TABLE gibran_roles ADD COLUMN is_break_glass BOOLEAN;
UPDATE gibran_roles SET is_break_glass = FALSE;

-- 3. Audit-log break-glass marker (same nullable + backfill pattern).
ALTER TABLE gibran_query_log ADD COLUMN is_break_glass BOOLEAN;
UPDATE gibran_query_log SET is_break_glass = FALSE;

-- 4. Approval workflow: pending changes queue.
CREATE TABLE gibran_pending_changes (
  change_id       TEXT PRIMARY KEY,
  change_type     TEXT NOT NULL,             -- 'policy' | 'column_sensitivity' | 'role'
  payload_json    JSON NOT NULL,
  requested_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  requested_by    TEXT,                      -- user_id of submitter (audit only)
  reason          TEXT,                      -- free-form rationale
  approved_at     TIMESTAMP,                 -- NULL = still pending
  approved_by     TEXT
);

CREATE INDEX gibran_pending_changes_status
  ON gibran_pending_changes (approved_at);
