-- Postgres translation of migrations/0009_tier4_governance.sql.
-- Tier 4 strategic primitives (anomaly rule type, break-glass roles,
-- approval workflow, webhook alerting).
--
-- Differences from DuckDB:
--   * JSON columns -> JSONB throughout.
--   * Postgres supports `ALTER TABLE ADD COLUMN ... DEFAULT ... NOT NULL`
--     directly, so the "add nullable then UPDATE" two-step is unnecessary
--     for new BOOLEAN columns. We keep the two-step pattern anyway for
--     parity with the DuckDB migration -- end state is identical.

-- 1. Widen quality_rule.rule_type to accept 'anomaly' and add
--    alert_webhook column.
CREATE TABLE gibran_quality_rules_new (
  rule_id            TEXT PRIMARY KEY,
  source_id          TEXT NOT NULL REFERENCES gibran_sources,
  rule_type          TEXT NOT NULL,  -- validated by Pydantic Literal
  rule_config        JSONB NOT NULL,
  cost_class         TEXT NOT NULL DEFAULT 'expensive' CHECK (cost_class IN ('cheap','expensive')),
  severity           TEXT NOT NULL CHECK (severity IN ('warn','block')),
  staleness_seconds  INTEGER,
  enabled            BOOLEAN NOT NULL DEFAULT TRUE,
  alert_webhook      TEXT
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

-- 2. Break-glass role flag. Same two-step pattern as DuckDB for parity.
ALTER TABLE gibran_roles ADD COLUMN is_break_glass BOOLEAN;
UPDATE gibran_roles SET is_break_glass = FALSE;

-- 3. Audit-log break-glass marker.
ALTER TABLE gibran_query_log ADD COLUMN is_break_glass BOOLEAN;
UPDATE gibran_query_log SET is_break_glass = FALSE;

-- 4. Approval workflow: pending changes queue.
CREATE TABLE gibran_pending_changes (
  change_id       TEXT PRIMARY KEY,
  change_type     TEXT NOT NULL,             -- 'policy' | 'column_sensitivity' | 'role'
  payload_json    JSONB NOT NULL,
  requested_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  requested_by    TEXT,                      -- user_id of submitter (audit only)
  reason          TEXT,                      -- free-form rationale
  approved_at     TIMESTAMP,                 -- NULL = still pending
  approved_by     TEXT
);

CREATE INDEX gibran_pending_changes_status
  ON gibran_pending_changes (approved_at);
