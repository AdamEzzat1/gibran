-- Catalog, metric versions, quality, freshness, audit log.
-- Independent of the governance schema in 0002.

CREATE TABLE rumi_sensitivity_levels (
  level_id     TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  description  TEXT
);

INSERT INTO rumi_sensitivity_levels (level_id, display_name, description) VALUES
  ('public',       'Public',       'No restrictions; safe for any role and audit context.'),
  ('internal',     'Internal',     'Internal use only; not for external partners.'),
  ('pii',          'PII',          'Personal identifying information; restricted by default.'),
  ('restricted',   'Restricted',   'Highest restriction; explicit grant required.'),
  ('unclassified', 'Unclassified', 'Auto-inferred; example values never flow until classified.');

CREATE TABLE rumi_sources (
  source_id      TEXT PRIMARY KEY,
  display_name   TEXT NOT NULL,
  source_type    TEXT NOT NULL CHECK (source_type IN ('parquet','csv','duckdb_table','sql_view')),
  uri            TEXT NOT NULL,
  primary_grain  TEXT,
  schema_version INTEGER NOT NULL DEFAULT 1,
  created_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE rumi_columns (
  source_id       TEXT NOT NULL REFERENCES rumi_sources,
  column_name     TEXT NOT NULL,
  data_type       TEXT NOT NULL,
  sensitivity     TEXT NOT NULL DEFAULT 'unclassified' REFERENCES rumi_sensitivity_levels,
  description     TEXT,
  expose_examples BOOLEAN,
  PRIMARY KEY (source_id, column_name)
);

CREATE TABLE rumi_dimensions (
  dimension_id  TEXT PRIMARY KEY,
  source_id     TEXT NOT NULL REFERENCES rumi_sources,
  column_name   TEXT NOT NULL,
  display_name  TEXT NOT NULL,
  dim_type      TEXT NOT NULL CHECK (dim_type IN ('categorical','temporal','numeric_bin')),
  description   TEXT,
  UNIQUE (source_id, column_name)
);

CREATE TABLE rumi_metrics (
  metric_id       TEXT PRIMARY KEY,
  source_id       TEXT NOT NULL REFERENCES rumi_sources,
  display_name    TEXT NOT NULL,
  metric_type     TEXT NOT NULL CHECK (metric_type IN
                    ('count','sum','avg','min','max','ratio','expression')),
  unit            TEXT,
  description     TEXT,
  owner           TEXT,
  current_version INTEGER NOT NULL DEFAULT 1
);

-- rumi_metric_versions, rumi_metric_dependencies, rumi_query_metrics:
-- metric_id is logically a foreign key to rumi_metrics, but DuckDB's FK
-- enforcement treats parent UPDATE as delete-then-insert (even on non-PK
-- columns), which blocks legitimate metric metadata edits. Integrity is
-- enforced by sync.loader for catalog writes; runtime writers must validate
-- against rumi_metrics before insert.
CREATE TABLE rumi_metric_versions (
  metric_id      TEXT NOT NULL,
  version        INTEGER NOT NULL,
  expression     TEXT NOT NULL,
  filter_sql     TEXT,
  effective_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  effective_to   TIMESTAMP,
  PRIMARY KEY (metric_id, version)
);

CREATE TABLE rumi_metric_dependencies (
  metric_id     TEXT NOT NULL,
  depends_on_id TEXT NOT NULL,
  PRIMARY KEY (metric_id, depends_on_id)
);

CREATE TABLE rumi_quality_rules (
  rule_id     TEXT PRIMARY KEY,
  source_id   TEXT NOT NULL REFERENCES rumi_sources,
  rule_type   TEXT NOT NULL CHECK (rule_type IN ('not_null','unique','range','custom_sql')),
  rule_config JSON NOT NULL,
  cost_class  TEXT NOT NULL DEFAULT 'expensive' CHECK (cost_class IN ('cheap','expensive')),
  severity    TEXT NOT NULL CHECK (severity IN ('warn','block')),
  enabled     BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE rumi_freshness_rules (
  rule_id          TEXT PRIMARY KEY,
  source_id        TEXT NOT NULL REFERENCES rumi_sources,
  watermark_column TEXT NOT NULL,
  max_age_seconds  INTEGER NOT NULL,
  severity         TEXT NOT NULL CHECK (severity IN ('warn','block'))
);

CREATE TABLE rumi_quality_runs (
  run_id         TEXT PRIMARY KEY,
  rule_id        TEXT NOT NULL,
  rule_kind      TEXT NOT NULL CHECK (rule_kind IN ('quality','freshness')),
  passed         BOOLEAN NOT NULL,
  observed_value JSON,
  ran_at         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX rumi_quality_runs_rule_idx ON rumi_quality_runs (rule_id, ran_at);

CREATE TABLE rumi_query_log (
  query_id      TEXT PRIMARY KEY,
  user_id       TEXT NOT NULL,
  role_id       TEXT,
  nl_prompt     TEXT,
  generated_sql TEXT NOT NULL,
  status        TEXT NOT NULL CHECK (status IN ('ok','denied','error','timeout')),
  deny_reason   TEXT,
  row_count     BIGINT,
  duration_ms   BIGINT,
  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX rumi_query_log_user_idx ON rumi_query_log (user_id, created_at);

CREATE TABLE rumi_query_metrics (
  query_id  TEXT NOT NULL REFERENCES rumi_query_log,
  metric_id TEXT NOT NULL,                              -- see rumi_metric_versions comment above
  version   INTEGER NOT NULL,
  PRIMARY KEY (query_id, metric_id, version)
);

CREATE INDEX rumi_query_metrics_metric_idx ON rumi_query_metrics (metric_id, version);
