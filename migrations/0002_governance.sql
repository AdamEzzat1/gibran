-- Roles, attributes, policies. Depends on rumi_sources from 0001.

CREATE TABLE rumi_roles (
  role_id      TEXT PRIMARY KEY,
  display_name TEXT NOT NULL
);

CREATE TABLE rumi_role_attributes (
  role_id         TEXT NOT NULL REFERENCES rumi_roles,
  attribute_key   TEXT NOT NULL,
  attribute_value TEXT,
  PRIMARY KEY (role_id, attribute_key)
);

CREATE TABLE rumi_user_attributes (
  user_id         TEXT NOT NULL,
  attribute_key   TEXT NOT NULL,
  attribute_value TEXT,
  PRIMARY KEY (user_id, attribute_key)
);

CREATE TABLE rumi_policies (
  policy_id           TEXT PRIMARY KEY,
  role_id             TEXT NOT NULL REFERENCES rumi_roles,
  source_id           TEXT NOT NULL REFERENCES rumi_sources,
  row_filter_ast      JSON,
  default_column_mode TEXT NOT NULL DEFAULT 'deny'
                      CHECK (default_column_mode IN ('allow','deny')),
  schema_version      INTEGER NOT NULL DEFAULT 1,
  UNIQUE (role_id, source_id)
);

CREATE TABLE rumi_policy_columns (
  policy_id   TEXT NOT NULL REFERENCES rumi_policies,
  column_name TEXT NOT NULL,
  granted     BOOLEAN NOT NULL,
  PRIMARY KEY (policy_id, column_name)
);
