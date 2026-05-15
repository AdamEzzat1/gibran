-- Postgres translation of migrations/0010_table_versions.sql.
-- Data-version tracking for the result cache (Phase 2B).
-- Fully portable -- no DuckDB-specific syntax.

CREATE TABLE gibran_table_versions (
  source_id    TEXT PRIMARY KEY REFERENCES gibran_sources(source_id),
  version      TEXT NOT NULL,
  updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
