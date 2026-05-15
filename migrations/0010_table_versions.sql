-- Phase 2B: data-version tracking for the result cache.
--
-- Before this migration, the result cache invalidates on
-- catalog_generation (bumped by `gibran sync`) + source_health_generation
-- (bumped by `gibran check`). A parquet file rewritten between syncs
-- still serves the old cached row -- the data-version hole.
--
-- This table fills the hole for the source types where gibran can't
-- probe a file system mtime:
--   * parquet / csv  -- handled at lookup time via os.stat().st_mtime_ns
--                       (no row in this table; cache_key reads the
--                       file system directly)
--   * duckdb_table   -- has no DuckDB-native version counter, so we
--                       store an opaque token here. Bumped by the
--                       `gibran touch <source>` CLI when the operator
--                       knows the underlying table changed externally.
--   * sql_view       -- treated like duckdb_table for V1 (touched
--                       manually). Recursive derivation from the
--                       view's referenced tables is deferred to
--                       Phase 3; the limitation is documented in the
--                       cache module's docstring.
--
-- An absent row means "version 0" -- semantically equivalent to "the
-- source has never been touched, treat the cache as authoritative
-- until sync/check bumps the other generations." The first `touch` on
-- a source inserts the row.

CREATE TABLE gibran_table_versions (
  source_id    TEXT PRIMARY KEY REFERENCES gibran_sources(source_id),
  version      TEXT NOT NULL,             -- opaque token (uuid hex)
  updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
