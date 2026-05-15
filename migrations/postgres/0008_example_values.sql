-- Postgres translation of migrations/0008_example_values.sql.
-- Sampled example values on low-cardinality public columns.
-- Difference from DuckDB: JSON -> JSONB.

ALTER TABLE gibran_columns ADD COLUMN example_values JSONB;
