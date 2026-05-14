-- Storage for sampled example values on low-cardinality public columns.
-- Populated by gibran/sync/example_values.py during `gibran sync` and
-- read by governance.preview_schema into AllowedSchema.ColumnView.
--
-- JSON list of strings (each value is str-coerced; NULL preserved as
-- the JSON null literal). NULL/absent for columns we didn't sample:
--   * sensitivity != 'public' -- never sample PII/internal/restricted
--   * expose_examples = FALSE  -- explicit per-column opt-out
--   * cardinality above the low-cardinality threshold
--   * source unreachable at sync time

ALTER TABLE gibran_columns ADD COLUMN example_values JSON;
