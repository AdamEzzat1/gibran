-- Postgres translation of migrations/0007_time_bound_policies.sql.
-- `valid_until` for time-bound access grants. Fully portable.

ALTER TABLE gibran_policies ADD COLUMN valid_until TIMESTAMP;
