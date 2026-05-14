-- Add `valid_until` to gibran_policies for time-bound access grants
-- (contractors, consultants, temporary credentials).
--
-- NULL means "never expires" -- the common case. A non-NULL timestamp is
-- compared against DuckDB's CURRENT_TIMESTAMP at evaluate-time inside the
-- _fetch_policy SQL, not in Python, to avoid the UTC-drift class of bugs
-- documented in 0001 (DATE_DIFF/CURRENT_TIMESTAMP vs datetime.now()).
--
-- No default; explicit opt-in by the policy author. No CHECK constraint --
-- expiry-vs-now is a runtime comparison, not a stored invariant.

ALTER TABLE gibran_policies ADD COLUMN valid_until TIMESTAMP;
