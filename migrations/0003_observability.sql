-- Observability V1.5: per-rule staleness windows.
-- Defaults are applied in code (rumi.observability.default) so that the
-- semantic of NULL = "use the default for this kind/cost_class" is preserved.
--
-- V2 (planned) introduces rumi_source_health as a denormalized cache table
-- refreshed by `rumi check`; this migration is forward-compatible -- the
-- staleness_seconds column drives both the V1.5 direct-aggregation path
-- and the V2 cache-refresh path.

ALTER TABLE rumi_quality_rules   ADD COLUMN staleness_seconds INTEGER;
ALTER TABLE rumi_freshness_rules ADD COLUMN staleness_seconds INTEGER;
