# Postgres-flavored migrations

Complete translations of all 9 DuckDB migrations as of 5A.5 cleanup.

## Status

| Migration | Translated | Notes |
|---|---|---|
| 0001_metadata.sql | ✅ | `JSON` → `JSONB` |
| 0002_governance.sql | ✅ | `JSON` → `JSONB` |
| 0003_observability.sql | ✅ | Fully portable |
| 0004_source_health.sql | ✅ | `JSON` → `JSONB`; `DEFAULT '[]'::jsonb` |
| 0005_metric_primitives.sql | ✅ | Same recreate-and-copy pattern |
| 0006_metric_config.sql | ✅ | `JSON` → `JSONB` |
| 0007_time_bound_policies.sql | ✅ | Fully portable |
| 0008_example_values.sql | ✅ | `JSON` → `JSONB` |
| 0009_tier4_governance.sql | ✅ | `JSON` → `JSONB`; kept two-step pattern for parity |

## Verification status

The translated SQL has **NOT been executed against a real Postgres** in
this repo's CI (no paid Postgres infrastructure). The translations are
mechanical and well-understood (JSON ↔ JSONB is the main divergence),
but a Postgres CI run is the only way to catch surprises.

To verify locally:

```bash
# Spin up a Postgres
docker run --rm -d --name gibran-pg \
  -e POSTGRES_PASSWORD=test -p 5432:5432 postgres:16

# Install gibran + Postgres extras
pip install -e .[dev,postgres]

# Apply all migrations through the engine
export GIBRAN_POSTGRES_URL=postgresql://postgres:test@localhost:5432/postgres
gibran init --engine $GIBRAN_POSTGRES_URL

# Or run the integration test
GIBRAN_POSTGRES_URL=postgresql://postgres:test@localhost:5432/postgres \
  pytest tests/test_migrations_engine.py::test_apply_all_for_engine_postgres -v
```

## Known divergences from DuckDB

These differences are intentional and documented in the per-migration
comments:

- `JSON` → `JSONB`: Postgres's JSONB supports indexing and is the
  recommended JSON storage type.
- `JSONB DEFAULT '[]'::jsonb`: Postgres requires the explicit `::jsonb`
  cast on default literals; DuckDB infers from the column type.
- Recreate-and-copy patterns: Postgres COULD use `ALTER TABLE ... DROP
  CONSTRAINT / ADD CONSTRAINT` directly, but the recreate-and-copy
  pattern matches the DuckDB original verbatim. Less divergence to
  maintain during dual-engine reviews.
