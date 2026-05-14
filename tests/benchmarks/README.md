# Benchmarks

Performance baselines for Gibran's hot paths. Not collected by `pytest tests`
(the module is named `bench.py`, not `test_*.py`, so the default collector
skips it).

## Run

```
python -m tests.benchmarks.bench
```

Output is a Markdown table on stdout with median + p99 latency per operation
at three data sizes (1k / 10k / 50k rows).

## What's measured

| operation | what it covers |
|---|---|
| `compile(count)` | `compile_intent` for a single-SELECT primitive — independent of data size, indicates Python overhead of the compile pipeline. |
| `e2e(count)` | `run_dsl_query` end-to-end: Pydantic parse + AllowedSchema lookup + semantic validation + compile + governance evaluate + sqlglot rewrite + DuckDB execute + audit-log write. |
| `e2e(cohort_retention)` | Same shape but for the 3-CTE cohort_retention primitive (Tier 3). Catches regressions in CTE compile speed and the column-walker. |

## Smoke coverage

`tests/test_benchmarks_smoke.py` runs `bench(sizes=[100], iterations=2)` so the
benchmark code stays in CI coverage. Doesn't measure performance — just
verifies the benchmark module runs end-to-end.

## When to add a benchmark

Add one whenever you ship a perf-sensitive change (plan caching, result
caching, materialized metrics). The before/after comparison is the whole
point.
