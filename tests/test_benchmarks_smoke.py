"""Smoke test for the benchmark module.

The benchmark code lives in `tests/benchmarks/bench.py` and is not
collected by pytest's default discovery (its filename doesn't start
with `test_`). This file exists so the code itself stays exercised by
the regular CI suite: it runs `bench()` at the smallest size with
minimal iterations, just enough to verify the imports + control flow
work. Real performance measurements live in `bench.py:main`.
"""
from __future__ import annotations

from tests.benchmarks.bench import bench, render_markdown


def test_bench_runs_at_smallest_size() -> None:
    results = bench(sizes=[100], iterations=2)
    assert len(results) == 3  # compile + e2e_count + e2e_cohort
    for r in results:
        assert r.median_ms > 0
        assert r.p99_ms >= r.median_ms
        assert r.iterations == 2


def test_render_markdown_produces_table() -> None:
    results = bench(sizes=[100], iterations=2)
    table = render_markdown(results)
    assert table.startswith("| operation |")
    assert "compile(count)" in table
    assert "e2e(cohort_retention)" in table
