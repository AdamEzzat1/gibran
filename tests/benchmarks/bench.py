"""Performance baselines for Gibran's hot paths.

Measures three things at three data sizes:
  1. DSL compile time (single SELECT primitive)
  2. End-to-end query time (compile + govern + execute + audit) for a
     single SELECT primitive
  3. End-to-end query time for cohort_retention (3-CTE shape)

Why this exists: per ROADMAP.md, until perf measurements exist any
"this is faster" claim is fiction. These baselines give us
apples-to-apples comparison points for future perf work (plan caching,
result caching, etc.).

Run from the repo root:

    python -m tests.benchmarks.bench

Output goes to stdout as a Markdown table. The `bench()` function is
also called by `tests/test_benchmarks_smoke.py` at the smallest size to
keep the benchmark code in CI coverage without slowing the regular
test suite.
"""
from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from pathlib import Path

# When run via `python -m tests.benchmarks.bench` from the repo root,
# `src/` isn't on sys.path -- conftest.py only sets it up for pytest.
# Add it here so the script is self-sufficient.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT / "src"))

import duckdb

from gibran.dsl.compile import Catalog, compile_intent
from gibran.dsl.run import run_dsl_query
from gibran.dsl.types import QueryIntent
from gibran.governance.default import DefaultGovernance
from gibran.governance.types import IdentityContext
from gibran.sync.applier import apply as apply_config
from gibran.sync.loader import load as load_config
from gibran.sync.migrations import apply_all as apply_migrations


MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "migrations"
FIXTURE_YAML = Path(__file__).parent.parent / "fixtures" / "gibran.yaml"


@dataclass
class BenchResult:
    label: str
    row_count: int
    median_ms: float
    p99_ms: float
    iterations: int


def build_db(row_count: int) -> duckdb.DuckDBPyConnection:
    """Build an in-memory DB with the fixture catalog applied, an `orders`
    table populated to `row_count` rows, and an allow-everything admin
    role so the bench can hit every column without governance churn."""
    con = duckdb.connect(":memory:")
    apply_migrations(con, MIGRATIONS_DIR)
    apply_config(con, load_config(FIXTURE_YAML))
    con.execute(
        "CREATE TABLE orders ("
        "order_id VARCHAR, amount DECIMAL(18,2), order_date TIMESTAMP, "
        "status VARCHAR, region VARCHAR, customer_email VARCHAR)"
    )
    # Synthesize rows. Customer pool ~= row_count / 5 so the cohort
    # benchmarks see real repeat-customer behavior.
    pool = max(row_count // 5, 1)
    statuses = ("paid", "pending", "refunded")
    regions = ("west", "east", "central")
    con.execute("BEGIN")
    try:
        for i in range(row_count):
            con.execute(
                "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)",
                [
                    f"o{i}",
                    10 + (i % 1000),
                    f"2026-{1 + (i % 12):02d}-{1 + (i % 27):02d} 00:00:00",
                    statuses[i % len(statuses)],
                    regions[i % len(regions)],
                    f"c{i % pool}@x",
                ],
            )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    # Allow-everything admin role for the bench identity.
    con.execute(
        "INSERT INTO gibran_roles (role_id, display_name) "
        "VALUES ('admin', 'Admin')"
    )
    con.execute(
        "INSERT INTO gibran_policies "
        "(policy_id, role_id, source_id, default_column_mode) "
        "VALUES ('admin_orders', 'admin', 'orders', 'allow')"
    )
    return con


def _admin() -> IdentityContext:
    return IdentityContext(
        user_id="bench", role_id="admin", attributes={}, source="bench",
    )


def time_block(thunk, iterations: int) -> tuple[float, float]:
    """Run `thunk` `iterations` times; return (median_ms, p99_ms)."""
    samples = []
    for _ in range(iterations):
        t0 = time.perf_counter_ns()
        thunk()
        samples.append((time.perf_counter_ns() - t0) / 1e6)
    samples.sort()
    median = samples[len(samples) // 2]
    p99 = samples[min(len(samples) - 1, int(len(samples) * 0.99))]
    return median, p99


def bench(sizes: list[int], iterations: int = 20) -> list[BenchResult]:
    """Run the standard suite at each size; return one BenchResult per
    (label, size) pair. `iterations` controls how many timed runs feed
    the percentile computation."""
    results: list[BenchResult] = []
    for n in sizes:
        con = build_db(n)
        gov = DefaultGovernance(con)
        ident = _admin()
        catalog = Catalog(con)

        # 1. compile-only: single SELECT primitive
        simple = QueryIntent(source="orders", metrics=["order_count"])
        med, p99 = time_block(
            lambda: compile_intent(simple, catalog), iterations,
        )
        results.append(BenchResult("compile(count)", n, med, p99, iterations))

        # 2. end-to-end: count
        med, p99 = time_block(
            lambda: run_dsl_query(
                con, gov, ident,
                {"source": "orders", "metrics": ["order_count"]},
            ),
            iterations,
        )
        results.append(BenchResult("e2e(count)", n, med, p99, iterations))

        # 3. end-to-end: cohort_retention (3 CTEs, real work)
        med, p99 = time_block(
            lambda: run_dsl_query(
                con, gov, ident,
                {"source": "orders", "metrics": ["customer_retention"]},
            ),
            iterations,
        )
        results.append(BenchResult(
            "e2e(cohort_retention)", n, med, p99, iterations,
        ))
        con.close()
    return results


def render_markdown(results: list[BenchResult]) -> str:
    lines = [
        "| operation | rows | median (ms) | p99 (ms) | iter |",
        "|---|---:|---:|---:|---:|",
    ]
    for r in results:
        lines.append(
            f"| {r.label} | {r.row_count:,} | {r.median_ms:.2f} | "
            f"{r.p99_ms:.2f} | {r.iterations} |"
        )
    return "\n".join(lines)


def main() -> None:
    results = bench(sizes=[1_000, 10_000, 50_000], iterations=20)
    print(render_markdown(results))


if __name__ == "__main__":
    main()
