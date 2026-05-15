"""Phase 5A.4 -- BigQueryEngine integration tests (DEFERRED).

These tests require:
  - `google-cloud-bigquery` installed (via `pip install gibran[bigquery]`)
  - A GCP project with billing enabled, exposed via:
      GIBRAN_BIGQUERY_PROJECT  -- the GCP project ID
    AND Application Default Credentials configured (run
    `gcloud auth application-default login` locally, or set
    GOOGLE_APPLICATION_CREDENTIALS to a service-account JSON path).

If credentials aren't present, the whole module skips. There is NO
automatic execution -- this test will never silently incur BigQuery
scan cost. A developer / CI environment that wants to verify the
BigQuery path against a real project must explicitly set
GIBRAN_BIGQUERY_PROJECT.

Cost note: even a `SELECT 1` BigQuery query incurs a small fixed slot
cost (10 MB minimum billable scan as of 2025-Q2). The single test
below is the minimum to verify the engine wires up correctly.

Scope of THIS file is what's testable at the 5A.4 boundary (engine
methods executing against real BigQuery). End-to-end runner-layer
tests against BigQuery require:
  - 5A.1c (result-cache + catalog-generation engine migration)
  - 5D (dialect-aware identifier quoting in dsl/compile.py so the
    compiled SQL uses backticks, not double-quotes)
Both are documented follow-ups.

Trademark note: "BigQuery" is a trademark of Google LLC. gibran is
not affiliated with or endorsed by Google LLC.
"""
from __future__ import annotations

import os

import pytest

# Skip module if SDK isn't installed.
google_cloud_bigquery = pytest.importorskip("google.cloud.bigquery")

GCP_PROJECT = os.environ.get("GIBRAN_BIGQUERY_PROJECT")

# Skip if no project is configured. Explicit opt-in only -- we never
# want to silently issue a BigQuery query from a default test run.
pytestmark = pytest.mark.skipif(
    not GCP_PROJECT,
    reason=(
        "GIBRAN_BIGQUERY_PROJECT not set -- skipping BigQueryEngine "
        "integration tests. These tests run against a real GCP project "
        "and incur scan cost (minimum 10MB billable per query as of "
        "2025-Q2). Opt in by setting GIBRAN_BIGQUERY_PROJECT=my-project."
    ),
)


@pytest.fixture
def bq_con():
    """Open a BigQuery dbapi connection per test and close it after."""
    from gibran.execution.engines.bigquery import connect
    con = connect(project=GCP_PROJECT)
    try:
        yield con
    finally:
        con.close()


def test_engine_holds_real_connection(bq_con):
    """Constructor accepts a live BigQuery dbapi connection."""
    from gibran.execution.engines.bigquery import BigQueryEngine

    engine = BigQueryEngine(con=bq_con)
    assert engine.con is bq_con


def test_select_one_works(bq_con):
    """Smallest possible end-to-end check: SELECT 1 returns 1.
    Verifies qmark-to-pyformat translation works against the real
    BigQuery dbapi cursor."""
    from gibran.execution.engines.bigquery import BigQueryEngine

    engine = BigQueryEngine(con=bq_con)
    rows, cols = engine.query("SELECT 1 AS x")
    assert rows == [(1,)]
    # BigQuery preserves the alias case
    assert cols[0] == "x"
