"""Phase 5A.3 -- SnowflakeEngine integration tests (DEFERRED).

These tests require:
  - `snowflake-connector-python` installed (via `pip install gibran[snowflake]`)
  - A Snowflake account with usable credentials, exposed via env vars:
      GIBRAN_SNOWFLAKE_URL  -- a `snowflake://user:pass@account/db/schema?...` URL
    OR the individual env vars the snowflake connector itself reads
    (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, etc.)

If credentials aren't present, the whole module skips. There is NO
automatic execution -- this test will never silently incur Snowflake
compute cost. A developer / CI environment that wants to verify the
Snowflake path against a real account must explicitly set
GIBRAN_SNOWFLAKE_URL.

Scope of THIS file is what's testable at the 5A.3 boundary (engine
methods executing against a real Snowflake). End-to-end runner-layer
tests against Snowflake (`gibran query` -> SnowflakeEngine) require
the result-cache / catalog-generation migrations from 5A.1c which
haven't landed yet.

Trademark note: "Snowflake" appears here in nominative use. gibran
is not affiliated with or endorsed by Snowflake Inc.
"""
from __future__ import annotations

import os

import pytest

# Skip module if connector isn't installed.
snowflake_connector = pytest.importorskip("snowflake.connector")

SNOWFLAKE_URL = os.environ.get("GIBRAN_SNOWFLAKE_URL")

# Skip if no URL is configured. Explicit opt-in only -- we never want
# to silently spin up a Snowflake warehouse from a default test run.
pytestmark = pytest.mark.skipif(
    not SNOWFLAKE_URL,
    reason=(
        "GIBRAN_SNOWFLAKE_URL not set -- skipping SnowflakeEngine "
        "integration tests. These tests run against a live Snowflake "
        "account and incur compute cost; opt in by setting "
        "GIBRAN_SNOWFLAKE_URL=snowflake://user:pass@account/db/schema."
    ),
)


@pytest.fixture
def sf_con():
    """Open a Snowflake connection per test and close it after."""
    from gibran.execution.engines.snowflake import connect
    con = connect(SNOWFLAKE_URL)
    try:
        yield con
    finally:
        con.close()


def test_engine_holds_real_connection(sf_con):
    """The constructor accepts a live Snowflake connection without
    erroring."""
    from gibran.execution.engines.snowflake import SnowflakeEngine

    engine = SnowflakeEngine(con=sf_con)
    assert engine.con is sf_con


def test_select_one_works(sf_con):
    """The smallest possible end-to-end check: SELECT 1 returns 1."""
    from gibran.execution.engines.snowflake import SnowflakeEngine

    engine = SnowflakeEngine(con=sf_con)
    rows, cols = engine.query("SELECT 1 AS x")
    assert rows == [(1,)]
    # Snowflake folds unquoted column aliases to uppercase
    assert cols[0].upper() == "X"
