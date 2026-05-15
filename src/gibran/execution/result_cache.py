"""In-process LRU cache for executed query results.

Wraps `run_sql_query`'s execute step. Cache key is the *rewritten* SQL
(after governance filter injection) plus the identity tuple, so two
identities running the same intent get separate cache entries (they
see different row sets via the injected row filter).

Invalidation
------------

The cache key includes three generation tokens, each bumped by a
distinct event class:

  * catalog_generation        -- bumped by `gibran sync`. Catches
                                 metric/dimension/policy YAML changes.
  * source_health_generation  -- bumped by `gibran check`. Catches
                                 quality-rule outcomes (block/warn).
  * source_data_version       -- recomputed per lookup (Phase 2B).
                                 Catches the source's actual data
                                 state between sync/check passes.

For parquet/csv sources the data version is `os.stat().st_mtime_ns`,
read fresh on each lookup. For duckdb_table / sql_view it's the opaque
token in `gibran_table_versions`, bumped by `gibran touch <source>`.
sql_view does NOT auto-derive its version from underlying tables in V1
-- recursive view-version derivation is Phase 3 work.

Trust model: if a user can manipulate the source file's mtime
(`touch foo.parquet`), they can either freeze the cache (set mtime
backwards) or force re-execution on every query (set mtime forward).
Neither is a *governance* break -- they still only see rows the policy
allows -- but the latter is a denial-of-service-shaped bug class.
Document, accept.

V1 design
---------

* The audit log STILL gets a row on a cache hit -- caching skips
  DuckDB execution, not the `INSERT INTO gibran_query_log`. So the
  audit-log invariant ("every attempt is logged") holds with caching
  on.
* `duration_ms` on the audit row for a cache hit reflects the cache
  lookup time (microseconds), which is the user-perceived latency.
  No `was_cached` flag yet -- if you need that distinction for
  reporting, the duration discontinuity is detectable downstream.
* Per-process scope, same as the plan cache.
"""
from __future__ import annotations

import json
import threading
from collections import OrderedDict
from dataclasses import dataclass

from typing import Union

import duckdb

from gibran._source_dispatch import SourceDispatchError, source_data_version
from gibran.dsl.plan_cache import catalog_generation
from gibran.execution.dialect import active_dialect
from gibran.execution.engine import DuckDBEngine, ExecutionEngine
from gibran.governance.types import IdentityContext


ConnectionOrEngine = Union[duckdb.DuckDBPyConnection, ExecutionEngine]


def _as_engine(target: ConnectionOrEngine) -> ExecutionEngine:
    """Normalize either a raw connection or an engine into an engine."""
    if hasattr(target, "dialect") and hasattr(target, "execute"):
        return target  # type: ignore[return-value]
    return DuckDBEngine(target)  # type: ignore[arg-type]


@dataclass(frozen=True)
class CachedResult:
    rows: tuple[tuple, ...]
    columns: tuple[str, ...]


def _ensure_meta_table(engine: ExecutionEngine) -> None:
    engine.execute(
        "CREATE TABLE IF NOT EXISTS gibran_meta ("
        "  key TEXT PRIMARY KEY, value TEXT NOT NULL"
        ")"
    )


def source_health_generation(target: ConnectionOrEngine) -> str:
    """Read the source-health generation token. Bumps each time
    `gibran check` records new runs (set by the check runner).

    Accepts a raw DuckDBPyConnection (backward compat) or an
    ExecutionEngine -- Phase 5A.1c migration so PostgresEngine can
    participate in result-cache invalidation."""
    engine = _as_engine(target)
    _ensure_meta_table(engine)
    row = engine.fetchone(
        "SELECT value FROM gibran_meta WHERE key = 'source_health_generation'"
    )
    if row is None:
        return "0"
    return str(row[0])


def bump_source_health_generation(target: ConnectionOrEngine) -> str:
    """Called by `run_checks` after a check pass. Invalidates the result
    cache so the next query re-evaluates against the new health state."""
    import uuid as _uuid

    engine = _as_engine(target)
    _ensure_meta_table(engine)
    new_gen = _uuid.uuid4().hex
    engine.execute(
        "INSERT INTO gibran_meta (key, value) "
        "VALUES ('source_health_generation', ?) "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        [new_gen],
    )
    engine.commit()
    return new_gen


class ResultCache:
    """In-memory LRU. Thread-safe via a single lock."""

    def __init__(self, max_size: int = 128) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        self.max_size = max_size
        self._cache: OrderedDict[str, CachedResult] = OrderedDict()
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> CachedResult | None:
        with self._lock:
            if key not in self._cache:
                self.misses += 1
                return None
            self._cache.move_to_end(key)
            self.hits += 1
            return self._cache[key]

    def set(self, key: str, value: CachedResult) -> None:
        with self._lock:
            self._cache[key] = value
            self._cache.move_to_end(key)
            while len(self._cache) > self.max_size:
                self._cache.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self.hits = 0
            self.misses = 0


_DEFAULT_CACHE = ResultCache()


def default_cache() -> ResultCache:
    return _DEFAULT_CACHE


def cache_key(
    rewritten_sql: str,
    identity: IdentityContext,
    catalog_gen: str,
    health_gen: str,
    data_version: str = "",
    dialect: str | None = None,
) -> str:
    """Stable hash key combining all the inputs that change result rows.

    `data_version` (Phase 2B) is the source's per-lookup data-state
    token (parquet mtime / table-versions row). Defaults to "" for
    callers without a source_id -- degrades to pre-Phase-2B behavior
    (data-state changes don't invalidate, only catalog/health bumps do).

    `dialect` (Phase 5A.0) ensures the same SQL string compiled by
    different engines (DuckDB vs Postgres vs Snowflake vs BigQuery)
    can't collide in the cache. Defaults to the active dialect.
    """
    payload = {
        "sql": rewritten_sql,
        "u": identity.user_id,
        "r": identity.role_id,
        "a": sorted((identity.attributes or {}).items()),
        "cg": catalog_gen,
        "hg": health_gen,
        "dv": data_version,
        "d": dialect or active_dialect().value,
    }
    return json.dumps(payload, sort_keys=True, default=str)


def lookup(
    target: ConnectionOrEngine,
    rewritten_sql: str,
    identity: IdentityContext,
    *,
    source_id: str | None = None,
    cache: ResultCache | None = None,
) -> tuple[str, CachedResult | None]:
    """Look up a cached result. Returns (key, cached_value_or_None).

    Accepts a raw DuckDBPyConnection (backward compat) or an
    ExecutionEngine (Phase 5A.1c). The caller is responsible for
    storing back on a miss via `store(key, CachedResult(rows, columns),
    cache=...)`.

    `source_id` enables data-version probing (Phase 2B). When provided,
    the cache key includes the source's current data version, so
    parquet rewrites / `gibran touch` calls invalidate cached results.
    Legacy callers that don't pass source_id degrade to the pre-Phase-2B
    behavior (catalog + health generations only).
    """
    if cache is None:
        cache = _DEFAULT_CACHE
    cg = catalog_generation(target)
    hg = source_health_generation(target)
    dv = ""
    if source_id is not None:
        # source_data_version still takes a raw connection (Phase 2B
        # predates the engine migration). Extract from engine if needed.
        con = target.con if hasattr(target, "con") else target
        if con is not None:
            try:
                dv = source_data_version(con, source_id)
            except SourceDispatchError:
                # Source missing / unrecognized type. Fall back to dv=""
                # rather than crashing the cache path -- the downstream
                # SQL execution surfaces the same error with clearer
                # context (and writes the audit log row).
                dv = ""
    key = cache_key(rewritten_sql, identity, cg, hg, dv)
    return key, cache.get(key)


def store(
    key: str,
    value: CachedResult,
    *,
    cache: ResultCache | None = None,
) -> None:
    if cache is None:
        cache = _DEFAULT_CACHE
    cache.set(key, value)
