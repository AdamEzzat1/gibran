"""In-process LRU cache for executed query results.

Wraps `run_sql_query`'s execute step. Cache key is the *rewritten* SQL
(after governance filter injection) plus the identity tuple, so two
identities running the same intent get separate cache entries (they
see different row sets via the injected row filter).

Invalidation
------------

The cache reads the same `catalog_generation` token as the plan cache,
so `gibran sync` invalidates results too. It additionally reads a
`source_health_generation` token that bumps each time `gibran check`
runs -- this guards against serving a cached result based on data the
quality rules now say is bad. Callers can also explicitly `clear()`.

The cache does NOT track the source's actual data version (last-write
timestamp on the underlying table / parquet file). For dynamic sources
that change between syncs/checks the cache will serve stale rows
until the next check pass. V1 accepts that tradeoff; V2 will likely
need a source-version probe.

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
    dialect: str | None = None,
) -> str:
    """Stable hash key combining all the inputs that change result rows.

    The dialect is included so that when 5A.2+ adds Postgres / Snowflake /
    BigQuery engines, the same SQL string compiled by different engines
    can't collide in the cache. Today DuckDB is the only engine, so the
    key is effectively identical to before; the dialect dimension is
    additive.
    """
    payload = {
        "sql": rewritten_sql,
        "u": identity.user_id,
        "r": identity.role_id,
        "a": sorted((identity.attributes or {}).items()),
        "cg": catalog_gen,
        "hg": health_gen,
        "d": dialect or active_dialect().value,
    }
    return json.dumps(payload, sort_keys=True, default=str)


def lookup(
    target: ConnectionOrEngine,
    rewritten_sql: str,
    identity: IdentityContext,
    *,
    cache: ResultCache | None = None,
) -> tuple[str, CachedResult | None]:
    """Look up a cached result. Returns (key, cached_value_or_None).

    Accepts a raw DuckDBPyConnection (backward compat) or an
    ExecutionEngine. The caller is responsible for storing back on a
    miss via `store(key, CachedResult(rows, columns), cache=...)`.
    """
    if cache is None:
        cache = _DEFAULT_CACHE
    cg = catalog_generation(target)
    hg = source_health_generation(target)
    key = cache_key(rewritten_sql, identity, cg, hg)
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
