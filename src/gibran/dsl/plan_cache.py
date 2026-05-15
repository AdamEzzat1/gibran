"""In-process LRU cache for compiled DSL queries.

The compile path is pure (QueryIntent + catalog state -> CompiledQuery),
so its output is safely cacheable. The hot path of a DSL query is:

    Pydantic-parse -> validate vs AllowedSchema -> compile -> run_sql_query

This module short-circuits the compile step when the same intent has
been compiled before AND the catalog hasn't changed since.

Invalidation
------------

The cache key includes a *catalog generation* token that bumps on every
`gibran sync` apply. The applier calls `bump_catalog_generation(con)` at
the end of a successful sync, which writes a fresh UUID into
`gibran_meta`. The plan cache reads that UUID alongside the intent JSON
so re-syncing the catalog never returns a stale compiled query.

Phase 5A.1c
-----------

`catalog_generation` and `bump_catalog_generation` accept either a raw
`DuckDBPyConnection` (backward-compat) OR an `ExecutionEngine`. Raw
connections are wrapped in a DuckDBEngine internally so the function
body has one code path. This is what lets the result cache (which calls
into here) work end-to-end against PostgresEngine.

Per-process scope
-----------------

The cache lives in module-level state -- one process, one cache. In a
multi-process deployment each process maintains its own. Cross-process
plan caching would need a shared store (e.g. Redis); deferred until the
deployment shape is decided (per ROADMAP.md).
"""
from __future__ import annotations

import json
import threading
import uuid
from collections import OrderedDict
from typing import Any, Union

import duckdb

from gibran.dsl.compile import Catalog, CompiledQuery, compile_intent
from gibran.dsl.types import QueryIntent
from gibran.execution.engine import DuckDBEngine, ExecutionEngine


ConnectionOrEngine = Union[duckdb.DuckDBPyConnection, ExecutionEngine]


def _as_engine(target: ConnectionOrEngine) -> ExecutionEngine:
    """Normalize either a raw connection or an engine into an engine."""
    if hasattr(target, "dialect") and hasattr(target, "execute"):
        return target  # type: ignore[return-value]
    return DuckDBEngine(target)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Catalog-generation token
# ---------------------------------------------------------------------------

_META_TABLE_INIT = (
    "CREATE TABLE IF NOT EXISTS gibran_meta ("
    "  key TEXT PRIMARY KEY, value TEXT NOT NULL"
    ")"
)


def _ensure_meta_table(engine: ExecutionEngine) -> None:
    engine.execute(_META_TABLE_INIT)


def catalog_generation(target: ConnectionOrEngine) -> str:
    """Read the current catalog generation token. Used by the plan cache
    to detect whether a sync has invalidated previously-cached plans.

    Accepts a raw DuckDBPyConnection (backward compat) or an
    ExecutionEngine. Both paths use the same engine API internally."""
    engine = _as_engine(target)
    _ensure_meta_table(engine)
    row = engine.fetchone(
        "SELECT value FROM gibran_meta WHERE key = 'catalog_generation'"
    )
    if row is None:
        return "0"
    return str(row[0])


def bump_catalog_generation(target: ConnectionOrEngine) -> str:
    """Write a fresh catalog generation token. Called by the applier
    at the end of a successful sync."""
    engine = _as_engine(target)
    _ensure_meta_table(engine)
    new_gen = uuid.uuid4().hex
    engine.execute(
        "INSERT INTO gibran_meta (key, value) VALUES ('catalog_generation', ?) "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        [new_gen],
    )
    engine.commit()
    return new_gen


# ---------------------------------------------------------------------------
# PlanCache
# ---------------------------------------------------------------------------


class PlanCache:
    """Per-process LRU cache for compiled DSL queries.

    The cache key is `(intent_json, catalog_generation)`. Both are
    needed: identical intents from before vs after a sync MUST NOT
    share a compiled-query entry, since the catalog (and therefore the
    metric definitions the compiler resolves) may have changed.
    """

    def __init__(self, max_size: int = 256) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        self.max_size = max_size
        self._cache: OrderedDict[str, CompiledQuery] = OrderedDict()
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def _make_key(self, intent: QueryIntent, generation: str) -> str:
        payload = {
            "intent": intent.model_dump(mode="json"),
            "gen": generation,
        }
        return json.dumps(payload, sort_keys=True, default=str)

    def get(
        self, intent: QueryIntent, generation: str
    ) -> CompiledQuery | None:
        key = self._make_key(intent, generation)
        with self._lock:
            if key not in self._cache:
                self.misses += 1
                return None
            self._cache.move_to_end(key)
            self.hits += 1
            return self._cache[key]

    def set(
        self,
        intent: QueryIntent,
        generation: str,
        compiled: CompiledQuery,
    ) -> None:
        key = self._make_key(intent, generation)
        with self._lock:
            self._cache[key] = compiled
            self._cache.move_to_end(key)
            while len(self._cache) > self.max_size:
                self._cache.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self.hits = 0
            self.misses = 0


_DEFAULT_CACHE = PlanCache()


def default_cache() -> PlanCache:
    return _DEFAULT_CACHE


def compile_intent_cached(
    intent: QueryIntent,
    catalog: Catalog,
    *,
    cache: PlanCache | None = None,
) -> CompiledQuery:
    """Memoized compile_intent. Reads the current catalog_generation
    from `catalog.con` so cache entries automatically invalidate on
    `gibran sync`."""
    cache = cache or _DEFAULT_CACHE
    gen = catalog_generation(catalog.con)
    cached = cache.get(intent, gen)
    if cached is not None:
        return cached
    compiled = compile_intent(intent, catalog)
    cache.set(intent, gen, compiled)
    return compiled


# Re-exports for backward compat (older imports may pull these names).
__all__ = [
    "Catalog",
    "CompiledQuery",
    "PlanCache",
    "bump_catalog_generation",
    "catalog_generation",
    "compile_intent",
    "compile_intent_cached",
    "default_cache",
]
