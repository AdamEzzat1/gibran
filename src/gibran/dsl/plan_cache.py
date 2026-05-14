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
from collections import OrderedDict
from typing import Any

import duckdb

from gibran.dsl.compile import Catalog, CompiledQuery, compile_intent
from gibran.dsl.types import QueryIntent


# ---------------------------------------------------------------------------
# Catalog-generation token
# ---------------------------------------------------------------------------

_META_TABLE_INIT = (
    "CREATE TABLE IF NOT EXISTS gibran_meta ("
    "  key TEXT PRIMARY KEY, value TEXT NOT NULL"
    ")"
)


def _ensure_meta_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(_META_TABLE_INIT)


def catalog_generation(con: duckdb.DuckDBPyConnection) -> str:
    """Read the current catalog generation token. Used by the plan cache
    to detect whether a sync has invalidated previously-cached plans."""
    _ensure_meta_table(con)
    row = con.execute(
        "SELECT value FROM gibran_meta WHERE key = 'catalog_generation'"
    ).fetchone()
    if row is None:
        return "0"
    return str(row[0])


def bump_catalog_generation(con: duckdb.DuckDBPyConnection) -> str:
    """Write a fresh catalog generation token. Called by the applier
    at the end of a successful sync."""
    import uuid as _uuid

    _ensure_meta_table(con)
    new_gen = _uuid.uuid4().hex
    con.execute(
        "INSERT INTO gibran_meta (key, value) VALUES ('catalog_generation', ?) "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        [new_gen],
    )
    return new_gen


# ---------------------------------------------------------------------------
# PlanCache
# ---------------------------------------------------------------------------

class PlanCache:
    """In-memory LRU cache. Thread-safe via a single lock."""

    def __init__(self, max_size: int = 256) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        self.max_size = max_size
        self._cache: OrderedDict[str, CompiledQuery] = OrderedDict()
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> CompiledQuery | None:
        with self._lock:
            if key not in self._cache:
                self.misses += 1
                return None
            self._cache.move_to_end(key)
            self.hits += 1
            return self._cache[key]

    def set(self, key: str, value: CompiledQuery) -> None:
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


# Process-level singleton. Callers that want isolation construct their
# own PlanCache instance and pass it explicitly to compile_intent_cached.
_DEFAULT_CACHE = PlanCache()


def default_cache() -> PlanCache:
    return _DEFAULT_CACHE


def cache_key(intent: QueryIntent, catalog_gen: str) -> str:
    """Stable hash key for a (intent, catalog generation) pair."""
    payload: dict[str, Any] = {
        "g": catalog_gen,
        "i": intent.model_dump(mode="json"),
    }
    return json.dumps(payload, sort_keys=True, default=str)


def compile_intent_cached(
    intent: QueryIntent,
    catalog: Catalog,
    *,
    cache: PlanCache | None = None,
) -> CompiledQuery:
    """Cached wrapper around `compile_intent`.

    Falls back to a no-op pass-through when `cache` is None and there's
    no default cache (e.g. for tests that want to measure raw compile
    cost). With the default cache, the same intent compiled twice -- with
    the same catalog generation -- pays the compile cost once.
    """
    if cache is None:
        cache = _DEFAULT_CACHE
    gen = catalog_generation(catalog.con)
    key = cache_key(intent, gen)
    cached = cache.get(key)
    if cached is not None:
        return cached
    compiled = compile_intent(intent, catalog)
    cache.set(key, compiled)
    return compiled
