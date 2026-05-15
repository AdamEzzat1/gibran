"""Dialect-aware SQL emitters (Phase 5D).

Some SQL fragments differ enough across dialects that sqlglot's
transpiler doesn't always produce the engine-idiomatic form. This
module centralizes per-dialect emission for the small set of
time-related operations where DuckDB / Postgres / Snowflake / BigQuery
diverge:

  - `now()`                 -- current timestamp
  - `interval(n, unit)`     -- interval literal
  - `date_diff_seconds(a,b)`-- difference in seconds between two timestamps
  - `current_date()`        -- today's date

Most primitives in the compile path are dialect-neutral (SELECT,
WHERE, GROUP BY, window functions, basic aggregates) -- sqlglot handles
those. The emitters here are the escape hatch for "sqlglot is close
but not idiomatic."

Usage:

    from gibran.dsl.dialect_emit import emitter_for
    from gibran.execution.dialect import Dialect

    em = emitter_for(Dialect.POSTGRES)
    sql = f"SELECT {em.now()} AS as_of, {em.interval(28, 'days')} AS window"
    #  -> "SELECT now() AS as_of, INTERVAL '28 days' AS window"

Scope of 5D as shipped:
  - Registry + 4 dialect emitters (DuckDB, Postgres, Snowflake, BigQuery)
  - 4 emitter methods (now, interval, date_diff_seconds, current_date)
  - Tests pinning the per-dialect output

Deferred to 5D follow-ups:
  - Wiring existing call sites (observability/default.py, sync/applier.py
    for rolling_window) to use the emitter instead of hardcoded DuckDB
    syntax. That migration is mechanical but touches many sites; doing
    it here would conflate the registry design with the migration.
  - More emitters as dialect-sensitive code is identified. Add as needed.
"""
from __future__ import annotations

from typing import Protocol

from gibran.execution.dialect import Dialect


# Recognized interval units. The emitters normalize the inputs (DuckDB
# accepts `days`/`day`/`DAYS` interchangeably; BigQuery requires
# singular UPPERCASE; etc.) so callers can pass a single style.
_UNIT_NORMALIZED = {
    "second": "second", "seconds": "second",
    "minute": "minute", "minutes": "minute",
    "hour": "hour",     "hours": "hour",
    "day": "day",       "days": "day",
    "week": "week",     "weeks": "week",
    "month": "month",   "months": "month",
    "year": "year",     "years": "year",
}


def _normalize_unit(unit: str) -> str:
    key = unit.strip().lower()
    if key not in _UNIT_NORMALIZED:
        raise ValueError(
            f"unrecognized interval unit {unit!r}; expected one of "
            f"{sorted(set(_UNIT_NORMALIZED.values()))}"
        )
    return _UNIT_NORMALIZED[key]


class DialectEmitter(Protocol):
    """Per-dialect SQL fragment emitter for time-related operations."""

    dialect: Dialect

    def now(self) -> str:
        """Current timestamp expression."""
        ...

    def current_date(self) -> str:
        """Today's date expression (no time component)."""
        ...

    def interval(self, amount: int, unit: str) -> str:
        """An interval literal expression. `unit` accepts singular or
        plural (`day` / `days` / `DAY`). Returns the engine-idiomatic
        form -- e.g. `INTERVAL '28 days'` for DuckDB/Postgres/Snowflake
        but `INTERVAL 28 DAY` for BigQuery."""
        ...

    def date_diff_seconds(self, start_sql: str, end_sql: str) -> str:
        """Difference in seconds between two timestamp expressions.

        Caller passes the inner SQL fragments (column references, function
        calls, etc.) -- the emitter wraps them in the dialect-correct
        DATE_DIFF / DATEDIFF / TIMESTAMP_DIFF function. Returns an int
        seconds count."""
        ...


# ---------------------------------------------------------------------------
# Per-dialect implementations
# ---------------------------------------------------------------------------


class _DuckDBEmitter:
    dialect = Dialect.DUCKDB

    def now(self) -> str:
        return "now()"

    def current_date(self) -> str:
        return "CURRENT_DATE"

    def interval(self, amount: int, unit: str) -> str:
        # DuckDB: INTERVAL '<amount> <unit>' -- unit can be singular or
        # plural; we use plural for readability (matches the YAML
        # `window: "28 days"` convention).
        u = _normalize_unit(unit)
        return f"INTERVAL '{int(amount)} {u}s'"

    def date_diff_seconds(self, start_sql: str, end_sql: str) -> str:
        return f"DATE_DIFF('second', {start_sql}, {end_sql})"


class _PostgresEmitter:
    dialect = Dialect.POSTGRES

    def now(self) -> str:
        return "now()"

    def current_date(self) -> str:
        return "CURRENT_DATE"

    def interval(self, amount: int, unit: str) -> str:
        # Postgres syntax is identical to DuckDB for interval literals.
        u = _normalize_unit(unit)
        return f"INTERVAL '{int(amount)} {u}s'"

    def date_diff_seconds(self, start_sql: str, end_sql: str) -> str:
        # Postgres doesn't have DATE_DIFF. EXTRACT(EPOCH FROM ...) of
        # the timestamp delta gives a float-seconds count; cast to int
        # for parity with DuckDB's integer return.
        return f"CAST(EXTRACT(EPOCH FROM ({end_sql} - {start_sql})) AS INTEGER)"


class _SnowflakeEmitter:
    dialect = Dialect.SNOWFLAKE

    def now(self) -> str:
        # Snowflake accepts now() (mapped to CURRENT_TIMESTAMP); we use
        # CURRENT_TIMESTAMP() with parens to match the SQL-standard form.
        return "CURRENT_TIMESTAMP()"

    def current_date(self) -> str:
        return "CURRENT_DATE()"

    def interval(self, amount: int, unit: str) -> str:
        # Snowflake's interval literal works like Postgres / DuckDB.
        u = _normalize_unit(unit)
        return f"INTERVAL '{int(amount)} {u}s'"

    def date_diff_seconds(self, start_sql: str, end_sql: str) -> str:
        # Snowflake uses DATEDIFF (one word). Argument order: unit,
        # start, end -- the result is `end - start` in units.
        return f"DATEDIFF('second', {start_sql}, {end_sql})"


class _BigQueryEmitter:
    dialect = Dialect.BIGQUERY

    def now(self) -> str:
        return "CURRENT_TIMESTAMP()"

    def current_date(self) -> str:
        return "CURRENT_DATE()"

    def interval(self, amount: int, unit: str) -> str:
        # BigQuery interval form is: INTERVAL <amount> <UNIT>
        # (no quotes around the value, singular UNIT in UPPERCASE).
        u = _normalize_unit(unit)
        return f"INTERVAL {int(amount)} {u.upper()}"

    def date_diff_seconds(self, start_sql: str, end_sql: str) -> str:
        # BigQuery: TIMESTAMP_DIFF(end, start, SECOND). Argument order
        # is end FIRST, then start (opposite of Snowflake / DuckDB).
        return f"TIMESTAMP_DIFF({end_sql}, {start_sql}, SECOND)"


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


_REGISTRY: dict[Dialect, DialectEmitter] = {
    Dialect.DUCKDB: _DuckDBEmitter(),
    Dialect.POSTGRES: _PostgresEmitter(),
    Dialect.SNOWFLAKE: _SnowflakeEmitter(),
    Dialect.BIGQUERY: _BigQueryEmitter(),
}


def emitter_for(dialect: Dialect) -> DialectEmitter:
    """Return the dialect-specific emitter. Raises KeyError if the
    dialect isn't registered -- this should never happen for a valid
    Dialect enum value, but the explicit lookup keeps the typing tight."""
    try:
        return _REGISTRY[dialect]
    except KeyError as e:
        raise KeyError(
            f"no DialectEmitter registered for {dialect!r}; "
            f"this is a bug -- every Dialect enum value should have an emitter"
        ) from e


__all__ = [
    "DialectEmitter",
    "emitter_for",
]
