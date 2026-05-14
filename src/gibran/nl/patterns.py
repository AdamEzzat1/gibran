"""Pattern-template NL layer.

Maps natural-language questions to DSL intents via a small set of
regex + builder pairs. EVERY slot extracted from the user's text is
validated against an AllowedSchema before becoming part of the intent
-- if a slot doesn't resolve to a real metric/dimension/column, the
pattern fails and the next one is tried. No invention.

This is Tier 5 of the HANDOFF roadmap: "an NL layer is in-scope IF
it's not LLM-based." The pattern matcher is deterministic given a
fixed pattern list + fixed schema. Coverage is intentionally narrow
(~6 question shapes); the goal isn't to answer every English
question, it's to answer the most-common ones safely.

Design notes
------------

* Patterns are ordered. More specific ones go FIRST -- e.g. "<metric>
  by <dim>" before "<metric>", so the longer pattern wins.
* Each builder may signal "I matched the regex but couldn't resolve
  the slots" by raising `NoMatch`. The pipeline then tries the next
  pattern.
* Resolution is fuzzy in the SHAPE sense (case-insensitive, accepts
  metric_id / display_name / column_name), but it's still a
  hard-edge match -- no Levenshtein, no embeddings. A typo'd metric
  name returns None.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable

from gibran.governance.types import AllowedSchema


class NoMatch(Exception):
    """Builder signal: regex matched but slot resolution failed."""


@dataclass(frozen=True)
class MatchResult:
    intent: dict[str, Any]
    pattern_name: str
    matched_text: str


# ---------------------------------------------------------------------------
# Slot resolvers
# ---------------------------------------------------------------------------

_GRAIN_WORDS = {
    "year": "year", "yearly": "year", "annual": "year", "annually": "year",
    "quarter": "quarter", "quarterly": "quarter",
    "month": "month", "monthly": "month",
    "week": "week", "weekly": "week",
    "day": "day", "daily": "day",
}


_MONTH_NAMES = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _resolve_metric(name: str, schema: AllowedSchema) -> str | None:
    """Map a user-supplied phrase to a metric_id. Exact-match wins over
    substring; case-insensitive; checks both metric_id and display_name."""
    n = name.lower().strip()
    if not n:
        return None
    # Pass 1: exact match
    for m in schema.metrics:
        if m.metric_id.lower() == n or m.display_name.lower() == n:
            return m.metric_id
    # Pass 2: substring match (shortest match wins -- favors specificity)
    candidates: list[tuple[int, str]] = []
    for m in schema.metrics:
        for haystack in (m.metric_id.lower(), m.display_name.lower()):
            if n in haystack:
                candidates.append((len(haystack), m.metric_id))
                break
    if candidates:
        return sorted(candidates)[0][1]
    return None


def _resolve_dimension(name: str, schema: AllowedSchema) -> str | None:
    """Map a phrase to a dimension_id. Checks dimension_id, display_name,
    and the underlying column_name (e.g. user says 'region' and the dim
    id is 'orders.region')."""
    n = name.lower().strip()
    if not n:
        return None
    for d in schema.dimensions:
        if (
            d.dimension_id.lower() == n
            or d.display_name.lower() == n
            or d.column_name.lower() == n
        ):
            return d.dimension_id
    # Substring: shortest match wins.
    candidates: list[tuple[int, str]] = []
    for d in schema.dimensions:
        for haystack in (
            d.dimension_id.lower(),
            d.display_name.lower(),
            d.column_name.lower(),
        ):
            if n in haystack:
                candidates.append((len(haystack), d.dimension_id))
                break
    if candidates:
        return sorted(candidates)[0][1]
    return None


def _resolve_temporal_dim(schema: AllowedSchema) -> str | None:
    """Find a temporal dimension on the schema -- used by 'by month/week'
    patterns that don't name the dimension explicitly."""
    for d in schema.dimensions:
        if d.dim_type == "temporal":
            return d.dimension_id
    return None


def _resolve_temporal_column(schema: AllowedSchema) -> str | None:
    """Return the underlying column name of the first temporal dimension.
    Used by period-filter patterns ('in 2026') that emit a WHERE clause
    on the temporal column directly, not a grouping dimension."""
    for d in schema.dimensions:
        if d.dim_type == "temporal":
            return d.column_name
    return None


def _resolve_column(name: str, schema: AllowedSchema) -> str | None:
    """Map a phrase to a column_name."""
    n = name.lower().strip()
    if not n:
        return None
    for c in schema.columns:
        if c.name.lower() == n or c.display_name.lower() == n:
            return c.name
    return None


# ---------------------------------------------------------------------------
# Pattern registry
# ---------------------------------------------------------------------------

_PATTERNS: list[tuple[re.Pattern, Callable[[re.Match, AllowedSchema], dict]]] = []


def register(pattern_str: str):
    """Decorator -- registers (compiled regex, builder) in order. The
    list order IS the priority order: earlier patterns win."""
    def decorator(fn):
        _PATTERNS.append((re.compile(pattern_str, re.IGNORECASE), fn))
        return fn
    return decorator


# ---------------------------------------------------------------------------
# Patterns -- ordered from most specific to most general
# ---------------------------------------------------------------------------

@register(r"^(?:top|biggest|largest|highest)\s+(\d+)\s+(.+?)\s+by\s+(.+)$")
def top_n_by_metric(m: re.Match, schema: AllowedSchema) -> dict:
    """top|biggest|largest|highest 10 <dim> by <metric> -- ORDER BY DESC LIMIT N."""
    n = int(m.group(1))
    dim_id = _resolve_dimension(m.group(2), schema)
    metric_id = _resolve_metric(m.group(3), schema)
    if not dim_id or not metric_id:
        raise NoMatch()
    return {
        "source": schema.source_id,
        "metrics": [metric_id],
        "dimensions": [{"id": dim_id}],
        "order_by": [{"key": metric_id, "direction": "desc"}],
        "limit": n,
    }


@register(r"^(?:bottom|smallest|lowest|fewest|least)\s+(\d+)\s+(.+?)\s+by\s+(.+)$")
def bottom_n_by_metric(m: re.Match, schema: AllowedSchema) -> dict:
    """bottom|smallest|lowest|fewest|least 5 <dim> by <metric> -- ORDER BY ASC LIMIT N.

    Mirror of top_n_by_metric: identical shape, ASC instead of DESC. Worth
    noting: ASC puts NULLs first in DuckDB (per SQL standard), so a metric
    that's NULL for some dims will surface those rows at the bottom. That's
    usually what the user means ("show me the worst performers") but is
    sometimes surprising. No special-casing here -- callers can add an
    IS NOT NULL filter if they want strict numeric-ordering."""
    n = int(m.group(1))
    dim_id = _resolve_dimension(m.group(2), schema)
    metric_id = _resolve_metric(m.group(3), schema)
    if not dim_id or not metric_id:
        raise NoMatch()
    return {
        "source": schema.source_id,
        "metrics": [metric_id],
        "dimensions": [{"id": dim_id}],
        "order_by": [{"key": metric_id, "direction": "asc"}],
        "limit": n,
    }


@register(r"^(?:show me |show |what(?:'s| is) the |what(?:'s| is) )?(.+?)\s+by\s+(year|yearly|quarter|quarterly|month|monthly|week|weekly|day|daily|annual|annually)$")
def metric_by_grain(m: re.Match, schema: AllowedSchema) -> dict:
    """<metric> by month/week/etc -- pick a temporal dim and apply the grain."""
    metric_id = _resolve_metric(m.group(1), schema)
    grain = _GRAIN_WORDS.get(m.group(2).lower())
    dim_id = _resolve_temporal_dim(schema)
    if not metric_id or not grain or not dim_id:
        raise NoMatch()
    return {
        "source": schema.source_id,
        "metrics": [metric_id],
        "dimensions": [{"id": dim_id, "grain": grain}],
    }


@register(r"^(?:show me |show |what(?:'s| is) the |what(?:'s| is) )?(.+?)\s+by\s+(.+)$")
def metric_by_dim(m: re.Match, schema: AllowedSchema) -> dict:
    """<metric> by <dim>."""
    metric_id = _resolve_metric(m.group(1), schema)
    dim_id = _resolve_dimension(m.group(2), schema)
    if not metric_id or not dim_id:
        raise NoMatch()
    return {
        "source": schema.source_id,
        "metrics": [metric_id],
        "dimensions": [{"id": dim_id}],
    }


@register(r"^(?:show me |show |what(?:'s| is) the |what(?:'s| is) )?(.+?)\s+in\s+(?:(\w+)\s+)?(\d{4})$")
def metric_in_period(m: re.Match, schema: AllowedSchema) -> dict:
    """<metric> in <year> | <metric> in <month-name> <year>.

    Emits a half-open date-range filter (>= period_start, < period_end)
    on the first temporal dimension's underlying column. Half-open avoids
    the TIMESTAMP edge case where BETWEEN '..-01' AND '..-31' silently
    drops anything at 23:59:59.

    Bare month names without a year (e.g. "in January") need a relative-
    time anchor and are deferred to Phase 3's `relative_time_filter`."""
    metric_id = _resolve_metric(m.group(1), schema)
    if not metric_id:
        raise NoMatch()
    col_name = _resolve_temporal_column(schema)
    if col_name is None:
        raise NoMatch()
    year = int(m.group(3))
    month_word = m.group(2)
    if month_word is not None:
        month = _MONTH_NAMES.get(month_word.lower())
        if month is None:
            raise NoMatch()
        start = f"{year}-{month:02d}-01"
        next_month, next_year = (1, year + 1) if month == 12 else (month + 1, year)
        end = f"{next_year}-{next_month:02d}-01"
    else:
        start = f"{year}-01-01"
        end = f"{year + 1}-01-01"
    return {
        "source": schema.source_id,
        "metrics": [metric_id],
        "filters": [{
            "op": "and",
            "args": [
                {"op": "gte", "column": col_name, "value": start},
                {"op": "lt", "column": col_name, "value": end},
            ],
        }],
    }


@register(r"^(?:count(?:\s+of)?|how many|total)\s+(.+)$")
def count_of_thing(m: re.Match, schema: AllowedSchema) -> dict:
    """count of <source>, how many orders, total customers.
    Picks the first count-type metric on the schema."""
    for metric in schema.metrics:
        if metric.metric_type == "count":
            return {"source": schema.source_id, "metrics": [metric.metric_id]}
    raise NoMatch()


@register(r"^(?:show me |show |what(?:'s| is) the |what(?:'s| is) )?(.+?)\s+for\s+(.+)$")
def metric_filtered_by_value(m: re.Match, schema: AllowedSchema) -> dict:
    """<metric> for <value> -- filter on a column whose values plausibly
    contain <value>. V1 strategy: find any column whose example_values
    contain the literal value (case-insensitive). Without example_values
    this pattern is inert."""
    metric_id = _resolve_metric(m.group(1), schema)
    if not metric_id:
        raise NoMatch()
    value = m.group(2).strip()
    target_col = None
    for c in schema.columns:
        if c.example_values and any(
            value.lower() == ev.lower() for ev in c.example_values
        ):
            target_col = c.name
            break
    if target_col is None:
        raise NoMatch()
    return {
        "source": schema.source_id,
        "metrics": [metric_id],
        "filters": [{"op": "eq", "column": target_col, "value": value}],
    }


@register(r"^(?:show me |show |what(?:'s| is) the |what(?:'s| is) )?(.+)$")
def single_metric(m: re.Match, schema: AllowedSchema) -> dict:
    """Bare metric reference -- the most permissive pattern, registered
    last so more specific shapes win first."""
    metric_id = _resolve_metric(m.group(1), schema)
    if not metric_id:
        raise NoMatch()
    return {"source": schema.source_id, "metrics": [metric_id]}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def nl_to_intent(text: str, schema: AllowedSchema) -> MatchResult | None:
    """Convert a user's natural-language question to a DSL intent.

    Returns None when no pattern matches and resolves. Callers should
    treat None as "I don't know how to answer that" -- never invent.
    """
    cleaned = text.strip().rstrip(".?!").strip()
    for pattern, builder in _PATTERNS:
        match = pattern.match(cleaned)
        if not match:
            continue
        try:
            intent = builder(match, schema)
        except NoMatch:
            continue
        return MatchResult(
            intent=intent,
            pattern_name=builder.__name__,
            matched_text=match.group(0),
        )
    return None
