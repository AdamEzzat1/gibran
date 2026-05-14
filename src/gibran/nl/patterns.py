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
from datetime import date, timedelta
from typing import Any, Callable

from gibran.governance.types import AllowedSchema
from gibran.nl.synonyms import (
    BOTTOM_WORDS,
    GRAIN_WORDS,
    MONTH_NAMES,
    OVER_TIME_WORDS,
    THIS_PERIOD_WORDS,
    TOP_WORDS,
    TYPE_KEYWORDS,
)


class NoMatch(Exception):
    """Builder signal: regex matched but slot resolution failed."""


@dataclass(frozen=True)
class MatchResult:
    intent: dict[str, Any]
    pattern_name: str
    matched_text: str


# Pre-computed regex alternations from the synonym lists. Built once at
# import time so each pattern's @register decorator is a constant string.
_TOP_ALT = "|".join(TOP_WORDS)
_BOTTOM_ALT = "|".join(BOTTOM_WORDS)
_GRAIN_ALT = "|".join(GRAIN_WORDS)
_OVER_TIME_ALT = "|".join(OVER_TIME_WORDS)
_TYPE_KEYWORD_ALT = "|".join(TYPE_KEYWORDS)
_THIS_PERIOD_ALT = "|".join(THIS_PERIOD_WORDS)
_HAVING_OP_MAP: dict[str, str] = {
    ">": "gt", "<": "lt", ">=": "gte", "<=": "lte", "=": "eq",
}


# Approximate day-counts per period unit. Used by metric_last_n_period.
# Phase 3's relative_time_filter primitive replaces these approximations
# with calendar-aware arithmetic (e.g. dateutil.relativedelta).
_PERIOD_UNIT_TO_DAYS: dict[str, int] = {
    "day": 1, "week": 7, "month": 30, "year": 365,
}


def _today() -> date:
    """Indirection over date.today() so tests can monkeypatch a fixed date
    without touching the public nl_to_intent signature."""
    return date.today()


# ---------------------------------------------------------------------------
# Slot resolvers
# ---------------------------------------------------------------------------


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


def _find_column_by_example_value(
    value: str, schema: AllowedSchema
) -> tuple[str, str] | None:
    """Find a column whose example_values contain `value` (case-insensitive).
    Returns (column_name, canonical_value_from_examples) or None.

    Shared by patterns that infer "which column does this value belong to?"
    from the populated example_values (status='paid' -> status column;
    region='west' -> region column). Without populated example_values
    these patterns are inert by design -- no inference, no fabrication."""
    v = value.lower()
    for c in schema.columns:
        if not c.example_values:
            continue
        for ev in c.example_values:
            if ev.lower() == v:
                return c.name, ev
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

@register(rf"^(?:{_TOP_ALT})\s+(\d+)\s+(.+?)\s+by\s+(.+?)\s+(?:where|with)\s+(.+?)\s+(>=|<=|>|<|=)\s*(-?\d+(?:\.\d+)?)$")
def top_n_with_having(m: re.Match, schema: AllowedSchema) -> dict:
    """top N <dim> by <metric> where <metric> {>|<|>=|<=|=} <value>.

    Combines a ranking with a post-aggregation HAVING filter. The HAVING
    metric is typically the SAME as the ordering metric ("top regions by
    revenue where revenue > 1000") but may differ. When it differs, both
    metrics are projected so the HAVING clause can reference the
    second's SELECT alias.

    V1 limitations: comparison ops only (no IN / NOT IN); numeric values
    only (integers or floats). The Phase 3 entity recognizer can later
    accept richer value shapes."""
    n = int(m.group(1))
    dim_id = _resolve_dimension(m.group(2), schema)
    metric_id = _resolve_metric(m.group(3), schema)
    having_metric_id = _resolve_metric(m.group(4), schema)
    if not dim_id or not metric_id or not having_metric_id:
        raise NoMatch()
    raw_value = m.group(6)
    value: int | float = float(raw_value)
    if value.is_integer():
        value = int(value)
    metrics = (
        [metric_id]
        if having_metric_id == metric_id
        else [metric_id, having_metric_id]
    )
    return {
        "source": schema.source_id,
        "metrics": metrics,
        "dimensions": [{"id": dim_id}],
        "order_by": [{"key": metric_id, "direction": "desc"}],
        "limit": n,
        "having": [{
            "op": _HAVING_OP_MAP[m.group(5)],
            "metric": having_metric_id,
            "value": value,
        }],
    }


@register(rf"^(?:{_TOP_ALT})\s+(\d+)\s+(.+?)\s+by\s+(.+)$")
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


@register(rf"^(?:{_BOTTOM_ALT})\s+(\d+)\s+(.+?)\s+by\s+(.+)$")
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


@register(rf"^(?:show me |show |what(?:'s| is) the |what(?:'s| is) )?({_TYPE_KEYWORD_ALT})\s+(.+)$")
def metric_by_type_keyword(m: re.Match, schema: AllowedSchema) -> dict:
    """<keyword> <thing> -- find a metric of the keyword's primitive type
    whose name contains <thing>.

    Examples:
      "unique customers"      -> count_distinct metric named ~customers
      "max order amount"      -> max metric named ~order amount
      "average order amount"  -> avg metric named ~order amount
      "median amount"         -> median metric named ~amount

    The keyword filters the candidate set FIRST so a query like "min
    order amount" doesn't match a max-type metric just because both
    contain "order amount". If no metric of the keyword's type matches,
    NoMatch (no fabrication)."""
    keyword = m.group(1).lower()
    allowed_types = TYPE_KEYWORDS[keyword]
    user_phrase = m.group(2).lower().strip()
    candidates: list[tuple[int, str]] = []
    for metric in schema.metrics:
        if metric.metric_type not in allowed_types:
            continue
        for haystack in (metric.metric_id.lower(), metric.display_name.lower()):
            if user_phrase == haystack or user_phrase in haystack:
                candidates.append((len(haystack), metric.metric_id))
                break
    if not candidates:
        raise NoMatch()
    return {
        "source": schema.source_id,
        "metrics": [sorted(candidates)[0][1]],
    }


@register(rf"^(?:show me |show |what(?:'s| is) the |what(?:'s| is) )?(.+?)\s+by\s+({_GRAIN_ALT})$")
def metric_by_grain(m: re.Match, schema: AllowedSchema) -> dict:
    """<metric> by month/week/etc -- pick a temporal dim and apply the grain."""
    metric_id = _resolve_metric(m.group(1), schema)
    grain = GRAIN_WORDS.get(m.group(2).lower())
    dim_id = _resolve_temporal_dim(schema)
    if not metric_id or not grain or not dim_id:
        raise NoMatch()
    return {
        "source": schema.source_id,
        "metrics": [metric_id],
        "dimensions": [{"id": dim_id, "grain": grain}],
    }


@register(rf"^(?:show me |show |what(?:'s| is) the |what(?:'s| is) )?(.+?)\s+(?:{_OVER_TIME_ALT})$")
def metric_over_time(m: re.Match, schema: AllowedSchema) -> dict:
    """<metric> trend | <metric> over time -- temporal grouping at month grain.

    Sugar for "<metric> by month" using the informal phrasing. The grain
    default is `month` rather than auto-detecting because most "trend"
    questions are about month-scale shape; a user asking specifically for
    daily or yearly granularity would phrase it as "by day" / "by year"."""
    metric_id = _resolve_metric(m.group(1), schema)
    dim_id = _resolve_temporal_dim(schema)
    if not metric_id or not dim_id:
        raise NoMatch()
    return {
        "source": schema.source_id,
        "metrics": [metric_id],
        "dimensions": [{"id": dim_id, "grain": "month"}],
    }


@register(r"^(?:show me |show |what(?:'s| is) the |what(?:'s| is) )?(.+?)\s+and\s+(.+?)(?:\s+by\s+(.+))?$")
def multi_metric(m: re.Match, schema: AllowedSchema) -> dict:
    """<metric1> and <metric2> [by <dim>] -- two metrics, optional grouping.

    Splits on ` and `. Both sides must resolve to distinct metrics;
    duplicates (e.g. "revenue and revenue") are rejected so the user
    gets a clear no-match instead of a silently-deduped intent.

    V1 limitations:
      * Exactly two metrics (no "X and Y and Z"). A real 3-metric query
        is rare in NL; users typing it can fall back to the DSL.
      * Single optional dimension via the trailing "by <dim>". For
        multi-dimensional questions the user types the DSL.
    """
    metric1_id = _resolve_metric(m.group(1), schema)
    metric2_id = _resolve_metric(m.group(2), schema)
    if not metric1_id or not metric2_id or metric1_id == metric2_id:
        raise NoMatch()
    intent: dict = {
        "source": schema.source_id,
        "metrics": [metric1_id, metric2_id],
    }
    if m.group(3):
        dim_id = _resolve_dimension(m.group(3), schema)
        if not dim_id:
            raise NoMatch()
        intent["dimensions"] = [{"id": dim_id}]
    return intent


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
        month = MONTH_NAMES.get(month_word.lower())
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


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@register(r"^(?:show me |show |what(?:'s| is) the |what(?:'s| is) )?(.+?)\s+from\s+(\S+)\s+to\s+(\S+)$")
def metric_in_date_range(m: re.Match, schema: AllowedSchema) -> dict:
    """<metric> from YYYY-MM-DD to YYYY-MM-DD -- half-open [start, end)
    filter on the temporal column.

    Both bounds are required; both must be ISO-format dates. The upper
    bound is treated as exclusive (consistent with metric_in_period /
    metric_last_n_period), so "from 2026-01-01 to 2026-02-01" covers
    all of January 2026. A user wanting an INCLUSIVE upper bound should
    write the day AFTER the target end."""
    metric_id = _resolve_metric(m.group(1), schema)
    if not metric_id:
        raise NoMatch()
    col_name = _resolve_temporal_column(schema)
    if col_name is None:
        raise NoMatch()
    start = m.group(2)
    end = m.group(3)
    if not _DATE_RE.match(start) or not _DATE_RE.match(end):
        raise NoMatch()
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


def _this_period_bounds(unit: str, today: date) -> tuple[str, str]:
    """Compute [start, end) bounds for the current week / month / quarter / year."""
    if unit == "year":
        start = date(today.year, 1, 1)
        end = date(today.year + 1, 1, 1)
    elif unit == "quarter":
        q_index = (today.month - 1) // 3      # 0..3
        start = date(today.year, q_index * 3 + 1, 1)
        end_month = q_index * 3 + 4
        end_year = today.year + (1 if end_month > 12 else 0)
        end_month = end_month - 12 if end_month > 12 else end_month
        end = date(end_year, end_month, 1)
    elif unit == "month":
        start = date(today.year, today.month, 1)
        end_month = today.month + 1
        end_year = today.year + (1 if end_month > 12 else 0)
        end_month = 1 if end_month > 12 else end_month
        end = date(end_year, end_month, 1)
    else:  # week -- ISO Monday-start
        start = today - timedelta(days=today.weekday())
        end = start + timedelta(days=7)
    return start.isoformat(), end.isoformat()


@register(rf"^(?:show me |show |what(?:'s| is) the |what(?:'s| is) )?(.+?)\s+this\s+({_THIS_PERIOD_ALT})$")
def metric_this_period(m: re.Match, schema: AllowedSchema) -> dict:
    """<metric> this {week|month|quarter|year} -- half-open filter on
    the current period's calendar bounds.

    Uses the same _today() indirection as metric_last_n_period so tests
    can monkeypatch a deterministic date. Week boundary is ISO (Monday
    start)."""
    metric_id = _resolve_metric(m.group(1), schema)
    if not metric_id:
        raise NoMatch()
    col_name = _resolve_temporal_column(schema)
    if col_name is None:
        raise NoMatch()
    start, end = _this_period_bounds(m.group(2).lower(), _today())
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


@register(r"^(?:show me |show |what(?:'s| is) the |what(?:'s| is) )?(.+?)\s+(?:last|past)\s+(\d+)\s+(days?|weeks?|months?|years?)$")
def metric_last_n_period(m: re.Match, schema: AllowedSchema) -> dict:
    """<metric> last|past N days|weeks|months|years -- half-open filter
    [today - N units, today + 1 day) on the first temporal column.

    Month/year units approximate (1 month = 30 days, 1 year = 365 days).
    Phase 3's relative_time_filter primitive replaces this with calendar-
    aware arithmetic. The approximation is acceptable for typical
    "rolling window" questions where being off by 1-3 days is rounding,
    not meaning."""
    metric_id = _resolve_metric(m.group(1), schema)
    if not metric_id:
        raise NoMatch()
    col_name = _resolve_temporal_column(schema)
    if col_name is None:
        raise NoMatch()
    n = int(m.group(2))
    unit_singular = m.group(3).rstrip("s").lower()
    unit_days = _PERIOD_UNIT_TO_DAYS[unit_singular]
    today = _today()
    start = today - timedelta(days=n * unit_days)
    end = today + timedelta(days=1)  # exclusive upper bound = end of today
    return {
        "source": schema.source_id,
        "metrics": [metric_id],
        "filters": [{
            "op": "and",
            "args": [
                {"op": "gte", "column": col_name, "value": start.isoformat()},
                {"op": "lt", "column": col_name, "value": end.isoformat()},
            ],
        }],
    }


@register(r"^(?:count(?:\s+of)?|how many|total)\s+(\w+)\s+(?:\w+)$")
def count_with_condition(m: re.Match, schema: AllowedSchema) -> dict:
    """count of <adjective> <noun> -- count metric with eq filter on the
    column whose example_values contain <adjective>.

    Example: "count of paid orders" -> count metric + status='paid'. The
    trailing noun ("orders") is user context, not part of the filter.

    Registered BEFORE count_of_thing so multi-word forms route here first;
    if the adjective doesn't resolve to a known example value, this raises
    NoMatch and count_of_thing handles the bare form."""
    adjective = m.group(1)
    found = _find_column_by_example_value(adjective, schema)
    if found is None:
        raise NoMatch()
    col_name, canonical_value = found
    for metric in schema.metrics:
        if metric.metric_type == "count":
            return {
                "source": schema.source_id,
                "metrics": [metric.metric_id],
                "filters": [
                    {"op": "eq", "column": col_name, "value": canonical_value},
                ],
            }
    raise NoMatch()


@register(r"^(?:count(?:\s+of)?|how many|total)\s+(.+)$")
def count_of_thing(m: re.Match, schema: AllowedSchema) -> dict:
    """count of <source>, how many orders, total customers.
    Picks the first count-type metric on the schema."""
    for metric in schema.metrics:
        if metric.metric_type == "count":
            return {"source": schema.source_id, "metrics": [metric.metric_id]}
    raise NoMatch()


@register(r"^(?:show me |show |what(?:'s| is) the |what(?:'s| is) )?(.+?)\s+excluding\s+(\w+)(?:\s+\w+)?$")
def metric_excluding_value(m: re.Match, schema: AllowedSchema) -> dict:
    """<metric> excluding <value> [<noun>] -- neq filter on the column
    whose example_values contain <value>. The optional trailing noun
    ("orders" in "excluding refunded orders") is matched but discarded
    -- it's user context, not part of the filter."""
    metric_id = _resolve_metric(m.group(1), schema)
    if not metric_id:
        raise NoMatch()
    found = _find_column_by_example_value(m.group(2).strip(), schema)
    if found is None:
        raise NoMatch()
    col_name, canonical_value = found
    return {
        "source": schema.source_id,
        "metrics": [metric_id],
        "filters": [{"op": "neq", "column": col_name, "value": canonical_value}],
    }


@register(r"^(?:show me |show |what(?:'s| is) the |what(?:'s| is) )?(.+?)\s+for\s+(\w+)\s+and\s+(\w+)$")
def metric_filter_compound(m: re.Match, schema: AllowedSchema) -> dict:
    """<metric> for <v1> and <v2> -- two eq filters AND-ed together.

    Each value is resolved against example_values independently; the
    two filters may bind to the SAME column (e.g. "status for paid and
    pending" -- though that's semantically weird, the V1 contract
    doesn't forbid it) or DIFFERENT columns ("for west and paid" ->
    region='west' AND status='paid'). If either value doesn't resolve,
    no fabrication: NoMatch."""
    metric_id = _resolve_metric(m.group(1), schema)
    if not metric_id:
        raise NoMatch()
    f1 = _find_column_by_example_value(m.group(2).strip(), schema)
    f2 = _find_column_by_example_value(m.group(3).strip(), schema)
    if f1 is None or f2 is None:
        raise NoMatch()
    col1, val1 = f1
    col2, val2 = f2
    return {
        "source": schema.source_id,
        "metrics": [metric_id],
        "filters": [
            {"op": "eq", "column": col1, "value": val1},
            {"op": "eq", "column": col2, "value": val2},
        ],
    }


@register(r"^(?:show me |show |what(?:'s| is) the |what(?:'s| is) )?(.+?)\s+for\s+(.+)$")
def metric_filtered_by_value(m: re.Match, schema: AllowedSchema) -> dict:
    """<metric> for <value> -- eq filter on a column whose example_values
    contain <value>. Without populated example_values this pattern is inert
    by design (no inference, no fabrication)."""
    metric_id = _resolve_metric(m.group(1), schema)
    if not metric_id:
        raise NoMatch()
    found = _find_column_by_example_value(m.group(2).strip(), schema)
    if found is None:
        raise NoMatch()
    col_name, canonical_value = found
    return {
        "source": schema.source_id,
        "metrics": [metric_id],
        "filters": [{"op": "eq", "column": col_name, "value": canonical_value}],
    }


@register(r"^(?:show me |show |what(?:'s| is) the |what(?:'s| is) )?(.+?)\s+distribution$")
def metric_distribution(m: re.Match, schema: AllowedSchema) -> dict:
    """<metric> distribution -- returns the named metric IF its type is
    median or percentile. Otherwise NoMatch.

    Phase 1 contract: returns a single summary statistic (the median or
    percentile value), not a full distribution shape. A proper
    "p10/p25/p50/p75/p90 in one query" output requires a new multi-row
    shape primitive, which is deferred to Phase 2A per the roadmap's
    stop-doing list (no new shape primitives on top of the current
    branch hack)."""
    metric_id = _resolve_metric(m.group(1), schema)
    if not metric_id:
        raise NoMatch()
    for metric in schema.metrics:
        if (
            metric.metric_id == metric_id
            and metric.metric_type in ("median", "percentile")
        ):
            return {"source": schema.source_id, "metrics": [metric_id]}
    raise NoMatch()


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
