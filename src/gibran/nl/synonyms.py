"""Synonym dictionaries for the NL pattern layer.

Centralizes the user-language -> canonical-form mappings used by the
regex patterns in `gibran.nl.patterns`. Each entry here exists because
real questions phrase the same concept multiple ways ("biggest" /
"top" / "highest") and the pattern matcher would otherwise need a
fan-out of near-duplicate patterns for each phrasing.

Why a separate module:
  - patterns.py stays focused on regex + builder logic
  - synonym additions land here without touching pattern code, which
    means a contributor adding "rank" to ranking-words doesn't have to
    grok the whole matcher first
  - single audit point: someone looking for "what words map to what
    intent" reads exactly one file

Adding a synonym: append to the relevant constant below. The patterns
that consume it rebuild their regex from the constant at import time,
so no other change is needed.
"""
from __future__ import annotations


# Ranking-direction synonyms.
# Used by top_n_by_metric / bottom_n_by_metric in patterns.py to build
# the regex alternation for the leading keyword.
TOP_WORDS: tuple[str, ...] = ("top", "biggest", "largest", "highest")
BOTTOM_WORDS: tuple[str, ...] = (
    "bottom", "smallest", "lowest", "fewest", "least",
)


# Time-grain synonyms.
# Used by metric_by_grain to canonicalize "yearly" -> "year", etc.
# The value side is the canonical grain string accepted by the DSL
# (see dsl.types.Grain).
GRAIN_WORDS: dict[str, str] = {
    "year": "year", "yearly": "year", "annual": "year", "annually": "year",
    "quarter": "quarter", "quarterly": "quarter",
    "month": "month", "monthly": "month",
    "week": "week", "weekly": "week",
    "day": "day", "daily": "day",
}


# "Over time" phrasings.
# Used by metric_over_time -- when the user says "<metric> trend" or
# "<metric> over time", route to a temporal grouping with the default
# monthly grain. The set is intentionally small: only phrasings that
# unambiguously mean "show this across time."
OVER_TIME_WORDS: tuple[str, ...] = ("trend", "over time", "across time")


# Type-keyword routing: words that signal "find a metric of this type."
# Used by metric_by_type_keyword to route phrases like "unique customers"
# to a count_distinct metric, "max order amount" to a max metric, etc.
# Multiple keywords can map to the same primitive group (avg / mean,
# unique / distinct, max / maximum, etc.). The pattern filters
# AllowedSchema.metrics down to the keyword's primitive types BEFORE
# substring-matching on the remaining noun, so "max amount" resolves
# only against type=max metrics even if other metrics share the noun.
TYPE_KEYWORDS: dict[str, tuple[str, ...]] = {
    "unique":   ("count_distinct", "count_distinct_approx"),
    "distinct": ("count_distinct", "count_distinct_approx"),
    "max":      ("max",),
    "maximum":  ("max",),
    "min":      ("min",),
    "minimum":  ("min",),
    "first":    ("first_value",),
    "last":     ("last_value",),
    "average":  ("avg",),
    "avg":      ("avg",),
    "mean":     ("avg",),
    "median":   ("median",),
}


# "This period" keywords for the metric_this_period pattern. Each maps
# to a DuckDB DATE_TRUNC unit so the builder can compute the period
# bounds relative to today.
THIS_PERIOD_WORDS: tuple[str, ...] = ("week", "month", "quarter", "year")


# Period-over-period phrasings. Each maps a user-language phrase to the
# canonical period unit it implies. The metric_period_over_period pattern
# uses this to route "<metric> yoy" or "<metric> vs last year" to a
# period_over_period metric whose name hints at the year unit (and to
# distinguish from monthly/quarterly variants). Order matters at the
# regex level -- longer phrases must alternate FIRST so "year over year"
# isn't shadowed by "year".
PERIOD_PHRASE_TO_UNIT: dict[str, str] = {
    "year over year":     "year",
    "vs last year":       "year",
    "yoy":                "year",
    "quarter over quarter": "quarter",
    "vs last quarter":    "quarter",
    "qoq":                "quarter",
    "month over month":   "month",
    "vs last month":      "month",
    "mom":                "month",
}


# Short codes for each unit -- used by the matcher to look for hints
# in metric names (e.g. a "revenue_yoy" metric_id signals year-unit).
PERIOD_UNIT_SHORT: dict[str, str] = {
    "year":    "yoy",
    "quarter": "qoq",
    "month":   "mom",
}


# Month-name lookup -- full English names plus standard 3-letter
# abbreviations (and "sept" since it shows up in real text). Used by
# metric_in_period to resolve a month word to its 1-12 integer.
MONTH_NAMES: dict[str, int] = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}
