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
