"""Shared SQL-rendering utilities.

These functions appear in TWO compile paths -- AST filter compilation
(governance.ast) and DSL intent compilation (dsl.compile). Centralizing
them here ensures both paths quote identifiers and escape literals the
same way; a bug in one would otherwise diverge from the other.
"""
from __future__ import annotations

from typing import Any


def qident(name: str) -> str:
    """Quote a SQL identifier (column / table / alias name) using DuckDB
    double-quote syntax. Refuses identifiers containing a double-quote
    to eliminate the entire injection-via-identifier vector."""
    if '"' in name:
        raise ValueError(f"identifier contains double-quote: {name!r}")
    return f'"{name}"'


def render_literal(v: Any) -> str:
    """Render a Python scalar as a SQL literal.

    Single quotes in strings are doubled (SQL escape). bool is checked
    BEFORE int because `bool` is a subclass of `int` in Python (and
    True would otherwise render as `1` instead of `TRUE`)."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    raise TypeError(f"unsupported literal type: {type(v).__name__}")
