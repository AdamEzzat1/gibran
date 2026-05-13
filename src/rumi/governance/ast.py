"""row_filter_ast validation and SQL compilation.

The AST is the only stored form of filter predicates; raw SQL never
crosses this boundary. There are TWO different trust contexts for AST
processing, intentionally exposed as separate functions:

1. POLICY ASTs (authored in rumi.yaml, stored in rumi_policies.row_filter_ast).
   These reference identity attributes via {"$attr": "key"} and are
   compiled with identity substitution at evaluation time.
     -> validate_policy_ast(ast, known_columns)
     -> compile_policy_to_sql(ast, identity)

2. INTENT ASTs (user-submitted filters in the DSL surface).
   These must NOT reference identity attributes -- only literal scalars.
   No identity argument exists in this code path.
     -> validate_intent_ast(ast, known_columns)
     -> compile_intent_to_sql(ast)

The trust boundary is encoded in the function signatures: anyone calling
the policy compiler must produce an identity; anyone calling the intent
compiler cannot. This prevents the "well-meaning developer reuses the
wrong function" failure mode by construction.

AST node shape:
    {"op": <op>, ...op-specific keys}

Examples:
    {"op": "eq", "column": "region", "value": "west"}
    {"op": "eq", "column": "region", "value": {"$attr": "region"}}   # POLICY-ONLY
    {"op": "in", "column": "tier", "value": ["gold", "platinum"]}
    {"op": "and", "args": [<ast>, <ast>, ...]}
"""
from __future__ import annotations

from typing import Any

from rumi._sql import qident as _qident, render_literal as _render_literal
from rumi.governance.types import ALLOWED_AST_OPS, IdentityContext


_BOOL_OPS = frozenset({"and", "or"})
_COMPARISON_OPS = frozenset({"eq", "neq", "lt", "lte", "gt", "gte"})
_SET_OPS = frozenset({"in", "not_in"})
_NULL_OPS = frozenset({"is_null", "is_not_null"})


class ASTValidationError(ValueError):
    pass


# ---------------------------------------------------------------------------
# Public validators
# ---------------------------------------------------------------------------

def validate_policy_ast(ast: Any, known_columns: frozenset[str]) -> None:
    """Validate AST for use in a row-filter policy (allows {"$attr": ...})."""
    _validate_impl(ast, known_columns, allow_attr_refs=True)


def validate_intent_ast(ast: Any, known_columns: frozenset[str]) -> None:
    """Validate AST for use in a DSL intent filter.

    Differs from validate_policy_ast in ONE way: rejects {"$attr": ...}
    anywhere. Intent filters are scalar-only; identity attributes are
    only meaningful in policy authorship."""
    _validate_impl(ast, known_columns, allow_attr_refs=False)


# ---------------------------------------------------------------------------
# Public compilers
# ---------------------------------------------------------------------------

_SQL_BINOPS = {
    "eq": "=", "neq": "<>", "lt": "<", "lte": "<=", "gt": ">", "gte": ">=",
}


def compile_policy_to_sql(ast: dict[str, Any], identity: IdentityContext) -> str:
    """Compile a validated policy AST to SQL. Substitutes {"$attr": "key"}
    references with identity.attributes[key] as quoted literals.

    Raises KeyError if AST references an attribute not in identity
    (caller maps to ATTRIBUTE_MISSING denial)."""
    return _compile_impl(ast, identity=identity)


def compile_intent_to_sql(ast: dict[str, Any]) -> str:
    """Compile a validated intent (DSL) AST to SQL. No identity, no
    attribute substitution. If a {"$attr": ...} reference is encountered
    (which validate_intent_ast should have rejected, but defense in
    depth), raises ValueError."""
    return _compile_impl(ast, identity=None)


# ---------------------------------------------------------------------------
# Shared implementation
# ---------------------------------------------------------------------------

def _validate_impl(
    ast: Any,
    known_columns: frozenset[str],
    *,
    allow_attr_refs: bool,
) -> None:
    if not isinstance(ast, dict):
        raise ASTValidationError(
            f"AST node must be a dict, got {type(ast).__name__}"
        )

    op = ast.get("op")
    if not isinstance(op, str) or op not in ALLOWED_AST_OPS:
        raise ASTValidationError(f"unknown or non-whitelisted op: {op!r}")

    if op in _BOOL_OPS:
        args = ast.get("args")
        if not isinstance(args, list) or not args:
            raise ASTValidationError(f"{op!r} requires non-empty 'args' list")
        for child in args:
            _validate_impl(child, known_columns, allow_attr_refs=allow_attr_refs)
        return

    if op == "not":
        args = ast.get("args")
        if not isinstance(args, list) or len(args) != 1:
            raise ASTValidationError("'not' requires 'args' list of length 1")
        _validate_impl(args[0], known_columns, allow_attr_refs=allow_attr_refs)
        return

    column = ast.get("column")
    if not isinstance(column, str):
        raise ASTValidationError(f"{op!r} requires 'column' string")
    if column not in known_columns:
        raise ASTValidationError(f"unknown column: {column!r}")

    if op in _COMPARISON_OPS:
        if "value" not in ast:
            raise ASTValidationError(f"{op!r} requires 'value'")
        _validate_value(ast["value"], allow_attr=allow_attr_refs)
        return

    if op in _SET_OPS:
        val = ast.get("value")
        if not isinstance(val, list) or not val:
            raise ASTValidationError(f"{op!r} requires non-empty 'value' list")
        for v in val:
            _validate_value(v, allow_attr=False)
        return

    if op in _NULL_OPS:
        return

    if op == "between":
        val = ast.get("value")
        if not isinstance(val, list) or len(val) != 2:
            raise ASTValidationError("'between' requires 'value' list of length 2")
        for v in val:
            _validate_value(v, allow_attr=False)
        return

    raise ASTValidationError(f"op handler missing for {op!r}")  # pragma: no cover


def _validate_value(v: Any, *, allow_attr: bool) -> None:
    if isinstance(v, dict):
        if not allow_attr:
            raise ASTValidationError(
                "attribute reference not allowed here (intent context or "
                "non-scalar position)"
            )
        if set(v.keys()) != {"$attr"} or not isinstance(v["$attr"], str):
            raise ASTValidationError(
                f"attribute reference must be {{'$attr': str}}, got {v!r}"
            )
        return
    if not isinstance(v, (str, int, float, bool)) and v is not None:
        raise ASTValidationError(
            f"unsupported value type: {type(v).__name__}"
        )


def _compile_impl(ast: dict[str, Any], *, identity: IdentityContext | None) -> str:
    op = ast["op"]

    if op == "and":
        return "(" + " AND ".join(
            _compile_impl(c, identity=identity) for c in ast["args"]
        ) + ")"
    if op == "or":
        return "(" + " OR ".join(
            _compile_impl(c, identity=identity) for c in ast["args"]
        ) + ")"
    if op == "not":
        return "(NOT " + _compile_impl(ast["args"][0], identity=identity) + ")"

    column = _qident(ast["column"])

    if op in _SQL_BINOPS:
        value = _resolve_value(ast["value"], identity)
        return f"({column} {_SQL_BINOPS[op]} {_render_literal(value)})"

    if op in ("in", "not_in"):
        values = [_resolve_value(v, identity) for v in ast["value"]]
        rendered = ", ".join(_render_literal(v) for v in values)
        keyword = "IN" if op == "in" else "NOT IN"
        return f"({column} {keyword} ({rendered}))"

    if op == "is_null":
        return f"({column} IS NULL)"
    if op == "is_not_null":
        return f"({column} IS NOT NULL)"

    if op == "between":
        low = _render_literal(_resolve_value(ast["value"][0], identity))
        high = _render_literal(_resolve_value(ast["value"][1], identity))
        return f"({column} BETWEEN {low} AND {high})"

    raise ValueError(f"unhandled op in compile: {op!r}")  # pragma: no cover


def _resolve_value(v: Any, identity: IdentityContext | None) -> Any:
    if isinstance(v, dict) and "$attr" in v:
        if identity is None:
            raise ValueError(
                "intent compilation encountered {$attr: ...}; "
                "validate_intent_ast must be called before compile_intent_to_sql"
            )
        return identity.attributes[v["$attr"]]
    return v
