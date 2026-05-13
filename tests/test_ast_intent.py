"""Tests for the intent-AST variants (validate_intent_ast, compile_intent_to_sql).

The policy variants are already tested in test_ast_validation.py and
test_ast_compile.py. These tests pin the *differences* between the two
trust contexts:

  - validate_intent_ast rejects {"$attr": ...} that validate_policy_ast accepts
  - compile_intent_to_sql refuses {"$attr": ...} unconditionally (would have
    been caught by validate_intent_ast in normal flow; defense-in-depth)
"""
import pytest

from rumi.governance.ast import (
    ASTValidationError,
    compile_intent_to_sql,
    compile_policy_to_sql,
    validate_intent_ast,
    validate_policy_ast,
)
from rumi.governance.types import IdentityContext


KNOWN_COLS = frozenset({"region", "tier", "amount", "status"})


def _ident(**attrs: str) -> IdentityContext:
    return IdentityContext(
        user_id="u", role_id="r", attributes=dict(attrs), source="test"
    )


class TestValidateIntentASTRejectsAttrRefs:
    def test_attr_ref_in_eq_rejected(self) -> None:
        with pytest.raises(ASTValidationError, match="attribute reference"):
            validate_intent_ast(
                {"op": "eq", "column": "region", "value": {"$attr": "region"}},
                KNOWN_COLS,
            )

    def test_attr_ref_in_nested_and_rejected(self) -> None:
        with pytest.raises(ASTValidationError, match="attribute reference"):
            validate_intent_ast(
                {
                    "op": "and",
                    "args": [
                        {"op": "eq", "column": "region", "value": "west"},
                        {"op": "eq", "column": "tier", "value": {"$attr": "tier"}},
                    ],
                },
                KNOWN_COLS,
            )

    def test_policy_ast_still_accepts_attr_refs(self) -> None:
        # Sanity: the policy variant must still accept {"$attr": ...}
        validate_policy_ast(
            {"op": "eq", "column": "region", "value": {"$attr": "region"}},
            KNOWN_COLS,
        )

    def test_intent_ast_accepts_pure_scalar_filters(self) -> None:
        validate_intent_ast(
            {"op": "eq", "column": "region", "value": "west"}, KNOWN_COLS
        )
        validate_intent_ast(
            {"op": "in", "column": "tier", "value": ["gold", "platinum"]}, KNOWN_COLS
        )
        validate_intent_ast(
            {"op": "between", "column": "amount", "value": [0, 1000]}, KNOWN_COLS
        )


class TestCompileIntentRefusesAttrRefs:
    def test_compile_intent_raises_on_attr_ref(self) -> None:
        # If validate_intent_ast is bypassed and an attr ref reaches the
        # compiler, it must refuse (defense in depth).
        with pytest.raises(ValueError, match=r"\$attr"):
            compile_intent_to_sql(
                {"op": "eq", "column": "region", "value": {"$attr": "region"}}
            )

    def test_compile_intent_no_identity_parameter(self) -> None:
        # Signature check: the intent compiler takes no identity argument
        # (this is the trust-boundary type-system guarantee).
        import inspect
        sig = inspect.signature(compile_intent_to_sql)
        assert "identity" not in sig.parameters
        assert len(sig.parameters) == 1
        assert "ast" in sig.parameters

    def test_compile_intent_produces_valid_sql_for_scalar_filter(self) -> None:
        sql = compile_intent_to_sql(
            {"op": "eq", "column": "region", "value": "west"}
        )
        assert sql == '("region" = \'west\')'

    def test_compile_policy_still_substitutes_attrs(self) -> None:
        # Sanity: policy compiler still works as before
        sql = compile_policy_to_sql(
            {"op": "eq", "column": "region", "value": {"$attr": "region"}},
            _ident(region="west"),
        )
        assert sql == '("region" = \'west\')'
