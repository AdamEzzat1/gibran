import pytest

from rumi.governance.ast import ASTValidationError
from rumi.governance.ast import validate_policy_ast as validate

KNOWN_COLS = frozenset({"region", "tier", "amount", "status"})


class TestHappyPath:
    def test_eq_simple_literal(self) -> None:
        validate({"op": "eq", "column": "region", "value": "west"}, KNOWN_COLS)

    def test_eq_with_attribute_reference(self) -> None:
        validate(
            {"op": "eq", "column": "region", "value": {"$attr": "region"}},
            KNOWN_COLS,
        )

    def test_in_with_list_of_literals(self) -> None:
        validate(
            {"op": "in", "column": "tier", "value": ["gold", "platinum"]},
            KNOWN_COLS,
        )

    def test_between(self) -> None:
        validate(
            {"op": "between", "column": "amount", "value": [0, 1000]},
            KNOWN_COLS,
        )

    def test_is_null(self) -> None:
        validate({"op": "is_null", "column": "status"}, KNOWN_COLS)

    def test_is_not_null(self) -> None:
        validate({"op": "is_not_null", "column": "status"}, KNOWN_COLS)

    def test_and_with_nested_predicates(self) -> None:
        validate(
            {
                "op": "and",
                "args": [
                    {"op": "eq", "column": "region", "value": "west"},
                    {"op": "in", "column": "tier", "value": ["gold"]},
                ],
            },
            KNOWN_COLS,
        )

    def test_or_with_three_branches(self) -> None:
        validate(
            {
                "op": "or",
                "args": [
                    {"op": "eq", "column": "region", "value": "west"},
                    {"op": "eq", "column": "region", "value": "east"},
                    {"op": "is_null", "column": "region"},
                ],
            },
            KNOWN_COLS,
        )

    def test_not_wraps_one_predicate(self) -> None:
        validate(
            {"op": "not", "args": [{"op": "is_null", "column": "status"}]},
            KNOWN_COLS,
        )


class TestRejections:
    def test_unknown_op_rejected(self) -> None:
        with pytest.raises(ASTValidationError, match="non-whitelisted op"):
            validate({"op": "regex", "column": "region", "value": "^w"}, KNOWN_COLS)

    def test_function_call_op_rejected(self) -> None:
        with pytest.raises(ASTValidationError, match="non-whitelisted op"):
            validate({"op": "lower", "column": "region"}, KNOWN_COLS)

    def test_unknown_column_rejected(self) -> None:
        with pytest.raises(ASTValidationError, match="unknown column"):
            validate(
                {"op": "eq", "column": "customer_email", "value": "x"},
                KNOWN_COLS,
            )

    def test_attribute_ref_not_allowed_inside_in_list(self) -> None:
        with pytest.raises(ASTValidationError, match="attribute reference not allowed"):
            validate(
                {"op": "in", "column": "tier", "value": [{"$attr": "tier"}]},
                KNOWN_COLS,
            )

    def test_attribute_ref_not_allowed_inside_between(self) -> None:
        with pytest.raises(ASTValidationError, match="attribute reference not allowed"):
            validate(
                {"op": "between", "column": "amount", "value": [{"$attr": "min"}, 100]},
                KNOWN_COLS,
            )

    def test_or_with_empty_args_rejected(self) -> None:
        with pytest.raises(ASTValidationError, match="non-empty"):
            validate({"op": "or", "args": []}, KNOWN_COLS)

    def test_not_must_have_exactly_one_arg(self) -> None:
        with pytest.raises(ASTValidationError, match="length 1"):
            validate({"op": "not", "args": []}, KNOWN_COLS)

    def test_between_needs_two_values(self) -> None:
        with pytest.raises(ASTValidationError, match="length 2"):
            validate({"op": "between", "column": "amount", "value": [0]}, KNOWN_COLS)

    def test_non_dict_root_rejected(self) -> None:
        with pytest.raises(ASTValidationError, match="must be a dict"):
            validate("oops", KNOWN_COLS)  # type: ignore[arg-type]

    def test_eq_missing_value_rejected(self) -> None:
        with pytest.raises(ASTValidationError, match="requires 'value'"):
            validate({"op": "eq", "column": "region"}, KNOWN_COLS)

    def test_malformed_attr_ref_rejected(self) -> None:
        with pytest.raises(ASTValidationError, match="attribute reference must be"):
            validate(
                {"op": "eq", "column": "region", "value": {"$attr": 42}},
                KNOWN_COLS,
            )
