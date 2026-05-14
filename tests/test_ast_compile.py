import duckdb
import pytest

from gibran.governance.ast import compile_policy_to_sql as compile_to_sql
from gibran.governance.types import IdentityContext


def _ident(**attrs: str) -> IdentityContext:
    return IdentityContext(
        user_id="u", role_id="r", attributes=dict(attrs), source="test",
    )


class TestCompileBasic:
    def test_eq_string_literal(self) -> None:
        sql = compile_to_sql(
            {"op": "eq", "column": "region", "value": "west"}, _ident()
        )
        assert sql == '("region" = \'west\')'

    def test_eq_int_literal(self) -> None:
        sql = compile_to_sql(
            {"op": "eq", "column": "tier", "value": 3}, _ident()
        )
        assert sql == '("tier" = 3)'

    def test_eq_bool_literal_renders_uppercase(self) -> None:
        sql = compile_to_sql(
            {"op": "eq", "column": "is_active", "value": True}, _ident()
        )
        assert sql == '("is_active" = TRUE)'

    def test_eq_null(self) -> None:
        sql = compile_to_sql(
            {"op": "eq", "column": "deleted_at", "value": None}, _ident()
        )
        assert sql == '("deleted_at" = NULL)'

    def test_neq_lt_lte_gt_gte(self) -> None:
        cases = [
            ("neq", "<>"), ("lt", "<"), ("lte", "<="), ("gt", ">"), ("gte", ">="),
        ]
        for op, expected_op in cases:
            sql = compile_to_sql(
                {"op": op, "column": "amount", "value": 100}, _ident()
            )
            assert sql == f'("amount" {expected_op} 100)'

    def test_in(self) -> None:
        sql = compile_to_sql(
            {"op": "in", "column": "tier", "value": ["gold", "platinum"]}, _ident()
        )
        assert sql == "(\"tier\" IN ('gold', 'platinum'))"

    def test_not_in(self) -> None:
        sql = compile_to_sql(
            {"op": "not_in", "column": "status", "value": ["draft", "void"]}, _ident()
        )
        assert sql == "(\"status\" NOT IN ('draft', 'void'))"

    def test_is_null_and_is_not_null(self) -> None:
        assert compile_to_sql(
            {"op": "is_null", "column": "completed_at"}, _ident()
        ) == '("completed_at" IS NULL)'
        assert compile_to_sql(
            {"op": "is_not_null", "column": "email"}, _ident()
        ) == '("email" IS NOT NULL)'

    def test_between(self) -> None:
        sql = compile_to_sql(
            {"op": "between", "column": "amount", "value": [0, 1000]}, _ident()
        )
        assert sql == '("amount" BETWEEN 0 AND 1000)'

    def test_and_combines(self) -> None:
        sql = compile_to_sql(
            {
                "op": "and",
                "args": [
                    {"op": "eq", "column": "region", "value": "west"},
                    {"op": "gt", "column": "amount", "value": 100},
                ],
            },
            _ident(),
        )
        assert sql == '(("region" = \'west\') AND ("amount" > 100))'

    def test_or_combines(self) -> None:
        sql = compile_to_sql(
            {
                "op": "or",
                "args": [
                    {"op": "eq", "column": "region", "value": "west"},
                    {"op": "eq", "column": "region", "value": "east"},
                ],
            },
            _ident(),
        )
        assert sql == '(("region" = \'west\') OR ("region" = \'east\'))'

    def test_not_wraps(self) -> None:
        sql = compile_to_sql(
            {"op": "not", "args": [{"op": "is_null", "column": "email"}]},
            _ident(),
        )
        assert sql == '(NOT ("email" IS NULL))'

    def test_nested_logic(self) -> None:
        sql = compile_to_sql(
            {
                "op": "and",
                "args": [
                    {"op": "eq", "column": "region", "value": "west"},
                    {
                        "op": "or",
                        "args": [
                            {"op": "eq", "column": "tier", "value": "gold"},
                            {"op": "eq", "column": "tier", "value": "platinum"},
                        ],
                    },
                ],
            },
            _ident(),
        )
        assert sql == (
            '(("region" = \'west\') AND '
            '(("tier" = \'gold\') OR ("tier" = \'platinum\')))'
        )


class TestAttributeSubstitution:
    def test_attr_substituted_to_string_literal(self) -> None:
        sql = compile_to_sql(
            {"op": "eq", "column": "region", "value": {"$attr": "region"}},
            _ident(region="west"),
        )
        assert sql == '("region" = \'west\')'

    def test_attr_inside_compound_logic(self) -> None:
        sql = compile_to_sql(
            {
                "op": "and",
                "args": [
                    {"op": "eq", "column": "partner_id", "value": {"$attr": "partner_id"}},
                    {"op": "eq", "column": "region", "value": {"$attr": "region"}},
                ],
            },
            _ident(partner_id="acme", region="us-west"),
        )
        assert sql == (
            '(("partner_id" = \'acme\') AND ("region" = \'us-west\'))'
        )

    def test_missing_attribute_raises_keyerror(self) -> None:
        with pytest.raises(KeyError):
            compile_to_sql(
                {"op": "eq", "column": "region", "value": {"$attr": "region"}},
                _ident(),  # no attributes
            )


class TestSQLEscaping:
    def test_single_quote_in_string_doubled(self) -> None:
        sql = compile_to_sql(
            {"op": "eq", "column": "name", "value": "O'Brien"}, _ident()
        )
        assert sql == "(\"name\" = 'O''Brien')"

    def test_attribute_value_with_quote_escaped(self) -> None:
        sql = compile_to_sql(
            {"op": "eq", "column": "name", "value": {"$attr": "name"}},
            _ident(name="O'Brien"),
        )
        assert sql == "(\"name\" = 'O''Brien')"

    def test_double_quote_in_column_rejected(self) -> None:
        with pytest.raises(ValueError, match="double-quote"):
            compile_to_sql(
                {"op": "eq", "column": 'col"injected', "value": "x"}, _ident()
            )

    def test_compiled_sql_actually_parses_in_duckdb(self) -> None:
        """Round-trip sanity: produced SQL is valid DuckDB syntax."""
        sql = compile_to_sql(
            {
                "op": "and",
                "args": [
                    {"op": "eq", "column": "region", "value": "west"},
                    {"op": "in", "column": "tier", "value": ["gold", "vip"]},
                    {"op": "between", "column": "amount", "value": [0, 1000]},
                    {"op": "is_not_null", "column": "email"},
                ],
            },
            _ident(),
        )
        con = duckdb.connect(":memory:")
        con.execute(
            "CREATE TABLE t (region VARCHAR, tier VARCHAR, amount INTEGER, email VARCHAR)"
        )
        con.execute(
            "INSERT INTO t VALUES "
            "('west', 'gold', 500, 'a@b.com'), "
            "('east', 'gold', 500, 'c@d.com')"
        )
        rows = con.execute(f"SELECT region FROM t WHERE {sql}").fetchall()
        assert rows == [("west",)]
