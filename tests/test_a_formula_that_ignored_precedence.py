"""A column formula must mean what it says in ordinary arithmetic.

The parser behind column_math and add_column split the text on + - * / and
folded the pieces left to right. ``a + b * 2`` with a=1, b=10 wrote 22 instead
of 21, under ``success: true``, and there was no way to write the intended
version: parentheses and unary minus were both refused as unknown tokens.

Formulas now go through Python's grammar and an allow-listed tree walk
(shared/expr.py). These tests pin the arithmetic, the names that must keep
resolving the way they always did, and the refusals -- including every way a
formula could reach code, since nothing here may ever amount to eval().
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from servers.data_basic.engine import apply_patch  # noqa: E402
from shared.expr import MAX_LENGTH, FormulaError, evaluate  # noqa: E402


@pytest.fixture
def df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [10, 20, 30],
            "Units Sold": [4, 5, 0],
            "clicks-2": [7, 8, 9],
            "2020": [100, 200, 300],
            "status": ["open", "closed", "Units Sold"],
            "maybe": [1.0, None, 3.0],
            "numtext": ["12", "3.5", "0"],
        }
    )


def values(series: pd.Series) -> list:
    return [None if (isinstance(v, float) and np.isnan(v)) else v for v in series.tolist()]


class TestArithmetic:
    def test_multiplication_binds_tighter_than_addition(self, df):
        # The reported case: 1 + 10*2 is 21, not (1 + 10) * 2 = 22.
        assert values(evaluate("a + b * 2", df)) == [21.0, 42.0, 63.0]

    def test_division_binds_tighter_than_subtraction(self, df):
        assert values(evaluate("b / a - 1", df)) == [9.0, 9.0, 9.0]
        assert values(evaluate("b - a / 1", df)) == [9.0, 18.0, 27.0]

    def test_parentheses(self, df):
        assert values(evaluate("(a + b) * 2", df)) == [22.0, 44.0, 66.0]

    def test_unary_minus_and_plus(self, df):
        assert values(evaluate("a * -1", df)) == [-1.0, -2.0, -3.0]
        assert values(evaluate("-a + +b", df)) == [9.0, 18.0, 27.0]

    def test_power_floor_division_and_modulo(self, df):
        assert values(evaluate("a ** 2", df)) == [1.0, 4.0, 9.0]
        assert values(evaluate("b // 3", df)) == [3.0, 6.0, 10.0]
        assert values(evaluate("b % 7", df)) == [3.0, 6.0, 2.0]

    def test_results_are_floats_as_before(self, df):
        assert evaluate("a * 2", df).dtype == float

    def test_a_constant_formula_fills_every_row(self, df):
        out = evaluate("2 + 3", df)
        assert values(out) == [5.0, 5.0, 5.0]
        assert list(out.index) == list(df.index)

    def test_division_by_zero_is_infinity_not_an_exception(self, df):
        out = evaluate("b / `Units Sold`", df)
        assert values(out)[:2] == [2.5, 4.0]
        assert np.isinf(out.iloc[2])
        assert np.isinf(evaluate("1 / 0", df).iloc[0])

    def test_numeric_text_is_still_read_as_a_number(self, df):
        assert values(evaluate("numtext * 2", df)) == [24.0, 7.0, 0.0]


class TestNames:
    def test_a_name_with_a_space_still_works_bare(self, df):
        assert values(evaluate("b / Units Sold", df))[:2] == [2.5, 4.0]

    def test_a_name_with_operator_characters_needs_backticks(self, df):
        assert values(evaluate("`clicks-2` * 2", df)) == [14.0, 16.0, 18.0]

    def test_a_number_that_is_also_a_column_is_refused_not_guessed(self, df):
        with pytest.raises(FormulaError, match="both a number and a column name"):
            evaluate("2020 - a", df)
        assert values(evaluate("`2020` - a", df)) == [99.0, 198.0, 297.0]

    def test_a_decimal_is_not_mistaken_for_a_column(self):
        frame = pd.DataFrame({"1": [5], "x": [2]})
        assert values(evaluate("x * 1.5", frame)) == [3.0]

    def test_a_name_inside_quotes_is_text_not_a_column(self, df):
        assert values(evaluate("status == 'Units Sold'", df)) == [False, False, True]

    def test_an_unknown_column_is_named_with_the_available_ones(self, df):
        with pytest.raises(FormulaError) as exc:
            evaluate("nosuch * 2", df)
        assert "nosuch" in str(exc.value)
        assert "Units Sold" in str(exc.value), "the refusal has to carry what is available"

    def test_an_unknown_backticked_column_is_refused(self, df):
        with pytest.raises(FormulaError, match="Unknown column 'nope'"):
            evaluate("`nope` + 1", df)


class TestLogic:
    def test_comparisons_return_booleans(self, df):
        assert values(evaluate("a >= 2", df)) == [False, True, True]
        assert values(evaluate("status != 'open'", df)) == [False, True, True]

    def test_chained_comparison(self, df):
        assert values(evaluate("1 < a < 3", df)) == [False, True, False]

    def test_and_or_not(self, df):
        assert values(evaluate("a > 1 and b < 30", df)) == [False, True, False]
        assert values(evaluate("a == 1 or b == 30", df)) == [True, False, True]
        assert values(evaluate("not a > 1", df)) == [True, False, False]

    def test_conditional_expression(self, df):
        assert values(evaluate("'big' if b > 15 else 'small'", df)) == ["small", "big", "big"]

    def test_scalar_logic(self, df):
        assert values(evaluate("1 if 2 > 1 else 0", df)) == [1.0, 1.0, 1.0]
        assert values(evaluate("not 0", df)) == [True, True, True]


class TestFunctions:
    def test_numeric_functions(self, df):
        assert values(evaluate("abs(a * -1)", df)) == [1.0, 2.0, 3.0]
        assert values(evaluate("round(b / 3, 1)", df)) == [3.3, 6.7, 10.0]
        assert values(evaluate("round(b / 3)", df)) == [3.0, 7.0, 10.0]
        assert values(evaluate("sqrt(a ** 2)", df)) == [1.0, 2.0, 3.0]
        assert values(evaluate("floor(b / 3)", df)) == [3.0, 6.0, 10.0]
        assert values(evaluate("ceil(b / 3)", df)) == [4.0, 7.0, 10.0]
        assert values(evaluate("clip(b, 15, 25)", df)) == [15.0, 20.0, 25.0]
        assert values(evaluate("log10(b)", df))[0] == pytest.approx(1.0)
        assert values(evaluate("exp(log(b))", df)) == pytest.approx([10.0, 20.0, 30.0])

    def test_null_handling(self, df):
        assert values(evaluate("coalesce(maybe, 0)", df)) == [1.0, 0.0, 3.0]
        assert values(evaluate("coalesce(maybe, a * 100)", df)) == [1.0, 200.0, 3.0]
        assert values(evaluate("isnull(maybe)", df)) == [False, True, False]
        assert values(evaluate("notnull(maybe)", df)) == [True, False, True]
        assert values(evaluate("isnull(1)", df)) == [False, False, False]
        assert values(evaluate("notnull(1)", df)) == [True, True, True]

    def test_if_else(self, df):
        assert values(evaluate("if_else(a > 1, b, 0)", df)) == [0.0, 20.0, 30.0]

    def test_an_unknown_function_lists_the_real_ones(self, df):
        with pytest.raises(FormulaError, match="not a formula function") as exc:
            evaluate("median(a)", df)
        assert "coalesce" in str(exc.value)

    def test_wrong_argument_count(self, df):
        with pytest.raises(FormulaError, match=r"clip\(\) takes 3 argument"):
            evaluate("clip(a, 1)", df)
        with pytest.raises(FormulaError, match=r"coalesce\(\) takes 2 or more"):
            evaluate("coalesce(a)", df)

    def test_keyword_arguments_are_refused(self, df):
        with pytest.raises(FormulaError, match="positional arguments only"):
            evaluate("round(a, ndigits=1)", df)


class TestRefusals:
    @pytest.mark.parametrize(
        "formula",
        [
            "__import__('os').system('true')",
            "a.__class__",
            "().__class__.__bases__[0]",
            "(lambda: 1)()",
            "a[0]",
            "[a, b]",
            "{'x': a}",
            "a, b",
            "open('/etc/passwd')",
            "eval('1')",
        ],
    )
    def test_nothing_reaches_code(self, df, formula):
        with pytest.raises(FormulaError):
            evaluate(formula, df)

    def test_a_bitwise_operator_suggests_the_word(self, df):
        with pytest.raises(FormulaError, match="write 'and'"):
            evaluate("a > 1 & b > 1", df)

    def test_text_in_arithmetic_is_named(self, df):
        with pytest.raises(FormulaError, match="Column 'status' is not numeric"):
            evaluate("status * 2", df)
        with pytest.raises(FormulaError, match="cannot be used in arithmetic"):
            evaluate("'x' * 2", df)

    def test_ordering_text_against_a_number_is_explained(self, df):
        with pytest.raises(FormulaError, match="cannot be ordered"):
            evaluate("status > 1", df)

    def test_unreadable_formulas(self, df):
        with pytest.raises(FormulaError, match="could not be read"):
            evaluate("a +", df)
        with pytest.raises(FormulaError, match="unclosed quote"):
            evaluate("status == 'open", df)
        with pytest.raises(FormulaError, match="unclosed backtick"):
            evaluate("`clicks-2 * 2", df)
        with pytest.raises(FormulaError, match="empty"):
            evaluate("   ", df)
        with pytest.raises(FormulaError, match="limit"):
            evaluate("a + " * MAX_LENGTH + "a", df)

    def test_deep_nesting_is_refused_cleanly(self, df):
        with pytest.raises(FormulaError):
            evaluate("(" * 400 + "a" + ")" * 400, df)

    def test_a_huge_power_does_not_hang(self, df):
        assert np.isinf(evaluate("10 ** 100000", df).iloc[0])


class TestThroughApplyPatch:
    def _csv(self, tmp_path: Path) -> Path:
        path = tmp_path / "data.csv"
        pd.DataFrame({"a": [1, 2], "b": [10, 20], "price": [50, 80], "cost": [30, 60]}).to_csv(path, index=False)
        return path

    def test_column_math_writes_the_right_answer(self, tmp_path):
        path = self._csv(tmp_path)
        r = apply_patch(str(path), [{"op": "column_math", "formula": "a + b * 2", "target_column": "t"}])
        assert r["success"] is True, r
        assert pd.read_csv(path)["t"].tolist() == [21.0, 42.0]

    def test_add_column_uses_the_same_grammar(self, tmp_path):
        path = self._csv(tmp_path)
        r = apply_patch(str(path), [{"op": "add_column", "name": "margin", "expr": "(price - cost) / price * 100"}])
        assert r["success"] is True, r
        assert pd.read_csv(path)["margin"].tolist() == [40.0, 25.0]

    def test_a_refused_formula_leaves_the_file_as_it_was(self, tmp_path):
        path = self._csv(tmp_path)
        before = path.read_bytes()
        r = apply_patch(str(path), [{"op": "column_math", "formula": "a.__class__", "target_column": "t"}])
        assert r["success"] is False
        assert "not allow" in r["op_errors"][0]["error"]
        assert path.read_bytes() == before
