"""A text column is checked by what it holds: email, phone, IBAN, postal code, card number.

auto_detect_schema said a column was text and stopped there; validate_dataset
flagged nulls, zeros and duplicates and never a malformed email or an IBAN with a
wrong check digit. Now (shared/semantic.py, standard library only):

- auto_detect_schema names the kind when 90% of the sample is one;
- validate_dataset checks every value of an inferred or declared kind and
  reports the invalid ones by row, as check_outliers reports flagged rows;
- run_cleaning_pipeline's normalize_format op gives each valid value one
  spelling and leaves the rest untouched, counted;
- none of it moves the shared quality score, which MCP_Machine_Learning
  computes the same way from the same file.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_basic"), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _med_inspect import auto_detect_schema, validate_dataset  # noqa: E402
from _med_transform import run_cleaning_pipeline  # noqa: E402

from shared.semantic import infer_kind, is_valid, normalize  # noqa: E402

EMAILS = [f"user{i}@example.com" for i in range(9)] + ["broken@"]
IBANS = ["GB82 WEST 1234 5698 7654 32", "DE89 3704 0044 0532 0130 00"] * 4 + [
    "FR14 2004 1010 0505 0001 3M02 606",
    "GB82 WEST 1234 5698 7654 33",
]
PHONES = ["+1 (555) 123-4567", "+44 20 7946 0958", "555-123-4567"] * 3 + ["12"]
CARDS = ["4111 1111 1111 1111", "5500-0000-0000-0004", "378282246310005"] * 3 + ["4111 1111 1111 1112"]
POSTAL = ["12345", "12345-6789", "SW1A 1AA", "K1A 0B1", "90210"] * 2


class TestEachKindHasItsRule:
    @pytest.mark.parametrize(
        ("kind", "good", "bad"),
        [
            ("iban", "GB82 WEST 1234 5698 7654 32", "GB82 WEST 1234 5698 7654 33"),  # check digits
            ("card", "4111 1111 1111 1111", "4111 1111 1111 1112"),  # Luhn
            ("email", "a.b@example.com", "a.b@example"),
            ("phone", "+44 20 7946 0958", "12345678"),  # bare digits are not a phone
            ("postal", "SW1A 1AA", "1234"),
        ],
    )
    def test_good_and_bad(self, kind, good, bad):
        assert is_valid(kind, good) and not is_valid(kind, bad)

    def test_a_non_text_value_is_never_valid(self):
        assert not any(is_valid(k, 12345) for k in ("postal", "card", "phone"))

    def test_ordinary_columns_are_not_claimed(self):
        assert infer_kind([str(10_000_000 + i) for i in range(20)]) == (None, 0.0)  # order numbers
        assert infer_kind(["North", "South", "East"]) == (None, 0.0)
        # Ad_Data.csv's Date column was inferred as phones, live: 8 digits and a separator.
        assert infer_kind([f"2019-10-{d:02d}" for d in range(1, 29)]) == (None, 0.0)
        assert infer_kind([f"{d:02d}.10.2019" for d in range(1, 29)]) == (None, 0.0)
        assert infer_kind([f"{1_000_000 + d}.25" for d in range(20)]) == (None, 0.0)

    @pytest.mark.parametrize(
        ("kind", "value", "canonical"),
        [
            ("email", "Ann@Example.COM", "Ann@example.com"),  # the local part is the owner's
            ("phone", "+1 (555) 123-4567", "+15551234567"),
            ("iban", "gb82 west 1234 5698 7654 32", "GB82WEST12345698765432"),
            ("card", "4111-1111-1111-1111", "4111111111111111"),
            ("postal", " sw1a   1aa ", "SW1A 1AA"),
            ("email", "broken@", "broken@"),  # invalid: as it came
        ],
    )
    def test_one_spelling(self, kind, value, canonical):
        assert normalize(kind, value) == canonical


@pytest.fixture
def people(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
    f = tmp_path / "people.csv"
    pd.DataFrame(
        {
            "email": EMAILS,
            "iban": IBANS,
            "phone": PHONES,
            "card": CARDS,
            "zip": POSTAL,
            "name": [f"person {i}" for i in range(10)],
            "order_no": [str(10_000_000 + i) for i in range(10)],
        }
    ).to_csv(f, index=False)
    return f


class TestTheSchemaNamesTheKind:
    def test_each_kind_column(self, people):
        cols = auto_detect_schema(str(people))["columns"]
        want = {"email": "email", "iban": "iban", "phone": "phone", "card": "card", "zip": "postal"}
        assert {c: cols[c].get("semantic_type") for c in want} == want
        assert cols["email"]["semantic_match_rate"] == 0.9
        assert "semantic_type" not in cols["name"] and "semantic_type" not in cols["order_no"]


class TestValidateReportsTheInvalidValuesByRow:
    def test_inferred_kinds(self, people):
        r = validate_dataset(str(people))
        checks = r["semantic_checks"]
        assert {c: (checks[c]["invalid"], checks[c]["failing_rows"]) for c in checks} == {
            "email": (1, [9]),
            "iban": (1, [9]),
            "phone": (1, [9]),
            "card": (1, [9]),
            "zip": (0, []),
        }
        assert checks["iban"]["examples"] == ["GB82 WEST 1234 5698 7654 33"]
        assert all(checks[c]["found_by"] == "inferred" for c in checks)
        issue = next(i for i in r["issues"] if i.get("column") == "email" and "invalid" in i["issue"])
        assert issue == {
            "severity": "error",
            "column": "email",
            "issue": "1 invalid email values (10.0%)",
            "failing_rows": [9],
        }

    def test_a_declared_kind_is_checked_however_bad_the_column(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
        f = tmp_path / "half.csv"
        pd.DataFrame({"contact": ["a@x.io", "nope", "b@y.io", "also nope"]}).to_csv(f, index=False)
        assert "contact" not in validate_dataset(str(f))["semantic_checks"], "50% is no kind to infer"
        check = validate_dataset(str(f), semantic_types={"contact": "email"})["semantic_checks"]["contact"]
        assert (check["found_by"], check["invalid"], check["failing_rows"]) == ("declared", 2, [1, 3])

    def test_an_unknown_kind_or_column_is_refused_by_name(self, people):
        r = validate_dataset(str(people), semantic_types={"email": "mail", "nope": "email"})
        assert r["success"] is False
        assert "'mail' is not a kind" in r["error"] and "'nope' is not a column" in r["error"]
        assert "email, iban, card, phone, postal" in r["hint"]


class TestNormalizeFormat:
    def test_valid_values_get_one_spelling_and_invalid_ones_are_left(self, people):
        out = people.parent / "out.csv"
        ops = [
            {"op": "normalize_format", "column": "phone"},
            {"op": "normalize_format", "column": "iban", "kind": "iban"},
        ]
        r = run_cleaning_pipeline(str(people), ops=ops, output_path=str(out))
        assert r["success"] is True, r
        df = pd.read_csv(out, dtype=str)
        assert df["phone"].tolist()[:3] == ["+15551234567", "+442079460958", "5551234567"]
        assert df.loc[9, "phone"] == "12" and df.loc[9, "iban"] == "GB82 WEST 1234 5698 7654 33"
        assert df.loc[0, "iban"] == "GB82WEST12345698765432"

    def test_the_op_reports_what_it_left(self, people):
        # 9 valid cards; the 3 unseparated Amex numbers were already one spelling
        r = run_cleaning_pipeline(
            str(people), ops=[{"op": "normalize_format", "column": "card"}], output_path=str(people.parent / "o.csv")
        )
        assert r["summary"] == [
            {
                "op": "normalize_format",
                "column": "card",
                "kind": "card",
                "changed": 6,
                "left_invalid": 1,
                "invalid_rows": [9],
            }
        ]

    def test_a_column_of_no_kind_needs_one(self, people):
        r = run_cleaning_pipeline(
            str(people), ops=[{"op": "normalize_format", "column": "name"}], output_path=str(people.parent / "o.csv")
        )
        assert r["success"] is False and "pass kind" in str(r)

    def test_an_unknown_kind_is_refused_before_anything_runs(self, people):
        r = run_cleaning_pipeline(
            str(people),
            ops=[{"op": "normalize_format", "column": "phone", "kind": "fax"}],
            output_path=str(people.parent / "o.csv"),
        )
        assert r["success"] is False and "invalid kind 'fax'" in str(r)


def test_the_shared_quality_score_does_not_move(tmp_path, monkeypatch):
    """Invalid emails are validate_dataset's finding, not a quality-score input:
    the score is shared with MCP_Machine_Learning and must stay the same there."""
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
    from servers.data_advanced._adv_eda import run_eda

    scores = []
    for label, last in (("good", "user9@example.com"), ("bad", "broken@")):
        f = tmp_path / f"{label}.csv"
        pd.DataFrame({"email": EMAILS[:9] + [last], "n": range(10)}).to_csv(f, index=False)
        r = run_eda(str(f), output_path=str(tmp_path / f"{label}.html"), open_after=False)
        scores.append(r["quality_breakdown"]["components"])
    assert scores[0] == scores[1]
