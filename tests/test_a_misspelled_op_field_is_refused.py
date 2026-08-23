"""apply_patch dropped every op field it did not recognise, without a word.

`validate_ops` checked required fields and enumerated values. Any other key was
discarded in silence, and `apply_patch` takes its ops as a `list[dict]` -- one
level below where `strict_args` can see -- so nothing else was going to catch
it. Round 11 measured the cost on a three-row frame, changing one character:

    fill_nulls column=a strategy=mean fill_zeros=True  ->  a = 1.0, 1.0, 1.0
    fill_nulls column=a strategy=mean fill_zero=True   ->  a = 1.0, 0.0, 0.5

Both `success: true`. The flag decides whether zeros count as missing, so the
typo does not fail; it writes different numbers into the caller's dataset and
reports the same thing either way.

Turning the check on immediately found what it was built to find: five ops
whose catalog entry advertised a field the handler never read.

    drop_duplicates   keep: first|last|False   read only subset
    cap_outliers      threshold                hardcoded 1.5 (iqr) and 3 (std)
    label_encode      new_column               wrote over the source column
    date_diff         start_col/end_col/new_col   reads date_col_a/_b/new_column
    concat_file       add_source               reads add_source_column

`list_patch_ops` is how a caller learns this vocabulary, so each of these was
the documentation telling someone to pass a field the code ignored. Three are
now implemented, one is aliased, and date_diff's is deliberately not: the
handler computes `date_col_a` minus `date_col_b`, and guessing which half of a
pair is the "start" silently flips the sign of every result -- so its catalog
entry was corrected to name the fields the handler reads.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_basic")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from servers.data_basic.engine import _OP_CATALOG, apply_patch  # noqa: E402
from shared import patch_validator as pv  # noqa: E402
from shared.patch_validator import known_fields  # noqa: E402


@pytest.fixture
def frame(tmp_path) -> Path:
    p = tmp_path / "t.csv"
    p.write_text("a,b\n1,\n0,5\n,7\n", encoding="utf-8")
    return p


class TestAMisspelledFieldIsRefused:
    def test_the_typo_is_refused(self, frame):
        r = apply_patch(str(frame), [{"op": "fill_nulls", "column": "a", "strategy": "mean", "fill_zero": True}])
        assert r["success"] is False, r

    def test_the_refusal_names_the_field_that_was_meant(self, frame):
        r = apply_patch(str(frame), [{"op": "fill_nulls", "column": "a", "strategy": "mean", "fill_zero": True}])
        assert "fill_zero" in r["error"], r["error"]
        assert "fill_zeros" in r["error"], r["error"]

    def test_the_data_is_left_alone(self, frame):
        before = frame.read_text(encoding="utf-8")
        apply_patch(str(frame), [{"op": "fill_nulls", "column": "a", "strategy": "mean", "fill_zero": True}])
        assert frame.read_text(encoding="utf-8") == before

    def test_the_correct_spelling_still_treats_zeros_as_missing(self, frame):
        r = apply_patch(str(frame), [{"op": "fill_nulls", "column": "a", "strategy": "mean", "fill_zeros": True}])
        assert r["success"] is True, r.get("error")
        assert pd.read_csv(frame)["a"].tolist() == [1.0, 1.0, 1.0]

    def test_without_the_flag_zeros_are_kept(self, frame):
        r = apply_patch(str(frame), [{"op": "fill_nulls", "column": "a", "strategy": "mean"}])
        assert r["success"] is True, r.get("error")
        assert pd.read_csv(frame)["a"].tolist() == [1.0, 0.0, 0.5]


class TestKeepIsHonoured:
    """drop_duplicates advertised keep: first|last|False and read only subset."""

    @pytest.fixture
    def dupes(self, tmp_path) -> Path:
        p = tmp_path / "d.csv"
        p.write_text("k,v\nx,1\nx,2\ny,3\n", encoding="utf-8")
        return p

    def test_keep_last_keeps_the_last(self, dupes):
        r = apply_patch(str(dupes), [{"op": "drop_duplicates", "subset": ["k"], "keep": "last"}])
        assert r["success"] is True, r.get("error")
        assert pd.read_csv(dupes)["v"].tolist() == [2, 3]

    def test_keep_first_is_still_the_default(self, dupes):
        r = apply_patch(str(dupes), [{"op": "drop_duplicates", "subset": ["k"]}])
        assert r["success"] is True, r.get("error")
        assert pd.read_csv(dupes)["v"].tolist() == [1, 3]

    def test_keep_false_drops_every_copy(self, dupes):
        r = apply_patch(str(dupes), [{"op": "drop_duplicates", "subset": ["k"], "keep": False}])
        assert r["success"] is True, r.get("error")
        assert pd.read_csv(dupes)["v"].tolist() == [3]

    def test_an_invalid_keep_is_refused(self, dupes):
        r = apply_patch(str(dupes), [{"op": "drop_duplicates", "subset": ["k"], "keep": "middle"}])
        assert r["success"] is False, r


class TestThresholdIsHonoured:
    """cap_outliers advertised threshold and hardcoded 1.5 / 3."""

    @pytest.fixture
    def spread(self, tmp_path) -> Path:
        p = tmp_path / "s.csv"
        rows = [str(v) for v in [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 500]]
        p.write_text("v\n" + "\n".join(rows) + "\n", encoding="utf-8")
        return p

    def test_a_tighter_threshold_caps_harder(self, spread, tmp_path):
        wide = pd.read_csv(spread)
        loose = tmp_path / "loose.csv"
        wide.to_csv(loose, index=False)

        apply_patch(str(spread), [{"op": "cap_outliers", "column": "v", "method": "iqr", "threshold": 0.5}])
        apply_patch(str(loose), [{"op": "cap_outliers", "column": "v", "method": "iqr", "threshold": 3.0}])
        assert pd.read_csv(spread)["v"].max() < pd.read_csv(loose)["v"].max()

    def test_the_default_is_unchanged(self, spread, tmp_path):
        explicit = tmp_path / "e.csv"
        pd.read_csv(spread).to_csv(explicit, index=False)
        apply_patch(str(spread), [{"op": "cap_outliers", "column": "v", "method": "iqr"}])
        apply_patch(str(explicit), [{"op": "cap_outliers", "column": "v", "method": "iqr", "threshold": 1.5}])
        assert pd.read_csv(spread)["v"].tolist() == pd.read_csv(explicit)["v"].tolist()

    def test_std_uses_it_as_a_sigma_count(self, spread, tmp_path):
        loose = tmp_path / "l.csv"
        pd.read_csv(spread).to_csv(loose, index=False)
        apply_patch(str(spread), [{"op": "cap_outliers", "column": "v", "method": "std", "threshold": 0.5}])
        apply_patch(str(loose), [{"op": "cap_outliers", "column": "v", "method": "std", "threshold": 3.0}])
        assert pd.read_csv(spread)["v"].max() < pd.read_csv(loose)["v"].max()

    def test_a_zero_threshold_is_refused(self, spread):
        r = apply_patch(str(spread), [{"op": "cap_outliers", "column": "v", "threshold": 0}])
        assert r["success"] is False, r


class TestLabelEncodeCanKeepTheOriginal:
    """label_encode advertised new_column and wrote over its source."""

    @pytest.fixture
    def cats(self, tmp_path) -> Path:
        p = tmp_path / "c.csv"
        p.write_text("kind\nred\nblue\nred\n", encoding="utf-8")
        return p

    def test_the_codes_go_to_the_new_column(self, cats):
        r = apply_patch(str(cats), [{"op": "label_encode", "column": "kind", "new_column": "kind_id"}])
        assert r["success"] is True, r.get("error")
        df = pd.read_csv(cats)
        assert df["kind_id"].tolist() == [1, 0, 1]

    def test_the_original_survives(self, cats):
        apply_patch(str(cats), [{"op": "label_encode", "column": "kind", "new_column": "kind_id"}])
        assert pd.read_csv(cats)["kind"].tolist() == ["red", "blue", "red"]

    def test_without_it_the_column_is_still_replaced(self, cats):
        r = apply_patch(str(cats), [{"op": "label_encode", "column": "kind"}])
        assert r["success"] is True, r.get("error")
        assert pd.read_csv(cats)["kind"].tolist() == [1, 0, 1]


class TestTheCatalogSpellingsAreAccepted:
    def test_new_col_is_accepted_for_new_column(self, tmp_path):
        p = tmp_path / "n.csv"
        p.write_text("v\n1\n2\n3\n", encoding="utf-8")
        r = apply_patch(str(p), [{"op": "lag", "column": "v", "new_col": "v_lag", "periods": 1}])
        assert r["success"] is True, r.get("error")
        assert "v_lag" in pd.read_csv(p).columns

    def test_add_source_is_accepted_for_add_source_column(self, tmp_path):
        a = tmp_path / "a.csv"
        b = tmp_path / "b.csv"
        a.write_text("v\n1\n", encoding="utf-8")
        b.write_text("v\n2\n", encoding="utf-8")
        r = apply_patch(str(a), [{"op": "concat_file", "file_path": str(b), "add_source": True}])
        assert r["success"] is True, r.get("error")

    def test_date_diffs_pair_is_not_guessed_at(self):
        # Aliasing start_col/end_col onto date_col_a/date_col_b would flip the
        # sign of every result if the guess were backwards, so the catalog was
        # corrected instead and the old spelling is refused, loudly.
        assert "start_col" not in known_fields("date_diff")
        assert "date_col_a" in known_fields("date_diff")


class TestTheFieldTableMatchesTheHandlers:
    """The table and the handlers must agree in both directions."""

    @staticmethod
    def _handler_reads() -> dict[str, set[str]]:
        src = (ROOT / "servers" / "data_basic" / "_patch_ops.py").read_text(encoding="utf-8")
        out: dict[str, set[str]] = {}
        for fn in ast.walk(ast.parse(src)):
            if not (isinstance(fn, ast.FunctionDef) and fn.name.startswith("_op_")):
                continue
            keys: set[str] = set()
            for n in ast.walk(fn):
                if (
                    isinstance(n, ast.Call)
                    and isinstance(n.func, ast.Attribute)
                    and n.func.attr == "get"
                    and isinstance(n.func.value, ast.Name)
                    and n.func.value.id == "op"
                    and n.args
                    and isinstance(n.args[0], ast.Constant)
                ):
                    keys.add(n.args[0].value)
                elif (
                    isinstance(n, ast.Subscript)
                    and isinstance(n.value, ast.Name)
                    and n.value.id == "op"
                    and isinstance(n.slice, ast.Constant)
                ):
                    keys.add(n.slice.value)
            out[fn.name[4:]] = keys
        return out

    def test_no_handler_reads_a_field_the_validator_refuses(self):
        for op, keys in self._handler_reads().items():
            if op not in pv.VALID_OPS:
                continue
            for key in keys:
                assert key in known_fields(op), f"_op_{op} reads {key!r}, which validate_ops refuses"

    def test_the_table_lists_no_field_no_handler_reads(self):
        reads = self._handler_reads()
        for op, fields in pv._OP_FIELDS.items():
            for field in fields:
                assert field in reads.get(op, set()), f"_OP_FIELDS lists {op}.{field}, unread by the handler"

    def test_every_valid_op_has_an_entry(self):
        assert set(pv._OP_FIELDS) == set(pv.VALID_OPS)


class TestTheCatalogAdvertisesNothingItCannotAccept:
    """The check that found all five: read list_patch_ops back as a caller would."""

    @staticmethod
    def _advertised(params: str) -> set[str]:
        # Drop parenthesised prose first -- it explains defaults, it does not
        # name fields.
        bare = re.sub(r"\([^)]*\)", "", params)
        out: set[str] = set()
        for chunk in re.split(r",(?![^{}\[\]]*[}\]])", bare):
            for name in re.split(r"[/|]", chunk.split(":")[0]):
                m = re.match(r"\s*([a-z_][a-z_0-9]*)\s*$", name)
                if m:
                    out.add(m.group(1))
        return out

    def test_every_advertised_field_is_accepted(self):
        offenders = []
        for entries in _OP_CATALOG.values():
            for entry in entries:
                name = entry["op"]
                for field in self._advertised(entry.get("params", "")):
                    if field not in known_fields(name):
                        offenders.append(f"{name}.{field}")
        assert not offenders, f"list_patch_ops advertises fields apply_patch refuses: {offenders}"
