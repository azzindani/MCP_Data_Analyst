"""Every op list_patch_ops advertises is an op both tools will actually run.

`list_patch_ops` printed 52 ops. `apply_patch` ran 52. `run_cleaning_pipeline`
built its own handler table over eight of them and refused the rest:

    run_cleaning_pipeline(f, ops=[{"op": "normalize", "column": "spend"}])
    -> Unknown op(s): ['normalize']
       Valid ops: add_column, cap_outliers, cast_column, clean_text,
                  drop_column, drop_duplicates, fill_nulls, replace_values

An honest error naming the wrong eight. The catalog is what misleads: a caller
who reads it and writes `normalize` is told the op does not exist, when what is
true is that this one tool never registered a handler for it. Nothing failed --
the second table was simply written once and never grown, and the catalog kept
advertising the first.

So the registry now lives beside the handlers in _patch_ops and both tools use
it, and these tests hold the four vocabularies together: the handler table, the
validator's VALID_OPS, the printed catalog, and what the pipeline dispatches on.
Adding the 53rd op fails here until all four know about it.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_basic"), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _med_transform import run_cleaning_pipeline  # noqa: E402
from _patch_ops import OP_HANDLERS  # noqa: E402

from servers.data_basic.engine import _OP_CATALOG, _OP_HANDLERS, apply_patch, list_patch_ops  # noqa: E402
from shared.patch_validator import _OP_FIELDS, VALID_OPS  # noqa: E402

CATALOG_OPS = {entry["op"] for ops in _OP_CATALOG.values() for entry in ops}


# --- the four vocabularies ---------------------------------------------------


def test_the_catalog_advertises_exactly_what_has_a_handler():
    assert CATALOG_OPS == set(OP_HANDLERS)


def test_the_validator_accepts_exactly_what_has_a_handler():
    assert VALID_OPS == set(OP_HANDLERS)


def test_every_op_declares_the_fields_it_reads():
    """_OP_FIELDS is what refuses a misspelled arg; an op absent from it takes
    anything and drops what it does not read."""
    assert set(_OP_FIELDS) == set(OP_HANDLERS)


def test_both_tools_dispatch_on_the_same_table():
    """Not merely equal -- the same object, so they cannot drift."""
    assert _OP_HANDLERS is OP_HANDLERS


def test_the_catalog_is_not_quietly_shrinking():
    """A floor, so deleting an op is a deliberate act and not a merge artefact."""
    assert list_patch_ops()["total_ops"] == len(OP_HANDLERS) >= 52


# --- the three ops named in the defect --------------------------------------


@pytest.fixture
def csv(tmp_path):
    f = tmp_path / "in.csv"
    rows = "\n".join(f"West,{i * 10}" for i in range(1, 7))
    f.write_text(f"region,spend\n{rows}\n")
    return f


@pytest.mark.parametrize(
    ("op", "extra"),
    [
        ("normalize", {"column": "spend", "method": "minmax"}),
        ("round_values", {"column": "spend", "decimals": 2}),
        ("rolling_agg", {"column": "spend", "window": 2, "agg": "mean", "new_column": "roll"}),
    ],
)
def test_an_op_the_catalog_lists_runs_in_the_pipeline(tmp_path, csv, op, extra):
    out = tmp_path / "out.csv"
    r = run_cleaning_pipeline(str(csv), ops=[{"op": op, **extra}], output_path=str(out))
    assert r["success"] is True, r.get("error")
    assert r["applied"] == 1
    assert out.exists() and out.read_text().strip()


def test_a_genuinely_unknown_op_is_still_refused(tmp_path, csv):
    r = run_cleaning_pipeline(
        str(csv), ops=[{"op": "teleport", "column": "spend"}], output_path=str(tmp_path / "o.csv")
    )
    assert r["success"] is False
    assert "teleport" in r["error"]
    assert r["applied"] == 0


# --- both tools write the same bytes ----------------------------------------


def test_the_two_tools_agree_on_every_op(tmp_path):
    """Read the files, not the responses: the point of one table is one result."""
    src = tmp_path / "src.csv"
    rows = [f"West {i % 3},{i * 10},{i * 3},2024-01-{i:02d},2024-02-{i:02d},a-{i}" for i in range(1, 13)]
    src.write_text("region,spend,clicks,d1,d2,code\n" + "\n".join(rows) + "\n")
    other = tmp_path / "other.csv"
    other.write_text(src.read_text())

    ops = {
        "drop_column": {"columns": ["code"]},
        "clean_text": {"scope": "values", "operations": ["strip"]},
        "cast_column": {"column": "spend", "dtype": "float"},
        "replace_values": {"column": "region", "mapping": {"West 1": "W1"}},
        "add_column": {"name": "cpc", "expr": "spend / clicks"},
        "cap_outliers": {"column": "spend", "method": "iqr"},
        "fill_nulls": {"column": "spend", "strategy": "mean"},
        "drop_duplicates": {"subset": ["region"], "keep": "first"},
        "normalize": {"column": "spend", "method": "minmax"},
        "label_encode": {"column": "region", "new_column": "region_code"},
        "extract_regex": {"column": "code", "pattern": r"(\d+)", "new_column": "n"},
        "date_diff": {"date_col_a": "d2", "date_col_b": "d1", "new_column": "gap", "unit": "days"},
        "rank_column": {"column": "spend", "new_column": "rk", "method": "dense"},
        "sort": {"by": ["spend"], "ascending": [False]},
        "filter_isin": {"column": "region", "values": ["West 1"]},
        "filter_not_isin": {"column": "region", "values": ["West 1"]},
        "filter_between": {"column": "spend", "min": 20, "max": 100},
        "filter_date_range": {"column": "d1", "start": "2024-01-03", "end": "2024-01-09"},
        "filter_regex": {"column": "code", "pattern": "a-1"},
        "filter_quantile": {"column": "spend", "min_q": 0.1, "max_q": 0.9},
        "filter_top_n": {"column": "spend", "n": 3, "keep": "top"},
        "dedup_subset": {"columns": ["region"], "keep": "last"},
        "log_transform": {"column": "spend", "method": "log1p", "new_column": "lg"},
        "sqrt_transform": {"column": "spend", "new_column": "sq", "safe": True},
        "boxcox_transform": {"column": "spend", "new_column": "bc"},
        "yeojohnson_transform": {"column": "spend", "new_column": "yj"},
        "robust_scale": {"column": "spend", "new_column": "rs"},
        "winsorize": {"column": "spend", "lower_q": 0.05, "upper_q": 0.95},
        "bin_column": {"column": "spend", "bins": 3, "new_column": "bin"},
        "qbin_column": {"column": "spend", "q": 3, "new_column": "qb"},
        "clip_values": {"column": "spend", "min": 20, "max": 100},
        "round_values": {"column": "spend", "decimals": 1},
        "abs_values": {"column": "spend", "new_column": "ab"},
        "ordinal_encode": {"column": "region", "order": ["West 0", "West 1", "West 2"], "new_column": "oe"},
        "binary_encode": {"column": "spend", "threshold": 50, "new_column": "be"},
        "frequency_encode": {"column": "region", "new_column": "fe"},
        "lag": {"column": "spend", "periods": 1, "new_column": "lg1"},
        "lead": {"column": "spend", "periods": 1, "new_column": "ld1"},
        "diff": {"column": "spend", "periods": 1, "new_column": "df1"},
        "pct_change": {"column": "spend", "periods": 1, "new_column": "pc"},
        "rolling_agg": {"column": "spend", "window": 3, "agg": "mean", "new_column": "rl"},
        "ewm": {"column": "spend", "span": 3, "new_column": "ew"},
        "cumulative": {"column": "spend", "agg": "sum", "new_column": "cs"},
        "group_transform": {"group_by": ["region"], "column": "spend", "agg": "mean", "new_column": "gm"},
        "column_math": {"formula": "spend + clicks", "target_column": "tot"},
        "conditional_assign": {
            "new_column": "band",
            "conditions": [{"column": "spend", "op": "gt", "value": 50, "label": "hi"}],
            "default": "lo",
        },
        "split_column": {"column": "code", "delimiter": "-", "new_columns": ["p", "q"]},
        "combine_columns": {"columns": ["region", "code"], "delimiter": "|", "new_column": "rc"},
        "regex_replace": {"column": "code", "pattern": "a", "replacement": "z"},
        "str_slice": {"column": "code", "start": 0, "end": 1, "new_column": "sl"},
        "concat_file": {"file_path": str(other), "direction": "rows"},
        "melt": {"id_vars": ["region"], "value_vars": ["spend"]},
    }
    # The registry decides the work list, so a new op cannot be added without
    # being exercised here.
    assert set(ops) == set(OP_HANDLERS), sorted(set(OP_HANDLERS) ^ set(ops))

    failures = []
    for name, params in ops.items():
        piped = tmp_path / f"pipe_{name}.csv"
        r = run_cleaning_pipeline(str(src), ops=[{"op": name, **dict(params)}], output_path=str(piped))
        if not r["success"]:
            failures.append(f"{name}: pipeline refused -- {r.get('error')}")
            continue

        patched = tmp_path / f"patch_{name}.csv"
        shutil.copy(src, patched)
        p = apply_patch(str(patched), ops=[{"op": name, **dict(params)}])
        if not p["success"]:
            failures.append(f"{name}: apply_patch refused -- {p.get('error')}")
            continue

        if piped.read_text() != patched.read_text():
            failures.append(f"{name}: the two tools wrote different files")

    assert not failures, "\n".join(failures)
