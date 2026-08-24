"""Success is not the same as having done something.

Three tools reported a clean run over work they had not been able to do:

  smart_impute           "filled" an all-null column with the median of no
                         numbers -- NaN -- and counted it under columns_imputed
  run_cleaning_pipeline  the same fill under `applied: 1`, beside a hint asking
                         the caller to verify the changes
  get_output_path        corrected an output_path of `outliers.csv` to `.html`,
                         which is right, and said nothing about it anywhere

None of them was wrong about a number. Each was wrong about what its numbers
meant, in the direction that reads as "done".
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for _p in (
    str(ROOT),
    str(ROOT / "servers" / "data_medium"),
    str(ROOT / "servers" / "data_basic"),
    str(ROOT / "servers" / "data_advanced"),
):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _adv_charts import generate_distribution_plot  # noqa: E402
from _med_inspect import check_outliers  # noqa: E402
from _med_transform import run_cleaning_pipeline, smart_impute  # noqa: E402

# Fully qualified: the bare name `engine` resolves to data_advanced's, which
# this file also puts on sys.path for _adv_charts.
from servers.data_basic.engine import apply_patch  # noqa: E402
from shared.html_layout import extension_note  # noqa: E402


def _all_null(tmp_path) -> Path:
    f = tmp_path / "all_null.csv"
    f.write_text("name,spend\nWest,\nEast,\n")
    return f


def _mixed(tmp_path) -> Path:
    f = tmp_path / "mixed.csv"
    f.write_text("name,spend\nWest,10\nEast,\nNorth,30\n")
    return f


# --- smart_impute -----------------------------------------------------------


def test_smart_impute_does_not_count_a_column_it_could_not_fill(tmp_path):
    r = smart_impute(str(_all_null(tmp_path)), output_path=str(tmp_path / "out.csv"), open_after=False)
    assert r["success"] is True
    assert r["columns_imputed"] == 0
    assert r["imputed"] == []
    assert r["columns_skipped"] == 1
    skipped = r["skipped"][0]
    assert skipped["column"] == "spend"
    assert skipped["skipped"] is True
    assert "entirely null" in skipped["reason"] or "are null" in skipped["reason"]
    # fill_value must be null, not the string "nan".
    assert skipped["fill_value"] is None
    assert "Nothing was filled" in r["hint"]


def test_smart_impute_still_fills_what_it_can(tmp_path):
    out = tmp_path / "out.csv"
    r = smart_impute(str(_mixed(tmp_path)), output_path=str(out), open_after=False)
    assert r["columns_imputed"] == 1
    assert r["columns_skipped"] == 0
    assert r["imputed"][0]["fill_value"] == "20.0"
    assert "20.0" in out.read_text()


def test_smart_impute_dry_run_separates_the_two_lists(tmp_path):
    f = tmp_path / "both.csv"
    f.write_text("a,b\n1,\n,\n3,\n")
    r = smart_impute(str(f), dry_run=True, open_after=False)
    assert r["columns_to_impute"] == 1
    assert r["would_change"][0]["column"] == "a"
    assert r["columns_skipped"] == 1
    assert r["skipped"][0]["column"] == "b"


# --- run_cleaning_pipeline --------------------------------------------------


def test_a_pipeline_op_that_changed_nothing_is_listed(tmp_path):
    r = run_cleaning_pipeline(
        str(_all_null(tmp_path)),
        ops=[{"op": "fill_nulls", "column": "spend", "strategy": "median"}],
        output_path=str(tmp_path / "cleaned.csv"),
    )
    assert r["success"] is True
    assert r["applied"] == 1  # it did run
    assert len(r["ops_with_no_effect"]) == 1
    note = r["ops_with_no_effect"][0]["note"]
    assert "entirely null" in note
    assert "unchanged" in note
    assert "without changing anything" in r["hint"]


def test_a_pipeline_that_worked_says_nothing_extra(tmp_path):
    r = run_cleaning_pipeline(
        str(_mixed(tmp_path)),
        ops=[{"op": "fill_nulls", "column": "spend", "strategy": "median"}],
        output_path=str(tmp_path / "cleaned.csv"),
    )
    assert r["ops_with_no_effect"] == []
    assert r["summary"][0]["filled"] == 1
    assert "verify the changes" in r["hint"]


# --- the corrected output extension -----------------------------------------


def test_extension_note_only_speaks_when_the_request_was_changed(tmp_path):
    assert extension_note("", tmp_path / "x.html") == ""
    assert extension_note("/tmp/x.html", Path("/tmp/x.html")) == ""
    assert extension_note("/tmp/x.HTML", Path("/tmp/x.html")) == ""
    note = extension_note("/tmp/x.csv", Path("/tmp/x.html"))
    assert ".csv" in note
    assert "HTML" in note
    assert "x.html" in note


def test_a_chart_tool_says_it_wrote_html_instead(tmp_path):
    src = tmp_path / "data.csv"
    src.write_text("spend\n1\n2\n3\n4\n5\n")
    r = check_outliers(str(src), output_path=str(tmp_path / "wanted.csv"), open_after=False)
    assert r["success"] is True
    assert r["output_name"] == "wanted.html"
    warnings = [p for p in r["progress"] if p["status"] == "warn"]
    assert any("extension" in p["message"].lower() for p in warnings), warnings


def test_a_chart_tool_stays_quiet_when_the_extension_matched(tmp_path):
    src = tmp_path / "data.csv"
    src.write_text("spend\n1\n2\n3\n4\n5\n")
    r = check_outliers(str(src), output_path=str(tmp_path / "wanted.html"), open_after=False)
    assert not any("extension" in p["message"].lower() for p in r["progress"])


# --- a plot that is accurate and unreadable ---------------------------------


def test_a_one_row_distribution_plot_says_what_it_drew(tmp_path):
    src = tmp_path / "one.csv"
    src.write_text("spend,clicks\n120,4\n")
    r = generate_distribution_plot(str(src), output_path=str(tmp_path / "d.html"), open_after=False)
    assert r["success"] is True
    assert r["values_plotted"] == {"spend": 1, "clicks": 1}
    assert sorted(r["columns_too_few_values"]) == ["clicks", "spend"]
    assert "not a distribution" in r["hint"]


def test_a_real_distribution_plot_carries_no_warning(tmp_path):
    src = tmp_path / "many.csv"
    src.write_text("spend\n" + "\n".join(str(i) for i in range(1, 40)) + "\n")
    r = generate_distribution_plot(str(src), output_path=str(tmp_path / "d.html"), open_after=False)
    assert r["columns_too_few_values"] == []
    assert "not a distribution" not in r.get("hint", "")


# --- normalize: a column it cannot transform must survive it -----------------


def test_normalize_does_not_zero_a_column_it_cannot_scale(tmp_path):
    """min-max over a constant column is 0/0, and this op rewrites the file.

    It substituted 0.0, so `spends` holding 7, 7, 7 came back 0, 0, 0 under
    success:true -- three real values replaced by a placeholder for an
    undefined result. A single-valued column is not rare: a filtered slice, a
    one-campaign export, a flag that happens not to vary.
    """
    f = tmp_path / "flat.csv"
    f.write_text("name,spends\nA,7\nB,7\nC,7\n")
    r = apply_patch(str(f), ops=[{"op": "normalize", "column": "spends", "method": "minmax"}])

    assert r["success"] is True
    assert f.read_text() == "name,spends\nA,7\nB,7\nC,7\n"
    assert r["changed_file"] is False
    entry = r["results"][0]
    assert entry["skipped"] is True
    assert "no spread" in entry["note"]
    assert len(r["ops_with_no_effect"]) == 1
    assert "without changing anything" in r["hint"]


def test_normalize_zscore_survives_a_single_row(tmp_path):
    """At n=1 the sample sd is NaN, not 0 -- `== 0` missed it and wrote NaN."""
    f = tmp_path / "one.csv"
    f.write_text("name,spends\nWest,7\n")
    r = apply_patch(str(f), ops=[{"op": "normalize", "column": "spends", "method": "zscore"}])

    assert r["success"] is True
    assert f.read_text() == "name,spends\nWest,7\n"
    assert r["results"][0]["skipped"] is True


def test_normalize_still_scales_a_column_that_varies(tmp_path):
    f = tmp_path / "vary.csv"
    f.write_text("name,spends\nA,1\nB,5\nC,9\n")
    r = apply_patch(str(f), ops=[{"op": "normalize", "column": "spends", "method": "minmax"}])

    assert r["success"] is True
    assert r["changed_file"] is True
    assert r["ops_with_no_effect"] == []
    assert "skipped" not in r["results"][0]
    assert f.read_text().splitlines()[1:] == ["A,0.0", "B,0.5", "C,1.0"]
