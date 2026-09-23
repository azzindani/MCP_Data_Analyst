"""What a tool tells a caller matches what the tool does.

Four sweep findings, each a gap between the words and the behaviour:

- list_patch_ops advertised `fill_nulls strategy: ...|value`, and apply_patch
  refused `value` ("There is no literal-value fill"): a caller who read the
  catalogue and used it was refused. It fills with a literal now.
- run_cleaning_pipeline refused an unknown op without naming the ops it runs,
  and sent the caller to list_patch_ops() -- a tool on another endpoint.
- run_eda(mode='minimal', output_path=...) wrote nothing and answered
  output_path '' with no word on why.
- generate_multi_chart(chart_type='multi_line') titled its chart
  "Multi-Multi Line".
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (
    str(ROOT),
    str(ROOT / "servers" / "data_basic"),
    str(ROOT / "servers" / "data_medium"),
    str(ROOT / "servers" / "data_advanced"),
):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import _adv_eda  # noqa: E402
import _med_transform  # noqa: E402

from servers.data_advanced.engine import generate_multi_chart  # noqa: E402
from servers.data_basic.engine import apply_patch, list_patch_ops  # noqa: E402


@pytest.fixture
def csv(tmp_path, monkeypatch) -> str:
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    frame = pd.DataFrame(
        {
            "region": ["north", None, "south", None, "east", "west"],
            "units": [1.0, None, 3.0, 4.0, None, 6.0],
            "price": [9.0, 8.0, 7.0, 6.0, 5.0, 4.0],
            "day": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-06"],
        }
    )
    path = tmp_path / "t.csv"
    frame.to_csv(path, index=False)
    return str(path)


class TestFillNullsWithAValue:
    def test_the_catalogue_names_it(self):
        entries = [e for group in list_patch_ops()["ops"].values() for e in group]
        entry = next(e for e in entries if e["op"] == "fill_nulls")
        assert "value" in entry["params"]

    @pytest.mark.parametrize(("column", "value"), [("units", 0), ("region", "unknown")])
    def test_it_fills_with_the_literal(self, csv, column, value):
        result = apply_patch(csv, [{"op": "fill_nulls", "column": column, "strategy": "value", "value": value}])
        assert result["success"] is True, result
        written = pd.read_csv(csv)[column]
        assert int(written.isna().sum()) == 0
        assert (written == value).sum() == 2

    def test_a_value_strategy_without_a_value_is_refused_by_name(self, csv):
        result = apply_patch(csv, [{"op": "fill_nulls", "column": "units", "strategy": "value"}])
        assert result["success"] is False
        assert "'value'" in str(result)


class TestAnUnknownCleaningOpNamesTheVocabulary:
    def test_the_ops_are_in_the_answer(self, csv):
        result = _med_transform.run_cleaning_pipeline(csv, [{"op": "fill_null", "column": "units"}])
        assert result["success"] is False and result["op"] == "run_cleaning_pipeline"
        assert "fill_nulls" in result["hint"] and "drop_duplicates" in result["hint"]
        assert "Did you mean 'fill_nulls'" in result["hint"]

    def test_it_does_not_send_the_caller_to_another_endpoint(self, csv):
        result = _med_transform.run_cleaning_pipeline(csv, [{"op": "zzz_not_an_op"}])
        assert "list_patch_ops" not in result["hint"]


class TestADroppedPageIsSaid:
    def test_minimal_with_an_output_path_says_it_wrote_nothing(self, csv, tmp_path):
        target = tmp_path / "eda.html"
        result = _adv_eda.run_eda(csv, mode="minimal", output_path=str(target))
        assert result["success"] is True and not target.exists()
        assert "was not written" in result["output_note"]

    def test_following_the_note_writes_it(self, csv, tmp_path):
        target = tmp_path / "eda.html"
        result = _adv_eda.run_eda(csv, mode="minimal", include={"html": True}, output_path=str(target))
        assert result["success"] is True and target.exists() and "output_note" not in result

    def test_no_output_path_no_note(self, csv):
        assert "output_note" not in _adv_eda.run_eda(csv, mode="minimal")


class TestAMultiChartTitleSaysMultiOnce:
    @pytest.mark.parametrize(
        ("chart_type", "extra", "title"),
        [
            ("multi_line", {"date_column": "day"}, "Multi Line"),
            ("multi_bar", {"category_column": "region"}, "Multi Bar"),
        ],
    )
    def test_the_default_title(self, csv, tmp_path, chart_type, extra, title):
        result = generate_multi_chart(
            csv,
            chart_type=chart_type,
            value_columns=["units", "price"],
            output_path=str(tmp_path / f"{chart_type}.html"),
            open_after=False,
            **extra,
        )
        assert result["success"] is True, result.get("error")
        page = (tmp_path / f"{chart_type}.html").read_text(encoding="utf-8")
        assert title in page and "Multi-Multi" not in page
