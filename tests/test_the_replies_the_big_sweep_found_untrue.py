"""Eight replies from the 2026-10-02 big-data sweep that said one thing and held another.

Each case is the smallest frame that reproduces the sweep's finding, asserted on the reply.

* correlation_analysis answered a 286-column file with 23 MB (5.7M tokens): the full matrix and
  one finding per redundant pair, all inlined although the findings were also saved to a file.
* detect_anomalies wrote its anomalies-only file to the output ROOT whatever output_path said.
* reshape_dataset(transpose) made the first column the header with no check that its values were
  names: a 119,390-row file became a CSV of 119,391 columns, two distinct, the labels gone.
* export_data read preview_rows for Excel only; json/csv wrote everything and said "success", and an
  Excel preview reported the whole file's row count.
* generate_geo_map: one "CN" among 176 ISO-3 codes turned the column into "country names".
* generate_chart(sankey): shared labels became self-loops, agg_func=count drew a SUM.
* generate_pairwise_plot drew every row in every panel.
* validate_dataset scored a 0/1 column's zeros as warnings (the same file: 41 here, 96 elsewhere).
* date_note called an ISO date "month-first".
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (
    str(ROOT),
    str(ROOT / "servers" / "data_advanced"),
    str(ROOT / "servers" / "data_medium"),
):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import plotly.graph_objects as go  # noqa: E402
from _adv_charts import MAX_PLOT_POINTS, export_data, generate_pairwise_plot  # noqa: E402
from _adv_gencharts import _build_sankey, generate_chart, generate_geo_map  # noqa: E402
from _adv_helpers import _detect_location_mode  # noqa: E402
from _med_analysis import INSIGHTS_INLINE_MAX, MATRIX_INLINE_MAX, correlation_analysis, detect_anomalies  # noqa: E402
from _med_inspect import validate_dataset  # noqa: E402

from servers.data_transform.engine import reshape_dataset  # noqa: E402
from shared.column_utils import date_note  # noqa: E402


@pytest.fixture(autouse=True)
def _home(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    return tmp_path


def _csv(home: Path, name: str, frame: pd.DataFrame) -> str:
    path = home / name
    frame.to_csv(path, index=False)
    return str(path)


class TestACorrelationReplyHasABound:
    @pytest.fixture
    def wide(self, _home) -> str:
        """90 columns that are nine latent signals plus noise: a great many redundant pairs."""
        rng = np.random.default_rng(0)
        base = rng.normal(0, 1, (300, 9))
        cols = {f"g{i}_{j}": base[:, i] + rng.normal(0, 0.05, 300) for i in range(9) for j in range(10)}
        return _csv(_home, "wide.csv", pd.DataFrame(cols))

    def test_the_matrix_and_the_findings_are_capped_and_say_so(self, wide, _home):
        result = correlation_analysis(wide, output_path=str(_home / "corr.html"), open_after=False)
        assert result["success"] is True, result.get("error")
        assert "matrix" not in result
        assert str(len(result["columns"])) in result["matrix_omitted"]
        assert len(result["insights"]) <= INSIGHTS_INLINE_MAX
        assert result["insights_total"] > INSIGHTS_INLINE_MAX
        assert "insights_path" in result["insights_note"]
        assert len(json.dumps(result)) < 150_000

    def test_the_whole_list_is_in_the_file(self, wide, _home):
        result = correlation_analysis(wide, output_path=str(_home / "corr.html"), open_after=False)
        saved = Path(result["insights_path"]).read_text(encoding="utf-8")
        assert len(saved) > len(json.dumps(result["insights"]))

    def test_a_small_file_still_returns_its_matrix(self, _home):
        rng = np.random.default_rng(1)
        a = rng.normal(0, 1, 100)
        path = _csv(
            _home, "small.csv", pd.DataFrame({"a": a, "b": a + rng.normal(0, 1, 100), "c": rng.normal(0, 1, 100)})
        )
        result = correlation_analysis(path, output_path=str(_home / "c.html"), open_after=False)
        assert len(result["matrix"]) == 3 <= MATRIX_INLINE_MAX
        assert "matrix_omitted" not in result


class TestAnomaliesOnlyLandBesideTheScoredFile:
    def test_in_the_folder_the_caller_chose(self, _home):
        rng = np.random.default_rng(2)
        values = np.concatenate([rng.normal(50, 2, 300), [400.0, 500.0, -300.0]])
        path = _csv(_home, "src.csv", pd.DataFrame({"v": values}))
        scratch = _home / "scratch"
        scratch.mkdir()
        result = detect_anomalies(path, output_path=str(scratch / "scored.csv"))
        assert result["success"] is True, result.get("error")
        only = Path(result["anomalies_only_path"])
        assert only.parent == scratch
        assert only.name == "scored_only.csv"
        assert not list(_home.glob("*_only*.csv")), "nothing may be written to the output root"
        assert len(pd.read_csv(only)) == result["anomalies_only_rows"] >= 3


class TestTransposeNeedsNames:
    def test_a_repeating_first_column_is_refused_and_nothing_is_written(self, _home):
        path = _csv(_home, "h.csv", pd.DataFrame({"hotel": ["City", "Resort"] * 50, "adr": range(100)}))
        before = set(_home.iterdir())
        result = reshape_dataset(path, mode="transpose")
        assert result["success"] is False
        assert "2 distinct" in result["error"] and "hotel" in result["error"]
        assert "aggregate_dataset" in result["hint"]
        assert set(_home.iterdir()) == before

    def test_a_dry_run_refuses_too(self, _home):
        path = _csv(_home, "h.csv", pd.DataFrame({"hotel": ["City", "Resort"] * 5, "adr": range(10)}))
        assert reshape_dataset(path, mode="transpose", dry_run=True)["success"] is False

    def test_unique_labels_still_transpose(self, _home):
        path = _csv(_home, "u.csv", pd.DataFrame({"name": ["a", "b", "c"], "x": [1, 2, 3], "y": [4, 5, 6]}))
        result = reshape_dataset(path, mode="transpose", output_path=str(_home / "t.csv"))
        assert result["success"] is True, result.get("error")
        out = pd.read_csv(_home / "t.csv")
        assert out.columns.tolist() == ["index", "a", "b", "c"]
        assert out.shape == (2, 4)


class TestAPreviewIsAPreviewInEveryFormat:
    @pytest.fixture
    def twenty(self, _home) -> str:
        return _csv(_home, "twenty.csv", pd.DataFrame({"a": range(20), "b": list("abcdefghijklmnopqrst")}))

    @pytest.mark.parametrize("fmt", ["csv", "json"])
    def test_text_formats_write_only_the_preview(self, twenty, _home, fmt):
        result = export_data(twenty, output_path=str(_home / f"p.{fmt}"), format=fmt, preview_rows=3, open_after=False)
        assert result["success"] is True, result.get("error")
        assert result["rows"] == 3 and result["rows_total"] == 20 and result["is_preview"] is True
        text = (_home / f"p.{fmt}").read_text(encoding="utf-8")
        written = len(pd.read_csv(_home / "p.csv")) if fmt == "csv" else len(json.loads(text))
        assert written == 3
        assert "3 of 20" in " ".join(
            p.get("detail", "") for p in result["progress"] if "exported" in p["message"].lower()
        )

    def test_excel_says_what_it_wrote(self, twenty, _home):
        result = export_data(
            twenty, output_path=str(_home / "p.xlsx"), format="excel", preview_rows=3, open_after=False
        )
        assert result["success"] is True, result.get("error")
        assert result["rows"] == 3
        assert any("3 of 20" in p.get("detail", "") for p in result["progress"] if "exported" in p["message"].lower())

    def test_no_preview_writes_everything(self, twenty, _home):
        result = export_data(twenty, output_path=str(_home / "all.json"), format="json", open_after=False)
        assert result["rows"] == 20 and "is_preview" not in result
        assert len(json.loads((_home / "all.json").read_text(encoding="utf-8"))) == 20


class TestOneStrayCodeDoesNotFlipTheMap:
    def _iso3(self, n: int = 60) -> list[str]:
        letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        return [letters[i % 26] + letters[(i // 26) % 26] + letters[(i * 7) % 26] for i in range(n)]

    def test_a_column_of_iso_codes_with_one_two_letter_value_is_iso3(self):
        values = [*self._iso3(), "CN"]
        assert _detect_location_mode(pd.DataFrame({"country": values}), "country") == "ISO-3"

    def test_names_are_names(self):
        frame = pd.DataFrame({"country": ["France", "Spain", "Germany", "Italy"] * 5})
        assert _detect_location_mode(frame, "country") == "country names"

    def test_us_states(self):
        frame = pd.DataFrame({"state": ["CA", "NY", "TX", "WA"] * 5})
        assert _detect_location_mode(frame, "state") == "USA-states"

    def test_mixed_halves_are_not_called_iso(self):
        frame = pd.DataFrame({"c": ["USA", "FRA", "Spain", "Italy", "Peru", "Chile"] * 3})
        assert _detect_location_mode(frame, "c") == "country names"

    def test_the_map_is_drawn_in_iso3_mode(self, _home):
        codes = ["PRT", "GBR", "FRA", "ESP", "DEU", "ITA", "IRL", "BRA", "USA", "NLD", "BEL", "CHE"]
        frame = pd.DataFrame({"country": [*(codes * 10), "CN"], "adr": 1.0})
        path = _csv(_home, "geo.csv", frame)
        result = generate_geo_map(
            path, location_column="country", value_column="adr", output_path=str(_home / "m.html"), open_after=False
        )
        assert result["success"] is True, result.get("error")
        html = (_home / "m.html").read_text(encoding="utf-8")
        assert "ISO-3" in html


class TestASankeyIsTheFlowAskedFor:
    @pytest.fixture
    def flows(self) -> pd.DataFrame:
        rows = [
            ("Direct", "Direct", 10.0),
            ("Direct", "Corporate", 20.0),
            ("Corporate", "Corporate", 30.0),
            ("Corporate", "Direct", 40.0),
            ("Corporate", "Direct", 50.0),
        ]
        return pd.DataFrame(rows, columns=["segment", "channel", "adr"])

    def test_shared_labels_are_separate_nodes_on_each_side(self, flows):
        fig = _build_sankey(flows, "segment", "channel", "adr", "t", "plotly_white", go)
        sankey = fig.data[0]
        labels = list(sankey.node.label)
        assert sorted(labels) == ["Corporate", "Corporate", "Direct", "Direct"]
        assert all(s != t for s, t in zip(sankey.link.source, sankey.link.target, strict=True))
        left = {labels[s] for s in sankey.link.source}
        right = {labels[t] for t in sankey.link.target}
        assert left == {"Direct", "Corporate"} and right == {"Direct", "Corporate"}

    def test_the_sum_is_the_sum(self, flows):
        sankey = _build_sankey(flows, "segment", "channel", "adr", "t", "plotly_white", go).data[0]
        assert sorted(sankey.link.value) == [10.0, 20.0, 30.0, 90.0]

    def test_count_counts_rows_not_adr(self, flows):
        sankey = _build_sankey(flows, "segment", "channel", "adr", "t", "plotly_white", go, "count").data[0]
        assert sorted(sankey.link.value) == [1, 1, 1, 2]

    def test_the_order_is_by_flow_and_repeatable(self, flows):
        a = list(_build_sankey(flows, "segment", "channel", "adr", "t", "plotly_white", go).data[0].node.label)
        b = list(
            _build_sankey(flows.sample(frac=1, random_state=1), "segment", "channel", "adr", "t", "plotly_white", go)
            .data[0]
            .node.label
        )
        assert a == b == ["Corporate", "Direct", "Direct", "Corporate"]

    def test_two_hierarchy_columns_are_the_source_and_target(self, flows, _home):
        path = _csv(_home, "flows.csv", flows)
        result = generate_chart(
            path,
            "sankey",
            "adr",
            hierarchy_columns=["segment", "channel"],
            agg_func="count",
            output_path=str(_home / "s.html"),
            open_after=False,
        )
        assert result["success"] is True, result.get("error")


class TestPairwiseDrawsAtMostTheCap:
    def test_a_large_file_is_sampled_and_says_so(self, _home):
        n = MAX_PLOT_POINTS * 3
        rng = np.random.default_rng(5)
        path = _csv(_home, "big.csv", pd.DataFrame(rng.normal(0, 1, (n, 3)), columns=["a", "b", "c"]))
        result = generate_pairwise_plot(path, output_path=str(_home / "pair.html"), open_after=False)
        assert result["success"] is True, result.get("error")
        assert result["rows_used"] == n
        assert result["rows_plotted"] == MAX_PLOT_POINTS

    def test_a_small_file_is_drawn_whole(self, _home):
        rng = np.random.default_rng(6)
        path = _csv(_home, "small.csv", pd.DataFrame(rng.normal(0, 1, (200, 3)), columns=["a", "b", "c"]))
        result = generate_pairwise_plot(path, output_path=str(_home / "pair.html"), open_after=False)
        assert result["rows_plotted"] == 200


class TestAZeroIsAValue:
    @pytest.fixture
    def frame(self, _home) -> str:
        rng = np.random.default_rng(8)
        n = 400
        return _csv(
            _home,
            "v.csv",
            pd.DataFrame(
                {
                    "status": rng.choice([0, 1], n, p=[0.75, 0.25]),  # a class
                    "babies": rng.choice([0, 1, 2], n, p=[0.97, 0.02, 0.01]),  # a count: 0 is "none"
                    "amount": rng.normal(100, 10, n),
                }
            ),
        )

    def test_a_binary_column_has_no_zero_issue(self, frame):
        issues = validate_dataset(frame)["issues"]
        assert not [i for i in issues if i["column"] == "status" and "zeros" in i["issue"]]

    def test_a_count_with_zeros_is_advice_not_a_warning(self, frame):
        issues = validate_dataset(frame)["issues"]
        babies = [i for i in issues if i["column"] == "babies" and "zeros" in i["issue"]]
        assert babies and all(i["severity"] == "advice" for i in babies)

    def test_advice_costs_no_score_and_does_not_fail_the_file(self, frame):
        result = validate_dataset(frame)
        assert result["score"] == 100 and result["passed"] is True

    def test_a_real_warning_still_scores(self, _home):
        frame = pd.DataFrame({"a": [1.0, None, 3.0, None, 5.0] * 20, "b": range(100)})
        result = validate_dataset(_csv(_home, "n.csv", frame), max_null_pct=5.0)
        assert result["passed"] is False and result["score"] < 100


class TestAnIsoDateIsNotCalledMonthFirst:
    def test_year_first_is_named(self):
        entry = date_note(
            {"dayfirst": False, "ambiguous": False, "reason": "ISO year-first dates"}, "reservation_status_date"
        )
        assert "year-first (YYYY-MM-DD)" in entry["message"]
        assert "month-first" not in entry["message"]

    def test_a_real_month_first_reading_still_says_so(self):
        entry = date_note({"dayfirst": False, "ambiguous": False, "reason": "13 only fits as a day second"}, "d")
        assert "month-first" in entry["message"]
