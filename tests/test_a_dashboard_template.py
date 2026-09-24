"""A dashboard designed once is a template another file's dashboard is built from.

The spec a page was built from could only rebuild that page. A person who
designs the Q3 sales dashboard wants Q4's to look the same without writing the
spec again. `save_template="sales.dashboard.json"` writes the spec to a file,
and `template="sales.dashboard.json"` builds another file's dashboard from it:

- every column the template names must be in the new file, and a template
  that asks for columns the file lacks is refused naming all of them and the
  file it was saved from;
- nothing in a template points at its first file: not the data path, and
  not a title that was only that file's name ("q3" on Q4's page);
- the new page is drawn from the new file's rows;
- `spec` changes the template on the way in, as it changes a detection.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_dashboard import TEMPLATE_FORMAT, customize_dashboard, generate_dashboard
from tests.dashboard_page import NODE, drawn

needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")


@pytest.fixture
def quarters(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
    for name, seed in (("q3", 1), ("q4", 2)):
        rng = np.random.default_rng(seed)
        n = 150
        pd.DataFrame(
            {
                "day": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
                "region": rng.choice(["North", "South", "East"], n),
                "revenue": rng.normal(100, 20, n).round(2),
                "units": rng.integers(1, 9, n),
            }
        ).to_csv(tmp_path / f"{name}.csv", index=False)
    return tmp_path


SPEC = {
    "layout": [
        {
            "chart": "bar",
            "cols": {"category": "region", "value": "revenue"},
            "title": "Revenue",
            "style": {"color": "#112233"},
        },
        {"chart": "kpi", "cols": {"value": "units"}},
    ],
    "filters": [{"column": "region", "default": ["North", "South"]}],
    "style": {"colors": {"North": "#ff0000"}},
}


def _build(folder: Path, name: str, **kw) -> dict:
    return generate_dashboard(
        str(folder / f"{name}.csv"), output_path=str(folder / f"{name}.html"), open_after=False, **kw
    )


def _saved(quarters, **kw) -> dict:
    r = _build(quarters, "q3", spec=SPEC, save_template="sales.dashboard.json", **kw)
    assert r["success"] is True, r
    return json.loads((quarters / "sales.dashboard.json").read_text(encoding="utf-8"))


class TestSaving:
    def test_the_template_is_the_spec_and_what_it_needs(self, quarters):
        doc = _saved(quarters)
        assert doc["format"] == TEMPLATE_FORMAT and doc["from"] == "q3.csv"
        assert doc["columns"] == ["region", "revenue", "units"]
        assert doc["spec"]["layout"] == SPEC["layout"] and doc["spec"]["style"] == SPEC["style"]

    def test_nothing_in_it_points_at_its_first_file(self, quarters):
        doc = _saved(quarters)
        assert "_source_path" not in doc["spec"]
        assert "title" not in doc["spec"], "the title was only the file's name"

    def test_a_title_the_caller_gave_is_kept(self, quarters):
        assert _saved(quarters, title="Sales")["spec"]["title"] == "Sales"

    def test_a_dry_run_saves_nothing(self, quarters):
        r = _build(quarters, "q3", spec=SPEC, save_template="t.json", dry_run=True)
        assert r["success"] is True and r["template_saved"] is None and not (quarters / "t.json").exists()

    def test_customize_saves_the_page_it_rebuilt(self, quarters):
        page = _build(quarters, "q3", spec=SPEC)["output_path"]
        r = customize_dashboard(
            page,
            ops=[{"op": "set_panel", "slot": 0, "chart": "pie", "style": None}],
            open_after=False,
            save_template="pie.json",
        )
        assert r["success"] is True, r
        doc = json.loads((quarters / "pie.json").read_text(encoding="utf-8"))
        assert doc["spec"]["layout"][0]["chart"] == "pie"

    def test_a_name_that_is_not_json_is_refused_before_anything_is_built(self, quarters):
        r = _build(quarters, "q3", spec=SPEC, save_template="sales.txt")
        assert r["success"] is False and "save_template 'sales.txt' must end in .json" in r["error"]
        assert not (quarters / "q3.html").exists()


class TestApplying:
    def test_the_new_page_is_the_template_over_the_new_rows(self, quarters):
        _saved(quarters)
        r = _build(quarters, "q4", template="sales.dashboard.json")
        assert r["success"] is True, r
        assert r["template"] == {"name": "sales.dashboard.json", "from": "q3.csv"}
        assert r["spec"]["title"] == "q4" and r["spec"]["layout"] == SPEC["layout"]
        assert r["spec"]["filters"] == SPEC["filters"] and r["spec"]["style"] == SPEC["style"]

    @needs_node
    def test_its_numbers_are_the_new_files(self, quarters):
        _saved(quarters)
        html = Path(_build(quarters, "q4", template="sales.dashboard.json")["output_path"]).read_text(encoding="utf-8")
        bar = drawn(html)["figures"]["p0_bar"]["data"][0]
        want = pd.read_csv(quarters / "q4.csv").groupby("region")["revenue"].sum()
        assert dict(zip(bar["x"], bar["y"], strict=True)) == pytest.approx(want.to_dict())
        assert bar["marker"]["color"][bar["x"].index("North")] == "#ff0000"

    def test_a_spec_changes_it_on_the_way_in(self, quarters):
        _saved(quarters)
        r = _build(quarters, "q4", template="sales.dashboard.json", spec={"theme": "dark", "title": "Q4"})
        assert r["success"] is True, r
        assert (r["spec"]["theme"], r["spec"]["title"], r["spec"]["layout"]) == ("dark", "Q4", SPEC["layout"])

    def test_a_dry_run_checks_it_against_the_file(self, quarters):
        _saved(quarters)
        r = _build(quarters, "q4", template="sales.dashboard.json", dry_run=True)
        assert r["success"] is True and r["dry_run"] is True and r["would_generate"]["charts"] == ["bar", "kpi"]

    def test_a_detected_page_is_detected_again(self, quarters):
        r = _build(quarters, "q3", save_template="auto.json")
        assert r["success"] is True, r
        again = _build(quarters, "q4", template="auto.json")
        assert again["success"] is True, again
        assert again["spec"]["_layout_source"] == "detected"


class TestWhatCannotBeAppliedIsRefused:
    def test_columns_the_file_lacks_are_named_with_the_file_it_came_from(self, quarters):
        _saved(quarters)
        pd.DataFrame({"region": ["a", "b"], "amount": [1, 2]}).to_csv(quarters / "other.csv", index=False)
        r = _build(quarters, "other", template="sales.dashboard.json")
        assert r["success"] is False
        assert (
            "template 'sales.dashboard.json' names column(s) other.csv does not have: revenue, units. "
            "It was saved from q3.csv. Columns here: amount, region"
        ) in r["error"]
        assert not (quarters / "other.html").exists()

    def test_a_file_that_is_not_a_template(self, quarters):
        (quarters / "notes.json").write_text('{"layout": []}', encoding="utf-8")
        r = _build(quarters, "q4", template="notes.json")
        assert r["success"] is False and "'notes.json' is not a dashboard template" in r["error"]

    def test_a_template_that_is_not_there(self, quarters):
        r = _build(quarters, "q4", template="gone.json")
        assert r["success"] is False and "template 'gone.json' does not exist" in r["error"]
