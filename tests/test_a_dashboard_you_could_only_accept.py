"""The detection was the only way in.

S1, from the review:

    `generate_*(spec)` + `customize_*` on every generator. Spec:
    `{title, theme, layout:[{slot, chart, cols, agg}], kpis:[], filters:[],
    tabs:[], interactions:{}}`

and from its dashboard notes: *"customization = small JSON edit, not full
rebuild"*.

`generate_dashboard` auto-detects everything -- which charts, from which
columns, with which aggregate. The detection is good. It was also the only way
in, so a caller who wanted the same dashboard with one chart swapped had two
options: accept what they got, or write the page themselves. An agent asked to
"make that bar chart a line chart" could not express the request at all.

The spec makes the detection an opening offer. Nothing is required, so the
zero-argument call is unchanged -- which is the first thing tested here, because
a spec parameter that quietly moves the default is a behaviour change wearing a
parameter's clothes.

S2 asked for real components. The dashboard already had KPI cards, a filter bar,
alerts and cross-filter. It had no table and no tabs, so those are here: the
table renders from the same filtered rows the charts use, so the two cannot
disagree, and tabs show and hide cards rather than re-plotting them, because a
tab switch that re-rendered every chart would make the cheapest interaction on
the page the most expensive.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_advanced")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _adv_dashboard import customize_dashboard, generate_dashboard  # noqa: E402

from shared.dashboard_spec import (  # noqa: E402
    CHART_KINDS,
    DEFAULT_INTERACTIONS,
    SPEC_KEYS,
    SpecError,
)
from shared.dashboard_spec import merge as merge_spec  # noqa: E402
from shared.dashboard_spec import validate as validate_spec  # noqa: E402
from shared.provenance import read_provenance, read_spec  # noqa: E402


@pytest.fixture
def sales(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(12)
    n = 200
    p = tmp_path / "sales.csv"
    pd.DataFrame(
        {
            "region": rng.choice(["North", "South", "East"], n),
            "channel": rng.choice(["web", "store"], n),
            "revenue": rng.normal(1000, 200, n).round(2),
            "units": rng.integers(1, 50, n),
        }
    ).to_csv(p, index=False)
    return p


# ---------------------------------------------------------------------------
# the default did not move
# ---------------------------------------------------------------------------


def test_no_spec_is_the_old_behaviour(sales):
    r = generate_dashboard(str(sales), open_after=False)
    assert r["success"] is True
    assert r["spec"]["title"] == sales.stem
    assert r["spec"]["interactions"] == DEFAULT_INTERACTIONS
    assert r["spec"]["tabs"] == []


def test_the_resolved_spec_comes_back(sales):
    """Without it, "customize" means re-deriving intent from rendered HTML."""
    r = generate_dashboard(str(sales), open_after=False)
    # Every caller-settable key, plus the generator's own underscore-prefixed
    # record of how the page was built.
    assert set(SPEC_KEYS) <= set(r["spec"])
    assert {k for k in r["spec"] if not k.startswith("_")} == set(SPEC_KEYS)
    assert r["spec"]["layout"], "the detection has to be visible to be editable"
    assert all(p["chart"] for p in r["spec"]["layout"])


def test_the_page_carries_the_spec_and_the_provenance(sales):
    r = generate_dashboard(str(sales), open_after=False)
    html = Path(r["output_path"]).read_text(encoding="utf-8")
    assert read_spec(html) == r["spec"]
    header = read_provenance(html)
    assert header["source"] == sales.name
    assert header["rows_total"] == 200


# ---------------------------------------------------------------------------
# the spec is honoured
# ---------------------------------------------------------------------------


def test_a_title_override_is_used(sales):
    r = generate_dashboard(str(sales), open_after=False, spec={"title": "Q3 Review"})
    assert r["dashboard_title"] == "Q3 Review"
    assert "Q3 Review" in Path(r["output_path"]).read_text(encoding="utf-8")


def test_a_layout_override_replaces_the_detected_charts(sales):
    spec = {"layout": [{"slot": 0, "chart": "pie", "cols": {"category": "region", "value": "revenue"}}]}
    r = generate_dashboard(str(sales), open_after=False, spec=spec)
    assert r["success"] is True
    assert r["charts_included"] == ["pie"]


def test_kpis_and_filters_can_be_narrowed(sales):
    r = generate_dashboard(str(sales), open_after=False, spec={"kpis": ["revenue"], "filters": ["region"]})
    assert r["spec"]["kpis"] == ["revenue"]
    assert r["spec"]["filters"] == ["region"]


# ---------------------------------------------------------------------------
# a bad spec is refused, never quietly ignored
# ---------------------------------------------------------------------------


def test_a_column_that_is_not_there_is_refused_by_name(sales):
    r = generate_dashboard(str(sales), open_after=False, spec={"kpis": ["profit"]})
    assert r["success"] is False
    assert "profit" in r["error"]
    assert "revenue" in r["error"], "the refusal has to carry what is available"


def test_a_chart_with_half_its_columns_named_is_refused(sales):
    """The caller plainly meant to choose and came up one role short.

    Detecting the rest would hand them a chart they did not ask for, under a
    spec that says they did.
    """
    spec = {"layout": [{"chart": "bar", "cols": {"category": "region"}}]}
    r = generate_dashboard(str(sales), open_after=False, spec=spec)
    assert r["success"] is False
    assert "value" in r["error"]
    assert "no cols at all" in r["error"], "the refusal has to name the way out"


def test_a_chart_with_no_columns_lets_the_detector_choose(sales):
    """Also the shape the detector emits, so the spec round-trips."""
    r = generate_dashboard(str(sales), open_after=False, spec={"layout": [{"chart": "bar", "cols": {}}]})
    assert r["success"] is True, r
    assert r["charts_included"] == ["bar"]


def test_an_undrawable_chart_lists_the_drawable_ones(sales):
    r = generate_dashboard(str(sales), open_after=False, spec={"layout": [{"chart": "sankey", "cols": {}}]})
    assert r["success"] is False
    for kind in ("bar", "pie", "scatter"):
        assert kind in r["error"] or kind in r["hint"]


def test_an_unknown_spec_key_is_refused(sales):
    r = generate_dashboard(str(sales), open_after=False, spec={"titel": "typo"})
    assert r["success"] is False
    assert "titel" in r["error"]


def test_a_tab_pointing_past_the_layout_is_refused(sales):
    spec = {
        "layout": [{"chart": "pie", "cols": {"category": "region", "value": "revenue"}}],
        "tabs": [{"name": "All", "slots": [0, 5]}],
    }
    r = generate_dashboard(str(sales), open_after=False, spec=spec)
    assert r["success"] is False
    assert "5" in r["error"]


# ---------------------------------------------------------------------------
# S2 components
# ---------------------------------------------------------------------------


def test_the_table_is_off_unless_asked_for(sales):
    r = generate_dashboard(str(sales), open_after=False)
    assert 'id="dash-table"' not in Path(r["output_path"]).read_text(encoding="utf-8")


def test_the_table_renders_when_asked_for(sales):
    r = generate_dashboard(str(sales), open_after=False, spec={"interactions": {"table": True}})
    html = Path(r["output_path"]).read_text(encoding="utf-8")
    assert 'id="dash-table"' in html
    for col in ("region", "channel", "revenue", "units"):
        assert f'data-col="{col}"' in html
    assert "renderTable" in html, "markup without behaviour is a table that does nothing"
    assert 'id="tbl-next"' in html and 'id="tbl-prev"' in html


def test_the_table_says_how_many_of_how_many(sales):
    """The same honesty the responses carry, in the page."""
    r = generate_dashboard(str(sales), open_after=False, spec={"interactions": {"table": True}})
    html = Path(r["output_path"]).read_text(encoding="utf-8")
    assert "' of '+data.length+' row'" in html


def test_the_page_size_is_the_callers(sales):
    r = generate_dashboard(str(sales), open_after=False, spec={"interactions": {"table": True, "table_page_size": 10}})
    assert 'data-page-size="10"' in Path(r["output_path"]).read_text(encoding="utf-8")


def test_tabs_render_and_hide_rather_than_replot(sales):
    spec = {
        "layout": [
            {"chart": "bar", "cols": {"category": "region", "value": "revenue"}},
            {"chart": "pie", "cols": {"category": "channel", "value": "units"}},
        ],
        "tabs": [{"name": "Revenue", "slots": [0]}, {"name": "Mix", "slots": [1]}],
    }
    r = generate_dashboard(str(sales), open_after=False, spec=spec)
    html = Path(r["output_path"]).read_text(encoding="utf-8")
    assert 'class="tab-btn"' in html
    assert ">Revenue<" in html and ">Mix<" in html
    assert "card.style.display" in html, "tabs must show/hide, not re-plot"


def test_no_tabs_means_no_tab_bar(sales):
    r = generate_dashboard(str(sales), open_after=False)
    assert 'class="tab-btn"' not in Path(r["output_path"]).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# customize_dashboard
# ---------------------------------------------------------------------------


def test_a_dashboard_can_be_customised_from_itself(sales):
    first = generate_dashboard(str(sales), open_after=False)
    r = customize_dashboard(first["output_path"], {"title": "Renamed"}, open_after=False)
    assert r["success"] is True, r
    assert r["op"] == "customize_dashboard"
    assert r["dashboard_title"] == "Renamed"
    assert r["changed_keys"] == ["title"]


def test_customizing_keeps_everything_it_was_not_asked_to_change(sales):
    first = generate_dashboard(str(sales), open_after=False, spec={"kpis": ["revenue"]})
    r = customize_dashboard(first["output_path"], {"title": "Renamed"}, open_after=False)
    assert r["spec"]["kpis"] == ["revenue"], "an edit is not a rebuild from scratch"
    assert r["previous_spec"]["title"] == sales.stem


def test_interactions_merge_rather_than_replace(sales):
    first = generate_dashboard(
        str(sales), open_after=False, spec={"interactions": {"table": True, "table_page_size": 10}}
    )
    r = customize_dashboard(first["output_path"], {"interactions": {"table_page_size": 50}}, open_after=False)
    assert r["spec"]["interactions"]["table"] is True, "the switch the caller did not touch survives"
    assert r["spec"]["interactions"]["table_page_size"] == 50


def test_a_page_with_no_spec_says_so(tmp_path):
    page = tmp_path / "hand_written.html"
    page.write_text("<html><body>not ours</body></html>", encoding="utf-8")
    r = customize_dashboard(str(page), {"title": "x"}, open_after=False)
    assert r["success"] is False
    assert "no spec" in r["error"]
    assert "generate_dashboard" in r["hint"]


def test_a_missing_dashboard_is_refused(tmp_path):
    r = customize_dashboard(str(tmp_path / "nope.html"), {"title": "x"}, open_after=False)
    assert r["success"] is False
    assert "not found" in r["error"]


def test_an_unknown_change_key_is_refused(sales):
    first = generate_dashboard(str(sales), open_after=False)
    r = customize_dashboard(first["output_path"], {"colour": "red"}, open_after=False)
    assert r["success"] is False
    assert "colour" in r["error"]


# ---------------------------------------------------------------------------
# the spec module
# ---------------------------------------------------------------------------


def test_validate_accepts_an_empty_spec():
    assert validate_spec(None, pd.DataFrame({"a": [1]})) == {}


def test_merge_replaces_lists_and_merges_interactions():
    base = {"kpis": ["a", "b"], "interactions": {"table": False, "hover": True}}
    out = merge_spec(base, {"kpis": ["c"], "interactions": {"table": True}})
    assert out["kpis"] == ["c"], "a list is replaced; 'add these' and 'these' are different requests"
    assert out["interactions"] == {"table": True, "hover": True}


def test_every_chart_kind_declares_what_it_needs():
    from shared.dashboard_spec import CHART_NEEDS

    assert set(CHART_NEEDS) == set(CHART_KINDS), "a chart with no declared roles cannot be refused precisely"


def test_merge_refuses_an_unknown_key():
    with pytest.raises(SpecError) as exc:
        merge_spec({}, {"nope": 1})
    assert "nope" in str(exc.value)


def test_the_spec_a_dashboard_emits_is_valid_input_to_the_same_validator(sales):
    """The load-bearing property, and the one the first version got wrong.

    `customize_dashboard` reads the spec a page was built from and hands it
    straight back to `generate_dashboard`. If the emitted spec does not
    validate, that round-trip cannot work -- and it did not, because the
    detected layout has no `cols` and validation demanded them.
    """
    emitted = generate_dashboard(str(sales), open_after=False)["spec"]
    validate_spec(emitted, pd.read_csv(sales))  # must not raise
    again = generate_dashboard(str(sales), open_after=False, spec=emitted)
    assert again["success"] is True, again
    assert again["spec"]["layout"] == emitted["layout"]


def test_customizing_works_when_the_data_is_not_beside_the_page(tmp_path, monkeypatch):
    """The deployed layout, and what the first version could not do.

    `MCP_OUTPUT_DIR` sends dashboards to the shared output directory while the
    CSV stays where the caller put it. The provenance block records only the
    file's *name* -- deliberately, because that block travels with the page --
    so customize had nothing to open. The build document records the path.
    """
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    data_dir = tmp_path / "data"
    out_dir = tmp_path / "out"
    data_dir.mkdir()
    out_dir.mkdir()
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(out_dir))
    csv = data_dir / "sales.csv"
    pd.DataFrame({"region": ["N", "S"] * 50, "revenue": np.arange(100.0)}).to_csv(csv, index=False)

    first = generate_dashboard(str(csv), open_after=False)
    assert Path(first["output_path"]).parent == out_dir, "fixture must actually split the two"
    r = customize_dashboard(first["output_path"], {"title": "Across dirs"}, open_after=False)
    assert r["success"] is True, r
    assert r["dashboard_title"] == "Across dirs"


def test_a_source_that_has_since_been_deleted_says_which_file(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    csv = tmp_path / "gone.csv"
    pd.DataFrame({"a": ["x", "y"] * 50, "b": np.arange(100.0)}).to_csv(csv, index=False)
    first = generate_dashboard(str(csv), open_after=False)
    csv.unlink()
    r = customize_dashboard(first["output_path"], {"title": "x"}, open_after=False)
    assert r["success"] is False
    assert "gone.csv" in r["hint"]


def test_the_recorded_path_does_not_break_the_round_trip(tmp_path, monkeypatch):
    """Validation has to accept the generator's own output, underscore keys included."""
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    csv = tmp_path / "rt.csv"
    pd.DataFrame({"a": ["x", "y"] * 50, "b": np.arange(100.0)}).to_csv(csv, index=False)
    emitted = generate_dashboard(str(csv), open_after=False)["spec"]
    assert emitted["_source_path"] == str(csv)
    validate_spec(emitted, pd.read_csv(csv))  # must not raise
