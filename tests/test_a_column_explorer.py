"""The EDA report has a column explorer: every column in a list, and a page for the one picked.

Sweetviz reports are read one column at a time. run_eda had the numbers --
and a target ranking and a drift measure that never reached its HTML at all
-- spread across the page or only in the response. The Column Explorer puts
them per column. What has to hold:

- one page per column, in the file's order, and its numbers are pandas's;
- a named target and a baseline reach each column's page, as the response
  reports them;
- the page is drawn by the page's own script with text only: a name or a
  value never becomes an element.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_eda import run_eda

NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node is not installed")


@pytest.fixture
def data(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
    rng = np.random.default_rng(61)
    n = 300
    df = pd.DataFrame(
        {
            "day": pd.date_range("2023-11-01", periods=n).strftime("%Y-%m-%d"),
            "region <b>": rng.choice(["North", "South", "<img src=x onerror=alert(1)>"], n),
            "revenue": rng.normal(1000, 200, n).round(2),
            "units": rng.integers(0, 20, n),
            "churned": rng.integers(0, 2, n),
            "note": [None] * (n - 3) + ["a", "b", "c"],
        }
    )
    df.to_csv(tmp_path / "cur.csv", index=False)
    df.sample(200, random_state=1).assign(revenue=lambda t: t.revenue * 1.3).to_csv(tmp_path / "base.csv", index=False)
    return tmp_path


def _report(data: Path, **kw) -> tuple[dict, str]:
    r = run_eda(str(data / "cur.csv"), output_path=str(data / "eda.html"), open_after=False, **kw)
    assert r["success"] is True, r
    return r, Path(r["output_path"]).read_text(encoding="utf-8")


def _pages(html: str) -> list[dict]:
    match = re.search(r"const _CX=(.*?);const _CXT=", html)
    assert match, "the page has no column explorer"
    return json.loads(match.group(1))


class TestEveryColumnHasAPage:
    def test_in_the_files_order_with_pandas_numbers(self, data):
        _, html = _report(data)
        pages = _pages(html)
        df = pd.read_csv(data / "cur.csv")
        assert [p["name"] for p in pages] == list(df.columns)
        assert [p["kind"] for p in pages] == ["date", "category", "number", "number", "number", "category"]
        revenue = next(p for p in pages if p["name"] == "revenue")
        stats = dict(revenue["stats"])
        want = df["revenue"]
        for key, value in (
            ("mean", want.mean()),
            ("25%", want.quantile(0.25)),
            ("median", want.median()),
            ("max", want.max()),
        ):
            assert stats[key] == pytest.approx(value), key
        assert sum(revenue["chart"]["counts"]) == want.notna().sum()
        counts, edges = np.histogram(want.dropna(), bins=20)
        assert revenue["chart"]["counts"] == counts.tolist() and revenue["chart"]["edges"] == pytest.approx(
            edges.tolist()
        )
        assert dict(next(p for p in pages if p["name"] == "units")["stats"])["zeros"] == int((df["units"] == 0).sum())

    def test_a_category_page_is_its_top_values(self, data):
        pages = _pages(_report(data)[1])
        region = next(p for p in pages if p["name"] == "region <b>")
        want = pd.read_csv(data / "cur.csv")["region <b>"].value_counts()
        assert dict(zip(region["chart"]["labels"], region["chart"]["counts"], strict=True)) == want.to_dict()
        assert region["chart"]["other"] == 0

    def test_a_date_page_is_its_span_and_months(self, data):
        day = _pages(_report(data)[1])[0]
        assert dict(day["stats"]) == {"first": "2023-11-01", "last": "2024-08-26", "span (days)": 299}
        assert day["chart"]["labels"][0] == "2023-11" and sum(day["chart"]["counts"]) == 300

    def test_missing_values_and_alerts_are_on_the_columns_page(self, data):
        r, html = _report(data)
        note = next(p for p in _pages(html) if p["name"] == "note")
        assert (note["nulls"], note["count"], note["null_pct"]) == (297, 3, 99.0)
        assert note["alerts"], "the report's alerts about 'note' are on its page"

    def test_the_section_is_in_the_navigation(self, data):
        html = _report(data)[1]
        assert '<a href="#explorer">Column Explorer</a>' in html and '<div id="explorer" class="section">' in html


class TestTheTargetAndTheBaselineReachEachPage:
    def test_as_the_response_reports_them(self, data):
        r, html = _report(data, target_column="churned", compare_to=str(data / "base.csv"))
        pages = {p["name"]: p for p in _pages(html)}
        assert pages["churned"].get("is_target") is True and "target" not in pages["churned"]
        for a in r["target_association"]:
            page = pages[a["column"]]["target"]
            assert (page["measure"], page.get("strength")) == (a["measure"], a.get("strength"))
        for d in r["comparison"]["drift"] if "comparison" in r else r["drift"]["drift"]:
            assert pages[d["column"]]["drift"]["measure"] == d["measure"]
            assert pages[d["column"]]["drift"]["drift"] == pytest.approx(d["drift"], nan_ok=True)

    def test_without_them_no_page_claims_either(self, data):
        pages = _pages(_report(data)[1])
        assert not any("target" in p or "drift" in p or p.get("is_target") for p in pages)


_STUB = r"""
var __made=[], __html=[], __plots=[];
function __el(tag){var e={tag:tag,children:[],attrs:{},className:'',id:'',style:{},
  set textContent(v){this._text=String(v);this.children=[];}, get textContent(){return this._text||'';},
  set innerHTML(v){__html.push(String(v));}, get innerHTML(){return '';},
  appendChild(c){this.children.push(c);return c;}, setAttribute(k,v){this.attrs[k]=String(v);},
  addEventListener(){}, focus(){}};__made.push(e);return e;}
var __list=__el('div'), __pane=__el('div');
var document={getElementById(id){return id==='cx-list'?__list:id==='cx-detail'?__pane:null;},
  createElement:__el, createTextNode(t){return {tag:'#text',textContent:String(t)};}, activeElement:null};
var window={matchMedia(){return {matches:false};}};
var Plotly={react(el,data,layout){__plots.push(data);}, purge(){}};
"""


def _texts(node) -> list[str]:
    out = [node.get("_text", node.get("textContent", ""))] if isinstance(node, dict) else []
    for child in node.get("children", []) if isinstance(node, dict) else []:
        out += _texts(child)
    return [t for t in out if t]


@needs_node
class TestThePageIsDrawnAsText:
    def test_every_column_opens_and_a_name_is_never_markup(self, data):
        r, html = _report(data, target_column="churned")
        script = next(s for s in re.findall(r"<script>(.*?)</script>", html, flags=re.S) if "const _CX=" in s)
        program = (
            _STUB
            + script
            + (
                "\nvar __seen=[];for(var i=0;i<_CX.length;i++){window.cxShow(i);__seen.push(JSON.parse(JSON.stringify(__pane)));}"
                "process.stdout.write(JSON.stringify({seen:__seen,html:__html,plots:__plots.length,"
                "made:__made.map(function(e){return e.tag;})}));"
            )
        )
        done = subprocess.run([NODE, "-"], input=program, capture_output=True, encoding="utf-8", timeout=60)
        assert done.returncode == 0, done.stderr[-2000:]
        out = json.loads(done.stdout)
        assert out["html"] == [], "nothing is written as HTML"
        assert set(out["made"]) <= {"div", "button", "span", "h3", "dl", "dt", "dd", "p", "b"}
        region = _texts(out["seen"][1])
        assert region[0] == "region <b>", "the name, as text"
        top = dict(_pages(html)[1]["stats"])["most common"]
        assert top in region, "the most common value, as text -- whatever it holds"
        churn = _texts(out["seen"][4])
        assert churn[0] == "churned (the target)"
        revenue = _texts(out["seen"][2])
        measure = next(a for a in r["target_association"] if a["column"] == "revenue")["measure"]
        at = revenue.index("Against the target: ")
        assert revenue[at + 1].startswith(f"{measure} "), revenue
        assert out["plots"] == 7, "the first column when the page opens, then each of the six picked"
