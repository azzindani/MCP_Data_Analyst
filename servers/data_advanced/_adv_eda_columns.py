"""The EDA report's column explorer: a list of every column and a page for the one chosen.

Sweetviz's reports are navigated one column at a time -- a list down the
side, and for the column picked its numbers, its shape, how it relates to the
target and how it moved against a baseline. run_eda had every one of those
numbers and showed them spread across the page, or only in the response: the
target ranking and the drift never reached the HTML at all. This section puts
them per column, where a reader looks for them.

Each column's page is computed here, in Python, and embedded as data. The
page's script only lays it out, and writes every value as text: a column
name or a cell is never markup.
"""

from __future__ import annotations

import html as _html
from typing import Any

import numpy as np
import pandas as pd

from shared.html_theme import theme_plot_colors
from shared.table_payload import json_for_script

MAX_BINS = 20
TOP_VALUES = 10
MAX_MONTHS = 120


def _plain(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return value


def _number_page(s: pd.Series) -> tuple[list, dict | None]:
    values = pd.to_numeric(s, errors="coerce").dropna().astype(float)
    if values.empty:
        return [], None
    stats = [
        ["mean", values.mean()],
        ["std", values.std()],
        ["min", values.min()],
        ["25%", values.quantile(0.25)],
        ["median", values.median()],
        ["75%", values.quantile(0.75)],
        ["max", values.max()],
        ["zeros", int((values == 0).sum())],
        ["skew", values.skew()],
    ]
    bins = max(1, min(MAX_BINS, int(values.nunique())))
    counts, edges = np.histogram(values.to_numpy(), bins=bins)
    chart = {"type": "hist", "edges": [float(e) for e in edges], "counts": [int(c) for c in counts]}
    return [[k, _plain(v)] for k, v in stats], chart


def _category_page(s: pd.Series) -> tuple[list, dict | None]:
    present = s.dropna().astype(str)
    if present.empty:
        return [], None
    counts = present.value_counts()
    top = counts.head(TOP_VALUES)
    stats = [["most common", str(top.index[0])], ["its share", f"{top.iloc[0] / len(present):.1%}"]]
    chart = {
        "type": "bar",
        "labels": [str(k) for k in top.index],
        "counts": [int(v) for v in top.to_numpy()],
        "other": int(counts.iloc[TOP_VALUES:].sum()),
    }
    return stats, chart


def _date_page(s: pd.Series) -> tuple[list, dict | None]:
    dates = pd.to_datetime(s, errors="coerce").dropna()
    if dates.empty:
        return [], None
    first, last = dates.min(), dates.max()
    stats = [["first", _plain(first)], ["last", _plain(last)], ["span (days)", int((last - first).days)]]
    by = "M" if dates.dt.to_period("M").nunique() <= MAX_MONTHS else "Y"
    per = dates.dt.to_period(by).value_counts().sort_index()
    chart = {"type": "dates", "labels": [str(p) for p in per.index], "counts": [int(v) for v in per.to_numpy()]}
    return stats, chart


def column_pages(
    df: pd.DataFrame,
    numeric_cols: list,
    datetime_cols: list,
    alerts: list[dict],
    associations: list[dict] | None = None,
    drift: list[dict] | None = None,
    target: str = "",
) -> list[dict[str, Any]]:
    """One page per column, in the file's order."""
    by_target = {str(a.get("column")): a for a in associations or []}
    by_drift = {str(d.get("column")): d for d in drift or []}
    rows = len(df)
    pages = []
    for c in df.columns:
        s = df[c]
        name = str(c)
        nulls = int(s.isna().sum())
        if c in numeric_cols:
            kind, (stats, chart) = "number", _number_page(s)
        elif c in datetime_cols:
            kind, (stats, chart) = "date", _date_page(s)
        else:
            kind, (stats, chart) = "category", _category_page(s)
        page: dict[str, Any] = {
            "name": name,
            "kind": kind,
            "count": rows - nulls,
            "nulls": nulls,
            "null_pct": round(nulls / rows * 100, 2) if rows else 0.0,
            "unique": int(s.nunique()),
            "stats": stats,
            "chart": chart,
            "alerts": [str(a.get("msg")) for a in alerts if str(a.get("col")) == name],
        }
        if target:
            if name == target:
                page["is_target"] = True
            elif name in by_target:
                a = by_target[name]
                page["target"] = {
                    k: _plain(a.get(k)) for k in ("measure", "strength", "measure_note", "note") if k in a
                }
        if name in by_drift:
            d = by_drift[name]
            page["drift"] = {k: _plain(d.get(k)) for k in ("measure", "drift", "reading", "note") if k in d}
        pages.append(page)
    return pages


_CSS = """
.cx{display:grid;grid-template-columns:minmax(10rem,16rem) minmax(0,1fr);gap:1rem;align-items:start}
.cx-list{display:flex;flex-direction:column;gap:.25rem;max-height:34rem;overflow-y:auto;padding-right:.25rem}
.cx-item{display:flex;justify-content:space-between;gap:.5rem;align-items:center;text-align:left;
  background:var(--bg);border:1px solid var(--border);color:var(--text);border-radius:.375rem;
  padding:.375rem .5rem;font-size:.8125rem;cursor:pointer;min-width:0}
.cx-item .cx-name{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.cx-item[aria-selected="true"]{border-color:var(--accent);box-shadow:inset 3px 0 0 var(--accent)}
.cx-kind{font-size:.6875rem;color:var(--text-muted);white-space:nowrap}
.cx-detail{min-width:0}
.cx-detail h3{margin:0 0 .5rem;color:var(--text);font-size:1rem;overflow-wrap:anywhere}
.cx-stats{display:grid;grid-template-columns:repeat(auto-fill,minmax(8.5rem,1fr));gap:.375rem;margin:0 0 .75rem}
.cx-stat{background:var(--bg);border:1px solid var(--border);border-radius:.375rem;padding:.375rem .5rem}
.cx-stat dt{font-size:.6875rem;color:var(--text-muted);text-transform:uppercase;letter-spacing:.03em}
.cx-stat dd{margin:0;font-size:.875rem;color:var(--text);overflow-wrap:anywhere}
.cx-line{font-size:.8125rem;color:var(--text);margin:.25rem 0}
.cx-line b{color:var(--text-muted);font-weight:600}
.cx-chart{height:16.25rem;margin-top:.5rem}
@media(max-width:48rem){.cx{grid-template-columns:minmax(0,1fr)}.cx-list{max-height:12rem}}
"""

_JS = r"""
(function(){
  var list=document.getElementById('cx-list'),pane=document.getElementById('cx-detail');
  if(!list||!pane)return;
  function colours(){
    var dark=_CXT.device&&window.matchMedia&&window.matchMedia('(prefers-color-scheme: dark)').matches;
    return _CXT.device?(dark?_CXT.dark:_CXT.light):_CXT;
  }
  function fmt(v){
    if(v===null||v===undefined)return '—';
    return typeof v==='number'?v.toLocaleString(undefined,{maximumFractionDigits:4}):String(v);
  }
  function el(tag,cls,text){var e=document.createElement(tag);if(cls)e.className=cls;if(text!==undefined)e.textContent=text;return e;}
  function line(label,text){var p=el('p','cx-line');p.appendChild(el('b','',label+' '));p.appendChild(document.createTextNode(text));return p;}
  var items=[];
  _CX.forEach(function(c,i){
    var b=el('button','cx-item');b.type='button';b.setAttribute('role','option');b.setAttribute('aria-selected','false');
    b.appendChild(el('span','cx-name',c.name));b.appendChild(el('span','cx-kind',c.kind));
    b.addEventListener('click',function(){show(i);});
    list.appendChild(b);items.push(b);
  });
  list.addEventListener('keydown',function(e){
    var at=items.indexOf(document.activeElement);if(at<0)return;
    var to=e.key==='ArrowDown'?at+1:e.key==='ArrowUp'?at-1:-1;
    if(to>=0&&to<items.length){e.preventDefault();items[to].focus();show(to);}
  });
  function draw(c){
    var box=el('div','cx-chart');box.id='cx-chart';pane.appendChild(box);
    if(!c.chart||typeof Plotly==='undefined')return;
    var t=colours(),ch=c.chart,trace;
    if(ch.type==='hist'){
      var mid=[],wid=[];for(var k=0;k<ch.counts.length;k++){mid.push((ch.edges[k]+ch.edges[k+1])/2);wid.push(ch.edges[k+1]-ch.edges[k]);}
      trace={type:'bar',x:mid,y:ch.counts,width:wid,marker:{color:t.accent},hovertemplate:'%{x}<br>%{y} rows<extra></extra>'};
    }else{
      var labels=ch.labels.slice(),counts=ch.counts.slice();
      if(ch.other){labels.push('(other)');counts.push(ch.other);}
      trace={type:'bar',x:labels,y:counts,marker:{color:t.accent},hovertemplate:'%{x}<br>%{y} rows<extra></extra>'};
    }
    Plotly.react(box,[trace],{paper_bgcolor:t.bg,plot_bgcolor:t.bg,font:{color:t.font},bargap:ch.type==='hist'?0.02:0.2,
      margin:{l:10,r:10,t:10,b:10},xaxis:{automargin:true,type:ch.type==='hist'?'linear':'category'},
      yaxis:{automargin:true,title:{text:'rows'}},autosize:true},{responsive:true,displayModeBar:false});
  }
  function show(i){
    var c=_CX[i];
    items.forEach(function(b,j){b.setAttribute('aria-selected',j===i?'true':'false');});
    if(typeof Plotly!=='undefined'){var old=document.getElementById('cx-chart');if(old)Plotly.purge(old);}
    pane.textContent='';
    pane.appendChild(el('h3','',c.name+(c.is_target?' (the target)':'')));
    var dl=el('dl','cx-stats');
    [['kind',c.kind],['values',c.count],['missing',c.nulls+' ('+c.null_pct+'%)'],['distinct',c.unique]].concat(c.stats).forEach(function(s){
      var d=el('div','cx-stat');d.appendChild(el('dt','',s[0]));d.appendChild(el('dd','',fmt(s[1])));dl.appendChild(d);
    });
    pane.appendChild(dl);
    if(c.target){
      var t=c.target;
      pane.appendChild(line('Against the target:',t.strength===null||t.strength===undefined?
        (t.note||'not measured'):t.measure+' '+fmt(t.strength)+(t.measure_note?' — '+t.measure_note:'')));
    }
    if(c.drift){
      var d=c.drift;
      pane.appendChild(line('Against the baseline:',d.drift===null||d.drift===undefined?
        d.measure+' not measured'+(d.note?' — '+d.note:''):d.measure+' '+fmt(d.drift)+(d.reading?' — '+d.reading:'')));
    }
    c.alerts.forEach(function(a){pane.appendChild(line('Alert:',a));});
    draw(c);
  }
  window.cxShow=show;
  if(_CX.length)show(0);
})();
"""


def _theme(theme: str) -> dict:
    if theme == "device":
        return {"device": True, "light": _theme("light"), "dark": _theme("dark")}
    bg, font, accent = theme_plot_colors(theme)
    return {"bg": bg, "font": font, "accent": accent}


def explorer_html(pages: list[dict[str, Any]], theme: str) -> str:
    """The section: a column list, a detail pane, and the script that fills it from the pages."""
    count = len(pages)
    return (
        f"<style>{_CSS}</style>"
        '<div id="explorer" class="section"><h2>Column Explorer</h2>'
        f'<div><p class="chart-note">{count} column{"s" if count != 1 else ""}: pick one to see it on its own.</p>'
        '<div class="cx"><div id="cx-list" class="cx-list" role="listbox" aria-label="Columns"></div>'
        '<div id="cx-detail" class="cx-detail" aria-live="polite"></div></div></div></div>'
        f"<script>const _CX={json_for_script(pages)};const _CXT={json_for_script(_theme(theme))};{_JS}</script>"
    )


def explorer_nav() -> str:
    return f'<a href="#explorer">{_html.escape("Column Explorer")}</a>'
