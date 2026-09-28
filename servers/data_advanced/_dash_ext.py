"""What a dashboard needs to be read by someone who did not build it.

The sweep's pages were one chart per column pair: every bar a column summed,
no ratio metrics, no comparison to anything, no words. This module holds the
parts that turn a page of charts into something a director can be handed:

- metrics (shared/metrics.py) evaluated by the page for every filter, and
  numbers written in their unit -- currency, percent, a ratio;
- KPI cards that compare the last complete period with the one before, and
  with a target, coloured by whether the change is good news (a falling CPC is);
- periods by day, week, month, quarter or year, a year-over-year overlay, and
  event markers on a time series;
- comparison charts: stacked and 100% bars, Pareto, variance against a target,
  a waterfall of what changed, small multiples; gauges and bullets;
- honest statistics on a bar of means: intervals, n, and which groups differ
  from the rest;
- reference lines, bands and annotations; drill-down through a hierarchy;
  click-to-filter; what-if parameters;
- written panels: markdown, insight cards, callouts, dividers, images -- each
  escaped here, once, and never re-drawn;
- the page itself: brand font and logo, no chart toolbars, a 16:9 slide mode
  and print styles.

The JavaScript extends the renderer in _adv_dashboard.py; it adds FIG and HTMLP
kinds and wraps `_ranked`, `getFilt` and `figure` where a panel asks for more.
A panel that asks for none of it is drawn exactly as before.
"""

from __future__ import annotations

import base64
import html
import re
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Written panels: rendered here, escaped, and never touched by the renderer
# ---------------------------------------------------------------------------

_LINK = re.compile(r"\[([^\]]+)\]\((https?://[^\s)]+)\)")
_BOLD = re.compile(r"\*\*(.+?)\*\*")
_ITALIC = re.compile(r"(?<![*\w])\*(?!\s)(.+?)(?<!\s)\*(?![*\w])")
_CODE = re.compile(r"`([^`]+)`")
TONES = ("info", "good", "warn", "bad")
MAX_IMAGE_BYTES = 1_000_000
_IMAGE_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
}
_DATA_IMAGE = re.compile(r"^data:image/(png|jpeg|gif|webp);base64,[A-Za-z0-9+/=\s]+$")


def _inline(text: str) -> str:
    """One line of markdown, escaped first so nothing in it can become markup."""
    out = html.escape(text, quote=True)
    out = _CODE.sub(lambda m: f"<code>{m.group(1)}</code>", out)
    out = _LINK.sub(lambda m: f'<a href="{m.group(2)}" target="_blank" rel="noopener noreferrer">{m.group(1)}</a>', out)
    out = _BOLD.sub(lambda m: f"<b>{m.group(1)}</b>", out)
    return _ITALIC.sub(lambda m: f"<i>{m.group(1)}</i>", out)


def markdown_html(text: str) -> str:
    """The small markdown a note needs: headings, bold, italic, code, links, bullets, numbered lists."""
    blocks: list[str] = []
    items: list[str] = []
    kind = ""
    para: list[str] = []

    def flush_list() -> None:
        nonlocal items, kind
        if items:
            blocks.append(f"<{kind}>" + "".join(f"<li>{i}</li>" for i in items) + f"</{kind}>")
        items, kind = [], ""

    def flush_para() -> None:
        nonlocal para
        if para:
            blocks.append("<p>" + "<br>".join(para) + "</p>")
        para = []

    for raw in str(text).splitlines():
        line = raw.rstrip()
        bullet = re.match(r"^\s*[-*]\s+(.*)$", line)
        number = re.match(r"^\s*\d+[.)]\s+(.*)$", line)
        heading = re.match(r"^(#{1,4})\s+(.*)$", line)
        if bullet or number:
            flush_para()
            want = "ul" if bullet else "ol"
            if kind and kind != want:
                flush_list()
            kind = want
            items.append(_inline((bullet or number).group(1)))  # type: ignore[union-attr]
        elif heading:
            flush_para()
            flush_list()
            blocks.append(f"<h4>{_inline(heading.group(2))}</h4>")
        elif not line.strip():
            flush_para()
            flush_list()
        else:
            flush_list()
            para.append(_inline(line.strip()))
    flush_para()
    flush_list()
    return f'<div class="md">{"".join(blocks)}</div>'


def insight_html(value: str, comparison: str, text: str, tone: str = "info") -> str:
    """An insight card: the number, what it is compared with, and what it means."""
    tone = tone if tone in TONES else "info"
    parts = [f'<div class="ins ins-{tone}">']
    if value:
        parts.append(f'<div class="ins-v">{html.escape(value)}</div>')
    if comparison:
        parts.append(f'<div class="ins-c">{html.escape(comparison)}</div>')
    if text:
        parts.append(markdown_html(text))
    parts.append("</div>")
    return "".join(parts)


def cohort_html(result: dict) -> str:
    """Retention by first period: a row per cohort, the share still active each period after."""
    if not result["cohorts"]:
        return '<p class="ptext">No cohort has a later period to be measured in.</p>'
    head = "".join(f"<th>+{k}</th>" for k in range(len(result["matrix"][0])))
    rows = []
    for cohort, size, shares in zip(result["cohorts"], result["sizes"], result["matrix"], strict=True):
        cells = "".join(
            f'<td class="num" style="background:rgba(0,114,178,{0.08 + 0.5 * v:.2f})">{v:.0%}</td>'
            if v is not None
            else "<td></td>"
            for v in shares
        )
        rows.append(f'<tr><td>{html.escape(cohort)}</td><td class="num">{size:,}</td>{cells}</tr>')
    return (
        f'<div class="cohort-wrap"><p class="ptext">{html.escape(result["note"])}</p>'
        f'<table class="ptable"><thead><tr><th>First {html.escape(result["grain"])}</th><th class="num">'
        f"{html.escape(result['noun'])}s</th>{head}</tr></thead><tbody>{''.join(rows)}</tbody></table></div>"
    )


def callout_html(text: str, tone: str = "info") -> str:
    tone = tone if tone in TONES else "info"
    return f'<div class="callout callout-{tone}">{markdown_html(text)}</div>'


def image_src(src: str, resolve: Any = None) -> str:
    """A data: URI for an image: one given as such, or a file inlined -- the page carries its own logo."""
    src = str(src or "").strip()
    if _DATA_IMAGE.match(src):
        if len(src) > MAX_IMAGE_BYTES * 4 // 3 + 64:
            raise ValueError(f"the image is over {MAX_IMAGE_BYTES // 1000} KB; a page carries a small one")
        return src
    if src.startswith(("http://", "https://")):
        raise ValueError("an image is carried in the page, not fetched: pass a PNG/JPEG/GIF/WebP file or a data: URI")
    path = Path(resolve(src) if resolve else src)
    kind = _IMAGE_TYPES.get(path.suffix.lower())
    if kind is None:
        raise ValueError(
            f"{path.name!r} is not a PNG, JPEG, GIF or WebP image (SVG can carry script, so it is not taken)"
        )
    if not path.is_file():
        raise ValueError(f"image {path.name!r} does not exist")
    data = path.read_bytes()
    if len(data) > MAX_IMAGE_BYTES:
        raise ValueError(
            f"{path.name!r} is {len(data) // 1000} KB; a page carries an image of at most {MAX_IMAGE_BYTES // 1000} KB"
        )
    return f"data:{kind};base64,{base64.b64encode(data).decode('ascii')}"


def image_html(src: str, alt: str) -> str:
    return f'<img class="pimg" src="{html.escape(src, quote=True)}" alt="{html.escape(alt, quote=True)}">'


DIVIDER_HTML = '<div class="cc-div" role="separator"></div>'

# System font stacks only: a web font is a fetch, and the page is carried whole.
FONTS: dict[str, str] = {
    "system": "system-ui,-apple-system,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif",
    "humanist": "'Segoe UI',Candara,'Trebuchet MS',Optima,sans-serif",
    "serif": "Georgia,'Iowan Old Style','Times New Roman',serif",
    "mono": "ui-monospace,SFMono-Regular,Menlo,Consolas,monospace",
    "condensed": "'Arial Narrow','Roboto Condensed','Helvetica Neue',sans-serif",
}

# Okabe-Ito: distinguishable with every common form of colour blindness.
SAFE_PALETTE = ["#0072B2", "#E69F00", "#009E73", "#D55E00", "#56B4E9", "#CC79A7", "#F0E442", "#999999"]

EXT_CSS = """
.kpi-delta{font-size:.8125rem;margin-top:.25rem;font-weight:600}
.kpi-delta span{font-weight:400;color:var(--text-muted)}
.kpi-delta.good{color:var(--green,#2da44e)}.kpi-delta.bad{color:var(--red,#cf222e)}.kpi-delta.flat{color:var(--text-muted)}
.md{font-size:.875rem;line-height:1.55;color:var(--text)}.md p{margin:.25rem 0 .5rem}.md ul,.md ol{margin:.25rem 0 .5rem 1.1rem;padding:0}
.md h4{margin:.25rem 0 .375rem;font-size:1.25rem;line-height:1.3}.md a{color:var(--accent)}.md code{font-size:.8125rem;padding:0 .2rem;border-radius:4px;background:rgba(127,127,127,.15)}
.ins-v{font-size:clamp(1.5rem,2.6vw,2rem);font-weight:700;color:var(--accent);line-height:1.15}
.ins-c{font-size:.8125rem;color:var(--text-muted);margin:.125rem 0 .375rem}
.ins{border-left:4px solid var(--accent);padding-left:.75rem}.ins-good{border-color:var(--green,#2da44e)}.ins-warn{border-color:var(--orange,#bf8700)}.ins-bad{border-color:var(--red,#cf222e)}
.callout{border-radius:8px;padding:.625rem .875rem;border:1px solid var(--border)}
.callout-info{background:rgba(9,105,218,.08)}.callout-good{background:rgba(45,164,78,.1)}.callout-warn{background:rgba(191,135,0,.12)}.callout-bad{background:rgba(207,34,46,.1)}
.cc-div{grid-column:1/-1;border-top:1px solid var(--border);margin:.25rem 0}
.pimg{max-width:100%;max-height:100%;object-fit:contain;display:block;margin:auto}
.dash-logo{height:2rem;width:auto;margin-right:.75rem;vertical-align:middle}
.clk-chip{display:inline-flex;align-items:center;gap:.25rem;margin:0 .375rem .25rem 0;padding:.125rem .5rem;border-radius:999px;border:1px solid var(--accent);font-size:.75rem;cursor:pointer;background:transparent;color:var(--text)}
.params{display:flex;flex-wrap:wrap;gap:1rem;align-items:center;margin:.25rem 0 .5rem}.params label{font-size:.8125rem;color:var(--text-muted)}
.params input[type=range]{vertical-align:middle;width:9rem}.params output{font-weight:600;color:var(--text);margin-left:.25rem}
.drill-back{margin-left:auto;font-size:.75rem;border:1px solid var(--border);border-radius:6px;background:transparent;color:var(--text);cursor:pointer;padding:.125rem .5rem}
.provn{font-size:.6875rem;color:var(--text-muted);padding:.25rem .75rem .5rem}
body.slide .cgrid{aspect-ratio:16/9;overflow:hidden}
.cgrid.g12{grid-auto-flow:row dense}
.cc-hdr .png{margin-left:auto;margin-right:.375rem;font:600 .625rem/1 system-ui,sans-serif;letter-spacing:.04em;
  padding:.25rem .4rem;border:1px solid var(--border,#d0d7de);border-radius:.3rem;background:transparent;
  color:var(--text-muted,#636c76);cursor:pointer}
.cc-hdr .png:hover{color:var(--accent,#0969da);border-color:var(--accent,#0969da)}
.cc-hdr .png+.exp{margin-left:0}
/* A retention matrix is read whole, not scrolled. */
.cc-body--auto:has(> .cohort-wrap){max-height:none}
/* The filters as a column down the left, on a screen wide enough for one. */
@media(min-width:68.75rem){
  body.sidebar .filter-bar{position:fixed;top:0;left:0;bottom:0;width:16rem;overflow:auto;display:flex;flex-direction:column;
    align-items:stretch;gap:.75rem;border-right:1px solid var(--border,#d0d7de);border-bottom:0;z-index:5}
  body.sidebar>*:not(.filter-bar):not(script):not(style){margin-left:16rem}
}
@media print{
  .fbar,.filterbar,.params,.tabs,.tab-bar,button,.exp,.drill-back,#back-to-top{display:none!important}
  .cc{break-inside:avoid;page-break-inside:avoid}
  .cgrid > *{display:block!important}
  body{background:#fff!important;color:#000!important}
}
"""

# ---------------------------------------------------------------------------
# The renderer's extensions
# ---------------------------------------------------------------------------

EXT_JS = r"""
// A page for reading, not exploring, carries no chart toolbars.
if(_STYLE.toolbar===false)PCFG.displayModeBar=false;
// --- a large page's rows are cells ------------------------------------------
// Above 100,000 rows each embedded row is a cell of the server's cube
// (shared/cube.py): a measure's sum, with its count, min and max beside it,
// and the rows behind the cell in __n. A cell is read as {s,n,lo,hi}.
function _isna(v){return v===null||v===undefined||(typeof v==='number'&&isNaN(v));}
function _cell(r,c){
  var s=_num(r[c]);if(!_CUBE||isNaN(s))return s;
  return{s:s,n:+r[c+'#n']||0,lo:_num(r[c+'#lo']),hi:_num(r[c+'#hi'])};
}
function _rows(d){if(!_CUBE)return d.length;var n=0;for(var i=0;i<d.length;i++)n+=+d[i].__n||0;return n;}
function _w(r){return _CUBE?(+r.__n||0):1;}
var _agg0=_agg;
_agg=function(v,how){
  if(!_CUBE||!v.length||typeof v[0]!=='object'||v[0]===null)return _agg0(v,how);
  var x=v.filter(function(c){return c&&!isNaN(c.s);});
  if(how==='count')return x.reduce(function(a,c){return a+c.n;},0);
  if(!x.length)return 0;
  if(how==='mean'){var s=0,n=0;x.forEach(function(c){s+=c.s;n+=c.n;});return n?s/n:0;}
  if(how==='min')return x.reduce(function(a,c){return c.lo<a?c.lo:a;},Infinity);
  if(how==='max')return x.reduce(function(a,c){return c.hi>a?c.hi:a;},-Infinity);
  return x.reduce(function(a,c){return a+c.s;},0);
};
// --- metrics, units and periods -----------------------------------------
// A metric is a tree compiled by shared/metrics.py: aggregates of columns,
// + - * /, numbers and what-if parameters. evaluate_tree there walks the same
// tree, so an insight quoting a metric and the card drawing it agree.
function _mval(n,d){
  if(n.k!==undefined)return n.k;
  if(n.param!==undefined)return +_PARAMS[n.param];
  if(n.op){
    var a=_mval(n.a,d);if(n.op==='neg')return -a;if(n.op==='abs')return Math.abs(a);
    var b=_mval(n.b,d);
    if(n.op==='+')return a+b;if(n.op==='-')return a-b;if(n.op==='*')return a*b;
    return b?a/b:NaN;
  }
  if(n.agg==='count'&&!n.col)return _rows(d);
  return _agg(d.map(function(r){return _cell(r,n.col);}),n.agg);
}
function _measure(p,d){
  if(p.metric&&_METRICS[p.metric])return _mval(_METRICS[p.metric].tree,d);
  return _agg(d.map(function(r){return _cell(r,p.value);}),p.agg||'sum');
}
function _rgroups(d,keyOf){
  var m=new Map();
  d.forEach(function(r){var k=keyOf(r);if(k===null||k===undefined)return;if(!m.has(k))m.set(k,[]);m.get(k).push(r);});
  return m;
}
function _unit(p){return(p.metric&&_METRICS[p.metric])?_METRICS[p.metric].unit:(p.unit||'');}
function _better(p){return(p.metric&&_METRICS[p.metric])?_METRICS[p.metric].better:(p.better===undefined?'up':p.better);}
// A value in its unit: a share as a percentage, a ratio as 2.4x, money with the page's currency.
function _fmtu(v,p){
  var s=p.style||{},u=_unit(p);
  if(v===null||v===undefined||!isFinite(v))return'–';
  if(s.format)return _fmtv(v,s);
  if(u==='percent')return(v*100).toFixed(Math.abs(v)<0.1?2:1)+'%';
  if(u==='ratio')return v.toFixed(2)+'×';
  if(u==='currency'){var c=s.prefix||_STYLE.currency||'';return c+(Math.abs(v)>=1e4?_fmt(v):v.toLocaleString('en-US',{maximumFractionDigits:2}))+(s.suffix||'');}
  return _fmtv(v,s);
}
function _utick(p){
  var u=_unit(p);
  if((p.style||{}).format)return _vaxis(p.style);
  if(u==='percent')return{tickformat:'.1%'};
  if(u==='ratio')return{ticksuffix:'×'};
  if(u==='currency'&&_STYLE.currency)return{tickprefix:_STYLE.currency};
  return _vaxis(p.style||{});
}
// The period a date falls in; dates arrive as YYYY-MM-DD text.
function _bucket(v,grain){
  if(v===null||v===undefined||v==='')return null;
  var s=String(v);
  if(grain==='year')return s.substring(0,4);
  if(grain==='month')return s.substring(0,7);
  if(grain==='quarter'){var m=+s.substring(5,7);return s.substring(0,4)+'-Q'+(Math.floor((m-1)/3)+1);}
  if(grain==='week'){var d=new Date(s.substring(0,10)+'T00:00:00Z');if(isNaN(d))return null;d.setUTCDate(d.getUTCDate()-((d.getUTCDay()+6)%7));return d.toISOString().substring(0,10);}
  return s.substring(0,10);
}
// The last complete period and the one before it, as a panel's rows hold them.
function _lastTwo(p,d){
  var g=_rgroups(d,function(r){return _bucket(r[p.date],p.grain||'month');});
  var keys=Array.from(g.keys()).filter(function(k){return!p.complete||k<=p.complete;}).sort();
  if(keys.length<2)return null;
  var cur=keys[keys.length-1],prev=keys[keys.length-2];
  return{cur:cur,prev:prev,a:_measure(p,g.get(cur)),b:_measure(p,g.get(prev))};
}
// A change, said in the unit's terms and coloured by whether it is good news.
function _delta(p,a,b,label){
  if(!isFinite(a)||!isFinite(b))return'';
  var u=_unit(p),txt,d1;
  if(u==='percent'){d1=+((a-b)*100).toFixed(1);txt=(d1>=0?'+':'')+d1.toFixed(1)+' pp';}
  else if(b===0)return'';
  else{d1=+((a-b)/Math.abs(b)*100).toFixed(1);txt=(d1>=0?'+':'')+d1.toFixed(1)+'%';}
  // A change that rounds to nothing is shown as none, not as a red "-0.0".
  if(d1===0){txt=u==='percent'?'0.0 pp':'0.0%';}
  var bt=_better(p),cls=d1===0||!bt?'flat':((bt==='up')===(a>b)?'good':'bad');
  return'<div class="kpi-delta '+cls+'">'+(d1===0?'■':a>b?'▲':'▼')+' '+_esc(txt)+' <span>'+_esc(label)+'</span></div>';
}

HTMLP.kpi=function(p,d){
  var s=p.style,v,sub='over '+_rows(d).toLocaleString('en-US')+' rows',out=[];
  var two=(p.date)?_lastTwo(p,d):null;
  if(s.period==='last'&&two){v=two.a;sub='in '+two.cur;}else{v=_measure(p,d);}
  if(two)out.push(_delta(p,two.a,two.b,two.cur+' vs '+two.prev));
  if(s.target!==undefined)out.push(_delta(p,v,+s.target,'vs target '+_fmtu(+s.target,p)));
  return'<div class="kpi-big"'+(s.color?' style="color:'+_esc(s.color)+'"':'')+'>'+_esc(_fmtu(v,p))+'</div>'+out.join('')
    +'<div class="kpi-sub">'+_esc(sub)+'</div>';
};

// Ranked groups with a metric, or with the rest folded into "Other".
var _ranked0=_ranked;
_ranked=function(p,d){
  var s=p.style;
  if(!p.metric&&!s.other)return _ranked0(p,d);
  var how=p.agg||'sum',g=_rgroups(d,function(r){return _key(r,p.category);});
  var e=Array.from(g,function(x){return[x[0],_measure(p,x[1]),x[1]];}).filter(function(x){return isFinite(x[1]);});
  var asc=function(x,y){return x[1]-y[1];},desc=function(x,y){return y[1]-x[1];};
  e.sort(how==='min'?asc:desc);
  // No top_n keeps every group: slice(undefined) would fold them all into "Other" a second time.
  var n=s.top_n||e.length,keep=e.slice(0,n),rest=e.slice(n);
  if(s.other&&rest.length){
    var rows=[];rest.forEach(function(x){rows=rows.concat(x[2]);});
    keep.push(['Other ('+rest.length+')',_measure(p,rows),rows]);
  }
  if(s.sort==='asc')keep.sort(asc);else if(s.sort==='desc')keep.sort(desc);
  else if(s.sort==='label')keep.sort(function(x,y){return String(x[0]).localeCompare(String(y[0]));});
  return keep.map(function(x){return[x[0],x[1]];});
};

// --- a bar of means says how sure it is ----------------------------------
function _tcrit(df){return df>=1?1.96+2.37/df+2.8/(df*df):NaN;}
function _stats(v){
  var x=v.filter(function(a){return!isNaN(a);}),n=x.length;if(!n)return{n:0,m:NaN,sd:NaN};
  var m=x.reduce(function(a,b){return a+b;},0)/n,ss=0;x.forEach(function(a){ss+=(a-m)*(a-m);});
  return{n:n,m:m,sd:n>1?Math.sqrt(ss/(n-1)):0};
}
var _bar0=FIG.bar;
FIG.bar=function(p,d){
  var s=p.style,f;
  if(s.ci||s.show_n||s.significance){
    var g=_groups(d,function(r){return _key(r,p.category);},p.value),all=[];
    g.forEach(function(v){all=all.concat(v);});
    var e=Array.from(g,function(x){var st=_stats(x[1]);return[x[0],st,x[1]];}).sort(function(a,b){return b[1].m-a[1].m;}).slice(0,s.top_n);
    var labels=e.map(function(x){
      var t=_fmtu(x[1].m,p);
      if(s.significance&&x[1].n>=2){
        var rest=all.slice(),seen=0;x[2].forEach(function(v){var i=rest.indexOf(v);if(i>=0)rest.splice(i,1);});
        var o=_stats(rest),se=Math.sqrt(x[1].sd*x[1].sd/x[1].n+(o.n>1?o.sd*o.sd/o.n:0));
        if(o.n>=2&&se>0&&Math.abs((x[1].m-o.m)/se)>_tcrit(Math.min(x[1].n,o.n)-1))t+=' *';
      }
      if(s.show_n)t+=' (n='+x[1].n.toLocaleString('en-US')+')';
      return t;
    });
    var t={x:e.map(function(x){return x[0];}),y:e.map(function(x){return x[1].m;}),type:'bar',marker:{color:s.color,opacity:0.85},text:labels,textposition:'outside',
      customdata:e.map(function(x){return x[1].n;}),hovertemplate:'%{x}: %{y:.4g} (n=%{customdata})<extra></extra>'};
    if(s.ci)t.error_y={type:'data',array:e.map(function(x){return x[1].n>1?_tcrit(x[1].n-1)*x[1].sd/Math.sqrt(x[1].n):0;}),visible:true,thickness:1.2};
    f={data:[t],layout:_axes({yaxis:_merge({title:'mean '+p.value+(s.ci?' (95% CI)':'')},_utick(p))})};
    if(s.significance)f.layout.annotations=[{text:'* differs from the other groups (p<0.05)',xref:'paper',yref:'paper',x:0,y:1.08,showarrow:false,font:{size:10}}];
  }else if(s.orientation==='h'){
    // Long names read across, largest on top, each bar labelled in its unit.
    var e=_ranked(p,d),named=e.map(function(i){return _catColor(p,i[0]);});
    var th={y:e.map(function(i){return i[0];}),x:e.map(function(i){return i[1];}),type:'bar',orientation:'h',cliponaxis:false,textangle:0,
      marker:{color:named.some(Boolean)?named.map(function(c){return c||s.color;}):s.color,opacity:0.85},
      hovertemplate:'%{y}: %{text}<extra></extra>',text:e.map(function(i){return _fmtu(i[1],p);})};
    // Labels sit past the bar end, so a thin bar keeps a readable one; the
    // axis leaves them room when every bar is positive.
    th.textposition=s.value_labels!==false?'outside':'none';
    var xs=e.map(function(i){return i[1];}),mx=Math.max.apply(null,xs.concat([0])),mn=Math.min.apply(null,xs.concat([0]));
    var xa=mn>=0&&mx>0?_merge(_utick(p),{range:[0,mx*1.18]}):_utick(p);
    f={data:[th],layout:_axes({margin:{l:10,r:30,t:10,b:40},xaxis:xa,yaxis:{autorange:'reversed',automargin:true,gridcolor:_T().grid}})};
  }else{
    f=_bar0(p,d);
    if(p.metric){f.data[0].text=_ranked(p,d).map(function(i){return _fmtu(i[1],p);});f.layout.yaxis=_merge(f.layout.yaxis||{},_utick(p));}
  }
  return f;
};

// --- time: a grain, a year-over-year overlay, events ----------------------
// The periods after `last`, as the page labels them.
function _after(last,grain,h){
  var out=[],y=+last.substring(0,4);
  for(var i=1;i<=h;i++){
    if(grain==='year')out.push(String(y+i));
    else if(grain==='quarter'){var q=+last.substring(6)+i-1;out.push((y+Math.floor(q/4))+'-Q'+(q%4+1));}
    else if(grain==='month'){var m=+last.substring(5,7)-1+i;out.push((y+Math.floor(m/12))+'-'+String(m%12+1).padStart(2,'0'));}
    else{var d=new Date(last+'T00:00:00Z');d.setUTCDate(d.getUTCDate()+(grain==='week'?7:1)*i);out.push(d.toISOString().substring(0,10));}
  }
  return out;
}
// A straight-line forecast from the complete periods, with its 80% band: the
// trend a reader would draw by eye, and how far off the line the past has run.
// A series that has never gone below zero (spend, orders) is not forecast below it.
function _forecast(keys,vals,grain,h,complete){
  var k=[],v=[];
  keys.forEach(function(x,i){if((!complete||x<=complete)&&isFinite(vals[i])){k.push(x);v.push(vals[i]);}});
  k=k.slice(-24);v=v.slice(-24);
  var n=v.length;if(n<6)return null;
  var mt=(n-1)/2,mv=v.reduce(function(a,b){return a+b;},0)/n,sxx=0,sxy=0;
  v.forEach(function(y,i){sxx+=(i-mt)*(i-mt);sxy+=(i-mt)*(y-mv);});
  var b=sxy/sxx,a=mv-b*mt,se=0;
  v.forEach(function(y,i){var e=y-(a+b*i);se+=e*e;});
  var s=Math.sqrt(se/Math.max(n-2,1)),xs=_after(k[n-1],grain,h),mid=[],lo=[],hi=[];
  var floor=v.every(function(y){return y>=0;})?0:-Infinity;
  for(var j=1;j<=h;j++){var t=n-1+j,f=a+b*t,w=1.2816*s*Math.sqrt(1+1/n+(t-mt)*(t-mt)/sxx);mid.push(Math.max(floor,f));lo.push(Math.max(floor,f-w));hi.push(Math.max(floor,f+w));}
  var from=[k[n-1]].concat(xs),at=[v[n-1]];
  return[
    {x:from,y:at.concat(lo),type:'scatter',mode:'lines',line:{width:0},showlegend:false,hoverinfo:'skip'},
    {x:from,y:at.concat(hi),type:'scatter',mode:'lines',line:{width:0},fill:'tonexty',fillcolor:'rgba(127,127,127,0.18)',name:'80% range'},
    {x:from,y:at.concat(mid),type:'scatter',mode:'lines',line:{dash:'dash',width:2,color:'#8b949e'},name:'forecast'}
  ];
}
FIG.ts=function(p,d){
  var s=p.style,grain=s.grain||p.grain||'month',w=s.ma;
  if(s.yoy){
    var byYear=_rgroups(d,function(r){var b=_bucket(r[p.date],'month');return b?b.substring(0,4):null;});
    var names=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    var tr=Array.from(byYear.keys()).sort().map(function(y,i){
      var m=_rgroups(byYear.get(y),function(r){return _bucket(r[p.date],'month').substring(5,7);});
      var ks=Array.from(m.keys()).sort();
      return{x:ks.map(function(k){return names[+k-1];}),y:ks.map(function(k){return _measure(p,m.get(k));}),type:'scatter',mode:'lines+markers',name:y,line:{width:2,color:_pal(p)[i%_pal(p).length]}};
    });
    return{data:tr,layout:_axes(_merge({xaxis:{title:'Month',categoryorder:'array',categoryarray:names},yaxis:_merge({title:p.metric||p.value},_utick(p))},_legend(s,{showlegend:true,legend:{x:0,y:1.1,orientation:'h'}})))};
  }
  var g=_rgroups(d,function(r){return _bucket(r[p.date],grain);});
  var keys=Array.from(g.keys()).sort(),vals=keys.map(function(k){return _measure(p,g.get(k));});
  var t=[{x:keys,y:vals,type:'scatter',mode:'lines+markers',name:p.metric||p.value,line:{color:s.color,width:2},marker:{size:4}}];
  if(w>0){
    var ma=vals.map(function(_,i){if(i<w-1)return null;var a=0;for(var j=i-w+1;j<=i;j++)a+=vals[j];return a/w;});
    t.push({x:keys.slice(w-1),y:ma.slice(w-1),type:'scatter',mode:'lines',name:w+'-period MA',line:{color:s.accent,width:2,dash:'dot'}});
  }
  var lay=_axes(_merge({xaxis:{title:'Date'},yaxis:_merge({title:p.metric||p.value},_utick(p))},_legend(s,{showlegend:true,legend:{x:0,y:1.1,orientation:'h'}})));
  // A period the data has only begun is drawn apart, so a half month is not read as a fall.
  if(p.complete&&keys.length&&keys[keys.length-1]>p.complete){
    var k=keys.length-1;t.push({x:[keys[k]],y:[vals[k]],type:'scatter',mode:'markers',name:'incomplete period',marker:{size:9,symbol:'circle-open',color:s.color}});
  }
  if(s.forecast>0){var fc=_forecast(keys,vals,grain,s.forecast,p.complete);if(fc)t=t.concat(fc);}
  if(s.events&&s.events.length){
    lay.shapes=(lay.shapes||[]).concat(s.events.map(function(e){var x=_bucket(e.date,grain);return{type:'line',xref:'x',yref:'paper',x0:x,x1:x,y0:0,y1:1,line:{dash:'dot',width:1,color:'#8b949e'}};}));
    lay.annotations=(lay.annotations||[]).concat(s.events.map(function(e){return{x:_bucket(e.date,grain),y:1,xref:'x',yref:'paper',text:_esc(e.label),showarrow:false,yanchor:'bottom',font:{size:10}};}));
  }
  return{data:t,layout:lay};
};

// --- comparisons ------------------------------------------------------------
FIG.stacked=function(p,d){
  var s=p.style,cats=_ranked(_merge(p,{style:_merge(s,{other:false,sort:'desc'})}),d).map(function(i){return i[0];});
  var groups=_rgroups(d,function(r){return _key(r,p.group);});
  var names=Array.from(groups,function(x){return[x[0],_measure(p,x[1])];}).sort(function(a,b){return b[1]-a[1];}).slice(0,s.series||10).map(function(x){return x[0];});
  var t=names.map(function(gname,i){
    var byCat=_rgroups(groups.get(gname),function(r){return _key(r,p.category);});
    return{x:cats,y:cats.map(function(c){return byCat.has(c)?_measure(p,byCat.get(c)):0;}),type:'bar',name:gname,marker:{color:_seriesColor(p,gname,i)}};
  });
  var lay=_axes(_merge({barmode:'stack',yaxis:s.normalize?{title:'share',ticksuffix:'%'}:_merge({},_utick(p))},_legend(s,{showlegend:true,legend:{orientation:'h',x:0,y:1.12}})));
  if(s.normalize)lay.barnorm='percent';
  return{data:t,layout:lay};
};
FIG.pareto=function(p,d){
  var e=_ranked(_merge(p,{style:_merge(p.style,{sort:'desc',other:false})}),d),tot=0,run=0;
  var all=_measure(p,d);tot=isFinite(all)&&all?all:e.reduce(function(a,b){return a+b[1];},0);
  var cum=e.map(function(i){run+=i[1];return run/tot*100;});
  return{data:[{x:e.map(function(i){return i[0];}),y:e.map(function(i){return i[1];}),type:'bar',name:p.metric||p.value,marker:{color:p.style.color,opacity:0.85}},
               {x:e.map(function(i){return i[0];}),y:cum,type:'scatter',mode:'lines+markers',yaxis:'y2',name:'cumulative %',line:{color:p.style.accent,width:2}}],
         layout:_axes({yaxis:_merge({title:p.metric||p.value},_utick(p)),yaxis2:{overlaying:'y',side:'right',range:[0,105],ticksuffix:'%',showgrid:false},
           shapes:[{type:'line',xref:'paper',yref:'y2',x0:0,x1:1,y0:80,y1:80,line:{dash:'dot',width:1,color:'#8b949e'}}],showlegend:true,legend:{orientation:'h',x:0,y:1.12}})};
};
FIG.variance=function(p,d){
  var s=p.style,e=_ranked(p,d),bt=_better(p);
  var tg=_rgroups(d,function(r){return _key(r,p.category);});
  var rows=e.map(function(i){
    var target=p.target?_agg(tg.get(i[0]).map(function(r){return _num(r[p.target]);}),'sum'):+s.target;
    return[i[0],i[1],target,i[1]-target];
  });
  var col=rows.map(function(r){var good=bt==='down'?r[3]<=0:r[3]>=0;return good?'#2da44e':'#cf222e';});
  return{data:[{x:rows.map(function(r){return r[0];}),y:rows.map(function(r){return r[3];}),type:'bar',marker:{color:col},
    customdata:rows.map(function(r){return[_fmtu(r[1],p),_fmtu(r[2],p)];}),hovertemplate:'%{x}: actual %{customdata[0]}, target %{customdata[1]}<extra></extra>',
    text:rows.map(function(r){return(r[3]>=0?'+':'')+_fmtu(r[3],p);}),textposition:'outside'}],
    layout:_axes({yaxis:_merge({title:'actual − target',zeroline:true},_utick(p))})};
};
FIG.waterfall=function(p,d){
  var s=p.style,x=[],y=[],m=[],label=function(v){return _fmtu(v,p);};
  if(p.date){
    var two=_lastTwo(p,d);if(!two)return null;
    var g=_rgroups(d,function(r){return _bucket(r[p.date],p.grain||'month');}),A=_rgroups(g.get(two.cur),function(r){return _key(r,p.category);}),B=_rgroups(g.get(two.prev),function(r){return _key(r,p.category);});
    var keys=Array.from(new Set(Array.from(A.keys()).concat(Array.from(B.keys()))));
    var ch=keys.map(function(k){return[k,(A.has(k)?_measure(p,A.get(k)):0)-(B.has(k)?_measure(p,B.get(k)):0)];}).sort(function(a,b){return Math.abs(b[1])-Math.abs(a[1]);});
    var top=ch.slice(0,s.top_n||8),rest=ch.slice(s.top_n||8).reduce(function(a,b){return a+b[1];},0);
    x.push(two.prev);y.push(two.b);m.push('absolute');
    top.forEach(function(c){x.push(c[0]);y.push(c[1]);m.push('relative');});
    if(rest)x.push('Other'),y.push(rest),m.push('relative');
    x.push(two.cur);y.push(two.a);m.push('total');
  }else{
    _ranked(_merge(p,{style:_merge(s,{other:true})}),d).forEach(function(i){x.push(i[0]);y.push(i[1]);m.push('relative');});
    x.push('Total');y.push(_measure(p,d));m.push('total');
  }
  return{data:[{type:'waterfall',x:x,y:y,measure:m,text:y.map(label),textposition:'outside',connector:{line:{color:'#8b949e',width:1}},
    increasing:{marker:{color:'#2da44e'}},decreasing:{marker:{color:'#cf222e'}},totals:{marker:{color:s.color||'#0072B2'}}}],layout:_axes({xaxis:{type:'category'},yaxis:_utick(p),showlegend:false})};
};
FIG.multiples=function(p,d){
  var s=p.style,f=_rgroups(d,function(r){return _key(r,p.facet);});
  var names=Array.from(f,function(x){return[x[0],_rows(x[1])];}).sort(function(a,b){return b[1]-a[1];}).slice(0,s.top_n||9).map(function(x){return x[0];});
  var cols=Math.min(3,names.length),rows=Math.ceil(names.length/cols),t=[],ann=[],lay={showlegend:false,margin:{l:40,r:10,t:30,b:30}};
  var gx=0.06,gy=0.16;
  names.forEach(function(nm,i){
    var c=i%cols,r=Math.floor(i/cols),x0=c/cols+(c?gx/2:0),x1=(c+1)/cols-(c<cols-1?gx/2:0),y1=1-r/rows-(r?gy/2:0)-0.06,y0=1-(r+1)/rows+(r<rows-1?gy/2:0);
    var ax=i?String(i+1):'',rowsOf=f.get(nm),xs,ys;
    if(p.date){var g=_rgroups(rowsOf,function(r){return _bucket(r[p.date],s.grain||p.grain||'month');});xs=Array.from(g.keys()).sort();ys=xs.map(function(k){return _measure(p,g.get(k));});}
    else{var e=_ranked(p,rowsOf);xs=e.map(function(q){return q[0];});ys=e.map(function(q){return q[1];});}
    t.push({x:xs,y:ys,type:p.date?'scatter':'bar',mode:p.date?'lines':undefined,xaxis:'x'+ax,yaxis:'y'+ax,line:{color:_seriesColor(p,nm,i),width:2},marker:{color:_seriesColor(p,nm,i)}});
    lay['xaxis'+ax]={domain:[x0,x1],anchor:'y'+ax,gridcolor:_T().grid,tickfont:{size:9},type:p.date?undefined:'category'};
    lay['yaxis'+ax]=_merge({domain:[y0,y1],anchor:'x'+ax,gridcolor:_T().grid,tickfont:{size:9}},_utick(p));
    ann.push({text:'<b>'+_esc(nm)+'</b>',xref:'paper',yref:'paper',x:x0,y:y1+0.01,showarrow:false,xanchor:'left',yanchor:'bottom',font:{size:11}});
  });
  lay.annotations=ann;
  return{data:t,layout:lay};
};
FIG.gauge=function(p,d){
  var s=p.style,v=_measure(p,d),target=s.target!==undefined?+s.target:null,hi=s.max!==undefined?+s.max:Math.max(v,target||0)*1.25||1;
  var pct=_unit(p)==='percent';
  var ind={type:'indicator',mode:'gauge+number'+(target!==null?'+delta':''),value:pct?v*100:v,
    number:{suffix:pct?'%':'',valueformat:pct?'.2f':',.4~g',prefix:_unit(p)==='currency'?(_STYLE.currency||''):''},
    gauge:{shape:p.type==='bullet'?'bullet':'angular',axis:{range:[s.min!==undefined?(pct?s.min*100:+s.min):0,pct?hi*100:hi]},bar:{color:s.color||'#0072B2'}}};
  if(target!==null){ind.delta={reference:pct?target*100:target,increasing:{color:_better(p)==='down'?'#cf222e':'#2da44e'},decreasing:{color:_better(p)==='down'?'#2da44e':'#cf222e'}};
    ind.gauge.threshold={line:{color:'#cf222e',width:3},thickness:0.8,value:pct?target*100:target};}
  return{data:[ind],layout:{margin:{l:30,r:30,t:20,b:20}}};
};
FIG.bullet=FIG.gauge;

// --- reference lines, bands and notes, on any chart that asks -------------
var _figure0=figure;
figure=function(p,d){
  var f=_figure0(p,d);if(!f)return f;
  var s=p.style,h=s.orientation==='h',sh=f.layout.shapes||[],an=f.layout.annotations||[];
  (s.ref_lines||[]).forEach(function(r){
    sh.push(h?{type:'line',xref:'x',yref:'paper',x0:r.value,x1:r.value,y0:0,y1:1,line:{dash:'dash',width:1.5,color:r.color||'#8b949e'}}
             :{type:'line',xref:'paper',yref:'y',x0:0,x1:1,y0:r.value,y1:r.value,line:{dash:'dash',width:1.5,color:r.color||'#8b949e'}});
    if(r.label)an.push(h?{x:r.value,y:1,xref:'x',yref:'paper',text:_esc(r.label),showarrow:false,yanchor:'bottom',font:{size:10}}
                        :{x:1,y:r.value,xref:'paper',yref:'y',text:_esc(r.label),showarrow:false,xanchor:'right',yanchor:'bottom',font:{size:10}});
  });
  (s.bands||[]).forEach(function(b){
    sh.push(h?{type:'rect',xref:'x',yref:'paper',x0:b.from,x1:b.to,y0:0,y1:1,fillcolor:b.color||'rgba(127,127,127,0.12)',line:{width:0},layer:'below'}
             :{type:'rect',xref:'paper',yref:'y',x0:0,x1:1,y0:b.from,y1:b.to,fillcolor:b.color||'rgba(127,127,127,0.12)',line:{width:0},layer:'below'});
    if(b.label)an.push({x:0,y:b.to,xref:'paper',yref:'y',text:_esc(b.label),showarrow:false,xanchor:'left',yanchor:'top',font:{size:10}});
  });
  (s.annotations||[]).forEach(function(a){an.push({x:a.x,y:a.y===undefined?null:a.y,yref:a.y===undefined?'paper':'y',text:_esc(a.text),showarrow:a.y!==undefined,arrowhead:2,font:{size:10}});});
  if(s.x_title)f.layout.xaxis=_merge(f.layout.xaxis||{},{title:s.x_title});
  if(s.y_title)f.layout.yaxis=_merge(f.layout.yaxis||{},{title:s.y_title});
  if(s.y_range)f.layout.yaxis=_merge(f.layout.yaxis||{},{range:s.y_range});
  f.layout.shapes=sh;f.layout.annotations=an;
  return f;
};

// --- click a bar to filter by it; click a parent to drill into its children --
var _CLK={},_DRILL={};
var _getFilt0=getFilt;
getFilt=function(){
  var d=_getFilt0(),ks=Object.keys(_CLK);
  return ks.length?d.filter(function(r){return ks.every(function(c){return _key(r,c)===_CLK[c];});}):d;
};
function _chips(){
  var el=document.getElementById('clk-chips');if(!el)return;
  el.innerHTML=Object.keys(_CLK).map(function(c){return'<button class="clk-chip" data-col="'+_esc(c)+'">'+_esc(c)+' = '+_esc(_CLK[c])+' ✕</button>';}).join('');
  el.querySelectorAll('.clk-chip').forEach(function(b){b.addEventListener('click',function(){delete _CLK[b.getAttribute('data-col')];_chips();applyF();});});
}
var _rowsFor0=_rowsFor;
_rowsFor=function(p,d){
  var rows=_rowsFor0(p,d),dv=_DRILL[p.id];
  if(dv===undefined)return rows;
  return rows.filter(function(r){return _key(r,p._parent||p.category)===dv;});
};
var _render0=renderPanel;
renderPanel=function(p,d){
  var dv=_DRILL[p.id],q=p;
  if(dv!==undefined&&p.style.drill)q=_merge(p,{category:p.style.drill,_parent:p.category});
  _render0(q,d);
  var el=document.getElementById(p.id);if(!el||el._bound||!el.on)return;
  el._bound=true;
  el.on('plotly_click',function(ev){
    var pt=ev&&ev.points&&ev.points[0];if(!pt)return;
    var v=String(p.style.orientation==='h'?pt.y:(pt.label!==undefined?pt.label:pt.x));
    if(p.style.drill&&_DRILL[p.id]===undefined){_DRILL[p.id]=v;_backBtn(p);applyF();return;}
    if(!_CROSS||!p.category||v.indexOf('Other (')===0)return;
    var col=_DRILL[p.id]!==undefined?p.style.drill:p.category;
    if(_CLK[col]===v)delete _CLK[col];else _CLK[col]=v;
    _chips();applyF();
  });
};
function _backBtn(p){
  var el=document.getElementById(p.id);if(!el)return;var hdr=el.closest('.cc');hdr=hdr&&hdr.querySelector('.cc-hdr');if(!hdr)return;
  var b=document.createElement('button');b.className='drill-back';b.textContent='◀ '+_DRILL[p.id];
  b.addEventListener('click',function(){delete _DRILL[p.id];b.remove();applyF();});
  hdr.appendChild(b);
}

// --- a panel that plots rows reads the sample, filtered as the page is --------
var _ROWLEVEL={scatter:1,cscat:1,box:1,corr:1,dist:1,geo_scatter:1};
function _sampled(p){return _CUBE&&(_ROWLEVEL[p.type]||(p.type==='bar'&&(p.style.ci||p.style.show_n||p.style.significance)));}
var _render1=renderPanel;
renderPanel=function(p,d){
  if(_sampled(p)){
    var s=_narrow(_SAMPLE,_on().filter(function(f){return!f.scope;})),ks=Object.keys(_CLK);
    if(ks.length)s=s.filter(function(r){return ks.every(function(c){return _key(r,c)===_CLK[c];});});
    d=_rowsFor(p,s);
  }
  _render1(p,d);
};

// --- a chart saved as a picture, for a slide or a message ------------------------
document.querySelectorAll('[data-png]').forEach(function(b){
  b.addEventListener('click',function(){
    var el=document.getElementById(b.getAttribute('data-png'));
    if(el&&window.Plotly&&Plotly.downloadImage)Plotly.downloadImage(el,{format:'png',width:1400,height:800,scale:2,
      filename:(b.getAttribute('data-png-name')||'chart').replace(/[^A-Za-z0-9 _-]+/g,'').trim().replace(/\s+/g,'_')||'chart'});
  });
});

// --- a tab's charts are sized when the tab is shown ---------------------------
// Drawn while their tab was hidden, they took Plotly's default 700x450 and
// overflowed their cards when the tab opened.
document.querySelectorAll('.tab-btn').forEach(function(b){
  b.addEventListener('click',function(){setTimeout(function(){
    document.querySelectorAll('.js-plotly-plot').forEach(function(el){if(el.offsetParent&&window.Plotly)Plotly.Plots.resize(el);});
  },0);});
});

// --- what-if parameters -------------------------------------------------------
(function(){
  var el=document.getElementById('params');if(!el)return;
  el.querySelectorAll('input[data-param]').forEach(function(inp){
    inp.addEventListener('input',function(){
      _PARAMS[inp.getAttribute('data-param')]=+inp.value;
      var o=inp.parentNode.querySelector('output');if(o)o.textContent=inp.value;
      renderAll(getFilt());
    });
  });
})();

// --- a table that reads like a report ---------------------------------------
var _table0=HTMLP.table;
HTMLP.table=function(p,d){
  var s=p.style;
  if(!s.heat&&!s.totals&&!p.date&&!p.metric)return _table0(p,d);
  var e=_ranked(p,d),vals=e.map(function(i){return i[1];}),lo=Math.min.apply(null,vals),hi=Math.max.apply(null,vals);
  var g=p.date?_rgroups(d,function(r){return _key(r,p.category);}):null;
  function spark(k){
    var q=_rgroups(g.get(k)||[],function(r){return _bucket(r[p.date],p.grain||'month');}),ks=Array.from(q.keys()).sort();
    if(ks.length<2)return'';
    var ys=ks.map(function(x){return _measure(p,q.get(x));}),a=Math.min.apply(null,ys),b=Math.max.apply(null,ys)||1;
    var pts=ys.map(function(y,i){return(i/(ys.length-1)*80).toFixed(1)+','+(16-(b===a?8:(y-a)/(b-a)*14)-1).toFixed(1);}).join(' ');
    return'<svg width="80" height="16" aria-hidden="true"><polyline fill="none" stroke="currentColor" stroke-width="1.2" points="'+pts+'"/></svg>';
  }
  var rows=e.map(function(i){
    var bg='';if(s.heat&&hi>lo){var t=(i[1]-lo)/(hi-lo);bg=' style="background:rgba(0,114,178,'+(0.08+0.4*t).toFixed(2)+')"';}
    return'<tr><td>'+_esc(i[0])+'</td>'+(p.date?'<td>'+spark(i[0])+'</td>':'')+'<td class="num"'+bg+'>'+_esc(_fmtu(i[1],p))+'</td></tr>';
  });
  if(s.totals)rows.push('<tr class="tot"><td><b>Total</b></td>'+(p.date?'<td></td>':'')+'<td class="num"><b>'+_esc(_fmtu(_measure(p,d),p))+'</b></td></tr>');
  return'<table class="ptable"><thead><tr><th>'+_esc(p.category)+'</th>'+(p.date?'<th>trend</th>':'')+'<th class="num">'+_esc(p.header)+'</th></tr></thead><tbody>'+rows.join('')+'</tbody></table>';
};
"""


def ext_state(
    metrics: list[dict], parameters: dict[str, Any], cross_filter: bool, cube: bool = False, sample_js: str = "[]"
) -> str:
    """The globals the extensions read, as JSON a <script> can hold."""
    from shared.table_payload import json_for_script

    return (
        f"const _METRICS={json_for_script({m['name']: m for m in metrics})};\n"
        f"let _PARAMS={json_for_script({k: v['default'] for k, v in (parameters or {}).items()})};\n"
        f"const _CROSS={json_for_script(bool(cross_filter))};\n"
        f"const _CUBE={json_for_script(bool(cube))};\n"
        f"const _SAMPLE={sample_js};\n"
    )


def params_html(parameters: dict[str, Any]) -> str:
    """Sliders for the page's what-if parameters."""
    if not parameters:
        return ""
    parts = ['<div class="params" id="params">']
    for name, spec in parameters.items():
        label = html.escape(str(spec.get("label") or name))
        step = spec.get("step") or (float(spec["max"]) - float(spec["min"])) / 100 or 1
        parts.append(
            f'<label>{label} <input type="range" data-param="{html.escape(str(name), quote=True)}" '
            f'min="{float(spec["min"])!r}" max="{float(spec["max"])!r}" step="{float(step)!r}" '
            f'value="{float(spec["default"])!r}"><output>{float(spec["default"])!r}</output></label>'
        )
    parts.append("</div>")
    return "".join(parts)
