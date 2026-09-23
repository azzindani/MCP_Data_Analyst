"""Run a generated dashboard's own script in node and return what it drew.

The page draws every card from its _PANELS document with one renderer. The
honest test of a card is the figure that renderer hands Plotly -- its traces,
axes and colours -- not the spec echoed back in the response, and not a
fragment of template text: a dashboard that ignored its configuration looks
exactly like one that obeyed until something reads the figure.

`drawn(html)` runs the page's main script against a stub DOM and a Plotly
that records each `Plotly.react`, then returns {card id: {"data", "layout"}},
the KPI texts by element id, and the page's console warnings. `rows` replaces
the embedded data, the way the filter bar hands the renderer a subset.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess

NODE = shutil.which("node")

_STUB = r"""
var __figs={}, __els={}, __warn=[];
function __el(id){
  if(!__els[id])__els[id]={id:id,textContent:'',style:{},dataset:{},
    classList:{add(){},remove(){},toggle(){},contains(){return false;}},
    addEventListener(){},setAttribute(){},getAttribute(){return null;},
    querySelector(){return null;},querySelectorAll(){return [];},appendChild(){}};
  return __els[id];
}
var document={getElementById:__el,querySelector(){return null;},querySelectorAll(){return [];},
  addEventListener(){},createElement(){return __el('_new');},body:__el('body'),documentElement:__el('html')};
var sessionStorage={getItem(){return null;},setItem(){}};
var CSS={escape:function(s){return s;}};
var window={addEventListener(){},matchMedia(){return{matches:__DARK__,addEventListener(){}};}};
var Plotly={react:function(id,data,layout){__figs[id]={data:data,layout:layout};},newPlot(){},purge(){},relayout(){}};
console.warn=function(){__warn.push(Array.prototype.map.call(arguments,String).join(' '));};
"""


def main_script(html: str) -> str:
    """The page's own script: the one that declares _PANELS."""
    for body in re.findall(r"<script>(.*?)</script>", html, flags=re.S):
        if "const _PANELS=" in body:
            return body
    raise AssertionError("the page has no script declaring _PANELS")


def drawn(html: str, rows: list[dict] | None = None, dark: bool = False) -> dict:
    """Every figure the page draws, by card id; the KPI texts; and console warnings.

    `dark` is the reader's prefers-color-scheme, for a page that follows it.
    """
    assert NODE, "node is not installed"
    tail = "__figs={};renderAll(" + (json.dumps(rows) if rows is not None else "_RAW") + ");"
    tail += (
        "var __k={};_KPIS.forEach(function(k){__k[k.el]=__el(k.el).textContent;});"
        "process.stdout.write(JSON.stringify({figures:__figs,kpis:__k,warnings:__warn,panels:_PANELS}));"
    )
    program = _STUB.replace("__DARK__", "true" if dark else "false") + main_script(html) + "\n" + tail
    done = subprocess.run([NODE, "-"], input=program, capture_output=True, encoding="utf-8", timeout=120)
    assert done.returncode == 0, done.stderr[-3000:]
    return json.loads(done.stdout)


def run_js(html: str, expression: str) -> object:
    """Evaluate `expression` in the page's script (after it has loaded) and return its JSON value."""
    assert NODE, "node is not installed"
    program = (
        _STUB.replace("__DARK__", "false")
        + main_script(html)
        + f"\nprocess.stdout.write(JSON.stringify({expression}));"
    )
    done = subprocess.run([NODE, "-"], input=program, capture_output=True, encoding="utf-8", timeout=120)
    assert done.returncode == 0, done.stderr[-3000:]
    return json.loads(done.stdout)
