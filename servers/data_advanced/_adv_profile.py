"""generate_auto_profile sub-module. No MCP imports."""
from __future__ import annotations

import logging
import math
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_HERE = str(Path(__file__).resolve().parent)
for _p in (str(_ROOT), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import pandas as pd
from _adv_helpers import (
    _dtype_label,
    _open_file,
    _token_estimate,
    _read_csv,
    css_vars,
    device_mode_js,
    fail,
    ok,
)
from shared.file_utils import resolve_path

logger = logging.getLogger(__name__)


def generate_auto_profile(
    file_path: str,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    """Full column profile: stats, charts, correlations, outliers, insights."""
    progress = []
    try:
        try:
            import plotly.express as px  # noqa: F401
            import plotly.graph_objects as go  # noqa: F401
            from plotly.subplots import make_subplots  # noqa: F401
        except ImportError:
            return {
                "success": False,
                "error": "plotly not installed",
                "hint": "Install: uv add plotly",
                "progress": [fail("Missing dependency", "plotly")],
                "token_estimate": 20,
            }

        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute and the file exists.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }

        df = _read_csv(str(path))
        rows, cols = df.shape

        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        cat_cols = [
            c for c in df.columns
            if not pd.api.types.is_numeric_dtype(df[c])
            and not pd.api.types.is_datetime64_any_dtype(df[c])
        ]
        datetime_cols = [
            c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])
        ]

        col_analysis = _compute_col_analysis(df, rows, numeric_cols, cat_cols)
        corr_matrix, corr_pairs = _compute_correlations(df, numeric_cols)
        missing_by_col = {c: col_analysis[c]["null_count"] for c in df.columns if col_analysis[c]["null_count"] > 0}
        dup_count = int(df.duplicated().sum())
        dup_pct = round(dup_count / rows * 100, 1) if rows > 0 else 0
        total_nulls = int(df.isna().sum().sum())
        null_pct = round(total_nulls / (rows * cols) * 100, 1) if rows * cols > 0 else 0

        for c in numeric_cols:
            col_analysis[c]["zero_count"] = int((df[c] == 0).sum())
            col_analysis[c]["zero_pct"] = round(col_analysis[c]["zero_count"] / rows * 100, 1) if rows > 0 else 0

        spearman_matrix = None
        if len(numeric_cols) >= 2:
            spearman_matrix = df[numeric_cols].corr(method="spearman")

        ap_alerts = _compute_ap_alerts(df, numeric_cols, cat_cols, corr_pairs, rows, dup_count)

        _profile_vars = css_vars(theme)
        if theme == "dark":
            _plot_bg = "#161b22"
            _font_color = "#c9d1d9"
        elif theme == "light":
            _plot_bg = "#f6f8fa"
            _font_color = "#1f2328"
        else:
            _plot_bg = "#f6f8fa"
            _font_color = "#1f2328"
        ap_accent = "#58a6ff" if theme in ("dark", "device") else "#0969da"

        h = []
        h.append(_profile_head_css(_profile_vars))
        h.append(_profile_sidebar(path, rows, cols, df, col_analysis, ap_alerts))
        h.append('<div class="main">')
        h.append(_profile_overview(rows, cols, numeric_cols, cat_cols, datetime_cols, total_nulls, null_pct, dup_count, dup_pct))
        h.append(_profile_alerts_section(_ap_alerts_html(ap_alerts), ap_alerts))
        h.append(_profile_sample(df))
        h.append(_profile_missing(df, missing_by_col, rows, ap_accent, _plot_bg, _font_color))
        h.append(_profile_correlations(corr_matrix, corr_pairs, spearman_matrix, _plot_bg, _font_color))
        h.append(_profile_insights(df, col_analysis, numeric_cols, cat_cols, corr_pairs, dup_count, dup_pct, rows))
        h.append(_profile_quality(df, col_analysis))
        h.append(_profile_stats_table(numeric_cols, col_analysis))
        h.append(_profile_categorical(cat_cols, col_analysis, rows))
        h.append(_profile_network(corr_pairs, _plot_bg, _font_color))
        h.append(_profile_recommendations(df, col_analysis, numeric_cols, cat_cols, corr_pairs))
        h.append(_profile_variables(df, col_analysis, numeric_cols, cat_cols, datetime_cols, rows, _plot_bg, _font_color))
        if theme == "device":
            h.append(device_mode_js())
        h.append("</div></body></html>")

        html_content = "\n".join(h)

        if output_path:
            out = Path(output_path)
        else:
            out = path.parent / f"{path.stem}_profile.html"

        out.write_text(html_content, encoding="utf-8")
        size_kb = round(out.stat().st_size / 1024)

        if open_after:
            _open_file(out)

        progress.append(ok(
            "Auto profile saved",
            f"{out.name} ({size_kb:,} KB) - {rows:,} rows x {cols} columns",
        ))

        result = {
            "success": True,
            "op": "generate_auto_profile",
            "output_path": str(out.resolve()),
            "output_name": out.name,
            "report_size_kb": size_kb,
            "rows": rows,
            "columns": cols,
            "numeric_columns": len(numeric_cols),
            "categorical_columns": len(cat_cols),
            "datetime_columns": len(datetime_cols),
            "total_nulls": total_nulls,
            "outlier_columns": sum(1 for c in numeric_cols if col_analysis[c].get("outlier_count", 0) > 0),
            "correlation_pairs": len(corr_pairs),
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_auto_profile error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# Helper builders
# ---------------------------------------------------------------------------

def _compute_col_analysis(df, rows, numeric_cols, cat_cols):
    col_analysis = {}
    for c in df.columns:
        info = {
            "name": c, "dtype": _dtype_label(df[c]),
            "count": int(df[c].notna().sum()),
            "null_count": int(df[c].isna().sum()),
            "null_pct": round(df[c].isna().sum() / rows * 100, 1) if rows > 0 else 0,
            "unique": int(df[c].nunique()),
            "unique_pct": round(df[c].nunique() / rows * 100, 1) if rows > 0 else 0,
        }
        if c in numeric_cols:
            info.update({
                "mean": round(float(df[c].mean()), 4),
                "median": round(float(df[c].median()), 4),
                "std": round(float(df[c].std()), 4),
                "min": round(float(df[c].min()), 4),
                "max": round(float(df[c].max()), 4),
                "q1": round(float(df[c].quantile(0.25)), 4),
                "q3": round(float(df[c].quantile(0.75)), 4),
                "skew": round(float(df[c].skew()), 4),
                "kurtosis": round(float(df[c].kurtosis()), 4),
            })
            q1, q3 = df[c].quantile(0.25), df[c].quantile(0.75)
            iqr = q3 - q1
            info["outlier_count"] = int(((df[c] < q1 - 1.5 * iqr) | (df[c] > q3 + 1.5 * iqr)).sum())
            info["outlier_pct"] = round(info["outlier_count"] / rows * 100, 1) if rows > 0 else 0
        elif c in cat_cols:
            info["top_values"] = df[c].value_counts().head(10).to_dict()
            info["mode"] = str(df[c].mode().iloc[0]) if len(df[c].mode()) > 0 else ""
        col_analysis[c] = info
    return col_analysis


def _compute_correlations(df, numeric_cols):
    if len(numeric_cols) < 2:
        return None, []
    corr_matrix = df[numeric_cols].corr()
    corr_pairs = []
    for i in range(len(numeric_cols)):
        for j in range(i + 1, len(numeric_cols)):
            val = corr_matrix.iloc[i, j]
            if not pd.isna(val):
                corr_pairs.append({
                    "col_a": numeric_cols[i],
                    "col_b": numeric_cols[j],
                    "correlation": round(float(val), 4),
                })
    corr_pairs.sort(key=lambda x: abs(x["correlation"]), reverse=True)
    return corr_matrix, corr_pairs


def _compute_ap_alerts(df, numeric_cols, cat_cols, corr_pairs, rows, dup_count):
    alerts = []
    for c in df.columns:
        if df[c].nunique(dropna=True) <= 1:
            alerts.append({"col": c, "type": "CONSTANT", "sev": "error",
                           "msg": f"'{c}' has only 1 unique value — constant, no predictive value."})
    for c in df.columns:
        np_ = round(df[c].isna().mean() * 100, 1)
        if np_ > 50:
            alerts.append({"col": c, "type": "HIGH NULLS", "sev": "error",
                           "msg": f"'{c}': {np_}% missing values — consider dropping."})
        elif np_ > 20:
            alerts.append({"col": c, "type": "HIGH NULLS", "sev": "warning",
                           "msg": f"'{c}': {np_}% missing — imputation needed."})
    for c in numeric_cols:
        zp = round((df[c] == 0).mean() * 100, 1)
        if zp > 50:
            alerts.append({"col": c, "type": "ZEROS", "sev": "warning",
                           "msg": f"'{c}': {zp}% zero values — zero-inflated distribution."})
    for c in cat_cols:
        uniq = df[c].nunique()
        if uniq > max(50, rows * 0.5):
            alerts.append({"col": c, "type": "HIGH CARDINALITY", "sev": "warning",
                           "msg": f"'{c}': {uniq:,} unique values — likely an ID, consider dropping."})
    for c in cat_cols:
        if df[c].notna().sum() > 0:
            top_pct = round(df[c].value_counts(normalize=True).iloc[0] * 100, 1)
            if top_pct > 90:
                alerts.append({"col": c, "type": "IMBALANCED", "sev": "warning",
                               "msg": f"'{c}': top category = {top_pct}% of values — highly imbalanced."})
    for c in numeric_cols:
        try:
            skv = round(float(df[c].skew()), 2)
            if abs(skv) > 2:
                alerts.append({"col": c, "type": "SKEWED", "sev": "warning",
                               "msg": f"'{c}': skewness={skv:+.2f} — consider log/sqrt transform."})
        except Exception:
            pass
    for c in numeric_cols:
        q1_, q3_ = df[c].quantile(0.25), df[c].quantile(0.75)
        iqr_ = q3_ - q1_
        oc_ = int(((df[c] < q1_ - 1.5 * iqr_) | (df[c] > q3_ + 1.5 * iqr_)).sum())
        op_ = round(oc_ / max(rows, 1) * 100, 1)
        if op_ > 10:
            alerts.append({"col": c, "type": "OUTLIERS", "sev": "warning",
                           "msg": f"'{c}': {oc_:,} outliers ({op_}%) — investigate or cap."})
    for p in corr_pairs[:20]:
        if abs(p["correlation"]) > 0.9:
            alerts.append({"col": p["col_a"], "type": "HIGH CORR", "sev": "warning",
                           "msg": f"'{p['col_a']}' \u2194 '{p['col_b']}': r={p['correlation']:+.3f} — possible multicollinearity."})
    if dup_count > 0:
        dp_ = round(dup_count / max(rows, 1) * 100, 1)
        alerts.append({"col": None, "type": "DUPLICATES",
                       "sev": "warning" if dp_ < 5 else "error",
                       "msg": f"{dup_count:,} duplicate rows ({dp_}%) — consider deduplication."})
    return alerts


def _ap_alerts_html(al):
    if not al:
        return '<div class="alert-panel"><div class="alert-item info"><span class="alert-badge info">OK</span> No data quality alerts detected.</div></div>'
    items = []
    for a in al:
        badge_cls = "error" if a["sev"] == "error" else "warning" if a["sev"] == "warning" else "info"
        items.append(f'<div class="alert-item {badge_cls}"><span class="alert-badge {badge_cls}">{a["type"]}</span> {a["msg"]}</div>')
    return f'<div class="alert-panel">{"".join(items)}</div>'


def _profile_head_css(profile_vars):
    return f"""<!DOCTYPE html><html><head><meta charset='utf-8'><meta name="viewport" content="width=device-width,initial-scale=1"><title>Data Profile</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
{profile_vars}
:root{{--sidebar-w:300px;--chart-h:420px;--heatmap-h:500px}}
*{{box-sizing:border-box;margin:0;padding:0}}
html{{scroll-behavior:smooth}}
body{{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;background:var(--bg);color:var(--text);display:flex;min-height:100vh}}
::-webkit-scrollbar{{width:8px}}::-webkit-scrollbar-track{{background:var(--bg)}}::-webkit-scrollbar-thumb{{background:#30363d;border-radius:4px}}
.sidebar{{width:var(--sidebar-w);background:var(--surface);border-right:1px solid var(--border);position:fixed;top:0;left:0;bottom:0;overflow-y:auto;z-index:100;display:flex;flex-direction:column}}
.sidebar-header{{padding:24px 20px 16px;border-bottom:1px solid var(--border)}}
.sidebar-header h2{{color:var(--accent);font-size:18px;margin-bottom:4px;font-weight:600}}
.sidebar-header .file-name{{color:var(--text-muted);font-size:13px;margin-bottom:2px;word-break:break-all}}
.sidebar-header .meta{{color:var(--text-muted);font-size:12px}}
.sidebar-nav{{padding:12px 0;flex:1}}
.sidebar-nav a{{display:block;padding:8px 20px;color:var(--text-muted);text-decoration:none;font-size:13px;border-left:3px solid transparent;transition:all 0.15s}}
.sidebar-nav a:hover{{color:var(--accent);background:rgba(88,166,255,0.06);border-left-color:var(--accent)}}
.sidebar-nav .st{{padding:16px 20px 6px;color:#484f58;font-size:10px;text-transform:uppercase;letter-spacing:1.2px;font-weight:600}}
.main{{margin-left:var(--sidebar-w);padding:32px;flex:1;max-width:1400px;width:100%}}
.section{{margin-bottom:48px}}
.section h1{{color:var(--accent);font-size:26px;margin-bottom:24px;padding-bottom:12px;border-bottom:2px solid var(--border);font-weight:600}}
.section h2{{color:var(--accent);font-size:20px;margin:32px 0 16px;padding-bottom:8px;border-bottom:1px solid var(--border);font-weight:600}}
.section h3{{color:var(--text);font-size:15px;margin:12px 0 8px;font-weight:500}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:14px;margin-bottom:24px}}
.card{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:18px 20px;text-align:center;transition:transform 0.15s,border-color 0.15s}}
.card:hover{{transform:translateY(-2px);border-color:var(--accent)}}
.card .num{{font-size:30px;font-weight:700;color:var(--accent);line-height:1.2}}
.card .label{{font-size:11px;color:var(--text-muted);margin-top:6px;text-transform:uppercase;letter-spacing:0.8px}}
.card.good .num{{color:var(--green)}}.card.warn .num{{color:var(--orange)}}.card.bad .num{{color:var(--red)}}
table{{width:100%;border-collapse:collapse;margin:12px 0;font-size:13px;background:var(--surface);border-radius:8px;overflow:hidden}}
th,td{{padding:12px 16px;text-align:left;border-bottom:1px solid var(--border)}}
th{{background:rgba(88,166,255,0.08);color:var(--accent);font-weight:600;font-size:12px;text-transform:uppercase;letter-spacing:0.5px}}
tr:hover{{background:rgba(88,166,255,0.04)}}
.split{{display:grid;grid-template-columns:1fr 1fr;gap:24px;margin:16px 0}}
.split-left table{{margin:0}}
.split-right .cc{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px}}
.cc-card{{background:var(--surface);border:1px solid var(--border);border-radius:10px;margin:16px 0;overflow:hidden;transition:border-color 0.15s}}
.cc-card:hover{{border-color:var(--accent)}}
.cc-hdr{{padding:14px 18px;background:rgba(88,166,255,0.04);border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center}}
.cc-hdr h3{{color:var(--text);font-size:15px;margin:0;font-weight:600}}
.badge{{font-size:11px;padding:3px 10px;border-radius:12px;background:var(--border);color:var(--text-muted);font-weight:500}}
.cc-body{{padding:18px}}
.insights{{list-style:none;padding:0}}
.insights li{{padding:12px 16px;margin:6px 0;background:var(--surface);border-radius:8px;border-left:4px solid var(--accent);font-size:13px;line-height:1.5}}
.insights li.warn{{border-left-color:var(--orange)}}.insights li.bad{{border-left-color:var(--red)}}.insights li.good{{border-left-color:var(--green)}}
.mbar{{height:28px;background:var(--border);border-radius:6px;overflow:hidden;margin:4px 0}}
.mbar-fill{{height:100%;background:linear-gradient(90deg,var(--orange),var(--red));border-radius:6px;transition:width 0.3s}}
.chart-container{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px;margin:16px 0}}
.alert-panel{{border-radius:10px;overflow:hidden;margin-bottom:20px}}
.alert-item{{padding:10px 14px;margin:3px 0;font-size:13px;border-radius:8px;display:flex;align-items:flex-start;gap:10px;background:var(--surface);border:1px solid var(--border)}}
.alert-item.error{{border-left:4px solid var(--red)}}.alert-item.warning{{border-left:4px solid var(--orange)}}.alert-item.info{{border-left:4px solid var(--green)}}
.alert-badge{{font-size:10px;font-weight:700;padding:2px 8px;border-radius:10px;white-space:nowrap;flex-shrink:0;margin-top:1px}}
.alert-badge.error{{background:var(--red);color:#fff}}.alert-badge.warning{{background:var(--orange);color:#fff}}.alert-badge.info{{background:var(--green);color:#fff}}
@media(max-width:1100px){{.split{{grid-template-columns:1fr}}.sidebar{{width:260px}}.main{{margin-left:260px}}}}
@media(max-width:768px){{.sidebar{{display:none}}.main{{margin-left:0;padding:20px}}.cards{{grid-template-columns:repeat(auto-fit,minmax(120px,1fr))}}}}
</style></head><body>"""


def _profile_sidebar(path, rows, cols, df, col_analysis, ap_alerts):
    h = [f'<div class="sidebar"><div class="sidebar-header"><h2>Data Profile</h2><p class="file-name">{path.name}</p><p class="meta">{rows:,} rows x {cols} columns</p></div>']
    h.append(f'<div class="sidebar-nav"><div class="st">Overview</div>')
    h.append('<a href="#overview">Dashboard</a>')
    h.append(f'<a href="#alerts">Alerts ({len(ap_alerts)})</a>')
    h.append('<a href="#sample">Data Sample</a><a href="#quality">Data Quality</a><a href="#stats">Statistics</a><a href="#categorical">Categorical</a><a href="#correlations">Correlations</a><a href="#network">Network</a><a href="#recommendations">Recommendations</a><a href="#insights">Insights</a>')
    h.append(f'<div class="st">Variables ({cols})</div>')
    for c in df.columns:
        info = col_analysis[c]
        h.append(f'<a href="#col-{c.replace(" ", "-")}">{c} <span class="badge">{info["dtype"]}</span></a>')
    h.append("</div></div>")
    return "\n".join(h)


def _profile_overview(rows, cols, numeric_cols, cat_cols, datetime_cols, total_nulls, null_pct, dup_count, dup_pct):
    h = ['<div id="overview" class="section"><h1>Dataset Overview</h1><div class="cards">']
    for num, label, cls in [
        (f"{rows:,}", "Rows", "good"), (str(cols), "Columns", ""),
        (str(len(numeric_cols)), "Numeric", ""), (str(len(cat_cols)), "Categorical", ""),
        (str(len(datetime_cols)), "Datetime", ""),
        (f"{total_nulls:,}", f"Nulls ({null_pct}%)", "good" if null_pct < 5 else "warn" if null_pct < 20 else "bad"),
        (f"{dup_count:,}", f"Duplicates ({dup_pct}%)", "good" if dup_pct < 1 else "warn"),
    ]:
        h.append(f'<div class="card {cls}"><div class="num">{num}</div><div class="label">{label}</div></div>')
    h.append("</div></div>")
    return "\n".join(h)


def _profile_alerts_section(alerts_html, ap_alerts):
    return f'<div id="alerts" class="section"><h1>Alerts &amp; Warnings</h1>{alerts_html}</div>'


def _profile_sample(df):
    cols = list(df.columns)
    header = "".join(f"<th>{c}</th>" for c in cols)
    body = ""
    for _, row in df.head(5).iterrows():
        cells = "".join(f"<td>{str(v)[:50]}</td>" for v in row.values)
        body += f"<tr>{cells}</tr>"
    if len(df) > 5:
        body += f'<tr><td colspan="{len(cols)}" style="text-align:center;color:var(--text-muted);font-style:italic">... {len(df) - 10:,} rows omitted ...</td></tr>'
        for _, row in df.tail(5).iterrows():
            cells = "".join(f"<td>{str(v)[:50]}</td>" for v in row.values)
            body += f"<tr>{cells}</tr>"
    return f'<div id="sample" class="section"><h1>Data Sample</h1><div style="overflow-x:auto"><table><tr>{header}</tr>{body}</table></div></div>'


def _profile_missing(df, missing_by_col, rows, ap_accent, _plot_bg, _font_color):
    if not missing_by_col:
        return ""
    h = ['<div id="missing" class="section"><h2>Missing Data Analysis</h2>']
    h.append("<table><tr><th>Column</th><th>Missing</th><th>%</th><th>Visual</th></tr>")
    for c, count in sorted(missing_by_col.items(), key=lambda x: -x[1]):
        pct = round(count / rows * 100, 1)
        h.append(f'<tr><td><b>{c}</b></td><td>{count:,}</td><td>{pct}%</td><td><div class="mbar"><div class="mbar-fill" style="width:{pct}%"></div></div></td></tr>')
    h.append("</table>")
    ap_miss_cols = list(missing_by_col.keys())
    ap_miss_sample = df[ap_miss_cols].isnull().astype(int)
    if len(ap_miss_sample) > 300:
        ap_miss_sample = ap_miss_sample.sample(300, random_state=42).reset_index(drop=True)
    else:
        ap_miss_sample = ap_miss_sample.reset_index(drop=True)
    ap_miss_z = ap_miss_sample.values.tolist()
    ap_miss_y = list(range(len(ap_miss_sample)))
    h.append(f"""<div class="chart-container">
  <p style="font-size:12px;color:var(--text-muted);margin-bottom:8px">White = present, colored = missing. Sampled {len(ap_miss_sample)} rows.</p>
  <div id="ap-miss-matrix" style="height:{min(400, max(200, len(ap_miss_sample)))}px"></div>
</div>
<script>
(function(){{
  var z={ap_miss_z};var x={ap_miss_cols};var y={ap_miss_y};
  var data=[{{z:z,x:x,y:y,type:'heatmap',colorscale:[['0','rgba(0,0,0,0)'],['1','{ap_accent}']],
    showscale:false,hovertemplate:'Column: %{{x}}<br>Row: %{{y}}<br>Missing: %{{z}}<extra></extra>'}}];
  var layout={{paper_bgcolor:'{_plot_bg}',plot_bgcolor:'{_plot_bg}',
    font:{{color:'{_font_color}'}},margin:{{l:60,r:10,t:10,b:80}},autosize:true,
    xaxis:{{tickangle:-45,tickfont:{{size:11}}}},yaxis:{{title:'Row index',tickfont:{{size:10}}}}
  }};
  Plotly.newPlot('ap-miss-matrix',data,layout,{{responsive:true,displayModeBar:true,scrollZoom:true}});
}})();
</script>""")
    h.append("</div>")
    return "\n".join(h)


def _profile_correlations(corr_matrix, corr_pairs, spearman_matrix, _plot_bg, _font_color):
    if not corr_pairs:
        return ""
    h = ['<div id="correlations" class="section"><h2>Correlation Analysis</h2>']
    h.append('<div class="chart-container" style="margin:16px 0"><div id="corr-heatmap" style="height:var(--heatmap-h)"></div></div>')
    corr_z = corr_matrix.values.tolist()
    corr_x = corr_matrix.columns.tolist()
    h.append(f"""<script>
(function() {{
    var z = {corr_z};
    var x = {corr_x};
    var data = [{{z: z, x: x, y: x, type: 'heatmap', colorscale: 'RdBu', zmid: 0, text: z.map(function(r) {{ return r.map(function(v) {{ return v.toFixed(2); }}); }}), texttemplate: '%{{text}}', textfont: {{size: 11}}}}];
    var layout = {{paper_bgcolor: '{_plot_bg}', plot_bgcolor: '{_plot_bg}', font: {{color: '{_font_color}'}}, margin: {{l: 120, r: 20, t: 20, b: 120}}, height: 500}};
    Plotly.newPlot('corr-heatmap', data, layout, {{responsive: true, displayModeBar: true, scrollZoom: true}});
}})();
</script>""")
    h.append("<h3>Strongest Correlations (Pearson)</h3><table><tr><th>Variable A</th><th>Variable B</th><th>r</th><th>Strength</th></tr>")
    for p in corr_pairs[:15]:
        s = "Very Strong" if abs(p["correlation"]) > 0.9 else "Strong" if abs(p["correlation"]) > 0.7 else "Moderate" if abs(p["correlation"]) > 0.5 else "Weak"
        cls = "good" if abs(p["correlation"]) > 0.7 else "warn" if abs(p["correlation"]) > 0.5 else ""
        h.append(f'<tr class="{cls}"><td>{p["col_a"]}</td><td>{p["col_b"]}</td><td>{p["correlation"]:+.4f}</td><td>{s}</td></tr>')
    h.append("</table>")
    if spearman_matrix is not None:
        sp_z = spearman_matrix.values.tolist()
        sp_x = spearman_matrix.columns.tolist()
        h.append(f"""<h3 style="color:var(--accent);font-size:15px;margin:20px 0 8px">Spearman Rank Correlation</h3>
<div class="chart-container" style="margin:16px 0"><div id="sp-corr-ap" style="height:var(--heatmap-h)"></div></div>
<script>
(function() {{
    var z = {sp_z};var x = {sp_x};
    var data = [{{z: z, x: x, y: x, type: 'heatmap', colorscale: 'RdBu', zmid: 0, text: z.map(function(r) {{ return r.map(function(v) {{ return v.toFixed(2); }}); }}), texttemplate: '%{{text}}', textfont: {{size: 11}}}}];
    var layout = {{paper_bgcolor: '{_plot_bg}', plot_bgcolor: '{_plot_bg}', font: {{color: '{_font_color}'}}, margin: {{l: 120, r: 20, t: 20, b: 120}}, height: 500}};
    Plotly.newPlot('sp-corr-ap', data, layout, {{responsive: true, displayModeBar: true, scrollZoom: true}});
}})();
</script>""")
    h.append("</div>")
    return "\n".join(h)


def _profile_insights(df, col_analysis, numeric_cols, cat_cols, corr_pairs, dup_count, dup_pct, rows):
    h = ['<div id="insights" class="section"><h2>Key Insights</h2><ul class="insights">']
    for c in df.columns:
        nc = col_analysis[c]["null_count"]
        if nc > 0:
            pct = col_analysis[c]["null_pct"]
            if pct > 50:
                h.append(f'<li class="bad"><b>{c}</b>: {pct}% null values - consider dropping</li>')
            elif pct > 10:
                h.append(f'<li class="warn"><b>{c}</b>: {pct}% null values - consider imputation</li>')
    for c in cat_cols:
        uniq = col_analysis[c]["unique"]
        if uniq > rows * 0.5 and uniq > 10:
            h.append(f'<li class="warn"><b>{c}</b>: high cardinality ({uniq:,} unique) - likely an ID column</li>')
    for p in corr_pairs[:5]:
        if abs(p["correlation"]) > 0.8:
            h.append(f"<li><b>{p['col_a']}</b> <-> <b>{p['col_b']}</b>: r={p['correlation']:+.3f} (very strong correlation)</li>")
    for c in numeric_cols:
        skew = col_analysis[c].get("skew", 0)
        if abs(skew) > 2:
            h.append(f'<li class="warn"><b>{c}</b>: highly skewed (skewness={skew:.2f}) - consider log transform</li>')
        oc = col_analysis[c].get("outlier_count", 0)
        if oc > 0:
            h.append(f'<li class="warn"><b>{c}</b>: {oc:,} outliers ({col_analysis[c]["outlier_pct"]}%) detected</li>')
    if dup_count > 0:
        h.append(f'<li class="warn"><b>{dup_count:,} duplicate rows</b> ({dup_pct}%) - consider removing</li>')
    h.append("</ul></div>")
    return "\n".join(h)


def _profile_quality(df, col_analysis):
    h = ['<div id="quality" class="section"><h2>Data Quality Dashboard</h2>']
    h.append("<table><tr><th>Column</th><th>Type</th><th>Completeness</th><th>Unique %</th><th>Quality</th></tr>")
    for c in df.columns:
        info = col_analysis[c]
        completeness = 100 - info["null_pct"]
        unique_pct = info["unique_pct"]
        quality_score = completeness * 0.7 + min(unique_pct, 100) * 0.3
        h.append(f"""<tr>
<td><b>{c}</b></td><td>{info["dtype"]}</td>
<td><div class="mbar"><div class="mbar-fill" style="width:{completeness}%;background:var(--green)"></div></div>{completeness:.1f}%</td>
<td>{unique_pct:.1f}%</td>
<td><span class="badge" style="background:{"var(--green)" if quality_score > 80 else "var(--orange)" if quality_score > 50 else "var(--red)"}">{quality_score:.0f}/100</span></td>
</tr>""")
    h.append("</table></div>")
    return "\n".join(h)


def _profile_stats_table(numeric_cols, col_analysis):
    if not numeric_cols:
        return ""
    h = ['<div id="stats" class="section"><h2>Summary Statistics (Numeric)</h2>']
    h.append("<table><tr><th>Column</th><th>Mean</th><th>Median</th><th>Std</th><th>Min</th><th>Q1</th><th>Q3</th><th>Max</th><th>Skew</th><th>Outliers</th></tr>")
    for c in numeric_cols:
        info = col_analysis[c]
        h.append(f"""<tr>
<td><b>{c}</b></td>
<td>{info["mean"]:,.2f}</td><td>{info["median"]:,.2f}</td><td>{info["std"]:,.2f}</td>
<td>{info["min"]:,.2f}</td><td>{info["q1"]:,.2f}</td><td>{info["q3"]:,.2f}</td><td>{info["max"]:,.2f}</td>
<td>{info["skew"]:+.2f}</td>
<td class="{"warn" if info["outlier_count"] > 0 else ""}">{info["outlier_count"]:,}</td>
</tr>""")
    h.append("</table></div>")
    return "\n".join(h)


def _profile_categorical(cat_cols, col_analysis, rows):
    if not cat_cols:
        return ""
    h = ['<div id="categorical" class="section"><h2>Categorical Distribution</h2><div class="cards">']
    for c in cat_cols[:8]:
        info = col_analysis[c]
        top_val = list(info["top_values"].keys())[0] if info["top_values"] else "N/A"
        top_cnt = list(info["top_values"].values())[0] if info["top_values"] else 0
        h.append(f"""<div class="card">
<div class="num" style="font-size:16px">{c}</div>
<div class="label">{info["unique"]} unique values</div>
<div style="margin-top:8px;font-size:12px;color:var(--text-muted)">Mode: <b>{top_val}</b> ({top_cnt:,})</div>
</div>""")
    h.append("</div></div>")
    return "\n".join(h)


def _profile_network(corr_pairs, _plot_bg, _font_color):
    if not corr_pairs:
        return ""
    strong_pairs = [p for p in corr_pairs if abs(p["correlation"]) > 0.5]
    if not strong_pairs:
        return ""
    h = ['<div id="network" class="section"><h2>Correlation Network (|r| > 0.5)</h2>']
    nodes = list(set([p["col_a"] for p in strong_pairs] + [p["col_b"] for p in strong_pairs]))
    n_nodes = len(nodes)
    radius = 200
    node_positions = []
    for i in range(n_nodes):
        angle = 2 * math.pi * i / n_nodes - math.pi / 2
        node_positions.append({"x": radius * math.cos(angle), "y": radius * math.sin(angle), "label": nodes[i]})
    edges = []
    for p in strong_pairs:
        edges.append({
            "x": [node_positions[nodes.index(p["col_a"])]["x"], node_positions[nodes.index(p["col_b"])]["x"]],
            "y": [node_positions[nodes.index(p["col_a"])]["y"], node_positions[nodes.index(p["col_b"])]["y"]],
            "text": f"{p['col_a']} ↔ {p['col_b']}: {p['correlation']:+.3f}",
            "width": max(1, abs(p["correlation"]) * 4),
        })
    h.append(f"""<div class="chart-container" style="margin:16px 0"><div id="corr-network" style="height:500px"></div></div>
<script>
(function() {{
    var nodePos = {node_positions};
    var edges = {edges};
    var traces = [];
    for (var i = 0; i < edges.length; i++) {{
        traces.push({{type: 'scatter', mode: 'lines',
            x: edges[i].x, y: edges[i].y,
            line: {{width: edges[i].width, color: '#8b949e'}},
            hoverinfo: 'text', text: edges[i].text, showlegend: false}});
    }}
    traces.push({{type: 'scatter', mode: 'markers+text',
        x: nodePos.map(function(n) {{ return n.x; }}),
        y: nodePos.map(function(n) {{ return n.y; }}),
        text: nodePos.map(function(n) {{ return n.label; }}),
        textposition: 'middle center', textfont: {{size: 12, color: '{_font_color}'}},
        marker: {{size: 20, color: '#58a6ff', line: {{width: 2, color: '{_plot_bg}'}}}},
        hoverinfo: 'text', showlegend: false}});
    var layout = {{paper_bgcolor: '{_plot_bg}', plot_bgcolor: '{_plot_bg}',
        font: {{color: '{_font_color}'}}, height: 500, margin: {{l: 20, r: 20, t: 20, b: 20}},
        xaxis: {{visible: false, range: [-250, 250]}}, yaxis: {{visible: false, range: [-250, 250]}},
        showlegend: false}};
    Plotly.newPlot('corr-network', traces, layout, {{responsive: true, displayModeBar: true, scrollZoom: true}});
}})();
</script>""")
    return "\n".join(h)


def _profile_recommendations(df, col_analysis, numeric_cols, cat_cols, corr_pairs):
    h = ['<div id="recommendations" class="section"><h2>EDA Recommendations</h2><ul class="insights">']
    for c in df.columns:
        nc = col_analysis[c]["null_count"]
        if nc > 0:
            pct = col_analysis[c]["null_pct"]
            if c in numeric_cols:
                if pct < 5:
                    h.append(f'<li class="good"><b>{c}</b>: {pct}% missing - fill with median/mean</li>')
                elif pct < 20:
                    h.append(f'<li class="warn"><b>{c}</b>: {pct}% missing - consider KNN imputation</li>')
                else:
                    h.append(f'<li class="bad"><b>{c}</b>: {pct}% missing - consider dropping</li>')
            elif c in cat_cols:
                if pct < 10:
                    h.append(f'<li class="good"><b>{c}</b>: {pct}% missing - fill with mode or "Unknown"</li>')
                else:
                    h.append(f'<li class="warn"><b>{c}</b>: {pct}% missing - consider dropping</li>')
    for c in numeric_cols:
        skew = col_analysis[c].get("skew", 0)
        if abs(skew) > 1:
            transform = "log" if skew > 0 else "log(-x + max + 1)"
            h.append(f'<li class="warn"><b>{c}</b>: skewed ({skew:+.2f}) - apply {transform} transform</li>')
    for c in numeric_cols:
        oc = col_analysis[c].get("outlier_count", 0)
        if oc > 0:
            pct = col_analysis[c]["outlier_pct"]
            if pct < 5:
                h.append(f'<li class="good"><b>{c}</b>: {oc:,} outliers ({pct}%) - consider capping at 1.5*IQR</li>')
            else:
                h.append(f'<li class="warn"><b>{c}</b>: {oc:,} outliers ({pct}%) - investigate data quality</li>')
    for c in cat_cols:
        uniq = col_analysis[c]["unique"]
        rows = col_analysis[c]["count"] + col_analysis[c]["null_count"]
        if uniq > rows * 0.5 and uniq > 10:
            h.append(f'<li class="warn"><b>{c}</b>: high cardinality ({uniq:,} unique) - likely an ID column</li>')
    for p in corr_pairs[:3]:
        if abs(p["correlation"]) > 0.8:
            h.append(f'<li class="warn"><b>{p["col_a"]}</b> ↔ <b>{p["col_b"]}</b>: r={p["correlation"]:+.3f} - multicollinearity detected</li>')
    h.append("</ul></div>")
    return "\n".join(h)


def _profile_variables(df, col_analysis, numeric_cols, cat_cols, datetime_cols, rows, _plot_bg, _font_color):
    h = ['<div class="section"><h2>Variable Analysis</h2>']
    for c in df.columns:
        info = col_analysis[c]
        anchor = c.replace(" ", "-")
        h.append(f'<div id="col-{anchor}" class="cc-card"><div class="cc-hdr"><h3>{c}</h3><span class="badge">{info["dtype"]}</span></div><div class="cc-body"><div class="split"><div class="split-left"><table>')
        h.append(f"<tr><td>Count</td><td>{info['count']:,}</td></tr>")
        h.append(f"<tr><td>Missing</td><td>{info['null_count']:,} ({info['null_pct']}%)</td></tr>")
        h.append(f"<tr><td>Unique</td><td>{info['unique']:,} ({info['unique_pct']}%)</td></tr>")
        if c in numeric_cols:
            for k in ["mean", "median", "std", "min", "q1", "q3", "max", "skew", "kurtosis"]:
                h.append(f"<tr><td>{k.title()}</td><td>{info[k]:,.4f}</td></tr>")
            h.append(f"<tr><td>Zeros</td><td>{info.get('zero_count', 0):,} ({info.get('zero_pct', 0)}%)</td></tr>")
            h.append(f"<tr><td>Outliers</td><td>{info['outlier_count']:,} ({info['outlier_pct']}%)</td></tr>")
        elif c in cat_cols:
            h.append(f"<tr><td>Mode</td><td>{info['mode']}</td></tr></table><h4 style='margin-top:10px;color:#8b949e;font-size:12px'>Top Values</h4><table><tr><th>Value</th><th>Count</th><th>%</th><th>Bar</th></tr>")
            for val, count in info["top_values"].items():
                pct = round(count / rows * 100, 1)
                h.append(f'<tr><td>{val}</td><td>{count:,}</td><td>{pct}%</td><td><div class="mbar"><div class="mbar-fill" style="width:{pct}%;background:var(--accent)"></div></div></td></tr>')
        elif c in datetime_cols:
            h.append(f"<tr><td>Min Date</td><td>{df[c].min()}</td></tr><tr><td>Max Date</td><td>{df[c].max()}</td></tr><tr><td>Time Span</td><td>{df[c].max() - df[c].min()}</td></tr>")
        h.append("</table></div>")
        h.append('<div class="split-right"><div class="chart-container">')
        chart_id = f"chart-{anchor}"
        h.append(f'<div id="{chart_id}" style="height:var(--chart-h)"></div>')
        h.append(_col_chart_script(c, chart_id, df, numeric_cols, cat_cols, datetime_cols, _plot_bg, _font_color))
        h.append("</div></div></div></div></div>")
    return "\n".join(h)


def _col_chart_script(c, chart_id, df, numeric_cols, cat_cols, datetime_cols, _plot_bg, _font_color):
    if c in numeric_cols:
        clean_data = df[c].dropna().tolist()
        return f"""<script>
(function() {{
    var d = {clean_data};
    var trace1 = {{x: d, type: 'histogram', nbinsx: 50, marker: {{color: '#58a6ff', opacity: 0.7}}, yaxis: 'y'}};
    var trace2 = {{y: d, type: 'box', marker: {{color: '#f0883e'}}, xaxis: 'x2', yaxis: 'y2'}};
    var layout = {{paper_bgcolor: '{_plot_bg}', plot_bgcolor: '{_plot_bg}', font: {{color: '{_font_color}'}},
        grid: {{rows: 2, columns: 1, pattern: 'independent'}},
        height: 420, margin: {{l: 50, r: 20, t: 10, b: 30}},
        yaxis: {{title: 'Count'}}, yaxis2: {{title: ''}}}};
    Plotly.newPlot('{chart_id}', [trace1, trace2], layout, {{responsive: true, displayModeBar: true, scrollZoom: true}});
}})();
</script>"""
    if c in cat_cols:
        tv = df[c].value_counts().head(15)
        return f"""<script>
(function() {{
    var data = [{{x: {tv.index.tolist()}, y: {tv.values.tolist()}, type: 'bar',
        marker: {{color: '#58a6ff'}}, text: {tv.values.tolist()}, textposition: 'outside'}}];
    var layout = {{paper_bgcolor: '{_plot_bg}', plot_bgcolor: '{_plot_bg}', font: {{color: '{_font_color}'}},
        height: 420, margin: {{l: 50, r: 20, t: 10, b: 80}}, xaxis: {{tickangle: -45}}}};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: true, scrollZoom: true}});
}})();
</script>"""
    if c in datetime_cols:
        ts = df[c].value_counts().sort_index()
        return f"""<script>
(function() {{
    var data = [{{x: {ts.index.tolist()}, y: {ts.values.tolist()}, type: 'scatter', mode: 'lines+markers',
        marker: {{color: '#3fb950'}}, line: {{color: '#3fb950'}}}}];
    var layout = {{paper_bgcolor: '{_plot_bg}', plot_bgcolor: '{_plot_bg}', font: {{color: '{_font_color}'}},
        height: 420, margin: {{l: 50, r: 20, t: 10, b: 30}}}};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: true, scrollZoom: true}});
}})();
</script>"""
    return ""
