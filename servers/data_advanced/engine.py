"""Tier 3 engine — fast EDA, visualizations, dashboards. Zero MCP imports."""

from __future__ import annotations

import logging
import subprocess
import sys
import webbrowser
from pathlib import Path

import pandas as pd

# Shared utilities
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from shared.file_utils import resolve_path
from shared.html_theme import css_vars, device_mode_js, plotly_template, save_chart as _html_save_chart, VIEWPORT_META
from shared.progress import fail, info, ok, warn

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stderr, level=logging.WARNING)


def _token_estimate(obj) -> int:
    return len(str(obj)) // 4


def _read_csv(
    file_path: str, encoding: str = "utf-8", separator: str = ",", max_rows: int = 0
) -> pd.DataFrame:
    kwargs: dict = {"encoding": encoding, "sep": separator, "low_memory": False}
    if max_rows > 0:
        kwargs["nrows"] = max_rows
    return pd.read_csv(file_path, **kwargs)


def _open_file(path: Path) -> None:
    """Open file in default browser/app."""
    try:
        webbrowser.open(f"file://{path.resolve()}")
    except Exception:
        # Fallback: use OS-specific open command
        try:
            if sys.platform == "win32":
                subprocess.Popen(["start", str(path.resolve())], shell=True)
            elif sys.platform == "darwin":
                subprocess.Popen(["open", str(path.resolve())])
            else:
                subprocess.Popen(["xdg-open", str(path.resolve())])
        except Exception:
            pass


def _save_chart(fig, output_path: str, stem_suffix: str, input_path: Path, open_after: bool, theme: str = "dark") -> tuple[str, str]:
    """Save plotly figure to themed responsive HTML."""
    return _html_save_chart(fig, output_path, stem_suffix, input_path, theme, open_after, _open_file)


def _dtype_label(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "int64"
    if pd.api.types.is_float_dtype(series):
        return "float64"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime64"
    return "object"


# ---------------------------------------------------------------------------
# run_eda — fast structured EDA (replaces heavy profilers)
# ---------------------------------------------------------------------------


def run_eda(
    file_path: str,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    """Fast EDA summary. Returns stats, nulls, correlations, outliers as HTML."""
    progress = []
    try:
        try:
            import plotly.graph_objects as go
            import plotly.express as px
            from plotly.subplots import make_subplots
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

        # Classify columns
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        cat_cols = [
            c
            for c in df.columns
            if not pd.api.types.is_numeric_dtype(df[c])
            and not pd.api.types.is_datetime64_any_dtype(df[c])
        ]
        datetime_cols = [
            c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])
        ]

        # Column summaries
        column_summaries = []
        for c in df.columns:
            s = {"column": c, "dtype": _dtype_label(df[c])}
            s["null_count"] = int(df[c].isna().sum())
            s["null_pct"] = round(s["null_count"] / rows * 100, 2) if rows > 0 else 0
            s["unique_count"] = int(df[c].nunique())
            if c in numeric_cols:
                s["mean"] = round(float(df[c].mean()), 4)
                s["median"] = round(float(df[c].median()), 4)
                s["std"] = round(float(df[c].std()), 4)
                s["min"] = round(float(df[c].min()), 4)
                s["max"] = round(float(df[c].max()), 4)
            elif c in cat_cols:
                top = df[c].value_counts().head(5)
                s["top_values"] = {str(k): int(v) for k, v in top.items()}
            column_summaries.append(s)

        # Correlation matrix (numeric only)
        corr_pairs = []
        if len(numeric_cols) >= 2:
            corr = df[numeric_cols].corr(method="pearson")
            pairs = []
            for i in range(len(numeric_cols)):
                for j in range(i + 1, len(numeric_cols)):
                    val = corr.iloc[i, j]
                    if not pd.isna(val):
                        pairs.append(
                            {
                                "col_a": numeric_cols[i],
                                "col_b": numeric_cols[j],
                                "correlation": round(float(val), 4),
                            }
                        )
            pairs.sort(key=lambda x: abs(x["correlation"]), reverse=True)
            corr_pairs = pairs[:10]

        # Outlier flags (IQR method)
        outlier_cols = []
        for c in numeric_cols:
            q1 = df[c].quantile(0.25)
            q3 = df[c].quantile(0.75)
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            count = int(((df[c] < lower) | (df[c] > upper)).sum())
            if count > 0:
                outlier_cols.append(
                    {
                        "column": c,
                        "outlier_count": count,
                        "outlier_pct": round(count / rows * 100, 2),
                        "lower_limit": round(float(lower), 4),
                        "upper_limit": round(float(upper), 4),
                    }
                )

        # Data quality score
        null_penalty = sum(s["null_pct"] for s in column_summaries) / max(cols, 1)
        dup_count = int(df.duplicated().sum())
        dup_penalty = dup_count / rows * 100 if rows > 0 else 0
        outlier_penalty = sum(o["outlier_pct"] for o in outlier_cols) / max(cols, 1)
        quality_score = max(
            0, round(100 - null_penalty - dup_penalty * 0.5 - outlier_penalty * 0.3)
        )

        # ---- ALERTS computation ----
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
            alerts.append({"col": None, "type": "DUPLICATES", "sev": "warning" if dp_ < 5 else "error",
                           "msg": f"{dup_count:,} duplicate rows ({dp_}%) — consider deduplication."})

        # ---- Spearman correlation ----
        spearman_matrix = None
        if len(numeric_cols) >= 2:
            spearman_matrix = df[numeric_cols].corr(method="spearman")

        # Add zero counts to column summaries
        for s in column_summaries:
            if s["column"] in numeric_cols:
                s["zero_count"] = int((df[s["column"]] == 0).sum())
                s["zero_pct"] = round(s["zero_count"] / rows * 100, 2) if rows > 0 else 0

        # Build polished HTML report
        vars_css = css_vars(theme)
        score_cls = "good" if quality_score >= 80 else "warn" if quality_score >= 60 else "bad"
        missing_by_col = {s["column"]: s["null_count"] for s in column_summaries if s["null_count"] > 0}

        # Correlation heatmap JSON for embedded plotly
        if theme == "dark":
            _plot_bg = "#161b22"
            _font_color = "#c9d1d9"
            accent_color = "#58a6ff"
        elif theme == "light":
            _plot_bg = "#f6f8fa"
            _font_color = "#1f2328"
            accent_color = "#0969da"
        else:  # device — start light, JS switches
            _plot_bg = "#f6f8fa"
            _font_color = "#1f2328"
            accent_color = "#58a6ff"

        corr_json = ""
        if len(numeric_cols) >= 2:
            corr = df[numeric_cols].corr(method="pearson")
            corr_z = corr.values.tolist()
            corr_x = list(corr.columns)

            corr_json = f"""
<div class="chart-container" id="corr-chart"></div>
<script>
(function(){{
  var z={corr_z};var x={corr_x};
  var dark=(typeof window!=='undefined')&&window.matchMedia&&window.matchMedia('(prefers-color-scheme:dark)').matches;
  var bg=dark?'#161b22':'{_plot_bg}';var fc=dark?'#c9d1d9':'{_font_color}';
  var data=[{{z:z,x:x,y:x,type:'heatmap',colorscale:'RdBu',zmid:0,
    text:z.map(function(r){{return r.map(function(v){{return v.toFixed(2);}});}}),
    texttemplate:'%{{text}}',textfont:{{size:11}}}}];
  var layout={{paper_bgcolor:bg,plot_bgcolor:bg,font:{{color:fc}},
    margin:{{l:120,r:20,t:20,b:120}},autosize:true}};
  Plotly.newPlot('corr-chart',data,layout,{{responsive:true,displayModeBar:false}});
}})();
</script>"""

        spearman_json = ""
        if spearman_matrix is not None:
            sp_z = spearman_matrix.values.tolist()
            sp_x = spearman_matrix.columns.tolist()
            spearman_json = f"""
<h3 style="color:var(--accent);font-size:15px;margin:20px 0 8px">Spearman Rank Correlation</h3>
<div class="chart-container" id="sp-corr-chart"></div>
<script>
(function(){{
  var z={sp_z};var x={sp_x};
  var dark=(typeof window!=='undefined')&&window.matchMedia&&window.matchMedia('(prefers-color-scheme:dark)').matches;
  var bg=dark?'#161b22':'{_plot_bg}';var fc=dark?'#c9d1d9':'{_font_color}';
  var data=[{{z:z,x:x,y:x,type:'heatmap',colorscale:'RdBu',zmid:0,
    text:z.map(function(r){{return r.map(function(v){{return v.toFixed(2);}});}}),
    texttemplate:'%{{text}}',textfont:{{size:11}}}}];
  var layout={{paper_bgcolor:bg,plot_bgcolor:bg,font:{{color:fc}},
    margin:{{l:120,r:20,t:20,b:120}},autosize:true}};
  Plotly.newPlot('sp-corr-chart',data,layout,{{responsive:true,displayModeBar:false}});
}})();
</script>"""

        # Build column summary table rows
        col_rows = []
        for s in column_summaries:
            nc_ = s["null_count"]
            np_ = s["null_pct"]
            cls = "good" if nc_ == 0 else "warn" if np_ < 10 else "bad"
            if "mean" in s:
                stats_str = f"μ={s['mean']}, σ={s['std']}, [{s['min']}–{s['max']}]"
            elif "top_values" in s:
                stats_str = "Top: " + ", ".join(f"{k}:{v}" for k, v in list(s["top_values"].items())[:3])
            else:
                stats_str = "—"
            zero_cell = f'{s.get("zero_count", "")} ({s.get("zero_pct", "")}%)' if "zero_count" in s else "—"
            col_rows.append(
                f'<tr><td><b>{s["column"]}</b></td><td><span class="badge">{s["dtype"]}</span></td>'
                f'<td class="{cls}">{nc_}</td><td class="{cls}">{np_}%</td>'
                f'<td>{zero_cell}</td>'
                f'<td>{s["unique_count"]}</td><td class="stats-cell">{stats_str}</td></tr>'
            )
        col_rows_html = "\n".join(col_rows)

        # Build insights list
        insights = []
        for s in column_summaries:
            if s["null_pct"] > 50:
                insights.append(f'<li class="bad"><b>{s["column"]}</b>: {s["null_pct"]}% null — consider dropping</li>')
            elif s["null_pct"] > 10:
                insights.append(f'<li class="warn"><b>{s["column"]}</b>: {s["null_pct"]}% null — consider imputation</li>')
        for p in corr_pairs[:5]:
            if abs(p["correlation"]) > 0.8:
                insights.append(f'<li><b>{p["col_a"]}</b> ↔ <b>{p["col_b"]}</b>: r={p["correlation"]:+.3f} (very strong)</li>')
        for s in column_summaries:
            if "std" in s and s.get("std", 0) > 0:
                skew_val = None
                try:
                    skew_val = round(float(df[s["column"]].skew()), 2)
                except Exception:
                    pass
                if skew_val is not None and abs(skew_val) > 2:
                    insights.append(f'<li class="warn"><b>{s["column"]}</b>: skewness={skew_val} — consider log transform</li>')
        if dup_count > 0:
            insights.append(f'<li class="warn">{dup_count:,} duplicate rows detected — use drop_duplicates</li>')
        if not insights:
            insights.append('<li class="good">No major data quality issues detected.</li>')
        insights_html = "\n".join(insights)

        # Outlier table rows
        outlier_rows = "".join(
            f'<tr><td><b>{o["column"]}</b></td><td class="warn">{o["outlier_count"]}</td>'
            f'<td>{o["outlier_pct"]}%</td><td>[{o["lower_limit"]} – {o["upper_limit"]}]</td></tr>'
            for o in outlier_cols
        ) if outlier_cols else '<tr><td colspan="4" class="good">No outliers detected</td></tr>'

        # Correlation table rows
        corr_rows = ""
        for p in corr_pairs[:10]:
            s_str = "Very Strong" if abs(p["correlation"]) > 0.9 else "Strong" if abs(p["correlation"]) > 0.7 else "Moderate" if abs(p["correlation"]) > 0.5 else "Weak"
            cls = "good" if abs(p["correlation"]) > 0.7 else "warn" if abs(p["correlation"]) > 0.5 else ""
            corr_rows += f'<tr class="{cls}"><td>{p["col_a"]}</td><td>{p["col_b"]}</td><td>{p["correlation"]:+.4f}</td><td>{s_str}</td></tr>'

        plotly_script = '<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>'

        missing_section = ""
        if missing_by_col:
            missing_rows = "".join(
                f'<tr><td><b>{c}</b></td><td>{cnt}</td><td>{round(cnt/rows*100,1)}%</td>'
                f'<td><div class="mbar"><div class="mbar-fill" style="width:{round(cnt/rows*100,1)}%"></div></div></td></tr>'
                for c, cnt in sorted(missing_by_col.items(), key=lambda x: -x[1])
            )
            miss_cols = list(missing_by_col.keys())
            miss_sample = df[miss_cols].isnull().astype(int)
            if len(miss_sample) > 300:
                miss_sample = miss_sample.sample(300, random_state=42).reset_index(drop=True)
            else:
                miss_sample = miss_sample.reset_index(drop=True)
            miss_z = miss_sample.values.tolist()
            miss_y = list(range(len(miss_sample)))
            miss_matrix_html = f"""<div class="chart-container" id="miss-matrix-wrap">
  <p style="font-size:12px;color:var(--text-muted);margin-bottom:8px">White = present, colored = missing. Sampled {len(miss_sample)} rows.</p>
  <div id="miss-matrix" style="height:{min(400, max(200, len(miss_sample)))}px"></div>
</div>
<script>
(function(){{
  var z={miss_z};var x={miss_cols};var y={miss_y};
  var data=[{{z:z,x:x,y:y,type:'heatmap',colorscale:[['0','rgba(0,0,0,0)'],['1','{accent_color}']],
    showscale:false,hovertemplate:'Column: %{{x}}<br>Row: %{{y}}<br>Missing: %{{z}}<extra></extra>'}}];
  var layout={{paper_bgcolor:'{_plot_bg}',plot_bgcolor:'{_plot_bg}',
    font:{{color:'{_font_color}'}},
    margin:{{l:60,r:10,t:10,b:80}},autosize:true,
    xaxis:{{tickangle:-45,tickfont:{{size:11}}}},
    yaxis:{{title:'Row index',tickfont:{{size:10}}}}
  }};
  Plotly.newPlot('miss-matrix',data,layout,{{responsive:true,displayModeBar:false}});
}})();
</script>"""
            missing_section = f'<div id="nulls" class="section"><h2>Missing Data</h2><table><tr><th>Column</th><th>Missing</th><th>%</th><th>Visual</th></tr>{missing_rows}</table>{miss_matrix_html}</div>'

        corr_section = ""
        if corr_pairs:
            corr_section = f'<div id="correlations" class="section"><h2>Correlations</h2>{corr_json}{spearman_json}<table><tr><th>Variable A</th><th>Variable B</th><th>r</th><th>Strength</th></tr>{corr_rows}</table></div>'

        nulls_nav = '<a href="#nulls">Missing Data</a>' if missing_by_col else ""
        corr_nav = '<a href="#correlations">Correlations</a>' if corr_pairs else ""

        # Data sample HTML (first 5 rows)
        sample_rows_df = df.head(5)
        sample_cols_list = list(sample_rows_df.columns)
        sample_header = "".join(f"<th>{c}</th>" for c in sample_cols_list)
        sample_body = ""
        for _, row in sample_rows_df.iterrows():
            cells = "".join(f"<td>{str(v)[:50]}</td>" for v in row.values)
            sample_body += f"<tr>{cells}</tr>"
        sample_html = f"""<div id="sample" class="section">
  <h2>Data Sample (first 5 rows)</h2>
  <div style="overflow-x:auto">
    <table><tr>{sample_header}</tr>{sample_body}</table>
  </div>
</div>"""

        # Alerts HTML helper
        def _alerts_html(al):
            if not al:
                return '<div class="alert-panel"><div class="alert-item info"><span class="alert-badge info">OK</span> No data quality alerts detected.</div></div>'
            items = []
            for a in al:
                badge_cls = "error" if a["sev"] == "error" else "warning" if a["sev"] == "warning" else "info"
                items.append(f'<div class="alert-item {badge_cls}"><span class="alert-badge {badge_cls}">{a["type"]}</span> {a["msg"]}</div>')
            return f'<div class="alert-panel">{"".join(items)}</div>'

        alerts_section = f'<div id="alerts" class="section"><h2>&#9888; Alerts ({len(alerts)})</h2>{_alerts_html(alerts)}</div>'

        html_content = f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
{VIEWPORT_META}
<title>EDA Report — {path.name}</title>
{plotly_script}
<style>
{vars_css}
*{{box-sizing:border-box;margin:0;padding:0}}
html{{scroll-behavior:smooth}}
body{{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text);display:flex;min-height:100vh;transition:background 0.2s,color 0.2s}}
::-webkit-scrollbar{{width:6px}}::-webkit-scrollbar-track{{background:var(--bg)}}::-webkit-scrollbar-thumb{{background:var(--border);border-radius:3px}}
.sidebar{{width:260px;background:var(--surface);border-right:1px solid var(--border);position:fixed;top:0;left:0;bottom:0;overflow-y:auto;z-index:100}}
.sidebar-hdr{{padding:20px;border-bottom:1px solid var(--border)}}
.sidebar-hdr h2{{color:var(--accent);font-size:16px;margin-bottom:4px}}
.sidebar-hdr .meta{{color:var(--text-muted);font-size:12px}}
.nav{{padding:8px 0}}
.nav a{{display:block;padding:7px 20px;color:var(--text-muted);text-decoration:none;font-size:13px;border-left:3px solid transparent;transition:all 0.15s}}
.nav a:hover,.nav a.active{{color:var(--accent);background:rgba(88,166,255,0.06);border-left-color:var(--accent)}}
.nav .st{{padding:14px 20px 4px;color:var(--border);font-size:10px;text-transform:uppercase;letter-spacing:1px;font-weight:600}}
.main{{margin-left:260px;padding:32px;flex:1;max-width:1400px}}
.section{{margin-bottom:48px}}
.section>h2{{color:var(--accent);font-size:20px;margin-bottom:20px;padding-bottom:10px;border-bottom:2px solid var(--border);font-weight:600}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:12px;margin-bottom:24px}}
.card{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px;text-align:center;transition:transform 0.15s,border-color 0.15s}}
.card:hover{{transform:translateY(-2px);border-color:var(--accent)}}
.card .num{{font-size:28px;font-weight:700;color:var(--accent);line-height:1.2}}
.card .lbl{{font-size:11px;color:var(--text-muted);margin-top:4px;text-transform:uppercase;letter-spacing:0.8px}}
.card.good .num{{color:var(--green)}}.card.warn .num{{color:var(--orange)}}.card.bad .num{{color:var(--red)}}
table{{width:100%;border-collapse:collapse;margin:12px 0;font-size:13px;background:var(--surface);border-radius:8px;overflow:hidden}}
th,td{{padding:10px 14px;text-align:left;border-bottom:1px solid var(--border)}}
th{{background:rgba(88,166,255,0.08);color:var(--accent);font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:0.5px}}
tr:hover{{background:rgba(88,166,255,0.03)}}
.good{{color:var(--green)}}.warn{{color:var(--orange)}}.bad{{color:var(--red)}}
.badge{{font-size:11px;padding:2px 8px;border-radius:10px;background:var(--border);color:var(--text-muted);font-weight:500}}
.stats-cell{{font-size:12px;color:var(--text-muted);font-family:monospace}}
.insights{{list-style:none;padding:0}}
.insights li{{padding:10px 14px;margin:6px 0;background:var(--surface);border-radius:8px;border-left:4px solid var(--accent);font-size:13px;line-height:1.5}}
.insights li.warn{{border-left-color:var(--orange)}}.insights li.bad{{border-left-color:var(--red)}}.insights li.good{{border-left-color:var(--green)}}
.mbar{{height:24px;background:var(--border);border-radius:6px;overflow:hidden;margin:4px 0}}
.mbar-fill{{height:100%;background:linear-gradient(90deg,var(--orange),var(--red));border-radius:6px}}
.chart-container{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px;margin:16px 0;min-height:300px}}
.alert-panel{{border-radius:10px;overflow:hidden;margin-bottom:20px}}
.alert-item{{padding:10px 14px;margin:3px 0;font-size:13px;border-radius:8px;display:flex;align-items:flex-start;gap:10px;background:var(--surface);border:1px solid var(--border)}}
.alert-item.error{{border-left:4px solid var(--red)}}.alert-item.warning{{border-left:4px solid var(--orange)}}.alert-item.info{{border-left:4px solid var(--green)}}
.alert-badge{{font-size:10px;font-weight:700;padding:2px 8px;border-radius:10px;white-space:nowrap;flex-shrink:0;margin-top:1px}}
.alert-badge.error{{background:var(--red);color:#fff}}.alert-badge.warning{{background:var(--orange);color:#fff}}.alert-badge.info{{background:var(--green);color:#fff}}
@media(max-width:1100px){{.sidebar{{width:220px}}.main{{margin-left:220px}}}}
@media(max-width:768px){{.sidebar{{display:none}}.main{{margin-left:0;padding:16px}}.cards{{grid-template-columns:repeat(2,1fr)}}}}
@media(max-width:480px){{.cards{{grid-template-columns:1fr}}th,td{{padding:8px 10px;font-size:12px}}}}
</style></head><body>
<div class="sidebar">
  <div class="sidebar-hdr"><h2>EDA Report</h2><p class="meta">{path.name}</p><p class="meta">{rows:,} rows × {cols} cols</p></div>
  <div class="nav">
    <div class="st">Sections</div>
    <a href="#alerts">Alerts ({len(alerts)})</a>
    <a href="#overview">Overview</a>
    <a href="#sample">Data Sample</a>
    <a href="#columns">Column Summary</a>
    {nulls_nav}
    {corr_nav}
    <a href="#outliers">Outliers</a>
    <a href="#insights">Insights</a>
  </div>
</div>
<div class="main">
  {alerts_section}

  <div id="overview" class="section">
    <h2>Dataset Overview</h2>
    <div class="cards">
      <div class="card good"><div class="num">{rows:,}</div><div class="lbl">Rows</div></div>
      <div class="card"><div class="num">{cols}</div><div class="lbl">Columns</div></div>
      <div class="card"><div class="num">{len(numeric_cols)}</div><div class="lbl">Numeric</div></div>
      <div class="card"><div class="num">{len(cat_cols)}</div><div class="lbl">Categorical</div></div>
      <div class="card"><div class="num">{len(datetime_cols)}</div><div class="lbl">Datetime</div></div>
      <div class="card {score_cls}"><div class="num">{quality_score}</div><div class="lbl">Quality Score</div></div>
      <div class="card {'warn' if dup_count > 0 else 'good'}"><div class="num">{dup_count:,}</div><div class="lbl">Duplicates</div></div>
    </div>
  </div>

  {sample_html}

  <div id="columns" class="section">
    <h2>Column Summary</h2>
    <table>
      <tr><th>Column</th><th>Type</th><th>Nulls</th><th>Null %</th><th>Zeros</th><th>Unique</th><th>Stats</th></tr>
      {col_rows_html}
    </table>
  </div>

  {missing_section}

  {corr_section}

  <div id="outliers" class="section">
    <h2>Outliers (IQR Method)</h2>
    <table><tr><th>Column</th><th>Count</th><th>%</th><th>IQR Range</th></tr>
    {outlier_rows}
    </table>
  </div>

  <div id="insights" class="section">
    <h2>Key Insights</h2>
    <ul class="insights">{insights_html}</ul>
  </div>
</div>
{device_mode_js() if theme == "device" else ""}
</body></html>"""

        if output_path:
            out = Path(output_path)
        else:
            out = path.parent / f"{path.stem}_eda.html"

        out.write_text(html_content, encoding="utf-8")
        size_kb = round(out.stat().st_size / 1024)

        if open_after:
            _open_file(out)

        progress.append(
            ok(
                "EDA report saved",
                f"{out.name} ({size_kb:,} KB) — {quality_score}/100 quality score",
            )
        )

        result = {
            "success": True,
            "op": "run_eda",
            "output_path": str(out.resolve()),
            "output_name": out.name,
            "report_size_kb": size_kb,
            "rows": rows,
            "columns": cols,
            "quality_score": quality_score,
            "numeric_columns": len(numeric_cols),
            "categorical_columns": len(cat_cols),
            "datetime_columns": len(datetime_cols),
            "duplicate_rows": dup_count,
            "null_summary": {
                s["column"]: s["null_count"]
                for s in column_summaries
                if s["null_count"] > 0
            },
            "top_correlations": corr_pairs[:5],
            "outlier_columns": outlier_cols,
            "column_summaries": column_summaries,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("run_eda error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# generate_distribution_plot
# ---------------------------------------------------------------------------


def generate_distribution_plot(
    file_path: str,
    columns: list[str] = None,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    """Histogram + box plot for numeric columns. Saves and optionally opens HTML."""
    progress = []
    try:
        try:
            import plotly.express as px
            import plotly.graph_objects as go
            from plotly.subplots import make_subplots
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
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

        if columns:
            cols_to_plot = [c for c in columns if c in numeric_cols]
        else:
            cols_to_plot = numeric_cols[:6]  # max 6 to avoid clutter

        if not cols_to_plot:
            return {
                "success": False,
                "error": "No numeric columns found to plot",
                "hint": f"Available numeric columns: {', '.join(numeric_cols)}",
                "progress": [fail("No numeric columns", "")],
                "token_estimate": 20,
            }

        n = len(cols_to_plot)
        fig = make_subplots(
            rows=n,
            cols=2,
            subplot_titles=[f"{c} — Histogram" for c in cols_to_plot]
            + [f"{c} — Box Plot" for c in cols_to_plot],
            vertical_spacing=0.3 / n,
        )

        for i, c in enumerate(cols_to_plot):
            # Histogram
            fig.add_trace(
                go.Histogram(x=df[c], nbinsx=30, name=c, showlegend=False),
                row=i + 1,
                col=1,
            )
            # Box plot
            fig.add_trace(
                go.Box(y=df[c], name=c, showlegend=False),
                row=i + 1,
                col=2,
            )

        fig.update_layout(
            height=300 * n,
            title_text=f"Distribution Analysis: {', '.join(cols_to_plot)}",
            template=plotly_template(theme),
            showlegend=False,
        )

        abs_p, fname = _save_chart(fig, output_path, "distributions", path, open_after, theme)

        progress.append(
            ok(
                "Distribution plots saved",
                f"{fname} — {n} columns",
            )
        )

        result = {
            "success": True,
            "op": "generate_distribution_plot",
            "output_path": abs_p,
            "output_name": fname,
            "columns_plotted": cols_to_plot,
            "chart_count": n * 2,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_distribution_plot error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# generate_correlation_heatmap
# ---------------------------------------------------------------------------


def generate_correlation_heatmap(
    file_path: str,
    method: str = "pearson",
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    """Interactive correlation heatmap for numeric columns. Opens HTML."""
    progress = []
    try:
        try:
            import plotly.express as px
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
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

        if len(numeric_cols) < 2:
            return {
                "success": False,
                "error": "Need at least 2 numeric columns",
                "hint": f"Only found {len(numeric_cols)} numeric columns",
                "progress": [fail("Insufficient numeric columns", "")],
                "token_estimate": 20,
            }

        corr = df[numeric_cols].corr(method=method)

        fig = px.imshow(
            corr,
            text_auto=".2f",
            color_continuous_scale="RdBu_r",
            title=f"Correlation Matrix ({method})",
            aspect="auto",
        )
        fig.update_layout(template=plotly_template(theme), height=300 + 50 * len(numeric_cols))

        abs_p, fname = _save_chart(fig, output_path, "correlation_heatmap", path, open_after, theme)

        progress.append(
            ok(
                "Correlation heatmap saved",
                f"{fname} — {len(numeric_cols)} columns",
            )
        )

        result = {
            "success": True,
            "op": "generate_correlation_heatmap",
            "output_path": abs_p,
            "output_name": fname,
            "columns": numeric_cols,
            "method": method,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_correlation_heatmap error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# generate_correlation_heatmap
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# generate_auto_profile - comprehensive EDA rivaling sweetviz/ydata-profiling
# ---------------------------------------------------------------------------


def generate_auto_profile(
    file_path: str,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    """Comprehensive auto-profile: sidebar nav, per-column stats+charts, correlations, outliers, insights. Opens HTML."""
    progress = []
    try:
        try:
            import plotly.express as px
            import plotly.graph_objects as go
            from plotly.subplots import make_subplots
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
            c
            for c in df.columns
            if not pd.api.types.is_numeric_dtype(df[c])
            and not pd.api.types.is_datetime64_any_dtype(df[c])
        ]
        datetime_cols = [
            c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])
        ]

        col_analysis = {}
        for c in df.columns:
            info = {
                "name": c,
                "dtype": _dtype_label(df[c]),
                "count": int(df[c].notna().sum()),
                "null_count": int(df[c].isna().sum()),
                "null_pct": round(df[c].isna().sum() / rows * 100, 1)
                if rows > 0
                else 0,
                "unique": int(df[c].nunique()),
                "unique_pct": round(df[c].nunique() / rows * 100, 1) if rows > 0 else 0,
            }
            if c in numeric_cols:
                info.update(
                    {
                        "mean": round(float(df[c].mean()), 4),
                        "median": round(float(df[c].median()), 4),
                        "std": round(float(df[c].std()), 4),
                        "min": round(float(df[c].min()), 4),
                        "max": round(float(df[c].max()), 4),
                        "q1": round(float(df[c].quantile(0.25)), 4),
                        "q3": round(float(df[c].quantile(0.75)), 4),
                        "skew": round(float(df[c].skew()), 4),
                        "kurtosis": round(float(df[c].kurtosis()), 4),
                    }
                )
                q1, q3 = df[c].quantile(0.25), df[c].quantile(0.75)
                iqr = q3 - q1
                info["outlier_count"] = int(
                    ((df[c] < q1 - 1.5 * iqr) | (df[c] > q3 + 1.5 * iqr)).sum()
                )
                info["outlier_pct"] = (
                    round(info["outlier_count"] / rows * 100, 1) if rows > 0 else 0
                )
            elif c in cat_cols:
                info["top_values"] = df[c].value_counts().head(10).to_dict()
                info["mode"] = (
                    str(df[c].mode().iloc[0]) if len(df[c].mode()) > 0 else ""
                )
            col_analysis[c] = info

        corr_matrix = None
        corr_pairs = []
        if len(numeric_cols) >= 2:
            corr_matrix = df[numeric_cols].corr()
            for i in range(len(numeric_cols)):
                for j in range(i + 1, len(numeric_cols)):
                    val = corr_matrix.iloc[i, j]
                    if not pd.isna(val):
                        corr_pairs.append(
                            {
                                "col_a": numeric_cols[i],
                                "col_b": numeric_cols[j],
                                "correlation": round(float(val), 4),
                            }
                        )
            corr_pairs.sort(key=lambda x: abs(x["correlation"]), reverse=True)

        missing_by_col = {
            c: col_analysis[c]["null_count"]
            for c in df.columns
            if col_analysis[c]["null_count"] > 0
        }
        dup_count = int(df.duplicated().sum())
        dup_pct = round(dup_count / rows * 100, 1) if rows > 0 else 0
        total_nulls = int(df.isna().sum().sum())
        null_pct = round(total_nulls / (rows * cols) * 100, 1) if rows * cols > 0 else 0

        # Add zero counts to col_analysis for numeric columns
        for c in numeric_cols:
            col_analysis[c]["zero_count"] = int((df[c] == 0).sum())
            col_analysis[c]["zero_pct"] = round(col_analysis[c]["zero_count"] / rows * 100, 1) if rows > 0 else 0

        # Spearman correlation
        spearman_matrix_ap = None
        if len(numeric_cols) >= 2:
            spearman_matrix_ap = df[numeric_cols].corr(method="spearman")

        # ---- ALERTS computation ----
        ap_alerts = []
        for c in df.columns:
            if df[c].nunique(dropna=True) <= 1:
                ap_alerts.append({"col": c, "type": "CONSTANT", "sev": "error",
                                  "msg": f"'{c}' has only 1 unique value — constant, no predictive value."})
        for c in df.columns:
            np_ = round(df[c].isna().mean() * 100, 1)
            if np_ > 50:
                ap_alerts.append({"col": c, "type": "HIGH NULLS", "sev": "error",
                                  "msg": f"'{c}': {np_}% missing values — consider dropping."})
            elif np_ > 20:
                ap_alerts.append({"col": c, "type": "HIGH NULLS", "sev": "warning",
                                  "msg": f"'{c}': {np_}% missing — imputation needed."})
        for c in numeric_cols:
            zp = round((df[c] == 0).mean() * 100, 1)
            if zp > 50:
                ap_alerts.append({"col": c, "type": "ZEROS", "sev": "warning",
                                  "msg": f"'{c}': {zp}% zero values — zero-inflated distribution."})
        for c in cat_cols:
            uniq = df[c].nunique()
            if uniq > max(50, rows * 0.5):
                ap_alerts.append({"col": c, "type": "HIGH CARDINALITY", "sev": "warning",
                                  "msg": f"'{c}': {uniq:,} unique values — likely an ID, consider dropping."})
        for c in cat_cols:
            if df[c].notna().sum() > 0:
                top_pct = round(df[c].value_counts(normalize=True).iloc[0] * 100, 1)
                if top_pct > 90:
                    ap_alerts.append({"col": c, "type": "IMBALANCED", "sev": "warning",
                                      "msg": f"'{c}': top category = {top_pct}% of values — highly imbalanced."})
        for c in numeric_cols:
            try:
                skv = round(float(df[c].skew()), 2)
                if abs(skv) > 2:
                    ap_alerts.append({"col": c, "type": "SKEWED", "sev": "warning",
                                      "msg": f"'{c}': skewness={skv:+.2f} — consider log/sqrt transform."})
            except Exception:
                pass
        for c in numeric_cols:
            q1_, q3_ = df[c].quantile(0.25), df[c].quantile(0.75)
            iqr_ = q3_ - q1_
            oc_ = int(((df[c] < q1_ - 1.5 * iqr_) | (df[c] > q3_ + 1.5 * iqr_)).sum())
            op_ = round(oc_ / max(rows, 1) * 100, 1)
            if op_ > 10:
                ap_alerts.append({"col": c, "type": "OUTLIERS", "sev": "warning",
                                  "msg": f"'{c}': {oc_:,} outliers ({op_}%) — investigate or cap."})
        for p in corr_pairs[:20]:
            if abs(p["correlation"]) > 0.9:
                ap_alerts.append({"col": p["col_a"], "type": "HIGH CORR", "sev": "warning",
                                  "msg": f"'{p['col_a']}' \u2194 '{p['col_b']}': r={p['correlation']:+.3f} — possible multicollinearity."})
        if dup_count > 0:
            dp_ = round(dup_count / max(rows, 1) * 100, 1)
            ap_alerts.append({"col": None, "type": "DUPLICATES", "sev": "warning" if dp_ < 5 else "error",
                              "msg": f"{dup_count:,} duplicate rows ({dp_}%) — consider deduplication."})

        def _ap_alerts_html(al):
            if not al:
                return '<div class="alert-panel"><div class="alert-item info"><span class="alert-badge info">OK</span> No data quality alerts detected.</div></div>'
            items = []
            for a in al:
                badge_cls = "error" if a["sev"] == "error" else "warning" if a["sev"] == "warning" else "info"
                items.append(f'<div class="alert-item {badge_cls}"><span class="alert-badge {badge_cls}">{a["type"]}</span> {a["msg"]}</div>')
            return f'<div class="alert-panel">{"".join(items)}</div>'

        _profile_vars = css_vars(theme)
        if theme == "dark":
            _plot_bg = "#161b22"
            _font_color = "#c9d1d9"
        elif theme == "light":
            _plot_bg = "#f6f8fa"
            _font_color = "#1f2328"
        else:  # device
            _plot_bg = "#f6f8fa"
            _font_color = "#1f2328"

        h = []
        _profile_css = f"""<!DOCTYPE html><html><head><meta charset='utf-8'><meta name="viewport" content="width=device-width,initial-scale=1"><title>Data Profile</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
{_profile_vars}
:root{{--sidebar-w:300px;--chart-h:420px;--heatmap-h:500px}}
*{{box-sizing:border-box;margin:0;padding:0}}
html{{scroll-behavior:smooth}}
body{{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;background:var(--bg);color:var(--text);display:flex;min-height:100vh}}
::-webkit-scrollbar{{width:8px}}::-webkit-scrollbar-track{{background:var(--bg)}}::-webkit-scrollbar-thumb{{background:#30363d;border-radius:4px}}::-webkit-scrollbar-thumb:hover{{background:#484f58}}
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
        h.append(_profile_css)

        # Sidebar
        h.append(f"""<div class="sidebar"><div class="sidebar-header"><h2>Data Profile</h2><p class="file-name">{path.name}</p><p class="meta">{rows:,} rows x {cols} columns</p></div>
<div class="sidebar-nav"><div class="st">Overview</div>
<a href="#overview">Dashboard</a><a href="#alerts">Alerts ({len(ap_alerts)})</a><a href="#sample">Data Sample</a><a href="#quality">Data Quality</a><a href="#stats">Statistics</a><a href="#categorical">Categorical</a><a href="#correlations">Correlations</a><a href="#network">Network</a><a href="#recommendations">Recommendations</a><a href="#insights">Insights</a>
<div class="st">Variables ({cols})</div>""")
        for c in df.columns:
            info = col_analysis[c]
            h.append(
                f'<a href="#col-{c.replace(" ", "-")}">{c} <span class="badge">{info["dtype"]}</span></a>'
            )
        h.append("</div></div>")

        # Main
        h.append('<div class="main">')

        # Overview cards
        h.append(
            '<div id="overview" class="section"><h1>Dataset Overview</h1><div class="cards">'
        )
        for num, label, cls in [
            (f"{rows:,}", "Rows", "good"),
            (str(cols), "Columns", ""),
            (str(len(numeric_cols)), "Numeric", ""),
            (str(len(cat_cols)), "Categorical", ""),
            (str(len(datetime_cols)), "Datetime", ""),
            (
                f"{total_nulls:,}",
                f"Nulls ({null_pct}%)",
                "good" if null_pct < 5 else "warn" if null_pct < 20 else "bad",
            ),
            (
                f"{dup_count:,}",
                f"Duplicates ({dup_pct}%)",
                "good" if dup_pct < 1 else "warn",
            ),
        ]:
            h.append(
                f'<div class="card {cls}"><div class="num">{num}</div><div class="label">{label}</div></div>'
            )
        h.append("</div></div>")

        # Alerts section
        h.append(f'<div id="alerts" class="section"><h1>Alerts &amp; Warnings</h1>{_ap_alerts_html(ap_alerts)}</div>')

        # Data Sample (head 5 + tail 5)
        ap_sample_cols = list(df.columns)
        ap_sample_header = "".join(f"<th>{c}</th>" for c in ap_sample_cols)
        ap_sample_body = ""
        for _, row in df.head(5).iterrows():
            cells = "".join(f"<td>{str(v)[:50]}</td>" for v in row.values)
            ap_sample_body += f"<tr>{cells}</tr>"
        if len(df) > 5:
            ap_sample_body += f'<tr><td colspan="{len(ap_sample_cols)}" style="text-align:center;color:var(--text-muted);font-style:italic">... {len(df) - 10:,} rows omitted ...</td></tr>'
            for _, row in df.tail(5).iterrows():
                cells = "".join(f"<td>{str(v)[:50]}</td>" for v in row.values)
                ap_sample_body += f"<tr>{cells}</tr>"
        h.append(f'<div id="sample" class="section"><h1>Data Sample</h1><div style="overflow-x:auto"><table><tr>{ap_sample_header}</tr>{ap_sample_body}</table></div></div>')

        # Missing Data
        if missing_by_col:
            h.append('<div id="missing" class="section"><h2>Missing Data Analysis</h2>')
            h.append(
                "<table><tr><th>Column</th><th>Missing</th><th>%</th><th>Visual</th></tr>"
            )
            for c, count in sorted(missing_by_col.items(), key=lambda x: -x[1]):
                pct = round(count / rows * 100, 1)
                h.append(
                    f'<tr><td><b>{c}</b></td><td>{count:,}</td><td>{pct}%</td><td><div class="mbar"><div class="mbar-fill" style="width:{pct}%"></div></div></td></tr>'
                )
            h.append("</table>")
            # Missing value matrix
            if missing_by_col:
                ap_miss_cols = list(missing_by_col.keys())
                ap_miss_sample = df[ap_miss_cols].isnull().astype(int)
                if len(ap_miss_sample) > 300:
                    ap_miss_sample = ap_miss_sample.sample(300, random_state=42).reset_index(drop=True)
                else:
                    ap_miss_sample = ap_miss_sample.reset_index(drop=True)
                ap_miss_z = ap_miss_sample.values.tolist()
                ap_miss_y = list(range(len(ap_miss_sample)))
                ap_accent = "#58a6ff" if theme in ("dark", "device") else "#0969da"
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
    font:{{color:'{_font_color}'}},
    margin:{{l:60,r:10,t:10,b:80}},autosize:true,
    xaxis:{{tickangle:-45,tickfont:{{size:11}}}},
    yaxis:{{title:'Row index',tickfont:{{size:10}}}}
  }};
  Plotly.newPlot('ap-miss-matrix',data,layout,{{responsive:true,displayModeBar:false}});
}})();
</script>""")
            h.append("</div>")

        # Correlations
        if corr_pairs:
            h.append(
                '<div id="correlations" class="section"><h2>Correlation Analysis</h2>'
            )
            h.append(
                '<div class="chart-container" style="margin:16px 0"><div id="corr-heatmap" style="height:var(--heatmap-h)"></div></div>'
            )
            # Use JSON.stringify for safe embedding
            corr_z = corr_matrix.values.tolist()
            corr_x = corr_matrix.columns.tolist()
            h.append(f"""<script>
(function() {{
    var z = {corr_z};
    var x = {corr_x};
    var data = [{{z: z, x: x, y: x, type: 'heatmap', colorscale: 'RdBu', zmid: 0, text: z.map(function(r) {{ return r.map(function(v) {{ return v.toFixed(2); }}); }}), texttemplate: '%{{text}}', textfont: {{size: 11}}}}];
    var layout = {{paper_bgcolor: '{_plot_bg}', plot_bgcolor: '{_plot_bg}', font: {{color: '{_font_color}'}}, margin: {{l: 120, r: 20, t: 20, b: 120}}, height: 500}};
    Plotly.newPlot('corr-heatmap', data, layout, {{responsive: true, displayModeBar: false}});
}})();
</script>""")
            h.append(
                "<h3>Strongest Correlations (Pearson)</h3><table><tr><th>Variable A</th><th>Variable B</th><th>r</th><th>Strength</th></tr>"
            )
            for p in corr_pairs[:15]:
                s = (
                    "Very Strong"
                    if abs(p["correlation"]) > 0.9
                    else "Strong"
                    if abs(p["correlation"]) > 0.7
                    else "Moderate"
                    if abs(p["correlation"]) > 0.5
                    else "Weak"
                )
                cls = (
                    "good"
                    if abs(p["correlation"]) > 0.7
                    else "warn"
                    if abs(p["correlation"]) > 0.5
                    else ""
                )
                h.append(
                    f'<tr class="{cls}"><td>{p["col_a"]}</td><td>{p["col_b"]}</td><td>{p["correlation"]:+.4f}</td><td>{s}</td></tr>'
                )
            h.append("</table>")
            # Spearman heatmap
            if spearman_matrix_ap is not None:
                sp_ap_z = spearman_matrix_ap.values.tolist()
                sp_ap_x = spearman_matrix_ap.columns.tolist()
                h.append(f"""<h3 style="color:var(--accent);font-size:15px;margin:20px 0 8px">Spearman Rank Correlation</h3>
<div class="chart-container" style="margin:16px 0"><div id="sp-corr-ap" style="height:var(--heatmap-h)"></div></div>
<script>
(function() {{
    var z = {sp_ap_z};
    var x = {sp_ap_x};
    var data = [{{z: z, x: x, y: x, type: 'heatmap', colorscale: 'RdBu', zmid: 0, text: z.map(function(r) {{ return r.map(function(v) {{ return v.toFixed(2); }}); }}), texttemplate: '%{{text}}', textfont: {{size: 11}}}}];
    var layout = {{paper_bgcolor: '{_plot_bg}', plot_bgcolor: '{_plot_bg}', font: {{color: '{_font_color}'}}, margin: {{l: 120, r: 20, t: 20, b: 120}}, height: 500}};
    Plotly.newPlot('sp-corr-ap', data, layout, {{responsive: true, displayModeBar: false}});
}})();
</script>""")
            h.append("</div>")

        # Key Insights
        h.append(
            '<div id="insights" class="section"><h2>Key Insights</h2><ul class="insights">'
        )
        for c in df.columns:
            nc = col_analysis[c]["null_count"]
            if nc > 0:
                pct = col_analysis[c]["null_pct"]
                if pct > 50:
                    h.append(
                        f'<li class="bad"><b>{c}</b>: {pct}% null values - consider dropping</li>'
                    )
                elif pct > 10:
                    h.append(
                        f'<li class="warn"><b>{c}</b>: {pct}% null values - consider imputation</li>'
                    )
        for c in cat_cols:
            uniq = col_analysis[c]["unique"]
            if uniq > rows * 0.5 and uniq > 10:
                h.append(
                    f'<li class="warn"><b>{c}</b>: high cardinality ({uniq:,} unique) - likely an ID column</li>'
                )
        for p in corr_pairs[:5]:
            if abs(p["correlation"]) > 0.8:
                h.append(
                    f"<li><b>{p['col_a']}</b> <-> <b>{p['col_b']}</b>: r={p['correlation']:+.3f} (very strong correlation)</li>"
                )
        for c in numeric_cols:
            skew = col_analysis[c].get("skew", 0)
            if abs(skew) > 2:
                h.append(
                    f'<li class="warn"><b>{c}</b>: highly skewed (skewness={skew:.2f}) - consider log transform</li>'
                )
            oc = col_analysis[c].get("outlier_count", 0)
            if oc > 0:
                h.append(
                    f'<li class="warn"><b>{c}</b>: {oc:,} outliers ({col_analysis[c]["outlier_pct"]}%) detected</li>'
                )
        if dup_count > 0:
            h.append(
                f'<li class="warn"><b>{dup_count:,} duplicate rows</b> ({dup_pct}%) - consider removing</li>'
            )
        h.append("</ul></div>")

        # Data Quality Dashboard
        h.append('<div id="quality" class="section"><h2>Data Quality Dashboard</h2>')
        h.append(
            "<table><tr><th>Column</th><th>Type</th><th>Completeness</th><th>Unique %</th><th>Quality</th></tr>"
        )
        for c in df.columns:
            info = col_analysis[c]
            completeness = 100 - info["null_pct"]
            unique_pct = info["unique_pct"]
            # Quality score based on completeness and reasonable uniqueness
            quality_score = completeness * 0.7 + min(unique_pct, 100) * 0.3
            h.append(f"""<tr>
<td><b>{c}</b></td><td>{info["dtype"]}</td>
<td><div class="mbar"><div class="mbar-fill" style="width:{completeness}%;background:var(--green)"></div></div>{completeness:.1f}%</td>
<td>{unique_pct:.1f}%</td>
<td><span class="badge" style="background:{"var(--green)" if quality_score > 80 else "var(--orange)" if quality_score > 50 else "var(--red)"}">{quality_score:.0f}/100</span></td>
</tr>""")
        h.append("</table></div>")

        # Summary Statistics Table (Numeric)
        if numeric_cols:
            h.append(
                '<div id="stats" class="section"><h2>Summary Statistics (Numeric)</h2>'
            )
            h.append(
                "<table><tr><th>Column</th><th>Mean</th><th>Median</th><th>Std</th><th>Min</th><th>Q1</th><th>Q3</th><th>Max</th><th>Skew</th><th>Outliers</th></tr>"
            )
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

        # Categorical Distribution Overview
        if cat_cols:
            h.append(
                '<div id="categorical" class="section"><h2>Categorical Distribution</h2>'
            )
            h.append('<div class="cards">')
            for c in cat_cols[:8]:  # Limit to 8 for readability
                info = col_analysis[c]
                top_val = (
                    list(info["top_values"].keys())[0] if info["top_values"] else "N/A"
                )
                top_cnt = (
                    list(info["top_values"].values())[0] if info["top_values"] else 0
                )
                h.append(f"""<div class="card">
<div class="num" style="font-size:16px">{c}</div>
<div class="label">{info["unique"]} unique values</div>
<div style="margin-top:8px;font-size:12px;color:var(--text-muted)">Mode: <b>{top_val}</b> ({top_cnt:,})</div>
</div>""")
            h.append("</div></div>")

        # Correlation Network (strong pairs only)
        if corr_pairs:
            strong_pairs = [p for p in corr_pairs if abs(p["correlation"]) > 0.5]
            if strong_pairs:
                h.append(
                    '<div id="network" class="section"><h2>Correlation Network (|r| > 0.5)</h2>'
                )
                nodes = list(
                    set(
                        [p["col_a"] for p in strong_pairs]
                        + [p["col_b"] for p in strong_pairs]
                    )
                )
                # Create a simple force-directed-like layout using circular positioning
                import math

                n_nodes = len(nodes)
                radius = 200
                node_positions = []
                for i in range(n_nodes):
                    angle = 2 * math.pi * i / n_nodes - math.pi / 2
                    node_positions.append(
                        {
                            "x": radius * math.cos(angle),
                            "y": radius * math.sin(angle),
                            "label": nodes[i],
                        }
                    )
                edges = []
                for p in strong_pairs:
                    edges.append(
                        {
                            "x": [
                                node_positions[nodes.index(p["col_a"])]["x"],
                                node_positions[nodes.index(p["col_b"])]["x"],
                            ],
                            "y": [
                                node_positions[nodes.index(p["col_a"])]["y"],
                                node_positions[nodes.index(p["col_b"])]["y"],
                            ],
                            "text": f"{p['col_a']} ↔ {p['col_b']}: {p['correlation']:+.3f}",
                            "width": max(1, abs(p["correlation"]) * 4),
                        }
                    )

                h.append(f"""<div class="chart-container" style="margin:16px 0"><div id="corr-network" style="height:500px"></div></div>
<script>
(function() {{
    var nodePos = {node_positions};
    var edges = {edges};
    var traces = [];
    // Edge traces
    for (var i = 0; i < edges.length; i++) {{
        traces.push({{
            type: 'scatter', mode: 'lines',
            x: edges[i].x, y: edges[i].y,
            line: {{width: edges[i].width, color: '#8b949e'}},
            hoverinfo: 'text', text: edges[i].text,
            showlegend: false
        }});
    }}
    // Node trace
    traces.push({{
        type: 'scatter', mode: 'markers+text',
        x: nodePos.map(function(n) {{ return n.x; }}),
        y: nodePos.map(function(n) {{ return n.y; }}),
        text: nodePos.map(function(n) {{ return n.label; }}),
        textposition: 'middle center', textfont: {{size: 12, color: '{_font_color}'}},
        marker: {{size: 20, color: '#58a6ff', line: {{width: 2, color: '{_plot_bg}'}}}},
        hoverinfo: 'text',
        text: nodePos.map(function(n) {{ return n.label; }}),
        showlegend: false
    }});
    var layout = {{
        paper_bgcolor: '{_plot_bg}', plot_bgcolor: '{_plot_bg}',
        font: {{color: '{_font_color}'}},
        height: 500, margin: {{l: 20, r: 20, t: 20, b: 20}},
        xaxis: {{visible: false, range: [-250, 250]}},
        yaxis: {{visible: false, range: [-250, 250]}},
        showlegend: false
    }};
    Plotly.newPlot('corr-network', traces, layout, {{responsive: true, displayModeBar: false}});
}})();
</script>""")

        # Actionable Recommendations
        h.append(
            '<div id="recommendations" class="section"><h2>EDA Recommendations</h2>'
        )
        h.append('<ul class="insights">')
        # Missing data recommendations
        for c in df.columns:
            nc = col_analysis[c]["null_count"]
            if nc > 0:
                pct = col_analysis[c]["null_pct"]
                if c in numeric_cols:
                    if pct < 5:
                        h.append(
                            f'<li class="good"><b>{c}</b>: {pct}% missing - fill with median/mean</li>'
                        )
                    elif pct < 20:
                        h.append(
                            f'<li class="warn"><b>{c}</b>: {pct}% missing - consider KNN imputation or model-based imputation</li>'
                        )
                    else:
                        h.append(
                            f'<li class="bad"><b>{c}</b>: {pct}% missing - consider dropping or creating a separate "missing" category</li>'
                        )
                elif c in cat_cols:
                    if pct < 10:
                        h.append(
                            f'<li class="good"><b>{c}</b>: {pct}% missing - fill with mode or "Unknown"</li>'
                        )
                    else:
                        h.append(
                            f'<li class="warn"><b>{c}</b>: {pct}% missing - consider dropping if not predictive</li>'
                        )
        # Skewness recommendations
        for c in numeric_cols:
            skew = col_analysis[c].get("skew", 0)
            if abs(skew) > 1:
                transform = "log" if skew > 0 else "log(-x + max + 1)"
                h.append(
                    f'<li class="warn"><b>{c}</b>: skewed ({skew:+.2f}) - apply {transform} transform for normality</li>'
                )
        # Outlier recommendations
        for c in numeric_cols:
            oc = col_analysis[c].get("outlier_count", 0)
            if oc > 0:
                pct = col_analysis[c]["outlier_pct"]
                if pct < 5:
                    h.append(
                        f'<li class="good"><b>{c}</b>: {oc:,} outliers ({pct}%) - consider capping at 1.5*IQR</li>'
                    )
                else:
                    h.append(
                        f'<li class="warn"><b>{c}</b>: {oc:,} outliers ({pct}%) - investigate data quality or use robust scaling</li>'
                    )
        # High cardinality recommendations
        for c in cat_cols:
            uniq = col_analysis[c]["unique"]
            if uniq > rows * 0.5 and uniq > 10:
                h.append(
                    f'<li class="warn"><b>{c}</b>: high cardinality ({uniq:,} unique) - likely an ID column, drop or use target encoding</li>'
                )
            elif uniq > 20:
                h.append(
                    f'<li class="good"><b>{c}</b>: moderate cardinality ({uniq} unique) - consider target encoding or embedding</li>'
                )
        # Correlation recommendations
        for p in corr_pairs[:3]:
            if abs(p["correlation"]) > 0.8:
                h.append(
                    f'<li class="warn"><b>{p["col_a"]}</b> ↔ <b>{p["col_b"]}</b>: r={p["correlation"]:+.3f} - multicollinearity detected, consider dropping one</li>'
                )
        h.append("</ul></div>")

        # Per-column detailed analysis with side-by-side charts
        h.append('<div class="section"><h2>Variable Analysis</h2>')
        for c in df.columns:
            info = col_analysis[c]
            anchor = c.replace(" ", "-")
            h.append(
                f'<div id="col-{anchor}" class="cc-card"><div class="cc-hdr"><h3>{c}</h3><span class="badge">{info["dtype"]}</span></div><div class="cc-body"><div class="split"><div class="split-left"><table>'
            )
            h.append(f"<tr><td>Count</td><td>{info['count']:,}</td></tr>")
            h.append(
                f"<tr><td>Missing</td><td>{info['null_count']:,} ({info['null_pct']}%)</td></tr>"
            )
            h.append(
                f"<tr><td>Unique</td><td>{info['unique']:,} ({info['unique_pct']}%)</td></tr>"
            )
            if c in numeric_cols:
                for k in [
                    "mean",
                    "median",
                    "std",
                    "min",
                    "q1",
                    "q3",
                    "max",
                    "skew",
                    "kurtosis",
                ]:
                    h.append(f"<tr><td>{k.title()}</td><td>{info[k]:,.4f}</td></tr>")
                h.append(
                    f"<tr><td>Zeros</td><td>{info.get('zero_count', 0):,} ({info.get('zero_pct', 0)}%)</td></tr>"
                )
                h.append(
                    f"<tr><td>Outliers</td><td>{info['outlier_count']:,} ({info['outlier_pct']}%)</td></tr>"
                )
            elif c in cat_cols:
                h.append(
                    f"<tr><td>Mode</td><td>{info['mode']}</td></tr></table><h4 style='margin-top:10px;color:#8b949e;font-size:12px'>Top Values</h4><table><tr><th>Value</th><th>Count</th><th>%</th><th>Bar</th></tr>"
                )
                for val, count in info["top_values"].items():
                    pct = round(count / rows * 100, 1)
                    h.append(
                        f'<tr><td>{val}</td><td>{count:,}</td><td>{pct}%</td>'
                        f'<td><div class="mbar"><div class="mbar-fill" style="width:{pct}%;background:var(--accent)"></div></div></td></tr>'
                    )
            elif c in datetime_cols:
                h.append(
                    f"<tr><td>Min Date</td><td>{df[c].min()}</td></tr><tr><td>Max Date</td><td>{df[c].max()}</td></tr><tr><td>Time Span</td><td>{df[c].max() - df[c].min()}</td></tr>"
                )
            h.append("</table></div>")
            h.append('<div class="split-right"><div class="chart-container">')
            chart_id = f"chart-{anchor}"
            h.append(f'<div id="{chart_id}" style="height:var(--chart-h)"></div>')
            if c in numeric_cols:
                # Use Plotly's built-in data reference for large datasets
                clean_data = df[c].dropna().tolist()
                h.append(f"""<script>
(function() {{
    var d = {clean_data};
    var trace1 = {{x: d, type: 'histogram', nbinsx: 50, marker: {{color: '#58a6ff', opacity: 0.7}}, yaxis: 'y'}};
    var trace2 = {{y: d, type: 'box', marker: {{color: '#f0883e'}}, xaxis: 'x2', yaxis: 'y2'}};
    var layout = {{
        paper_bgcolor: '{_plot_bg}', plot_bgcolor: '{_plot_bg}',
        font: {{color: '{_font_color}'}},
        grid: {{rows: 2, columns: 1, pattern: 'independent'}},
        height: 420, margin: {{l: 50, r: 20, t: 10, b: 30}},
        yaxis: {{title: 'Count'}},
        yaxis2: {{title: ''}}
    }};
    Plotly.newPlot('{chart_id}', [trace1, trace2], layout, {{responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d']}});
}})();
</script>""")
            elif c in cat_cols:
                tv = df[c].value_counts().head(15)
                h.append(f"""<script>
(function() {{
    var data = [{{
        x: {tv.index.tolist()},
        y: {tv.values.tolist()},
        type: 'bar',
        marker: {{color: '#58a6ff'}},
        text: {tv.values.tolist()},
        textposition: 'outside'
    }}];
    var layout = {{
        paper_bgcolor: '{_plot_bg}', plot_bgcolor: '{_plot_bg}',
        font: {{color: '{_font_color}'}},
        height: 420, margin: {{l: 50, r: 20, t: 10, b: 80}},
        xaxis: {{tickangle: -45}}
    }};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d']}});
}})();
</script>""")
            elif c in datetime_cols:
                ts = df[c].value_counts().sort_index()
                h.append(f"""<script>
(function() {{
    var data = [{{
        x: {ts.index.tolist()},
        y: {ts.values.tolist()},
        type: 'scatter', mode: 'lines+markers',
        marker: {{color: '#3fb950'}},
        line: {{color: '#3fb950'}}
    }}];
    var layout = {{
        paper_bgcolor: '{_plot_bg}', plot_bgcolor: '{_plot_bg}',
        font: {{color: '{_font_color}'}},
        height: 420, margin: {{l: 50, r: 20, t: 10, b: 30}}
    }};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d']}});
}})();
</script>""")
            h.append("</div></div></div></div></div>")

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

        progress.append(
            ok(
                "Auto profile saved",
                f"{out.name} ({size_kb:,} KB) - {rows:,} rows x {cols} columns",
            )
        )

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
            "outlier_columns": sum(
                1 for c in numeric_cols if col_analysis[c].get("outlier_count", 0) > 0
            ),
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
# generate_pairwise_plot
# ---------------------------------------------------------------------------


def generate_pairwise_plot(
    file_path: str,
    columns: list[str] = None,
    max_cols: int = 6,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    """Pairwise scatter + histogram matrix for numeric columns. Opens HTML."""
    progress = []
    try:
        try:
            import plotly.express as px
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
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

        if columns:
            cols_to_plot = [c for c in columns if c in numeric_cols]
        else:
            cols_to_plot = numeric_cols[:max_cols]

        if len(cols_to_plot) < 2:
            return {
                "success": False,
                "error": "Need at least 2 numeric columns",
                "hint": f"Only found {len(cols_to_plot)} numeric columns to plot",
                "progress": [fail("Insufficient columns", "")],
                "token_estimate": 20,
            }

        # Drop NaN rows for clean pairwise plot
        plot_df = df[cols_to_plot].dropna()
        if len(plot_df) == 0:
            return {
                "success": False,
                "error": "No complete rows after dropping NaN values",
                "hint": "Check data quality or reduce columns",
                "progress": [fail("No complete data", "")],
                "token_estimate": 20,
            }

        fig = px.scatter_matrix(
            plot_df,
            title=f"Pairwise Plot: {', '.join(cols_to_plot)}",
            template=plotly_template(theme),
        )
        fig.update_layout(autosize=True)

        abs_p, fname = _save_chart(fig, output_path, "pairwise", path, open_after, theme)

        progress.append(
            ok("Pairwise plot saved", f"{fname} — {len(cols_to_plot)} columns")
        )

        result = {
            "success": True,
            "op": "generate_pairwise_plot",
            "output_path": abs_p,
            "output_name": fname,
            "columns_plotted": cols_to_plot,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_pairwise_plot error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and column names.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# export_data
# ---------------------------------------------------------------------------


def export_data(
    file_path: str,
    output_path: str = "",
    format: str = "csv",
    encoding: str = "utf-8",
    separator: str = ",",
    open_after: bool = True,
) -> dict:
    """Export dataset to CSV, Excel, or JSON format."""
    progress = []
    try:
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

        valid_formats = {"csv", "json", "excel"}
        if format not in valid_formats:
            return {
                "success": False,
                "error": f"Invalid format: {format}",
                "hint": f"Valid formats: {', '.join(sorted(valid_formats))}",
                "progress": [fail("Invalid format", format)],
                "token_estimate": 20,
            }

        if output_path:
            out = Path(output_path)
        else:
            ext_map = {"csv": ".csv", "json": ".json", "excel": ".xlsx"}
            out = path.parent / f"{path.stem}_export{ext_map[format]}"

        if format == "csv":
            df.to_csv(str(out), index=False, encoding=encoding, sep=separator)
        if open_after:
            _open_file(out)
        elif format == "json":
            df.to_json(str(out), orient="records", indent=2)
        if open_after:
            _open_file(out)
        elif format == "excel":
            df.to_excel(str(out), index=False)

        size_kb = round(out.stat().st_size / 1024)

        progress.append(
            ok("Data exported", f"{out.name} ({size_kb:,} KB, {len(df)} rows)")
        )

        result = {
            "success": True,
            "op": "export_data",
            "output_path": str(out.resolve()),
            "output_name": out.name,
            "format": format,
            "rows": len(df),
            "columns": len(df.columns),
            "file_size_kb": size_kb,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("export_data error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path and format.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# generate_multi_chart — multi-variable comparison
# ---------------------------------------------------------------------------


def generate_multi_chart(
    file_path: str,
    chart_type: str,
    value_columns: list[str],
    category_column: str = "",
    date_column: str = "",
    agg_func: str = "sum",
    output_path: str = "",
    title: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    """Multi-variable bar/line chart. Compares 2+ metrics. Opens HTML."""
    progress = []
    try:
        try:
            import plotly.graph_objects as go
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

        valid_types = {"multi_bar", "multi_line"}
        if chart_type not in valid_types:
            return {
                "success": False,
                "error": f"Invalid chart_type: {chart_type}",
                "hint": f"Valid types: {', '.join(sorted(valid_types))}",
                "progress": [fail("Invalid chart type", chart_type)],
                "token_estimate": 30,
            }

        df = _read_csv(str(path))

        if chart_type == "multi_line" and not date_column:
            return {
                "success": False,
                "error": "multi_line requires date_column",
                "hint": "Provide date_column for time-based multi-line chart.",
                "progress": [fail("Missing param", "date_column")],
                "token_estimate": 30,
            }

        chart_title = (
            title if title else f"Multi-{chart_type.replace('_', ' ').title()}"
        )
        fig = go.Figure()

        if chart_type == "multi_bar":
            if category_column:
                grouped = df.groupby(category_column, as_index=False)[
                    value_columns
                ].agg(agg_func)
                x_vals = grouped[category_column]
            else:
                x_vals = range(len(df))
                grouped = df
            for vc in value_columns:
                fig.add_trace(go.Bar(x=x_vals, y=grouped[vc], name=vc))
        elif chart_type == "multi_line":
            df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
            df = df.dropna(subset=[date_column])
            df["period"] = df[date_column].dt.to_period("M").astype(str)
            grouped = df.groupby("period", as_index=False)[value_columns].agg(agg_func)
            x_vals = grouped["period"]
            for vc in value_columns:
                fig.add_trace(
                    go.Scatter(x=x_vals, y=grouped[vc], name=vc, mode="lines+markers")
                )

        fig.update_layout(
            title=chart_title,
            template=plotly_template(theme),
            xaxis_title=category_column or "Period",
            yaxis_title=agg_func.title(),
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
            margin=dict(l=20, r=20, t=40, b=20),
        )

        abs_p, fname = _save_chart(fig, output_path, f"multi_{chart_type}", path, open_after, theme)

        progress.append(
            ok("Multi-chart saved", f"{fname} - {len(value_columns)} metrics")
        )

        result = {
            "success": True,
            "op": "generate_multi_chart",
            "chart_type": chart_type,
            "output_path": abs_p,
            "output_name": fname,
            "title": chart_title,
            "metrics_plotted": value_columns,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_multi_chart error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path, column names, and chart_type.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# generate_chart (kept, with open_after support)
# ---------------------------------------------------------------------------
# generate_chart (kept, with open_after support)
# ---------------------------------------------------------------------------


def generate_chart(
    file_path: str,
    chart_type: str,
    value_column: str,
    category_column: str = "",
    agg_func: str = "sum",
    color_column: str = "",
    date_column: str = "",
    period: str = "M",
    hierarchy_columns: list[str] = None,
    geo_file_path: str = "",
    geo_join_column: str = "",
    output_path: str = "",
    title: str = "",
    theme: str = "dark",
    open_after: bool = True,
) -> dict:
    """Generate Plotly chart. type: bar pie line scatter geo treemap radius."""
    progress = []
    try:
        try:
            import plotly.express as px
            import plotly.graph_objects as go
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

        valid_types = {
            "bar",
            "pie",
            "line",
            "scatter",
            "geo",
            "treemap",
            "time_series",
            "radius",
        }
        if chart_type not in valid_types:
            return {
                "success": False,
                "error": f"Invalid chart_type: {chart_type}",
                "hint": f"Valid types: {', '.join(sorted(valid_types))}",
                "progress": [fail("Invalid chart type", chart_type)],
                "token_estimate": 30,
            }

        df = _read_csv(str(path))

        # Validate required columns
        if chart_type == "geo":
            if not geo_file_path or not geo_join_column:
                return {
                    "success": False,
                    "error": "geo chart requires geo_file_path and geo_join_column",
                    "hint": "Provide both geo_file_path and geo_join_column.",
                    "progress": [
                        fail("Missing params", "geo_file_path, geo_join_column")
                    ],
                    "token_estimate": 30,
                }

        if chart_type == "treemap":
            if not hierarchy_columns:
                return {
                    "success": False,
                    "error": "treemap requires hierarchy_columns",
                    "hint": "Provide hierarchy_columns list.",
                    "progress": [fail("Missing params", "hierarchy_columns")],
                    "token_estimate": 30,
                }

        if chart_type == "time_series":
            if not date_column:
                return {
                    "success": False,
                    "error": "time_series requires date_column",
                    "hint": "Provide date_column parameter.",
                    "progress": [fail("Missing params", "date_column")],
                    "token_estimate": 30,
                }

        # Aggregate data for chart
        if chart_type in ("bar", "pie", "line", "scatter"):
            if category_column:
                grouped = df.groupby(category_column, as_index=False)[value_column].agg(
                    agg_func
                )
                grouped = grouped.sort_values(by=value_column, ascending=False)
                chart_df = grouped
            else:
                chart_df = df
        elif chart_type == "time_series":
            df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
            df = df.dropna(subset=[date_column])
            df["period"] = df[date_column].dt.to_period(period).astype(str)
            grouped = df.groupby("period", as_index=False)[value_column].agg(agg_func)
            chart_df = grouped
        elif chart_type == "treemap":
            chart_df = df
        elif chart_type == "geo":
            try:
                import geopandas as gpd
            except ImportError:
                return {
                    "success": False,
                    "error": "geopandas required for geo charts",
                    "hint": "Install: uv add geopandas",
                    "progress": [fail("Missing dependency", "geopandas")],
                    "token_estimate": 20,
                }
            gdf = gpd.read_file(geo_file_path)
            grouped = df.groupby(category_column, as_index=False)[value_column].agg(
                agg_func
            )
            merged = gdf.merge(
                grouped, left_on=geo_join_column, right_on=category_column, how="left"
            )
            chart_df = merged
        elif chart_type == "radius":
            chart_df = df
        else:
            chart_df = df

        # Generate chart
        chart_title = title if title else f"{agg_func} of {value_column}"
        if category_column:
            chart_title += f" by {category_column}"

        fig = None
        if chart_type == "bar":
            fig = px.bar(
                chart_df,
                x=category_column,
                y=value_column,
                title=chart_title,
                template=plotly_template(theme),
                color=color_column if color_column else None,
            )
        elif chart_type == "pie":
            fig = px.pie(
                chart_df,
                names=category_column,
                values=value_column,
                title=chart_title,
                template=plotly_template(theme),
                hole=0.5,
            )
        elif chart_type == "line":
            fig = px.line(
                chart_df,
                x=category_column,
                y=value_column,
                title=chart_title,
                template=plotly_template(theme),
                color=color_column if color_column else None,
            )
        elif chart_type == "scatter":
            fig = px.scatter(
                chart_df,
                x=category_column,
                y=value_column,
                title=chart_title,
                template=plotly_template(theme),
                color=color_column if color_column else None,
            )
        elif chart_type == "geo":
            fig = px.choropleth_mapbox(
                chart_df,
                geojson=chart_df.geometry,
                locations=chart_df.index,
                color=value_column,
                title=chart_title,
                template=plotly_template(theme),
                mapbox_style="carto-positron",
                center={"lat": 37.09, "lon": -73.94},
                zoom=3,
            )
        elif chart_type == "treemap":
            fig = px.treemap(
                chart_df,
                path=hierarchy_columns,
                values=value_column,
                title=chart_title,
                template=plotly_template(theme),
            )
        elif chart_type == "time_series":
            fig = px.line(
                chart_df,
                x="period",
                y=value_column,
                title=chart_title,
                template=plotly_template(theme),
                markers=True,
            )
            fig.update_xaxes(title_text="Period")
        elif chart_type == "radius":
            fig = go.Figure()
            fig.add_trace(
                go.Scatterpolar(
                    r=chart_df[value_column].tolist(),
                    theta=chart_df[category_column].tolist(),
                    fill="toself",
                    name=value_column,
                )
            )
            fig.update_layout(
                polar=dict(radialaxis=dict(visible=True)),
                title=chart_title,
                template=plotly_template(theme),
            )

        if fig is None:
            return {
                "success": False,
                "error": f"Failed to create {chart_type} chart",
                "hint": "Check column names and chart type.",
                "progress": [fail("Chart creation failed", chart_type)],
                "token_estimate": 20,
            }

        fig.update_layout(margin=dict(l=20, r=20, t=40, b=20))
        rows_plotted = len(chart_df)

        abs_p, fname = _save_chart(fig, output_path, chart_type, path, open_after, theme)

        progress.append(ok("Chart saved", f"{fname} ({rows_plotted} rows)"))

        result = {
            "success": True,
            "op": "generate_chart",
            "chart_type": chart_type,
            "output_path": abs_p,
            "output_name": fname,
            "title": chart_title,
            "rows_plotted": rows_plotted,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_chart error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path, column names, and chart_type.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# generate_dashboard — interactive HTML dashboard (fully client-side)
# ---------------------------------------------------------------------------



def generate_dashboard(
    file_path: str,
    output_path: str = "",
    title: str = "",
    chart_types: list[str] = None,
    geo_file_path: str = "",
    theme: str = "dark",
    dry_run: bool = False,
    open_after: bool = True,
) -> dict:
    """Generate interactive HTML dashboard with auto-detected charts. Opens HTML."""
    import json as _json
    import re as _re

    def _safe(s: str) -> str:
        return _re.sub(r"[^a-zA-Z0-9]", "_", str(s))

    progress = []
    try:
        try:
            import plotly.graph_objects as _go  # noqa: F401
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
        dashboard_title = title if title else path.stem

        # ── Column classification ──────────────────────────────────────────────
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        datetime_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
        cat_cols = [
            c for c in df.columns
            if c not in numeric_cols and c not in datetime_cols
            and df[c].nunique() <= 100
        ]

        # ── Chart auto-detection ───────────────────────────────────────────────
        detected: list[str] = []
        if numeric_cols and cat_cols:
            detected.append("bar")
        if datetime_cols and numeric_cols:
            detected.append("time_series")
        if len(numeric_cols) >= 2:
            detected.append("scatter")
        if cat_cols:
            detected.append("pie")
        charts = chart_types if chart_types else detected

        if dry_run:
            progress.append(info("Dry run — no file written", path.name))
            result: dict = {
                "success": True,
                "dry_run": True,
                "op": "generate_dashboard",
                "would_generate": {
                    "title": dashboard_title,
                    "charts": charts,
                    "kpi_columns": numeric_cols[:8],
                    "filter_columns": cat_cols[:8],
                },
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        # ── Embed data (cap at 20 K rows) ──────────────────────────────────────
        EMBED_LIMIT = 20_000
        was_sampled = len(df) > EMBED_LIMIT
        embed_df = (
            df.sample(EMBED_LIMIT, random_state=42) if was_sampled else df.copy()
        )
        embed_clean = embed_df.copy()
        for c in datetime_cols:
            if c in embed_clean.columns:
                embed_clean[c] = (
                    pd.to_datetime(embed_clean[c], errors="coerce")
                    .dt.strftime("%Y-%m-%d")
                    .fillna("")
                )
        raw_json = embed_clean.to_json(orient="records", date_format="iso")

        # ── Pre-computed static elements ───────────────────────────────────────
        sparklines: dict = {}
        for nc in numeric_cols[:8]:
            n_pts = min(30, len(df))
            step = max(1, len(df) // n_pts)
            sv = df[nc].iloc[::step].head(n_pts).fillna(0).tolist()
            sparklines[nc] = [0 if (isinstance(v, float) and v != v) else v for v in sv]

        def _trend(col: str) -> tuple[str, str]:
            mid = len(df) // 2
            if mid == 0:
                return "→", "trend-flat"
            a = df[col].iloc[:mid].mean()
            b = df[col].iloc[mid:].mean()
            if pd.isna(a) or pd.isna(b):
                return "→", "trend-flat"
            if b > a * 1.02:
                return "↑", "trend-up"
            if b < a * 0.98:
                return "↓", "trend-down"
            return "→", "trend-flat"

        # Filter controls (pills ≤10, searchable dropdown 11-50)
        filter_controls: list[dict] = []
        for cc in cat_cols[:8]:
            uniq = sorted(df[cc].dropna().astype(str).unique().tolist())
            if 1 < len(uniq) <= 50:
                filter_controls.append(
                    {
                        "col": cc,
                        "values": uniq[:50],
                        "style": "pills" if len(uniq) <= 10 else "dropdown",
                    }
                )

        # Numeric range filters (top 3)
        num_ranges: list[dict] = []
        for nc in numeric_cols[:3]:
            mn, mx = float(df[nc].min()), float(df[nc].max())
            if mn < mx:
                num_ranges.append({"col": nc, "min": mn, "max": mx})

        # Quality score
        null_pct = float(df.isnull().mean().mean() * 100)
        dup_pct = float(df.duplicated().sum() / max(len(df), 1) * 100)
        quality = max(0, round(100 - null_pct * 2 - dup_pct * 0.5))
        qual_clr = (
            "var(--green)" if quality >= 80
            else "var(--orange)" if quality >= 60
            else "var(--red)"
        )

        # ── Theme ──────────────────────────────────────────────────────────────
        _css = css_vars(theme)
        if theme == "dark":
            bg, font_c, grid_c = "#161b22", "#c9d1d9", "rgba(255,255,255,0.07)"
        elif theme == "light":
            bg, font_c, grid_c = "#ffffff", "#1f2328", "rgba(0,0,0,0.07)"
        else:
            bg, font_c, grid_c = "#f6f8fa", "#1f2328", "rgba(0,0,0,0.07)"

        # ── HTML HEAD ──────────────────────────────────────────────────────────
        h: list[str] = []
        h.append(
            f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
{VIEWPORT_META}
<title>{dashboard_title} — Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
{_css}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text);overflow-x:hidden}}
::-webkit-scrollbar{{width:6px}}::-webkit-scrollbar-thumb{{background:var(--border);border-radius:3px}}
header{{background:var(--surface);border-bottom:1px solid var(--border);padding:14px 28px;display:flex;flex-wrap:wrap;gap:10px;align-items:center}}
header h1{{color:var(--accent);font-size:20px;font-weight:700;flex:1 1 auto}}
.row-ctr{{color:var(--text-muted);font-size:12px}}
.btn{{background:var(--bg);border:1px solid var(--border);color:var(--text-muted);border-radius:6px;padding:5px 12px;font-size:12px;cursor:pointer}}
.btn:hover{{border-color:var(--accent);color:var(--accent)}}
.btn-p{{background:var(--accent);color:#fff;border-color:var(--accent)}}
.btn-p:hover{{opacity:0.88;color:#fff}}
.filter-bar{{background:var(--surface);border-bottom:1px solid var(--border);padding:12px 28px;display:flex;flex-wrap:wrap;gap:16px;align-items:flex-end}}
.fgrp{{display:flex;flex-direction:column;gap:5px}}
.flbl{{font-size:11px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.04em;font-weight:600}}
.pills{{display:flex;flex-wrap:wrap;gap:4px}}
.pill{{background:var(--bg);border:1px solid var(--border);color:var(--text-muted);border-radius:100px;padding:3px 11px;font-size:12px;cursor:pointer;transition:.12s}}
.pill.active{{background:var(--accent);border-color:var(--accent);color:#fff}}
.pill:hover:not(.active){{border-color:var(--accent);color:var(--text)}}
.ddw{{position:relative}}
.ddbtn{{background:var(--bg);border:1px solid var(--border);color:var(--text);border-radius:6px;padding:5px 10px;font-size:12px;cursor:pointer;min-width:140px;text-align:left;white-space:nowrap}}
.ddmenu{{position:absolute;top:calc(100% + 4px);left:0;z-index:200;background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:8px;min-width:200px;max-height:280px;overflow-y:auto;box-shadow:0 8px 28px rgba(0,0,0,.35)}}
.ddmenu.hid{{display:none}}
.ddsrch{{width:100%;background:var(--bg);border:1px solid var(--border);color:var(--text);border-radius:4px;padding:4px 8px;font-size:12px;margin-bottom:6px;outline:none}}
.ddacts{{display:flex;gap:6px;margin-bottom:8px}}
.ddacts .btn{{padding:2px 8px;font-size:11px}}
.optlbl{{display:flex;align-items:center;gap:7px;padding:3px 4px;border-radius:4px;font-size:12px;cursor:pointer;user-select:none}}
.optlbl:hover{{background:var(--bg)}}
.optlbl input{{accent-color:var(--accent)}}
.nrng{{display:flex;gap:6px;align-items:center}}
.ninp{{width:88px;background:var(--bg);border:1px solid var(--border);color:var(--text);border-radius:6px;padding:4px 8px;font-size:12px;outline:none}}
.ninp:focus{{border-color:var(--accent)}}
.nsep{{color:var(--text-muted);font-size:13px}}
.kpi-row{{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:12px;padding:18px 28px}}
.kpi-card{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:14px 16px}}
.kpi-val{{font-size:22px;font-weight:700;color:var(--accent);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.kpi-lbl{{font-size:10px;color:var(--text-muted);margin-top:2px;text-transform:uppercase;letter-spacing:.04em;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.kpi-trend{{font-size:12px;margin-top:3px;font-weight:600}}
.kpi-spark{{height:34px;margin-top:6px;pointer-events:none}}
.trend-up{{color:var(--green)}}.trend-down{{color:var(--red)}}.trend-flat{{color:var(--text-muted)}}
.sec-hdr{{padding:16px 28px 4px;font-size:11px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.06em;font-weight:600}}
.cgrid{{padding:0 28px 28px;display:grid;grid-template-columns:repeat(auto-fill,minmax(480px,1fr));gap:14px}}
.cc{{background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:hidden}}
.cc-hdr{{display:flex;align-items:center;padding:10px 14px;border-bottom:1px solid var(--border)}}
.cc-hdr h3{{font-size:12px;font-weight:500;color:var(--text);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.exp{{background:none;border:none;color:var(--text-muted);cursor:pointer;font-size:15px;line-height:1;padding:0 2px;flex-shrink:0}}
.exp:hover{{color:var(--accent)}}
.cc-body{{padding:4px}}
.full{{grid-column:1/-1}}
.modal{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.72);z-index:1000;align-items:center;justify-content:center}}
.modal.open{{display:flex}}
.mbox{{background:var(--surface);border:1px solid var(--border);border-radius:12px;width:92vw;max-width:1200px;height:88vh;display:flex;flex-direction:column}}
.mhdr{{display:flex;align-items:center;padding:14px 18px;border-bottom:1px solid var(--border)}}
.mhdr h3{{flex:1;font-size:14px;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.mclose{{background:none;border:none;color:var(--text-muted);font-size:20px;cursor:pointer;line-height:1;flex-shrink:0}}
.mclose:hover{{color:var(--red)}}
#mdiv{{flex:1;min-height:0}}
@media(max-width:1100px){{.cgrid{{grid-template-columns:1fr}}}}
@media(max-width:600px){{header,.filter-bar,.kpi-row,.cgrid,.sec-hdr{{padding-left:14px;padding-right:14px}}.kpi-row{{grid-template-columns:repeat(2,1fr)}}}}
</style></head><body>"""
        )

        # ── HEADER ─────────────────────────────────────────────────────────────
        sampled_note = " (sampled)" if was_sampled else ""
        h.append(
            f"""<header>
  <h1>{dashboard_title}</h1>
  <span class="row-ctr" id="row-ctr">{len(embed_df):,} of {len(embed_df):,} rows{sampled_note}</span>
  <button class="btn" onclick="clearAll()">Clear Filters</button>
  <button class="btn btn-p" onclick="exportCSV()">&#x2193; Export CSV</button>
</header>"""
        )

        # ── FILTER BAR ─────────────────────────────────────────────────────────
        if filter_controls or num_ranges:
            h.append('<div class="filter-bar">')
            for fc in filter_controls:
                col, vals, style = fc["col"], fc["values"], fc["style"]
                lbl = col.replace('"', "&quot;")
                col_js = col.replace("\\", "\\\\").replace("'", "\\'")
                h.append(f'<div class="fgrp"><div class="flbl">{lbl}</div>')
                if style == "pills":
                    h.append(f'<div class="pills" data-col="{lbl}">')
                    for v in vals:
                        ve = str(v).replace('"', "&quot;").replace("'", "&#39;")
                        h.append(
                            f'<button class="pill active" data-val="{ve}"'
                            f' onclick="pilClick(this)">{ve}</button>'
                        )
                    h.append("</div>")
                else:
                    opts = "".join(
                        f'<label class="optlbl"><input type="checkbox" data-val='
                        f'"{str(v).replace(chr(34), "&quot;")}" checked'
                        f' onchange="ddChange(\'{col_js}\')">'
                        f"{str(v).replace('<', '&lt;')}</label>"
                        for v in vals
                    )
                    h.append(
                        f'<div class="ddw" data-col="{lbl}">'
                        f'<button class="ddbtn" onclick="ddToggle(this)">All &#x25BE;</button>'
                        f'<div class="ddmenu hid">'
                        f'<input class="ddsrch" placeholder="Search..."'
                        f' oninput="ddSrch(this,\'{col_js}\')">'
                        f'<div class="ddacts">'
                        f'<button class="btn" onclick="ddAll(\'{col_js}\',true)">All</button>'
                        f'<button class="btn" onclick="ddAll(\'{col_js}\',false)">None</button>'
                        f"</div>{opts}</div></div>"
                    )
                h.append("</div>")
            for nr in num_ranges:
                nc = nr["col"]
                nc_js = nc.replace("\\", "\\\\").replace("'", "\\'")
                mn_s = f'{nr["min"]:,.0f}' if abs(nr["min"]) >= 1 else f'{nr["min"]:.3f}'
                mx_s = f'{nr["max"]:,.0f}' if abs(nr["max"]) >= 1 else f'{nr["max"]:.3f}'
                h.append(
                    f'<div class="fgrp"><div class="flbl">{nc}</div>'
                    f'<div class="nrng">'
                    f'<input type="number" class="ninp" placeholder="Min ({mn_s})"'
                    f' onchange="numCh(\'{nc_js}\',\'min\',this.value)">'
                    f'<span class="nsep">–</span>'
                    f'<input type="number" class="ninp" placeholder="Max ({mx_s})"'
                    f' onchange="numCh(\'{nc_js}\',\'max\',this.value)">'
                    f"</div></div>"
                )
            h.append("</div>")

        # ── KPI ROW ────────────────────────────────────────────────────────────
        h.append('<div class="kpi-row">')
        h.append(
            f'<div class="kpi-card">'
            f'<div class="kpi-val" style="color:{qual_clr}">{quality}</div>'
            f'<div class="kpi-lbl">Quality Score</div>'
            f"</div>"
        )
        for nc in numeric_cols[:7]:
            arrow, acls = _trend(nc)
            sc = _safe(nc)
            sv = sparklines.get(nc, [])
            init_sum = df[nc].sum()
            if abs(init_sum) >= 1_000_000:
                iv = f"{init_sum / 1_000_000:.1f}M"
            elif abs(init_sum) >= 1_000:
                iv = f"{init_sum / 1_000:.1f}K"
            else:
                iv = f"{init_sum:,.0f}"
            h.append(
                f'<div class="kpi-card">'
                f'<div class="kpi-val" id="kv-{sc}">{iv}</div>'
                f'<div class="kpi-lbl">{nc}</div>'
                f'<div class="kpi-trend {acls}">{arrow}</div>'
                f'<div class="kpi-spark" id="ks-{sc}"></div>'
                f"</div>"
            )
            h.append(
                f"<script>(function(){{"
                f"Plotly.newPlot('ks-{sc}',"
                f"[{{y:{_json.dumps(sv)},type:'scatter',mode:'lines',"
                f"line:{{color:'var(--accent)',width:1.5}},"
                f"fill:'tozeroy',fillcolor:'rgba(88,166,255,0.08)'}}],"
                f"{{paper_bgcolor:'rgba(0,0,0,0)',plot_bgcolor:'rgba(0,0,0,0)',"
                f"margin:{{l:0,r:0,t:0,b:0}},xaxis:{{visible:false}},"
                f"yaxis:{{visible:false}},showlegend:false}},"
                f"{{responsive:true,displayModeBar:false,staticPlot:true}});"
                f"}})();</script>"
            )
        h.append("</div>")

        # ── CHART CONTAINERS ───────────────────────────────────────────────────
        spec: list[dict] = []
        h.append('<div class="sec-hdr">Charts</div><div class="cgrid">')

        def _card(cid: str, ttl: str, full: bool, height: int) -> None:
            cls = "cc full" if full else "cc"
            te = ttl.replace("'", "&#39;").replace('"', "&quot;")
            h.append(
                f'<div class="{cls}">'
                f'<div class="cc-hdr"><h3>{ttl}</h3>'
                f'<button class="exp" onclick="expand(\'{cid}\',\'{te}\')">&#x2922;</button>'
                f'</div><div class="cc-body">'
                f'<div id="{cid}" style="height:{height}px"></div>'
                f"</div></div>"
            )

        if "bar" in charts and cat_cols and numeric_cols:
            for cc in cat_cols[:3]:
                for nc in numeric_cols[:2]:
                    cid = f"bar_{_safe(cc)}_{_safe(nc)}"
                    _card(cid, f"Total {nc} by {cc}", False, 340)
                    spec.append({"id": cid, "type": "bar", "cc": cc, "nc": nc})

        if "pie" in charts and cat_cols:
            for cc in cat_cols[:3]:
                cid = f"pie_{_safe(cc)}"
                _card(cid, f"{cc} Distribution", False, 340)
                spec.append({"id": cid, "type": "pie", "cc": cc})

        if "scatter" in charts and len(numeric_cols) >= 2:
            pairs = [
                (numeric_cols[i], numeric_cols[j])
                for i in range(min(2, len(numeric_cols)))
                for j in range(i + 1, min(i + 3, len(numeric_cols)))
            ]
            for nc1, nc2 in pairs:
                cid = f"scat_{_safe(nc1)}_{_safe(nc2)}"
                _card(cid, f"{nc1} vs {nc2}", False, 340)
                spec.append({"id": cid, "type": "scatter", "nc1": nc1, "nc2": nc2})

        if len(cat_cols) >= 2 and numeric_cols:
            cc1, cc2, nc = cat_cols[0], cat_cols[1], numeric_cols[0]
            cid = f"grp_{_safe(cc1)}_{_safe(cc2)}"
            _card(cid, f"{nc} by {cc1}, grouped by {cc2}", True, 380)
            spec.append({"id": cid, "type": "grouped_bar", "cc1": cc1, "cc2": cc2, "nc": nc})

        if len(numeric_cols) >= 2 and cat_cols:
            nc1, nc2, cc = numeric_cols[0], numeric_cols[1], cat_cols[0]
            cid = f"cscat_{_safe(nc1)}_{_safe(nc2)}"
            _card(cid, f"{nc1} vs {nc2} by {cc}", True, 380)
            spec.append({"id": cid, "type": "cscat", "nc1": nc1, "nc2": nc2, "cc": cc})

        if numeric_cols and cat_cols:
            nc, cc = numeric_cols[0], cat_cols[0]
            cid = f"box_{_safe(nc)}_{_safe(cc)}"
            _card(cid, f"{nc} distribution by {cc}", True, 380)
            spec.append({"id": cid, "type": "box", "nc": nc, "cc": cc})

        if len(numeric_cols) >= 2:
            _card("corr_hm", "Correlation Matrix", True, 480)
            spec.append({"id": "corr_hm", "type": "corr"})

        if len(cat_cols) >= 2 and numeric_cols:
            cc1, cc2, nc = cat_cols[0], cat_cols[1], numeric_cols[0]
            cid = f"aghm_{_safe(cc1)}_{_safe(cc2)}"
            _card(cid, f"Sum {nc}: {cc1} \u00d7 {cc2}", True, 460)
            spec.append({"id": cid, "type": "agg_hm", "cc1": cc1, "cc2": cc2, "nc": nc})

        if "time_series" in charts and datetime_cols and numeric_cols:
            for dc in datetime_cols[:2]:
                for nc in numeric_cols[:2]:
                    cid = f"ts_{_safe(dc)}_{_safe(nc)}"
                    _card(cid, f"{nc} Over Time", True, 380)
                    spec.append({"id": cid, "type": "ts", "dc": dc, "nc": nc})

        for nc in numeric_cols[:6]:
            cid = f"dist_{_safe(nc)}"
            _card(cid, f"{nc} Distribution", False, 320)
            spec.append({"id": cid, "type": "dist", "nc": nc})

        h.append("</div>")

        # ── MODAL ──────────────────────────────────────────────────────────────
        h.append(
            '<div id="modal" class="modal">'
            '<div class="mbox">'
            '<div class="mhdr"><h3 id="mttl"></h3>'
            '<button class="mclose" onclick="closeM()">&#x2715;</button></div>'
            '<div id="mdiv"></div>'
            "</div></div>"
        )

        # ── JS: render function factory ────────────────────────────────────────
        COLORS = "['#58a6ff','#3fb950','#f0883e','#f85149','#bc8cff','#79c0ff','#7ee787','#ffa657','#ff7b72','#d2a8ff','#a5d6ff','#aff5b4','#ffd6a5','#ffabab','#e0b0ff']"
        PCFG = "{responsive:true,displayModeBar:true,modeBarButtonsToRemove:['lasso2d','select2d']}"

        def _lyt(h_px: int, extra: str = "") -> str:
            return (
                f"{{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',"
                f"font:{{color:'{font_c}',size:12}},"
                f"height:{h_px},margin:{{l:55,r:20,t:10,b:65}},"
                f"xaxis:{{gridcolor:'{grid_c}',tickangle:-38}},"
                f"yaxis:{{gridcolor:'{grid_c}'}}{extra}}}"
            )

        rfns: list[str] = []
        for s in spec:
            cid, t = s["id"], s["type"]

            if t == "bar":
                cc, nc = s["cc"], s["nc"]
                rfns.append(
                    f"function rf_{cid}(d){{\n"
                    f"  var a={{}};\n"
                    f"  d.forEach(function(r){{var k=String(r['{cc}']??''),v=+r['{nc}'];if(!isNaN(v))a[k]=(a[k]||0)+v;}});\n"
                    f"  var e=Object.entries(a).sort((x,y)=>y[1]-x[1]).slice(0,25);\n"
                    f"  var fmt=function(v){{return v>=1e6?(v/1e6).toFixed(1)+'M':v>=1e3?(v/1e3).toFixed(1)+'K':Math.round(v).toString();}};\n"
                    f"  Plotly.react('{cid}',[{{x:e.map(i=>i[0]),y:e.map(i=>i[1]),type:'bar',"
                    f"marker:{{color:'#58a6ff',opacity:0.85}},text:e.map(i=>fmt(i[1])),textposition:'outside'}}],"
                    f"{_lyt(340)},{PCFG});\n"
                    f"}}"
                )
            elif t == "pie":
                cc = s["cc"]
                rfns.append(
                    f"function rf_{cid}(d){{\n"
                    f"  var c={{}};\n"
                    f"  d.forEach(function(r){{var k=String(r['{cc}']??'');c[k]=(c[k]||0)+1;}});\n"
                    f"  var e=Object.entries(c).sort((x,y)=>y[1]-x[1]).slice(0,15);\n"
                    f"  var layout={{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',font:{{color:'{font_c}',size:12}},"
                    f"height:340,margin:{{l:20,r:20,t:10,b:20}},showlegend:true,legend:{{orientation:'h',y:-0.14}}}};\n"
                    f"  Plotly.react('{cid}',[{{values:e.map(i=>i[1]),labels:e.map(i=>i[0]),type:'pie',"
                    f"hole:0.38,marker:{{colors:{COLORS}}},textinfo:'label+percent',textfont:{{size:11}},"
                    f"pull:e.map((_,i)=>i===0?0.04:0)}}],layout,{{responsive:true,displayModeBar:false}});\n"
                    f"}}"
                )
            elif t == "scatter":
                nc1, nc2 = s["nc1"], s["nc2"]
                rfns.append(
                    f"function rf_{cid}(d){{\n"
                    f"  var xs=[],ys=[];\n"
                    f"  d.forEach(function(r){{var x=+r['{nc1}'],y=+r['{nc2}'];if(!isNaN(x)&&!isNaN(y)){{xs.push(x);ys.push(y);}}}});\n"
                    f"  var traces=[{{x:xs,y:ys,type:'scatter',mode:'markers',"
                    f"marker:{{color:'#58a6ff',opacity:0.5,size:5}},name:'data'}}];\n"
                    f"  if(xs.length>1){{\n"
                    f"    var n=xs.length,sx=xs.reduce((a,b)=>a+b,0),sy=ys.reduce((a,b)=>a+b,0),sxy=0,sxx=0,syy=0;\n"
                    f"    for(var i=0;i<n;i++){{sxy+=xs[i]*ys[i];sxx+=xs[i]*xs[i];syy+=ys[i]*ys[i];}}\n"
                    f"    var sl=(n*sxy-sx*sy)/(n*sxx-sx*sx||1),ic=(sy-sl*sx)/n;\n"
                    f"    var r=(n*sxy-sx*sy)/Math.sqrt(((n*sxx-sx*sx)*(n*syy-sy*sy))||1);\n"
                    f"    var xmn=Math.min(...xs),xmx=Math.max(...xs);\n"
                    f"    traces.push({{x:[xmn,xmx],y:[sl*xmn+ic,sl*xmx+ic],type:'scatter',mode:'lines',"
                    f"line:{{color:'#f0883e',width:2,dash:'dash'}},name:'r='+r.toFixed(2)}});\n"
                    f"  }}\n"
                    f"  var layout=Object.assign({{}},{_lyt(340)},{{showlegend:true,legend:{{x:0,y:1.1,orientation:'h'}},"
                    f"xaxis:{{title:'{nc1}',gridcolor:'{grid_c}'}},yaxis:{{title:'{nc2}',gridcolor:'{grid_c}'}}}});\n"
                    f"  Plotly.react('{cid}',traces,layout,{PCFG});\n"
                    f"}}"
                )
            elif t == "grouped_bar":
                cc1, cc2, nc = s["cc1"], s["cc2"], s["nc"]
                rfns.append(
                    f"function rf_{cid}(d){{\n"
                    f"  var a={{}};\n"
                    f"  d.forEach(function(r){{var k1=String(r['{cc1}']??''),k2=String(r['{cc2}']??''),v=+r['{nc}'];"
                    f"if(!isNaN(v)){{if(!a[k2])a[k2]={{}};a[k2][k1]=(a[k2][k1]||0)+v;}}}});\n"
                    f"  var gs=Array.from(new Set(d.map(r=>String(r['{cc1}']??'')))).slice(0,20);\n"
                    f"  var ks=Object.keys(a).slice(0,10),C={COLORS};\n"
                    f"  var traces=ks.map(function(k,i){{return{{x:gs,y:gs.map(g=>(a[k]&&a[k][g])||0),"
                    f"type:'bar',name:k,marker:{{color:C[i%15],opacity:0.85}}}};}});\n"
                    f"  var layout=Object.assign({{}},{_lyt(380)},{{barmode:'group',"
                    f"showlegend:true,legend:{{orientation:'h',y:-0.3}}}});\n"
                    f"  Plotly.react('{cid}',traces,layout,{PCFG});\n"
                    f"}}"
                )
            elif t == "cscat":
                nc1, nc2, cc = s["nc1"], s["nc2"], s["cc"]
                rfns.append(
                    f"function rf_{cid}(d){{\n"
                    f"  var g={{}};\n"
                    f"  d.forEach(function(r){{var x=+r['{nc1}'],y=+r['{nc2}'],k=String(r['{cc}']??'');"
                    f"if(!isNaN(x)&&!isNaN(y)){{if(!g[k])g[k]={{x:[],y:[]}};g[k].x.push(x);g[k].y.push(y);}}}});\n"
                    f"  var ks=Object.keys(g).slice(0,15),C={COLORS};\n"
                    f"  var traces=ks.map(function(k,i){{return{{x:g[k].x,y:g[k].y,type:'scatter',"
                    f"mode:'markers',name:k,marker:{{color:C[i%15],opacity:0.6,size:5}}}};}});\n"
                    f"  var layout=Object.assign({{}},{_lyt(380)},{{showlegend:true,"
                    f"legend:{{orientation:'h',y:-0.3}},"
                    f"xaxis:{{title:'{nc1}',gridcolor:'{grid_c}'}},"
                    f"yaxis:{{title:'{nc2}',gridcolor:'{grid_c}'}}}});\n"
                    f"  Plotly.react('{cid}',traces,layout,{PCFG});\n"
                    f"}}"
                )
            elif t == "box":
                nc, cc = s["nc"], s["cc"]
                rfns.append(
                    f"function rf_{cid}(d){{\n"
                    f"  var g={{}};\n"
                    f"  d.forEach(function(r){{var v=+r['{nc}'],k=String(r['{cc}']??'');"
                    f"if(!isNaN(v)){{if(!g[k])g[k]=[];g[k].push(v);}}}});\n"
                    f"  var ks=Object.keys(g).sort().slice(0,20),C={COLORS};\n"
                    f"  var traces=ks.map(function(k,i){{return{{y:g[k],type:'box',name:k,"
                    f"marker:{{color:C[i%15],size:3}},boxpoints:'outliers'}};}});\n"
                    f"  var layout=Object.assign({{}},{_lyt(380)},{{showlegend:false,"
                    f"yaxis:{{title:'{nc}',gridcolor:'{grid_c}'}}}});\n"
                    f"  Plotly.react('{cid}',traces,layout,{PCFG});\n"
                    f"}}"
                )
            elif t == "corr":
                nc_list = _json.dumps(numeric_cols[:15])
                rfns.append(
                    f"function rf_{cid}(d){{\n"
                    f"  var cols={nc_list},n=d.length;if(n<2)return;\n"
                    f"  var z=cols.map(function(r){{return cols.map(function(c){{\n"
                    f"    var xv=d.map(row=>+row[r]),yv=d.map(row=>+row[c]),pr=[];\n"
                    f"    for(var i=0;i<n;i++)if(!isNaN(xv[i])&&!isNaN(yv[i]))pr.push([xv[i],yv[i]]);\n"
                    f"    if(pr.length<2)return 0;\n"
                    f"    var mx=pr.reduce((s,p)=>s+p[0],0)/pr.length,my=pr.reduce((s,p)=>s+p[1],0)/pr.length;\n"
                    f"    var num=0,dx=0,dy=0;pr.forEach(p=>{{num+=(p[0]-mx)*(p[1]-my);dx+=(p[0]-mx)**2;dy+=(p[1]-my)**2;}});\n"
                    f"    return dx&&dy?num/Math.sqrt(dx*dy):0;\n"
                    f"  }});}});\n"
                    f"  var layout={{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',"
                    f"font:{{color:'{font_c}',size:11}},height:480,margin:{{l:120,r:20,t:10,b:120}}}};\n"
                    f"  Plotly.react('{cid}',[{{z:z,x:cols,y:cols,type:'heatmap',"
                    f"colorscale:'RdBu_r',zmid:0,zmin:-1,zmax:1,"
                    f"text:z.map(r=>r.map(v=>v.toFixed(2))),texttemplate:'%{{text}}',"
                    f"textfont:{{size:10}}}}],layout,{{responsive:true,displayModeBar:false}});\n"
                    f"}}"
                )
            elif t == "agg_hm":
                cc1, cc2, nc = s["cc1"], s["cc2"], s["nc"]
                rfns.append(
                    f"function rf_{cid}(d){{\n"
                    f"  var a={{}},Rs=new Set(),Cs=new Set();\n"
                    f"  d.forEach(function(r){{var r1=String(r['{cc1}']??''),c1=String(r['{cc2}']??''),v=+r['{nc}'];"
                    f"if(!isNaN(v)){{Rs.add(r1);Cs.add(c1);if(!a[r1])a[r1]={{}};a[r1][c1]=(a[r1][c1]||0)+v;}}}});\n"
                    f"  var rl=Array.from(Rs).sort().slice(0,30),cl=Array.from(Cs).sort().slice(0,30);\n"
                    f"  var z=rl.map(r=>cl.map(c=>(a[r]&&a[r][c])||0));\n"
                    f"  var fmt=function(v){{return v>=1e6?(v/1e6).toFixed(1)+'M':v>=1e3?(v/1e3).toFixed(1)+'K':Math.round(v).toString();}};\n"
                    f"  var layout={{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',"
                    f"font:{{color:'{font_c}',size:11}},height:460,margin:{{l:130,r:20,t:10,b:130}}}};\n"
                    f"  Plotly.react('{cid}',[{{z:z,x:cl,y:rl,type:'heatmap',"
                    f"colorscale:'YlOrRd',text:z.map(r=>r.map(fmt)),"
                    f"texttemplate:'%{{text}}',textfont:{{size:9}}}}],layout,"
                    f"{{responsive:true,displayModeBar:false}});\n"
                    f"}}"
                )
            elif t == "ts":
                dc, nc = s["dc"], s["nc"]
                rfns.append(
                    f"function rf_{cid}(d){{\n"
                    f"  var bm={{}};\n"
                    f"  d.forEach(function(r){{var dt=r['{dc}'],v=+r['{nc}'];"
                    f"if(dt&&!isNaN(v)){{var ym=String(dt).substring(0,7);bm[ym]=(bm[ym]||0)+v;}}}});\n"
                    f"  var dates=Object.keys(bm).sort(),vals=dates.map(d=>bm[d]);\n"
                    f"  var ma=vals.map(function(_,i){{if(i<2)return null;return(vals[i]+vals[i-1]+vals[i-2])/3;}});\n"
                    f"  var traces=[\n"
                    f"    {{x:dates,y:vals,type:'scatter',mode:'lines+markers',name:'{nc}',"
                    f"line:{{color:'#3fb950',width:2}},marker:{{size:4}}}},\n"
                    f"    {{x:dates.slice(2),y:ma.slice(2),type:'scatter',mode:'lines',name:'3-period MA',"
                    f"line:{{color:'#f0883e',width:2,dash:'dot'}}}}\n"
                    f"  ];\n"
                    f"  var layout=Object.assign({{}},{_lyt(380)},{{showlegend:true,"
                    f"legend:{{x:0,y:1.1,orientation:'h'}},"
                    f"xaxis:{{title:'Date',gridcolor:'{grid_c}'}},"
                    f"yaxis:{{title:'{nc}',gridcolor:'{grid_c}'}}}});\n"
                    f"  Plotly.react('{cid}',traces,layout,{PCFG});\n"
                    f"}}"
                )
            elif t == "dist":
                nc = s["nc"]
                rfns.append(
                    f"function rf_{cid}(d){{\n"
                    f"  var vals=d.map(r=>+r['{nc}']).filter(v=>!isNaN(v));if(!vals.length)return;\n"
                    f"  var layout={{paper_bgcolor:'{bg}',plot_bgcolor:'{bg}',"
                    f"font:{{color:'{font_c}',size:12}},height:320,"
                    f"margin:{{l:50,r:20,t:10,b:30}},"
                    f"grid:{{rows:1,columns:2,pattern:'independent'}},"
                    f"xaxis:{{gridcolor:'{grid_c}'}},yaxis:{{title:'Count',gridcolor:'{grid_c}'}},"
                    f"xaxis2:{{gridcolor:'{grid_c}'}},yaxis2:{{gridcolor:'{grid_c}'}}}};\n"
                    f"  Plotly.react('{cid}',"
                    f"[{{x:vals,type:'histogram',nbinsx:50,"
                    f"marker:{{color:'#58a6ff',opacity:0.75}},xaxis:'x',yaxis:'y',name:'hist'}},"
                    f"{{y:vals,type:'box',marker:{{color:'#f0883e',size:3}},"
                    f"xaxis:'x2',yaxis:'y2',boxpoints:'outliers',name:'box'}}],"
                    f"layout,{{responsive:true,displayModeBar:false}});\n"
                    f"}}"
                )

        # KPI dynamic updaters
        kpi_upd = "\n".join(
            f"  (function(){{var s=d.map(r=>+r['{nc}']).filter(v=>!isNaN(v)).reduce((a,b)=>a+b,0);"
            f"var el=document.getElementById('kv-{_safe(nc)}');"
            f"if(el)el.textContent=s>=1e6?(s/1e6).toFixed(1)+'M':s>=1e3?(s/1e3).toFixed(1)+'K':Math.round(s).toLocaleString();}})();"
            for nc in numeric_cols[:7]
        )
        render_calls = "\n".join(f"  rf_{s['id']}(d);" for s in spec)
        rfns_str = "\n\n".join(rfns)

        h.append(
            f"""<script>
var _RAW={raw_json};
var _TOTAL=_RAW.length;
var _CF={{}};
var _NF={{}};

function getFilt(){{
  return _RAW.filter(function(row){{
    for(var c in _CF){{var s=_CF[c];if(s&&s.size>0&&!s.has(String(row[c]??'')))return false;}}
    for(var c in _NF){{var r=_NF[c],v=+row[c];if(!isNaN(v)){{if(r.min!==null&&v<r.min)return false;if(r.max!==null&&v>r.max)return false;}}}}
    return true;
  }});
}}

function applyF(){{
  var d=getFilt();
  document.getElementById('row-ctr').textContent=d.length.toLocaleString()+' of '+_TOTAL.toLocaleString()+' rows';
  renderAll(d);
}}

function pilClick(btn){{
  btn.classList.toggle('active');
  var ct=btn.closest('.pills');if(!ct)return;
  var col=ct.dataset.col,all=ct.querySelectorAll('.pill'),act=ct.querySelectorAll('.pill.active');
  if(act.length===all.length||act.length===0){{delete _CF[col];}}
  else{{_CF[col]=new Set(Array.from(act).map(p=>p.dataset.val));}}
  applyF();
}}

function ddToggle(btn){{
  var m=btn.nextElementSibling;m.classList.toggle('hid');
  document.querySelectorAll('.ddmenu').forEach(function(x){{if(x!==m)x.classList.add('hid');}});
}}

function ddChange(col){{
  var ct=document.querySelector('.ddw[data-col="'+col+'"]');if(!ct)return;
  var cbs=ct.querySelectorAll('input[data-val]'),chk=Array.from(cbs).filter(c=>c.checked);
  var btn=ct.querySelector('.ddbtn');
  if(chk.length===cbs.length||chk.length===0){{delete _CF[col];if(btn)btn.textContent='All \u25be';}}
  else{{_CF[col]=new Set(chk.map(c=>c.dataset.val));if(btn)btn.textContent=chk.length+' selected \u25be';}}
  applyF();
}}

function ddAll(col,val){{
  var ct=document.querySelector('.ddw[data-col="'+col+'"]');if(!ct)return;
  ct.querySelectorAll('input[data-val]').forEach(function(cb){{cb.checked=val;}});
  ddChange(col);
}}

function ddSrch(inp,col){{
  var q=inp.value.toLowerCase(),ct=document.querySelector('.ddw[data-col="'+col+'"]');if(!ct)return;
  ct.querySelectorAll('.optlbl').forEach(function(el){{el.style.display=el.textContent.toLowerCase().includes(q)?'':'none';}});
}}

function numCh(col,bound,val){{
  if(!_NF[col])_NF[col]={{min:null,max:null}};
  _NF[col][bound]=val===''?null:+val;
  applyF();
}}

function clearAll(){{
  _CF={{}};_NF={{}};
  document.querySelectorAll('.pill').forEach(p=>p.classList.add('active'));
  document.querySelectorAll('.ddw input[data-val]').forEach(cb=>{{cb.checked=true;}});
  document.querySelectorAll('.ddbtn').forEach(btn=>{{btn.textContent='All \u25be';}});
  document.querySelectorAll('.ninp').forEach(inp=>{{inp.value='';}});
  applyF();
}}

function exportCSV(){{
  var d=getFilt();if(!d.length)return;
  var cols=Object.keys(d[0]);
  var lines=[cols.map(c=>'"'+c.replace(/"/g,'""')+'"').join(',')];
  d.forEach(function(row){{
    lines.push(cols.map(function(c){{
      var v=row[c];if(v===null||v===undefined)return'';
      var s=String(v);return(s.includes(',')||s.includes('"')||s.includes('\\n'))?'"'+s.replace(/"/g,'""')+'"':s;
    }}).join(','));
  }});
  var b=new Blob([lines.join('\\n')],{{type:'text/csv'}});
  var u=URL.createObjectURL(b);var a=document.createElement('a');a.href=u;a.download='export.csv';a.click();URL.revokeObjectURL(u);
}}

function expand(id,ttl){{
  var src=document.getElementById(id);if(!src||!src.data)return;
  document.getElementById('mttl').textContent=ttl;
  document.getElementById('modal').classList.add('open');
  Plotly.newPlot('mdiv',src.data,Object.assign({{}},src.layout,{{height:null,autosize:true}}),{{responsive:true}});
}}
function closeM(){{document.getElementById('modal').classList.remove('open');Plotly.purge('mdiv');}}
document.getElementById('modal').addEventListener('click',function(e){{if(e.target===this)closeM();}});
document.addEventListener('keydown',function(e){{if(e.key==='Escape')closeM();}});
document.addEventListener('click',function(e){{if(!e.target.closest('.ddw'))document.querySelectorAll('.ddmenu').forEach(m=>m.classList.add('hid'));}});

function updKPIs(d){{
{kpi_upd}
}}

{rfns_str}

function renderAll(d){{
  updKPIs(d);
{render_calls}
}}

applyF();
</script>"""
        )

        if theme == "device":
            h.append(device_mode_js())

        h.append("</body></html>")

        html_content = "\n".join(h)

        if output_path:
            out = Path(output_path)
        else:
            out = path.parent / f"{path.stem}_dashboard.html"

        out.write_text(html_content, encoding="utf-8")
        size_kb = round(out.stat().st_size / 1024)

        if open_after:
            _open_file(out)

        if was_sampled:
            progress.append(
                warn("Large dataset sampled", f"{EMBED_LIMIT:,} of {len(df):,} rows embedded")
            )
        progress.append(ok("Dashboard saved", f"{out.name} ({size_kb:,} KB)"))

        result = {
            "success": True,
            "op": "generate_dashboard",
            "output_path": str(out.resolve()),
            "output_name": out.name,
            "dashboard_title": dashboard_title,
            "charts_included": charts,
            "kpi_columns": numeric_cols[:7],
            "filter_columns": [fc["col"] for fc in filter_controls],
            "rows_embedded": len(embed_df),
            "rows_total": len(df),
            "was_sampled": was_sampled,
            "report_size_kb": size_kb,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_dashboard error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
