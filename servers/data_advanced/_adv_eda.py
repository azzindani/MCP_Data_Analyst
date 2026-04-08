"""run_eda sub-module. No MCP imports."""

from __future__ import annotations

import logging
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
    VIEWPORT_META,
    fail,
    ok,
)
from shared.file_utils import resolve_path

logger = logging.getLogger(__name__)


def run_eda(
    file_path: str,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "dark",
) -> dict:
    """Fast EDA summary. Stats, nulls, correlations, outliers. Opens HTML."""
    progress = []
    try:
        try:
            import plotly.graph_objects as go  # noqa: F401
            import plotly.express as px  # noqa: F401
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
            c
            for c in df.columns
            if not pd.api.types.is_numeric_dtype(df[c])
            and not pd.api.types.is_datetime64_any_dtype(df[c])
        ]
        datetime_cols = [
            c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])
        ]

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

        null_penalty = sum(s["null_pct"] for s in column_summaries) / max(cols, 1)
        dup_count = int(df.duplicated().sum())
        dup_penalty = dup_count / rows * 100 if rows > 0 else 0
        outlier_penalty = sum(o["outlier_pct"] for o in outlier_cols) / max(cols, 1)
        quality_score = max(
            0, round(100 - null_penalty - dup_penalty * 0.5 - outlier_penalty * 0.3)
        )

        alerts = _compute_alerts(
            df, numeric_cols, cat_cols, corr_pairs, rows, dup_count
        )

        spearman_matrix = None
        if len(numeric_cols) >= 2:
            spearman_matrix = df[numeric_cols].corr(method="spearman")

        for s in column_summaries:
            if s["column"] in numeric_cols:
                s["zero_count"] = int((df[s["column"]] == 0).sum())
                s["zero_pct"] = (
                    round(s["zero_count"] / rows * 100, 2) if rows > 0 else 0
                )

        html_content = _build_eda_html(
            df,
            path,
            rows,
            cols,
            numeric_cols,
            cat_cols,
            datetime_cols,
            column_summaries,
            corr_pairs,
            outlier_cols,
            quality_score,
            alerts,
            spearman_matrix,
            dup_count,
            theme,
        )

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


def _compute_alerts(df, numeric_cols, cat_cols, corr_pairs, rows, dup_count):
    alerts = []
    for c in df.columns:
        if df[c].nunique(dropna=True) <= 1:
            alerts.append(
                {
                    "col": c,
                    "type": "CONSTANT",
                    "sev": "error",
                    "msg": f"'{c}' has only 1 unique value — constant, no predictive value.",
                }
            )
    for c in df.columns:
        np_ = round(df[c].isna().mean() * 100, 1)
        if np_ > 50:
            alerts.append(
                {
                    "col": c,
                    "type": "HIGH NULLS",
                    "sev": "error",
                    "msg": f"'{c}': {np_}% missing values — consider dropping.",
                }
            )
        elif np_ > 20:
            alerts.append(
                {
                    "col": c,
                    "type": "HIGH NULLS",
                    "sev": "warning",
                    "msg": f"'{c}': {np_}% missing — imputation needed.",
                }
            )
    for c in numeric_cols:
        zp = round((df[c] == 0).mean() * 100, 1)
        if zp > 50:
            alerts.append(
                {
                    "col": c,
                    "type": "ZEROS",
                    "sev": "warning",
                    "msg": f"'{c}': {zp}% zero values — zero-inflated distribution.",
                }
            )
    for c in cat_cols:
        uniq = df[c].nunique()
        if uniq > max(50, rows * 0.5):
            alerts.append(
                {
                    "col": c,
                    "type": "HIGH CARDINALITY",
                    "sev": "warning",
                    "msg": f"'{c}': {uniq:,} unique values — likely an ID, consider dropping.",
                }
            )
    for c in cat_cols:
        if df[c].notna().sum() > 0:
            top_pct = round(df[c].value_counts(normalize=True).iloc[0] * 100, 1)
            if top_pct > 90:
                alerts.append(
                    {
                        "col": c,
                        "type": "IMBALANCED",
                        "sev": "warning",
                        "msg": f"'{c}': top category = {top_pct}% of values — highly imbalanced.",
                    }
                )
    for c in numeric_cols:
        try:
            skv = round(float(df[c].skew()), 2)
            if abs(skv) > 2:
                alerts.append(
                    {
                        "col": c,
                        "type": "SKEWED",
                        "sev": "warning",
                        "msg": f"'{c}': skewness={skv:+.2f} — consider log/sqrt transform.",
                    }
                )
        except Exception:
            pass
    for c in numeric_cols:
        q1_, q3_ = df[c].quantile(0.25), df[c].quantile(0.75)
        iqr_ = q3_ - q1_
        oc_ = int(((df[c] < q1_ - 1.5 * iqr_) | (df[c] > q3_ + 1.5 * iqr_)).sum())
        op_ = round(oc_ / max(rows, 1) * 100, 1)
        if op_ > 10:
            alerts.append(
                {
                    "col": c,
                    "type": "OUTLIERS",
                    "sev": "warning",
                    "msg": f"'{c}': {oc_:,} outliers ({op_}%) — investigate or cap.",
                }
            )
    for p in corr_pairs[:20]:
        if abs(p["correlation"]) > 0.9:
            alerts.append(
                {
                    "col": p["col_a"],
                    "type": "HIGH CORR",
                    "sev": "warning",
                    "msg": f"'{p['col_a']}' \u2194 '{p['col_b']}': r={p['correlation']:+.3f} — possible multicollinearity.",
                }
            )
    if dup_count > 0:
        dp_ = round(dup_count / max(rows, 1) * 100, 1)
        alerts.append(
            {
                "col": None,
                "type": "DUPLICATES",
                "sev": "warning" if dp_ < 5 else "error",
                "msg": f"{dup_count:,} duplicate rows ({dp_}%) — consider deduplication.",
            }
        )
    return alerts


def _alerts_html(al):
    if not al:
        return '<div class="alert-panel"><div class="alert-item info"><span class="alert-badge info">OK</span> No data quality alerts detected.</div></div>'
    items = []
    for a in al:
        badge_cls = (
            "error"
            if a["sev"] == "error"
            else "warning"
            if a["sev"] == "warning"
            else "info"
        )
        items.append(
            f'<div class="alert-item {badge_cls}"><span class="alert-badge {badge_cls}">{a["type"]}</span> {a["msg"]}</div>'
        )
    return f'<div class="alert-panel">{"".join(items)}</div>'


def _build_eda_html(
    df,
    path,
    rows,
    cols,
    numeric_cols,
    cat_cols,
    datetime_cols,
    column_summaries,
    corr_pairs,
    outlier_cols,
    quality_score,
    alerts,
    spearman_matrix,
    dup_count,
    theme,
):
    vars_css = css_vars(theme)
    score_cls = (
        "good" if quality_score >= 80 else "warn" if quality_score >= 60 else "bad"
    )
    missing_by_col = {
        s["column"]: s["null_count"] for s in column_summaries if s["null_count"] > 0
    }

    if theme == "dark":
        _plot_bg = "#161b22"
        _font_color = "#c9d1d9"
        accent_color = "#58a6ff"
    elif theme == "light":
        _plot_bg = "#f6f8fa"
        _font_color = "#1f2328"
        accent_color = "#0969da"
    else:
        _plot_bg = "#f6f8fa"
        _font_color = "#1f2328"
        accent_color = "#58a6ff"

    corr_json = ""
    if len(numeric_cols) >= 2:
        corr = df[numeric_cols].corr(method="pearson")
        corr_z = corr.values.tolist()
        corr_x = list(corr.columns)
        corr_json = f"""
<div class="chart-container" id="corr-chart" style="min-height:480px"></div>
<script>
(function(){{
  var z={corr_z};var x={corr_x};
  var dark=(typeof window!=='undefined')&&window.matchMedia&&window.matchMedia('(prefers-color-scheme:dark)').matches;
  var bg=dark?'#161b22':'{_plot_bg}';var fc=dark?'#c9d1d9':'{_font_color}';
  var data=[{{z:z,x:x,y:x,type:'heatmap',colorscale:'RdBu',zmid:0,
    text:z.map(function(r){{return r.map(function(v){{return v.toFixed(2);}});}}),
    texttemplate:'%{{text}}',textfont:{{size:11}}}}];
  var layout={{paper_bgcolor:bg,plot_bgcolor:bg,font:{{color:fc}},
    margin:{{l:120,r:20,t:20,b:120}},height:480,autosize:false}};
  Plotly.newPlot('corr-chart',data,layout,{{responsive:true,displayModeBar:true,scrollZoom:true}});
}})();
</script>"""

    spearman_json = ""
    if spearman_matrix is not None:
        sp_z = spearman_matrix.values.tolist()
        sp_x = spearman_matrix.columns.tolist()
        spearman_json = f"""
<h3 style="color:var(--accent);font-size:15px;margin:20px 0 8px">Spearman Rank Correlation</h3>
<div class="chart-container" id="sp-corr-chart" style="min-height:480px"></div>
<script>
(function(){{
  var z={sp_z};var x={sp_x};
  var dark=(typeof window!=='undefined')&&window.matchMedia&&window.matchMedia('(prefers-color-scheme:dark)').matches;
  var bg=dark?'#161b22':'{_plot_bg}';var fc=dark?'#c9d1d9':'{_font_color}';
  var data=[{{z:z,x:x,y:x,type:'heatmap',colorscale:'RdBu',zmid:0,
    text:z.map(function(r){{return r.map(function(v){{return v.toFixed(2);}});}}),
    texttemplate:'%{{text}}',textfont:{{size:11}}}}];
  var layout={{paper_bgcolor:bg,plot_bgcolor:bg,font:{{color:fc}},
    margin:{{l:120,r:20,t:20,b:120}},height:480,autosize:false}};
  Plotly.newPlot('sp-corr-chart',data,layout,{{responsive:true,displayModeBar:true,scrollZoom:true}});
}})();
</script>"""

    col_rows = []
    for s in column_summaries:
        nc_ = s["null_count"]
        np_ = s["null_pct"]
        cls = "good" if nc_ == 0 else "warn" if np_ < 10 else "bad"
        if "mean" in s:
            stats_str = f"μ={s['mean']}, σ={s['std']}, [{s['min']}–{s['max']}]"
        elif "top_values" in s:
            stats_str = "Top: " + ", ".join(
                f"{k}:{v}" for k, v in list(s["top_values"].items())[:3]
            )
        else:
            stats_str = "—"
        zero_cell = (
            f"{s.get('zero_count', '')} ({s.get('zero_pct', '')}%)"
            if "zero_count" in s
            else "—"
        )
        col_rows.append(
            f'<tr><td><b>{s["column"]}</b></td><td><span class="badge">{s["dtype"]}</span></td>'
            f'<td class="{cls}">{nc_}</td><td class="{cls}">{np_}%</td>'
            f"<td>{zero_cell}</td>"
            f'<td>{s["unique_count"]}</td><td class="stats-cell">{stats_str}</td></tr>'
        )
    col_rows_html = "\n".join(col_rows)

    insights = []
    for s in column_summaries:
        if s["null_pct"] > 50:
            insights.append(
                f'<li class="bad"><b>{s["column"]}</b>: {s["null_pct"]}% null — consider dropping</li>'
            )
        elif s["null_pct"] > 10:
            insights.append(
                f'<li class="warn"><b>{s["column"]}</b>: {s["null_pct"]}% null — consider imputation</li>'
            )
    for p in corr_pairs[:5]:
        if abs(p["correlation"]) > 0.8:
            insights.append(
                f"<li><b>{p['col_a']}</b> ↔ <b>{p['col_b']}</b>: r={p['correlation']:+.3f} (very strong)</li>"
            )
    for s in column_summaries:
        if "std" in s and s.get("std", 0) > 0:
            skew_val = None
            try:
                skew_val = round(float(df[s["column"]].skew()), 2)
            except Exception:
                pass
            if skew_val is not None and abs(skew_val) > 2:
                insights.append(
                    f'<li class="warn"><b>{s["column"]}</b>: skewness={skew_val} — consider log transform</li>'
                )
    if dup_count > 0:
        insights.append(
            f'<li class="warn">{dup_count:,} duplicate rows detected — use drop_duplicates</li>'
        )
    if not insights:
        insights.append('<li class="good">No major data quality issues detected.</li>')
    insights_html = "\n".join(insights)

    outlier_rows = (
        "".join(
            f'<tr><td><b>{o["column"]}</b></td><td class="warn">{o["outlier_count"]}</td>'
            f"<td>{o['outlier_pct']}%</td><td>[{o['lower_limit']} – {o['upper_limit']}]</td></tr>"
            for o in outlier_cols
        )
        if outlier_cols
        else '<tr><td colspan="4" class="good">No outliers detected</td></tr>'
    )

    corr_rows = ""
    for p in corr_pairs[:10]:
        s_str = (
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
        corr_rows += f'<tr class="{cls}"><td>{p["col_a"]}</td><td>{p["col_b"]}</td><td>{p["correlation"]:+.4f}</td><td>{s_str}</td></tr>'

    plotly_script = '<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>'

    missing_section = _build_missing_section(
        df, missing_by_col, rows, accent_color, _plot_bg, _font_color
    )
    corr_section = ""
    if corr_pairs:
        corr_section = f'<div id="correlations" class="section"><h2>Correlations</h2>{corr_json}{spearman_json}<table><tr><th>Variable A</th><th>Variable B</th><th>r</th><th>Strength</th></tr>{corr_rows}</table></div>'

    nulls_nav = '<a href="#nulls">Missing Data</a>' if missing_by_col else ""
    corr_nav = '<a href="#correlations">Correlations</a>' if corr_pairs else ""

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

    alerts_section = f'<div id="alerts" class="section"><h2>&#9888; Alerts ({len(alerts)})</h2>{_alerts_html(alerts)}</div>'

    css_block = _eda_css(vars_css)
    dev_js = device_mode_js() if theme == "device" else ""

    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
{VIEWPORT_META}
<title>EDA Report — {path.name}</title>
{plotly_script}
<style>
{css_block}
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
      <div class="card {"warn" if dup_count > 0 else "good"}"><div class="num">{dup_count:,}</div><div class="lbl">Duplicates</div></div>
    </div>
  </div>
  {sample_html}
  <div id="columns" class="section">
    <h2>Column Summary</h2>
    <div style="overflow-x:auto">
      <table>
        <tr><th>Column</th><th>Type</th><th>Nulls</th><th>Null %</th><th>Zeros</th><th>Unique</th><th>Stats</th></tr>
        {col_rows_html}
      </table>
    </div>
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
{dev_js}
</body></html>"""


def _build_missing_section(
    df, missing_by_col, rows, accent_color, _plot_bg, _font_color
):
    if not missing_by_col:
        return ""
    missing_rows = "".join(
        f"<tr><td><b>{c}</b></td><td>{cnt}</td><td>{round(cnt / rows * 100, 1)}%</td>"
        f'<td><div class="mbar"><div class="mbar-fill" style="width:{round(cnt / rows * 100, 1)}%"></div></div></td></tr>'
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
  Plotly.newPlot('miss-matrix',data,layout,{{responsive:true,displayModeBar:true,scrollZoom:true}});
}})();
</script>"""
    return f'<div id="nulls" class="section"><h2>Missing Data</h2><table><tr><th>Column</th><th>Missing</th><th>%</th><th>Visual</th></tr>{missing_rows}</table>{miss_matrix_html}</div>'


def _eda_css(vars_css):
    return f"""{vars_css}
*{{box-sizing:border-box;margin:0;padding:0}}
html{{scroll-behavior:smooth}}
body{{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;transition:background 0.2s,color 0.2s}}
::-webkit-scrollbar{{width:6px}}::-webkit-scrollbar-track{{background:var(--bg)}}::-webkit-scrollbar-thumb{{background:var(--border);border-radius:3px}}
.sidebar{{width:260px;background:var(--surface);border-right:1px solid var(--border);position:fixed;top:0;left:0;bottom:0;overflow-y:auto;z-index:100}}
.sidebar-hdr{{padding:20px;border-bottom:1px solid var(--border)}}
.sidebar-hdr h2{{color:var(--accent);font-size:16px;margin-bottom:4px}}
.sidebar-hdr .meta{{color:var(--text-muted);font-size:12px}}
.nav{{padding:8px 0}}
.nav a{{display:block;padding:7px 20px;color:var(--text-muted);text-decoration:none;font-size:13px;border-left:3px solid transparent;transition:all 0.15s}}
.nav a:hover,.nav a.active{{color:var(--accent);background:rgba(88,166,255,0.06);border-left-color:var(--accent)}}
.nav .st{{padding:14px 20px 4px;color:var(--border);font-size:10px;text-transform:uppercase;letter-spacing:1px;font-weight:600}}
.main{{margin-left:260px;padding:32px;min-height:100vh;overflow-x:auto}}
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
.chart-container{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px;margin:16px 0;min-height:420px;overflow:hidden;max-width:100%}}
.alert-panel{{border-radius:10px;overflow:hidden;margin-bottom:20px}}
.alert-item{{padding:10px 14px;margin:3px 0;font-size:13px;border-radius:8px;display:flex;align-items:flex-start;gap:10px;background:var(--surface);border:1px solid var(--border)}}
.alert-item.error{{border-left:4px solid var(--red)}}.alert-item.warning{{border-left:4px solid var(--orange)}}.alert-item.info{{border-left:4px solid var(--green)}}
.alert-badge{{font-size:10px;font-weight:700;padding:2px 8px;border-radius:10px;white-space:nowrap;flex-shrink:0;margin-top:1px}}
.alert-badge.error{{background:var(--red);color:#fff}}.alert-badge.warning{{background:var(--orange);color:#fff}}.alert-badge.info{{background:var(--green);color:#fff}}
@media(max-width:1100px){{.sidebar{{width:220px}}.main{{margin-left:220px}}}}
@media(max-width:768px){{.sidebar{{display:none}}.main{{margin-left:0;padding:16px}}.cards{{grid-template-columns:repeat(2,1fr)}}}}
@media(max-width:480px){{.cards{{grid-template-columns:1fr}}th,td{{padding:8px 10px;font-size:12px}}}}"""
