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

import html as _html
import json

import pandas as pd
from _adv_helpers import (
    _BACK_TO_TOP_HTML,
    _BACK_TO_TOP_JS,
    _COLLAPSIBLE_SECTIONS_JS,
    _COPY_CLIPBOARD_JS,
    _KPI_COUNTER_JS,
    _SCROLL_SPY_JS,
    _SIDEBAR_JS,
    _SORTABLE_TABLES_JS,
    PLOTLY_CFG_JS,
    VIEWPORT_META,
    _dtype_label,
    _open_file,
    _read_csv,
    _token_estimate,
    css_report,
    css_vars,
    device_mode_js,
    extension_note,
    fail,
    get_output_path,
    is_numeric_col,
    ok,
    plotly_script_tag,
    theme_plot_colors,
    warn,
)

from shared.data_alerts import alerts_html, compute_alerts
from shared.data_alerts import quality_score as compute_quality_score
from shared.depth import MODES, SECTIONS, Depth, UnknownMode, sampled_frame
from shared.file_utils import embed_content, error_text, hint_for_error, resolve_path
from shared.provenance import frame_hash, provenance, provenance_script

logger = logging.getLogger(__name__)


def run_eda(
    file_path: str,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "device",
    return_content: bool = False,
    mode: str = "standard",
    sample_n: int = 0,
    include: dict | None = None,
) -> dict:
    """Fast EDA summary. Stats, nulls, correlations, outliers. Opens HTML.

    `mode` is "minimal", "standard" or "full". **"standard" is exactly what this
    tool did before the parameter existed**, so a caller who passes nothing gets
    yesterday's answer. "minimal" computes column summaries, nulls and the
    quality score and skips the correlation matrices, the outlier scan and the
    HTML page -- the parts that made nine reports from one dataset weigh 4.7 MB
    each. "full" adds every correlation pair rather than the top ten.

    `sample_n` profiles a random sample of that many rows. The response then
    carries `was_sampled`, `sample_n` and `rows_total`, because statistics from
    5,000 of 38,576 rows are estimates and a report that does not say so invites
    them to be quoted as counts.

    `include` overrides the mode per section in either direction, e.g.
    `mode="minimal", include={"correlations": True}`. An unknown section name is
    refused rather than ignored.
    """
    progress = []
    try:
        try:
            depth = Depth(mode, include)
        except UnknownMode as exc:
            return {
                "success": False,
                "error": str(exc),
                "hint": f"mode is one of {', '.join(MODES)}; include keys are: {', '.join(SECTIONS)}.",
                "progress": [fail("Invalid depth", str(exc))],
                "token_estimate": 30,
            }
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

        full_df = _read_csv(str(path))
        # Sampling happens once, here, so every section below profiles the same
        # rows. `sample_note` goes in the response rather than only in a log.
        df, sample_fields = sampled_frame(full_df, sample_n)
        rows, cols = df.shape

        numeric_cols = [c for c in df.columns if is_numeric_col(df[c])]
        cat_cols = [
            c for c in df.columns if not is_numeric_col(df[c]) and not pd.api.types.is_datetime64_any_dtype(df[c])
        ]
        datetime_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]

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
        if depth.wants("correlations") and len(numeric_cols) >= 2:
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
            # "full" is the mode a caller picks when they want the matrix, not a
            # summary of it. Ten was never a considered number; it was the only
            # number.
            corr_pairs = pairs if depth.mode == "full" else pairs[:10]

        outlier_cols = []
        for c in numeric_cols if depth.wants("outliers") else []:
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

        outlier_cols.sort(key=lambda o: o["outlier_count"], reverse=True)

        null_pct = sum(s["null_pct"] for s in column_summaries) / max(cols, 1)
        dup_count = int(df.duplicated().sum())
        dup_pct = dup_count / rows * 100 if rows > 0 else 0

        # Computed before the score, so the headline can see what the panel
        # under it says. Scored from nulls and duplicates alone this report
        # printed "QUALITY SCORE 98" directly beneath "Alerts (16)" -- constant
        # columns, 89.7% zero-inflation, skewness of +17 and outliers at 13% all
        # cost nothing. The shared scorer prices the same alerts the dashboard
        # does, so the two tools can no longer give one frame two verdicts.
        alerts = (
            _compute_alerts(df, numeric_cols, cat_cols, corr_pairs, rows, dup_count) if depth.wants("alerts") else []
        )
        quality_score = compute_quality_score(null_pct, dup_pct, alerts)

        spearman_matrix = None
        if depth.wants("spearman") and len(numeric_cols) >= 2:
            spearman_matrix = df[numeric_cols].corr(method="spearman")

        for s in column_summaries:
            if s["column"] in numeric_cols:
                s["zero_count"] = int((df[s["column"]] == 0).sum())
                s["zero_pct"] = round(s["zero_count"] / rows * 100, 2) if rows > 0 else 0

        # What the page and the response both say they are a picture of.
        # `was_sampled` is derived from the two counts rather than passed, for
        # the same reason `truncated` is: a flag set independently of the
        # numbers beside it eventually disagrees with them.
        page_header = provenance(
            rows_plotted=rows,
            rows_total=len(full_df),
            source=path.name,
            data_hash=frame_hash(df),
            tool="run_eda",
        )

        out = None
        size_kb = 0
        if depth.wants("html"):
            # Resolved first: the output path decides where the page is written,
            # and the <head> is assembled around it.
            out = get_output_path(output_path, path, "eda", "html")
            if note := extension_note(output_path, out):
                progress.append(warn("Output extension changed", note))

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
                out.parent,
                page_header,
            )

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
        else:
            # The 4.7 MB is the page, not the numbers. A caller who wanted the
            # figures should not have to write one to disk to read them.
            progress.append(
                ok(
                    "EDA computed",
                    f"{quality_score}/100 quality score — no HTML written at mode={depth.mode!r}",
                )
            )

        result = {
            "success": True,
            "op": "run_eda",
            "file_path": str(path),
            "output_path": str(out.resolve()) if out else "",
            "output_name": out.name if out else "",
            "report_size_kb": size_kb,
            "rows": rows,
            "columns": cols,
            "quality_score": quality_score,
            "numeric_columns": len(numeric_cols),
            "categorical_columns": len(cat_cols),
            "datetime_columns": len(datetime_cols),
            "duplicate_rows": dup_count,
            "null_summary": {s["column"]: s["null_count"] for s in column_summaries if s["null_count"] > 0},
            "top_correlations": corr_pairs[:5],
            "outlier_columns": outlier_cols,
            "column_summaries": column_summaries,
            # Which parts were computed, so an empty section can be read as
            # "not asked for" rather than "nothing found". Those are different
            # answers and they call for different next steps.
            **depth.report(),
            **sample_fields,
            # Repeated in the response so a caller does not have to open the
            # page to learn what it is a picture of.
            "provenance": page_header,
            "progress": progress,
        }
        if depth.mode == "full":
            result["all_correlations"] = corr_pairs
        if out is not None:
            embed_content(result, out, return_content)
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("run_eda error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path is absolute and the file is a valid CSV."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# The alert engine moved to shared/data_alerts.py so the interactive dashboard
# can show the same judgement; these names stay as the local spelling.
_compute_alerts = compute_alerts
_alerts_html = alerts_html


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
    output_dir,
    header=None,
):
    # The page carries what it is a picture of. A chart drawn from a sample
    # looks exactly like one drawn from everything, and nothing in the file
    # used to say which this was.
    provenance_block = provenance_script(header or {})
    vars_css = css_vars(theme)
    score_cls = "good" if quality_score >= 80 else "warn" if quality_score >= 60 else "bad"
    missing_by_col = {s["column"]: s["null_count"] for s in column_summaries if s["null_count"] > 0}

    _plot_bg, _font_color, accent_color = theme_plot_colors(theme)

    # For device theme, colours are resolved at runtime via matchMedia.
    # For dark/light themes, embed static values so the theme is always honoured.
    if theme == "device":
        _bg_init = "var dark=window.matchMedia&&window.matchMedia('(prefers-color-scheme:dark)').matches;var bg=dark?'#161b22':'#f6f8fa';var fc=dark?'#c9d1d9':'#1f2328';"
        _bg_ref, _fc_ref = "bg", "fc"
    else:
        _bg_init = ""
        _bg_ref, _fc_ref = f"'{_plot_bg}'", f"'{_font_color}'"

    corr_json = ""
    if len(numeric_cols) >= 2:
        corr = df[numeric_cols].corr(method="pearson")
        corr_z = corr.values.tolist()
        corr_x = list(corr.columns)
        corr_json = f"""
<div class="chart-box"><div id="corr-chart" class="chart-div heatmap"></div></div>
<script>
(function(){{
  var z={corr_z};var x={json.dumps(corr_x)};
  {_bg_init}
  var data=[{{z:z,x:x,y:x,type:'heatmap',colorscale:'RdBu',zmid:0,
    text:z.map(function(r){{return r.map(function(v){{return v.toFixed(2);}});}}),
    texttemplate:'%{{text}}',textfont:{{size:11}}}}];
  var layout={{paper_bgcolor:{_bg_ref},plot_bgcolor:{_bg_ref},font:{{color:{_fc_ref}}},
    margin:{{l:120,r:20,t:20,b:120}},autosize:true}};
  Plotly.newPlot('corr-chart',data,layout,{PLOTLY_CFG_JS});
}})();
</script>"""

    spearman_json = ""
    if spearman_matrix is not None:
        sp_z = spearman_matrix.values.tolist()
        sp_x = spearman_matrix.columns.tolist()
        spearman_json = f"""
<h3 style="color:var(--accent);margin:1.25rem 0 .5rem">Spearman Rank Correlation</h3>
<div class="chart-box"><div id="sp-corr-chart" class="chart-div heatmap"></div></div>
<script>
(function(){{
  var z={sp_z};var x={json.dumps(sp_x)};
  {_bg_init}
  var data=[{{z:z,x:x,y:x,type:'heatmap',colorscale:'RdBu',zmid:0,
    text:z.map(function(r){{return r.map(function(v){{return v.toFixed(2);}});}}),
    texttemplate:'%{{text}}',textfont:{{size:11}}}}];
  var layout={{paper_bgcolor:{_bg_ref},plot_bgcolor:{_bg_ref},font:{{color:{_fc_ref}}},
    margin:{{l:120,r:20,t:20,b:120}},autosize:true}};
  Plotly.newPlot('sp-corr-chart',data,layout,{PLOTLY_CFG_JS});
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
            stats_str = "Top: " + ", ".join(f"{k}:{v}" for k, v in list(s["top_values"].items())[:3])
        else:
            stats_str = "—"
        zero_cell = f"{s.get('zero_count', '')} ({s.get('zero_pct', '')}%)" if "zero_count" in s else "—"
        col_rows.append(
            f'<tr><td><b>{_html.escape(s["column"])}</b></td><td><span class="badge">{s["dtype"]}</span></td>'
            f'<td class="{cls}">{nc_}</td><td class="{cls}">{np_}%</td>'
            f"<td>{zero_cell}</td>"
            f'<td>{s["unique_count"]}</td><td class="stats-cell">{stats_str}</td></tr>'
        )
    col_rows_html = "\n".join(col_rows)

    insights = []
    for s in column_summaries:
        if s["null_pct"] > 50:
            insights.append(
                f'<li class="bad"><b>{_html.escape(s["column"])}</b>: {s["null_pct"]}% null — consider dropping</li>'
            )
        elif s["null_pct"] > 10:
            insights.append(
                f'<li class="warn"><b>{_html.escape(s["column"])}</b>: {s["null_pct"]}% null — consider imputation</li>'
            )
    for p in corr_pairs[:5]:
        if abs(p["correlation"]) > 0.8:
            insights.append(
                f"<li><b>{_html.escape(p['col_a'])}</b> ↔ <b>{_html.escape(p['col_b'])}</b>: r={p['correlation']:+.3f} (very strong)</li>"
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
        insights.append(f'<li class="warn">{dup_count:,} duplicate rows detected — use drop_duplicates</li>')
    if not insights:
        insights.append('<li class="good">No major data quality issues detected.</li>')
    insights_html = "\n".join(insights)

    outlier_rows = (
        "".join(
            f'<tr><td><b>{_html.escape(o["column"])}</b></td><td class="warn">{o["outlier_count"]}</td>'
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
        cls = "good" if abs(p["correlation"]) > 0.7 else "warn" if abs(p["correlation"]) > 0.5 else ""
        corr_rows += f'<tr class="{cls}"><td>{_html.escape(p["col_a"])}</td><td>{_html.escape(p["col_b"])}</td><td>{p["correlation"]:+.4f}</td><td>{s_str}</td></tr>'

    plotly_script = plotly_script_tag(output_dir)

    missing_section = _build_missing_section(df, missing_by_col, rows, accent_color, _plot_bg, _font_color)
    corr_section = ""
    if corr_pairs:
        corr_section = f'<div id="correlations" class="section"><h2>Correlations</h2>{corr_json}{spearman_json}<table><tr><th>Variable A</th><th>Variable B</th><th>r</th><th>Strength</th></tr>{corr_rows}</table></div>'

    nulls_nav = '<a href="#nulls">Missing Data</a>' if missing_by_col else ""
    corr_nav = '<a href="#correlations">Correlations</a>' if corr_pairs else ""

    sample_rows_df = df.head(5)
    sample_cols_list = list(sample_rows_df.columns)
    sample_header = "".join(f"<th>{_html.escape(str(c))}</th>" for c in sample_cols_list)
    sample_body = ""
    for _, row in sample_rows_df.iterrows():
        cells = "".join(f'<td title="{_html.escape(str(v))}">{_html.escape(str(v)[:50])}</td>' for v in row.values)
        sample_body += f"<tr>{cells}</tr>"
    sample_html = f"""<div id="sample" class="section">
  <h2>Data Sample (first 5 rows)</h2>
  <div style="overflow-x:auto">
    <table><tr>{sample_header}</tr>{sample_body}</table>
  </div>
</div>"""

    alerts_section = (
        f'<div id="alerts" class="section"><h2>&#9888; Alerts ({len(alerts)})</h2>{_alerts_html(alerts)}</div>'
    )

    css_block = _eda_css(vars_css)
    dev_js = device_mode_js() if theme == "device" else ""

    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8">
{VIEWPORT_META}
<title>EDA Report — {_html.escape(path.name)}</title>
{provenance_block}
{plotly_script}
<style>
{css_block}
</style></head><body>
<button id="sb-toggle" aria-label="Open navigation">&#9776;</button>
<div id="sb-overlay"></div>
{_BACK_TO_TOP_HTML}
<div class="sidebar">
  <div class="sidebar-hdr"><h2>EDA Report</h2><p class="meta" title="{_html.escape(str(path))}">{_html.escape(path.name)} <button class="btn-print" data-copy="{_html.escape(str(path))}">&#x29C7;</button></p><p class="meta">{rows:,} rows \u00d7 {cols} cols</p><button class="btn-print" onclick="window.print()">&#x2399; Print</button></div>
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
      <div class="card good"><div class="num" data-val="{rows}" data-fmt="int">{rows:,}</div><div class="lbl">Rows</div></div>
      <div class="card"><div class="num" data-val="{cols}" data-fmt="int">{cols}</div><div class="lbl">Columns</div></div>
      <div class="card"><div class="num" data-val="{len(numeric_cols)}" data-fmt="int">{len(numeric_cols)}</div><div class="lbl">Numeric</div></div>
      <div class="card"><div class="num" data-val="{len(cat_cols)}" data-fmt="int">{len(cat_cols)}</div><div class="lbl">Categorical</div></div>
      <div class="card"><div class="num" data-val="{len(datetime_cols)}" data-fmt="int">{len(datetime_cols)}</div><div class="lbl">Datetime</div></div>
      <div class="card {score_cls}"><div class="num" data-val="{quality_score}" data-fmt="int">{quality_score}</div><div class="lbl">Quality Score</div></div>
      <div class="card {"warn" if dup_count > 0 else "good"}"><div class="num" data-val="{dup_count}" data-fmt="int">{dup_count:,}</div><div class="lbl">Duplicates</div></div>
    </div>
  </div>
  {sample_html}
  <div id="columns" class="section">
    <h2>Column Summary</h2>
    <div class="tbl-wrap">
      <table>
        <tr><th data-sort>Column</th><th data-sort>Type</th><th data-sort>Nulls</th><th data-sort>Null %</th><th data-sort>Zeros</th><th data-sort>Unique</th><th>Stats</th></tr>
        <tbody>{col_rows_html}</tbody>
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
{_SIDEBAR_JS}
{_SCROLL_SPY_JS}
{_SORTABLE_TABLES_JS}
{_COLLAPSIBLE_SECTIONS_JS}
{_KPI_COUNTER_JS}
{_COPY_CLIPBOARD_JS}
{_BACK_TO_TOP_JS}
</body></html>"""


def _build_missing_section(df, missing_by_col, rows, accent_color, _plot_bg, _font_color):
    if not missing_by_col:
        return ""
    missing_rows = "".join(
        f'<tr><td title="{_html.escape(c)}"><b>{_html.escape(c)}</b></td><td>{cnt}</td><td>{round(cnt / rows * 100, 1)}%</td>'
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
    miss_matrix_html = f"""<div class="chart-box">
  <p class="chart-note">White = present, coloured = missing. Sampled {len(miss_sample)} rows.</p>
  <div id="miss-matrix" class="chart-div heatmap"></div>
</div>
<script>
(function(){{
  var z={miss_z};var x={json.dumps(miss_cols)};var y={json.dumps(miss_y)};
  var data=[{{z:z,x:x,y:y,type:'heatmap',colorscale:[['0','rgba(0,0,0,0)'],['1','{accent_color}']],
    showscale:false,hovertemplate:'Column: %{{x}}<br>Row: %{{y}}<br>Missing: %{{z}}<extra></extra>'}}];
  var layout={{paper_bgcolor:'{_plot_bg}',plot_bgcolor:'{_plot_bg}',
    font:{{color:'{_font_color}'}},
    margin:{{l:60,r:10,t:10,b:80}},autosize:true,
    xaxis:{{tickangle:-45,tickfont:{{size:11}}}},
    yaxis:{{title:'Row index',tickfont:{{size:10}}}}
  }};
  Plotly.newPlot('miss-matrix',data,layout,{PLOTLY_CFG_JS});
}})();
</script>"""
    return f'<div id="nulls" class="section"><h2>Missing Data</h2><table><tr><th>Column</th><th>Missing</th><th>%</th><th>Visual</th></tr>{missing_rows}</table>{miss_matrix_html}</div>'


def _eda_css(vars_css):
    return css_report(vars_css)
