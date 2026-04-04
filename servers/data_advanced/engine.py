"""Tier 3 engine — fast EDA, visualizations, dashboards. Zero MCP imports."""

from __future__ import annotations

import logging
import subprocess
import sys
import textwrap
import webbrowser
from pathlib import Path

import pandas as pd

# Shared utilities
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from shared.file_utils import resolve_path
from shared.platform_utils import get_max_rows
from shared.progress import fail, info, ok, warn
from shared.receipt import append_receipt

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

        # Build HTML report
        html_parts = [
            "<!DOCTYPE html><html><head><meta charset='utf-8'>",
            "<title>EDA Report</title>",
            "<style>",
            "body{font-family:system-ui,sans-serif;background:#1a1a2e;color:#eee;padding:20px}",
            "h1{color:#e94560}h2{color:#0f3460;border-bottom:1px solid #333;padding-bottom:5px}",
            "table{border-collapse:collapse;width:100%;margin:10px 0}",
            "th,td{padding:8px 12px;text-align:left;border:1px solid #333}",
            "th{background:#16213e}.good{color:#4ecca3}.warn{color:#f9a825}.bad{color:#e94560}",
            ".card{background:#16213e;padding:15px;border-radius:8px;margin:10px 0;display:inline-block;min-width:150px}",
            ".score{font-size:48px;font-weight:bold}",
            "</style></head><body>",
            f"<h1>EDA Report: {path.name}</h1>",
            f"<p>{rows:,} rows × {cols} columns</p>",
            f"<div class='card'><div class='score {'good' if quality_score >= 80 else 'warn' if quality_score >= 60 else 'bad'}'>{quality_score}</div>Data Quality Score</div>",
            f"<div class='card'><div>{len(numeric_cols)}</div>Numeric Columns</div>",
            f"<div class='card'><div>{len(cat_cols)}</div>Categorical Columns</div>",
            f"<div class='card'><div>{len(datetime_cols)}</div>Datetime Columns</div>",
            f"<div class='card'><div>{dup_count:,}</div>Duplicate Rows</div>",
            "<h2>Column Summary</h2>",
            "<table><tr><th>Column</th><th>Type</th><th>Nulls</th><th>Null %</th><th>Unique</th><th>Stats</th></tr>",
        ]

        for s in column_summaries:
            cls = (
                "good"
                if s["null_pct"] == 0
                else "warn"
                if s["null_pct"] < 10
                else "bad"
            )
            stats = ""
            if "mean" in s:
                stats = f"μ={s['mean']}, σ={s['std']}, [{s['min']}–{s['max']}]"
            elif "top_values" in s:
                top_str = ", ".join(
                    f"{k}: {v}" for k, v in list(s["top_values"].items())[:3]
                )
                stats = f"Top: {top_str}"
            html_parts.append(
                f"<tr><td>{s['column']}</td><td>{s['dtype']}</td>"
                f"<td class='{cls}'>{s['null_count']}</td><td>{s['null_pct']}%</td>"
                f"<td>{s['unique_count']}</td><td>{stats}</td></tr>"
            )

        html_parts.append("</table>")

        if corr_pairs:
            html_parts.append("<h2>Top Correlations</h2>")
            html_parts.append(
                "<table><tr><th>Column A</th><th>Column B</th><th>Correlation</th></tr>"
            )
            for p in corr_pairs:
                html_parts.append(
                    f"<tr><td>{p['col_a']}</td><td>{p['col_b']}</td>"
                    f"<td class={'good' if abs(p['correlation']) > 0.7 else 'warn'}>{p['correlation']}</td></tr>"
                )
            html_parts.append("</table>")

        if outlier_cols:
            html_parts.append("<h2>Outliers (IQR Method)</h2>")
            html_parts.append(
                "<table><tr><th>Column</th><th>Count</th><th>%</th><th>Range</th></tr>"
            )
            for o in outlier_cols:
                html_parts.append(
                    f"<tr><td>{o['column']}</td><td>{o['outlier_count']}</td>"
                    f"<td>{o['outlier_pct']}%</td><td>[{o['lower_limit']} – {o['upper_limit']}]</td></tr>"
                )
            html_parts.append("</table>")

        html_parts.append("</body></html>")

        html_content = "\n".join(html_parts)

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
                f"EDA report saved",
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
            width=1000,
            title_text=f"Distribution Analysis: {', '.join(cols_to_plot)}",
            template="plotly_dark",
            showlegend=False,
        )

        if output_path:
            out = Path(output_path)
        else:
            out = path.parent / f"{path.stem}_distributions.html"

        fig.write_html(str(out), include_plotlyjs=True, full_html=True)

        if open_after:
            _open_file(out)

        progress.append(
            ok(
                f"Distribution plots saved",
                f"{out.name} — {n} columns",
            )
        )

        result = {
            "success": True,
            "op": "generate_distribution_plot",
            "output_path": str(out.resolve()),
            "output_name": out.name,
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
        fig.update_layout(template="plotly_dark", height=300 + 50 * len(numeric_cols))

        if output_path:
            out = Path(output_path)
        else:
            out = path.parent / f"{path.stem}_correlation_heatmap.html"

        fig.write_html(str(out), include_plotlyjs=True, full_html=True)

        if open_after:
            _open_file(out)

        progress.append(
            ok(
                f"Correlation heatmap saved",
                f"{out.name} — {len(numeric_cols)} columns",
            )
        )

        result = {
            "success": True,
            "op": "generate_correlation_heatmap",
            "output_path": str(out.resolve()),
            "output_name": out.name,
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
        cat_cols = [c for c in df.columns if df[c].dtype == "object"]
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

        h = []
        h.append("""<!DOCTYPE html><html><head><meta charset='utf-8'><meta name="viewport" content="width=device-width,initial-scale=1"><title>Data Profile</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
:root{--bg:#0d1117;--surface:#161b22;--border:#21262d;--text:#c9d1d9;--text-muted:#8b949e;--accent:#58a6ff;--green:#3fb950;--orange:#f0883e;--red:#f85149;--sidebar-w:300px;--chart-h:420px;--heatmap-h:500px}
*{box-sizing:border-box;margin:0;padding:0}
html{scroll-behavior:smooth}
body{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;background:var(--bg);color:var(--text);display:flex;min-height:100vh}
::-webkit-scrollbar{width:8px}::-webkit-scrollbar-track{background:var(--bg)}::-webkit-scrollbar-thumb{background:#30363d;border-radius:4px}::-webkit-scrollbar-thumb:hover{background:#484f58}
.sidebar{width:var(--sidebar-w);background:var(--surface);border-right:1px solid var(--border);position:fixed;top:0;left:0;bottom:0;overflow-y:auto;z-index:100;display:flex;flex-direction:column}
.sidebar-header{padding:24px 20px 16px;border-bottom:1px solid var(--border)}
.sidebar-header h2{color:var(--accent);font-size:18px;margin-bottom:4px;font-weight:600}
.sidebar-header .file-name{color:var(--text-muted);font-size:13px;margin-bottom:2px;word-break:break-all}
.sidebar-header .meta{color:var(--text-muted);font-size:12px}
.sidebar-nav{padding:12px 0;flex:1}
.sidebar-nav a{display:block;padding:8px 20px;color:var(--text-muted);text-decoration:none;font-size:13px;border-left:3px solid transparent;transition:all 0.15s}
.sidebar-nav a:hover{color:var(--accent);background:rgba(88,166,255,0.06);border-left-color:var(--accent)}
.sidebar-nav .st{padding:16px 20px 6px;color:#484f58;font-size:10px;text-transform:uppercase;letter-spacing:1.2px;font-weight:600}
.main{margin-left:var(--sidebar-w);padding:32px;flex:1;max-width:1400px;width:100%}
.section{margin-bottom:48px}
.section h1{color:var(--accent);font-size:26px;margin-bottom:24px;padding-bottom:12px;border-bottom:2px solid var(--border);font-weight:600}
.section h2{color:var(--accent);font-size:20px;margin:32px 0 16px;padding-bottom:8px;border-bottom:1px solid var(--border);font-weight:600}
.section h3{color:var(--text);font-size:15px;margin:12px 0 8px;font-weight:500}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:14px;margin-bottom:24px}
.card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:18px 20px;text-align:center;transition:transform 0.15s,border-color 0.15s}
.card:hover{transform:translateY(-2px);border-color:var(--accent)}
.card .num{font-size:30px;font-weight:700;color:var(--accent);line-height:1.2}
.card .label{font-size:11px;color:var(--text-muted);margin-top:6px;text-transform:uppercase;letter-spacing:0.8px}
.card.good .num{color:var(--green)}.card.warn .num{color:var(--orange)}.card.bad .num{color:var(--red)}
table{width:100%;border-collapse:collapse;margin:12px 0;font-size:13px;background:var(--surface);border-radius:8px;overflow:hidden}
th,td{padding:12px 16px;text-align:left;border-bottom:1px solid var(--border)}
th{background:rgba(88,166,255,0.08);color:var(--accent);font-weight:600;font-size:12px;text-transform:uppercase;letter-spacing:0.5px}
tr:hover{background:rgba(88,166,255,0.04)}
.split{display:grid;grid-template-columns:1fr 1fr;gap:24px;margin:16px 0}
.split-left table{margin:0}
.split-right .cc{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px}
.cc-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;margin:16px 0;overflow:hidden;transition:border-color 0.15s}
.cc-card:hover{border-color:var(--accent)}
.cc-hdr{padding:14px 18px;background:rgba(88,166,255,0.04);border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center}
.cc-hdr h3{color:var(--text);font-size:15px;margin:0;font-weight:600}
.badge{font-size:11px;padding:3px 10px;border-radius:12px;background:var(--border);color:var(--text-muted);font-weight:500}
.cc-body{padding:18px}
.insights{list-style:none;padding:0}
.insights li{padding:12px 16px;margin:6px 0;background:var(--surface);border-radius:8px;border-left:4px solid var(--accent);font-size:13px;line-height:1.5}
.insights li.warn{border-left-color:var(--orange)}.insights li.bad{border-left-color:var(--red)}.insights li.good{border-left-color:var(--green)}
.mbar{height:28px;background:var(--border);border-radius:6px;overflow:hidden;margin:4px 0}
.mbar-fill{height:100%;background:linear-gradient(90deg,var(--orange),var(--red));border-radius:6px;transition:width 0.3s}
.chart-container{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px;margin:16px 0}
@media(max-width:1100px){.split{grid-template-columns:1fr}.sidebar{width:260px}.main{margin-left:260px}}
@media(max-width:768px){.sidebar{display:none}.main{margin-left:0;padding:20px}.cards{grid-template-columns:repeat(auto-fit,minmax(120px,1fr))}}
</style></head><body>""")

        # Sidebar
        h.append(f"""<div class="sidebar"><div class="sidebar-header"><h2>Data Profile</h2><p class="file-name">{path.name}</p><p class="meta">{rows:,} rows x {cols} columns</p></div>
<div class="sidebar-nav"><div class="st">Overview</div>
<a href="#overview">Dashboard</a><a href="#quality">Data Quality</a><a href="#stats">Statistics</a><a href="#categorical">Categorical</a><a href="#correlations">Correlations</a><a href="#network">Network</a><a href="#recommendations">Recommendations</a><a href="#insights">Insights</a>
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
            h.append("</table></div>")

        # Correlations
        if corr_pairs:
            h.append(
                '<div id="correlations" class="section"><h2>Correlation Analysis</h2>'
            )
            h.append(
                f'<div class="chart-container" style="margin:16px 0"><div id="corr-heatmap" style="height:var(--heatmap-h)"></div></div>'
            )
            # Use JSON.stringify for safe embedding
            corr_z = corr_matrix.values.tolist()
            corr_x = corr_matrix.columns.tolist()
            h.append(f"""<script>
(function() {{
    var z = {corr_z};
    var x = {corr_x};
    var data = [{{z: z, x: x, y: x, type: 'heatmap', colorscale: 'RdBu', zmid: 0, text: z.map(function(r) {{ return r.map(function(v) {{ return v.toFixed(2); }}); }}), texttemplate: '%{{text}}', textfont: {{size: 11}}}}];
    var layout = {{paper_bgcolor: '#161b22', plot_bgcolor: '#161b22', font: {{color: '#c9d1d9'}}, margin: {{l: 120, r: 20, t: 20, b: 120}}, height: 500}};
    Plotly.newPlot('corr-heatmap', data, layout, {{responsive: true, displayModeBar: false}});
}})();
</script>""")
            h.append(
                "<h3>Strongest Correlations</h3><table><tr><th>Variable A</th><th>Variable B</th><th>r</th><th>Strength</th></tr>"
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
            h.append("</table></div>")

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
            quality_cls = (
                "good"
                if quality_score > 80
                else "warn"
                if quality_score > 50
                else "bad"
            )
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
        textposition: 'middle center', textfont: {{size: 12, color: '#c9d1d9'}},
        marker: {{size: 20, color: '#58a6ff', line: {{width: 2, color: '#0d1117'}}}},
        hoverinfo: 'text',
        text: nodePos.map(function(n) {{ return n.label; }}),
        showlegend: false
    }});
    var layout = {{
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
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
                    f"<tr><td>Outliers</td><td>{info['outlier_count']:,} ({info['outlier_pct']}%)</td></tr>"
                )
            elif c in cat_cols:
                h.append(
                    f"<tr><td>Mode</td><td>{info['mode']}</td></tr></table><h4 style='margin-top:10px;color:#8b949e;font-size:12px'>Top Values</h4><table><tr><th>Value</th><th>Count</th><th>%</th></tr>"
                )
                for val, count in info["top_values"].items():
                    pct = round(count / rows * 100, 1)
                    h.append(
                        f"<tr><td>{val}</td><td>{count:,}</td><td>{pct}%</td></tr>"
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
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
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
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
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
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
        height: 420, margin: {{l: 50, r: 20, t: 10, b: 30}}
    }};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d']}});
}})();
</script>""")
            h.append("</div></div></div></div></div>")

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
                f"Auto profile saved",
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
            template="plotly_dark",
        )
        fig.update_layout(height=200 * len(cols_to_plot), width=200 * len(cols_to_plot))

        if output_path:
            out = Path(output_path)
        else:
            out = path.parent / f"{path.stem}_pairwise.html"

        fig.write_html(str(out), include_plotlyjs=True, full_html=True)

        if open_after:
            _open_file(out)

        progress.append(
            ok(f"Pairwise plot saved", f"{out.name} — {len(cols_to_plot)} columns")
        )

        result = {
            "success": True,
            "op": "generate_pairwise_plot",
            "output_path": str(out.resolve()),
            "output_name": out.name,
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
            ok(f"Data exported", f"{out.name} ({size_kb:,} KB, {len(df)} rows)")
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
# generate_pairwise_plot
# ---------------------------------------------------------------------------


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
            ok(f"Data exported", f"{out.name} ({size_kb:,} KB, {len(df)} rows)")
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
            template="plotly_dark",
            xaxis_title=category_column or "Period",
            yaxis_title=agg_func.title(),
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
            margin=dict(l=20, r=20, t=40, b=20),
        )

        if output_path:
            out = Path(output_path)
        else:
            out = path.parent / f"{path.stem}_multi_{chart_type}.html"

        fig.write_html(str(out), include_plotlyjs=True, full_html=True)
        if open_after:
            _open_file(out)

        progress.append(
            ok(f"Multi-chart saved", f"{out.name} - {len(value_columns)} metrics")
        )

        result = {
            "success": True,
            "op": "generate_multi_chart",
            "chart_type": chart_type,
            "output_path": str(out.resolve()),
            "output_name": out.name,
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
    theme: str = "plotly_dark",
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
                template=theme,
                color=color_column if color_column else None,
            )
        elif chart_type == "pie":
            fig = px.pie(
                chart_df,
                names=category_column,
                values=value_column,
                title=chart_title,
                template=theme,
                hole=0.5,
            )
        elif chart_type == "line":
            fig = px.line(
                chart_df,
                x=category_column,
                y=value_column,
                title=chart_title,
                template=theme,
                color=color_column if color_column else None,
            )
        elif chart_type == "scatter":
            fig = px.scatter(
                chart_df,
                x=category_column,
                y=value_column,
                title=chart_title,
                template=theme,
                color=color_column if color_column else None,
            )
        elif chart_type == "geo":
            fig = px.choropleth_mapbox(
                chart_df,
                geojson=chart_df.geometry,
                locations=chart_df.index,
                color=value_column,
                title=chart_title,
                template=theme,
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
                template=theme,
            )
        elif chart_type == "time_series":
            fig = px.line(
                chart_df,
                x="period",
                y=value_column,
                title=chart_title,
                template=theme,
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
                template=theme,
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

        if output_path:
            out = Path(output_path)
        else:
            out = path.parent / f"{path.stem}_{chart_type}.html"

        fig.write_html(str(out), include_plotlyjs=True, full_html=True)
        rows_plotted = len(chart_df)

        if open_after:
            _open_file(out)

        progress.append(ok(f"Chart saved", f"{out.name} ({rows_plotted} rows)"))

        result = {
            "success": True,
            "op": "generate_chart",
            "chart_type": chart_type,
            "output_path": str(out.resolve()),
            "output_name": out.name,
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
# generate_dashboard - auto-generated interactive HTML dashboard
# ---------------------------------------------------------------------------


def generate_dashboard(
    file_path: str,
    output_path: str = "",
    title: str = "",
    chart_types: list[str] = None,
    geo_file_path: str = "",
    theme: str = "plotly_dark",
    dry_run: bool = False,
    open_after: bool = True,
) -> dict:
    """Generate interactive HTML dashboard with auto-detected charts. Opens HTML."""
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
        dashboard_title = title if title else path.stem

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

        # Auto-detect charts
        detected = []
        if numeric_cols and cat_cols:
            detected.append("bar")
        if datetime_cols and numeric_cols:
            detected.append("time_series")
        if len(numeric_cols) >= 2:
            detected.append("scatter")
        if cat_cols:
            detected.append("pie")
        if geo_file_path:
            detected.append("geo")

        charts = chart_types if chart_types else detected

        if dry_run:
            progress.append(info("Dry run — no file written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "generate_dashboard",
                "would_generate": {
                    "title": dashboard_title,
                    "charts": charts,
                    "kpi_columns": numeric_cols[:5],
                    "filter_columns": cat_cols[:3],
                    "run_command": "Open in browser",
                },
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        # Build interactive HTML dashboard
        h = []
        h.append("""<!DOCTYPE html>
<html><head><meta charset='utf-8'><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
:root{--bg:#0d1117;--surface:#161b22;--border:#21262d;--text:#c9d1d9;--text-muted:#8b949e;--accent:#58a6ff;--green:#3fb950;--orange:#f0883e;--red:#f85149}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;overflow-x:hidden;max-width:100vw}
::-webkit-scrollbar{width:8px}::-webkit-scrollbar-track{background:var(--bg)}::-webkit-scrollbar-thumb{background:#30363d;border-radius:4px}
header{background:var(--surface);border-bottom:1px solid var(--border);padding:20px 30px;display:flex;justify-content:space-between;align-items:center}
header h1{color:var(--accent);font-size:22px;font-weight:600}
header .meta{color:var(--text-muted);font-size:13px}
.filters{background:var(--surface);border-bottom:1px solid var(--border);padding:16px 30px;display:flex;flex-wrap:wrap;gap:12px;align-items:center}
.filters label{color:var(--text-muted);font-size:12px;text-transform:uppercase;font-weight:600}
.filters select{background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:6px 12px;font-size:13px;min-width:150px}
.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:14px;padding:20px 30px}
.kpi-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:18px;text-align:center}
.kpi-card .num{font-size:26px;font-weight:700;color:var(--accent)}
.kpi-card .label{font-size:11px;color:var(--text-muted);margin-top:4px;text-transform:uppercase}
.charts{padding:20px 30px;display:grid;grid-template-columns:repeat(auto-fill,minmax(480px,1fr));gap:16px}
.chart-box{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px;overflow:hidden;min-width:0}
.chart-box h3{color:var(--text);font-size:13px;margin-bottom:8px;padding-left:4px;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.full-width{grid-column:1/-1}
@media(max-width:1100px){.charts{grid-template-columns:1fr}}
@media(max-width:600px){header,.filters,.kpi-row,.charts{padding-left:16px;padding-right:16px}.kpi-row{grid-template-columns:repeat(2,1fr)}.charts{grid-template-columns:1fr}}
@media(max-width:600px){header,.filters,.kpi-row,.charts{padding-left:16px;padding-right:16px}.kpi-row{grid-template-columns:repeat(2,1fr)}.charts{grid-template-columns:1fr}}
</style></head><body>""")

        # Header
        h.append(f"""<header><h1>:bar_chart: {dashboard_title}</h1>
<p class="meta">{len(df):,} rows x {len(df.columns)} columns | {len(numeric_cols)} numeric, {len(cat_cols)} categorical</p></header>""")

        # Filters
        if cat_cols:
            h.append('<div class="filters"><label>Filters:</label>')
            for fc in cat_cols[:5]:
                unique_vals = df[fc].dropna().unique().tolist()
                if len(unique_vals) <= 50:
                    options = "".join(
                        f'<option value="{v}">{v}</option>' for v in unique_vals[:50]
                    )
                    h.append(
                        f'<div><label>{fc}</label><select id="filter-{fc}" multiple size="1" onchange="applyFilters()">{options}</select></div>'
                    )
            h.append("</div>")

        # KPI cards
        h.append('<div class="kpi-row">')
        for nc in numeric_cols[:6]:
            val = df[nc].sum()
            h.append(
                f'<div class="kpi-card"><div class="num">{val:,.0f}</div><div class="label">{nc}</div></div>'
            )
        h.append("</div>")

        # Charts grid
        h.append('<div class="charts">')

        # Bar charts
        if "bar" in charts and cat_cols and numeric_cols:
            for cc in cat_cols[:3]:
                for nc in numeric_cols[:2]:
                    agg_df = (
                        df.groupby(cc, as_index=False)[nc]
                        .sum()
                        .sort_values(nc, ascending=False)
                        .head(20)
                    )
                    chart_id = f"bar-{cc}-{nc}"
                    h.append(
                        f'<div class="chart-box"><h3>Total {nc} by {cc}</h3><div id="{chart_id}" style="height:380px"></div></div>'
                    )
                    h.append(f"""<script>
(function() {{
    var data = [{{
        x: {agg_df[cc].tolist()},
        y: {agg_df[nc].tolist()},
        type: 'bar',
        marker: {{color: '#58a6ff'}},
        text: {agg_df[nc].apply(lambda x: f"{{x:,.0f}}").tolist()},
        textposition: 'outside'
    }}];
    var layout = {{
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
        height: 380, margin: {{l: 20, r: 20, t: 10, b: 60}},
        xaxis: {{tickangle: -45}}
    }};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d']}});
}})();
</script>""")

        # Pie charts
        if "pie" in charts and cat_cols:
            for cc in cat_cols[:4]:
                val_counts = df[cc].value_counts().head(15)
                chart_id = f"pie-{cc}"
                h.append(
                    f'<div class="chart-box"><h3>{cc} Distribution</h3><div id="{chart_id}" style="height:380px"></div></div>'
                )
                h.append(f"""<script>
(function() {{
    var data = [{{
        values: {val_counts.values.tolist()},
        labels: {val_counts.index.tolist()},
        type: 'pie',
        hole: 0.4,
        marker: {{colors: ['#58a6ff', '#3fb950', '#f0883e', '#f85149', '#bc8cff', '#79c0ff', '#7ee787', '#ffa657', '#ff7b72', '#d2a8ff', '#a5d6ff', '#aff5b4', '#ffd6a5', '#ffabab', '#e0b0ff']}},
        textinfo: 'label+percent',
        textfont: {{size: 12}}
    }}];
    var layout = {{
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
        height: 380, margin: {{l: 20, r: 20, t: 10, b: 20}}
    }};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d']}});
}})();
</script>""")

        # Scatter plots
        if "scatter" in charts and len(numeric_cols) >= 2:
            for i in range(min(3, len(numeric_cols))):
                for j in range(i + 1, min(4, len(numeric_cols))):
                    nc1, nc2 = numeric_cols[i], numeric_cols[j]
                    chart_id = f"scatter-{nc1}-{nc2}"
                    h.append(
                        f'<div class="chart-box"><h3>{nc1} vs {nc2}</h3><div id="{chart_id}" style="height:380px"></div></div>'
                    )
                    h.append(f"""<script>
(function() {{
    var data = [{{
        x: {df[nc1].dropna().tolist()},
        y: {df[nc2].dropna().tolist()},
        type: 'scatter', mode: 'markers',
        marker: {{color: '#58a6ff', opacity: 0.6, size: 6}},
        text: ['{nc1}: ' + {df[nc1].dropna().apply(lambda x: f"{{x:,.1f}}").tolist()} + '<br>{nc2}: ' + {df[nc2].dropna().apply(lambda x: f"{{x:,.1f}}").tolist()}],
        hoverinfo: 'text'
    }}];
    var layout = {{
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
        height: 380, margin: {{l: 20, r: 20, t: 10, b: 40}},
        xaxis: {{title: '{nc1}'}},
        yaxis: {{title: '{nc2}'}}
    }};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d']}});
}})();
</script>""")

        # Correlation heatmap
        if len(numeric_cols) >= 2:
            corr = df[numeric_cols].corr()
            chart_id = "corr-heatmap"
            h.append(
                f'<div class="chart-box full-width"><h3>Correlation Matrix</h3><div id="{chart_id}" style="height:500px"></div></div>'
            )
            h.append(f"""<script>
(function() {{
    var z = {corr.values.tolist()};
    var x = {corr.columns.tolist()};
    var data = [{{z: z, x: x, y: x, type: 'heatmap', colorscale: 'RdBu_r', zmid: 0,
        text: z.map(function(r) {{ return r.map(function(v) {{ return v.toFixed(2); }}); }}),
        texttemplate: '%{{text}}', textfont: {{size: 11}}}}];
    var layout = {{
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
        height: 500, margin: {{l: 120, r: 20, t: 10, b: 120}}
    }};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: false}});
}})();
</script>""")

        # Multi-condition: Grouped bar chart
        if len(cat_cols) >= 2 and numeric_cols:
            cc1, cc2 = cat_cols[0], cat_cols[1]
            nc = numeric_cols[0]
            if all(c in df.columns for c in [cc1, cc2, nc]):
                agg_df = df.groupby([cc1, cc2], as_index=False)[nc].sum()
                chart_id = f"grouped-bar-{cc1}-{cc2}-{nc}"
                h.append(
                    f'<div class="chart-box full-width"><h3>{nc} by {cc1}, grouped by {cc2}</h3><div id="{chart_id}" style="height:400px"></div></div>'
                )
                traces = []
                for val in agg_df[cc2].unique()[:10]:
                    sub = agg_df[agg_df[cc2] == val]
                    traces.append(f"""{{
        x: {sub[cc1].tolist()},
        y: {sub[nc].tolist()},
        type: 'bar',
        name: '{val}',
        marker: {{opacity: 0.85}}
    }}""")
                h.append(f"""<script>
(function() {{
    var data = [{",".join(traces)}];
    var layout = {{
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
        barmode: 'group',
        height: 400, margin: {{l: 20, r: 20, t: 10, b: 80}},
        xaxis: {{tickangle: -45}},
        legend: {{orientation: 'h', y: -0.3}}
    }};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d']}});
}})();
</script>""")

                # Stacked bar
                chart_id2 = f"stacked-bar-{cc1}-{cc2}-{nc}"
                h.append(
                    f'<div class="chart-box full-width"><h3>{nc} by {cc1}, stacked by {cc2}</h3><div id="{chart_id2}" style="height:400px"></div></div>'
                )
                traces2 = []
                for val in agg_df[cc2].unique()[:10]:
                    sub = agg_df[agg_df[cc2] == val]
                    traces2.append(f"""{{
        x: {sub[cc1].tolist()},
        y: {sub[nc].tolist()},
        type: 'bar',
        name: '{val}',
        marker: {{opacity: 0.85}}
    }}""")
                h.append(f"""<script>
(function() {{
    var data = [{",".join(traces2)}];
    var layout = {{
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
        barmode: 'stack',
        height: 400, margin: {{l: 20, r: 20, t: 10, b: 80}},
        xaxis: {{tickangle: -45}},
        legend: {{orientation: 'h', y: -0.3}}
    }};
    Plotly.newPlot('{chart_id2}', data, layout, {{responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d']}});
}})();
</script>""")

        # Multi-condition: Colored scatter plot
        if len(numeric_cols) >= 2 and cat_cols:
            nc1, nc2 = numeric_cols[0], numeric_cols[1]
            cc = cat_cols[0]
            if all(c in df.columns for c in [nc1, nc2, cc]):
                chart_id = f"colored-scatter-{nc1}-{nc2}-{cc}"
                h.append(
                    f'<div class="chart-box full-width"><h3>{nc1} vs {nc2}, colored by {cc}</h3><div id="{chart_id}" style="height:400px"></div></div>'
                )
                traces = []
                for val in df[cc].dropna().unique()[:15]:
                    sub = df[df[cc] == val]
                    traces.append(f"""{{
        x: {sub[nc1].dropna().tolist()},
        y: {sub[nc2].dropna().tolist()},
        type: 'scatter', mode: 'markers',
        name: '{val}',
        marker: {{opacity: 0.7, size: 5}}
    }}""")
                h.append(f"""<script>
(function() {{
    var data = [{",".join(traces)}];
    var layout = {{
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
        height: 400, margin: {{l: 20, r: 20, t: 10, b: 40}},
        xaxis: {{title: '{nc1}'}},
        yaxis: {{title: '{nc2}'}},
        legend: {{orientation: 'h', y: -0.25}}
    }};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d']}});
}})();
</script>""")

        # Multi-condition: Grouped box plot
        if numeric_cols and cat_cols:
            nc = numeric_cols[0]
            cc = cat_cols[0]
            if nc in df.columns and cc in df.columns:
                chart_id = f"box-{nc}-by-{cc}"
                h.append(
                    f'<div class="chart-box full-width"><h3>{nc} distribution by {cc}</h3><div id="{chart_id}" style="height:400px"></div></div>'
                )
                h.append(f"""<script>
(function() {{
    var data = [];
    var categories = {df[cc].dropna().unique()[:20].tolist()};
    var vals = {df[df[cc].isin(df[cc].dropna().unique()[:20].tolist())].groupby(cc)[nc].apply(list).to_dict()};
    for (var i = 0; i < categories.length; i++) {{
        if (vals[categories[i]]) {{
            data.push({{
                type: 'box', y: vals[categories[i]], name: categories[i],
                boxpoints: 'outliers', marker: {{size: 3}}
            }});
        }}
    }}
    var layout = {{
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
        height: 400, margin: {{l: 20, r: 20, t: 10, b: 80}},
        xaxis: {{tickangle: -45}},
        showlegend: false
    }};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d']}});
}})();
</script>""")

        # Multi-condition: Aggregation heatmap
        if len(cat_cols) >= 2 and numeric_cols:
            cc1, cc2 = cat_cols[0], cat_cols[1]
            nc = numeric_cols[0]
            if all(c in df.columns for c in [cc1, cc2, nc]):
                pivot = df.pivot_table(
                    index=cc1, columns=cc2, values=nc, aggfunc="sum", fill_value=0
                )
                chart_id = f"agg-heatmap-{cc1}-{cc2}-{nc}"
                h.append(
                    f'<div class="chart-box full-width"><h3>Sum {nc}: {cc1} x {cc2}</h3><div id="{chart_id}" style="height:500px"></div></div>'
                )
                h.append(f"""<script>
(function() {{
    var z = {pivot.values.tolist()};
    var x = {pivot.columns.tolist()};
    var y = {pivot.index.tolist()};
    var data = [{{z: z, x: x, y: y, type: 'heatmap', colorscale: 'YlOrRd',
        text: z.map(function(r) {{ return r.map(function(v) {{ return v.toFixed(0); }}); }}),
        texttemplate: '%{{text}}', textfont: {{size: 10}}}}];
    var layout = {{
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
        height: 500, margin: {{l: 120, r: 20, t: 10, b: 120}}
    }};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: false}});
}})();
</script>""")

        # Time series
        if "time_series" in charts and datetime_cols and numeric_cols:
            for dc in datetime_cols[:2]:
                for nc in numeric_cols[:3]:
                    try:
                        ts_df = df.copy()
                        ts_df[dc] = pd.to_datetime(ts_df[dc])
                        ts_df = (
                            ts_df.set_index(dc).resample("ME")[nc].sum().reset_index()
                        )
                        chart_id = f"ts-{dc}-{nc}"
                        h.append(
                            f'<div class="chart-box"><h3>{nc} Over Time (Monthly)</h3><div id="{chart_id}" style="height:380px"></div></div>'
                        )
                        h.append(f"""<script>
(function() {{
    var data = [{{
        x: {ts_df[dc].astype(str).tolist()},
        y: {ts_df[nc].tolist()},
        type: 'scatter', mode: 'lines+markers',
        line: {{color: '#3fb950', width: 2}},
        marker: {{size: 4, color: '#3fb950'}}
    }}];
    var layout = {{
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
        height: 380, margin: {{l: 20, r: 20, t: 10, b: 60}}
    }};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d']}});
}})();
</script>""")
                    except:
                        pass

        # Distribution charts
        for nc in numeric_cols[:6]:
            clean_data = df[nc].dropna().tolist()
            chart_id = f"dist-{nc}"
            h.append(
                f'<div class="chart-box"><h3>{nc} Distribution</h3><div id="{chart_id}" style="height:350px"></div></div>'
            )
            h.append(f"""<script>
(function() {{
    var d = {clean_data};
    var trace1 = {{x: d, type: 'histogram', nbinsx: 50, marker: {{color: '#58a6ff', opacity: 0.7}}, yaxis: 'y'}};
    var trace2 = {{y: d, type: 'box', marker: {{color: '#f0883e'}}, xaxis: 'x2', yaxis: 'y2'}};
    var layout = {{
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
        grid: {{rows: 1, columns: 2, pattern: 'independent'}},
        height: 350, margin: {{l: 50, r: 20, t: 10, b: 30}},
        yaxis: {{title: 'Count'}},
        yaxis2: {{title: ''}}
    }};
    Plotly.newPlot('{chart_id}', [trace1, trace2], layout, {{responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d']}});
}})();
</script>""")

        h.append("</div></body></html>")

        html_content = "\n".join(h)

        if output_path:
            out = Path(output_path)
        else:
            out = path.parent / f"{path.stem}_dashboard.html"

        out.write_text(html_content, encoding="utf-8")
        size_kb = round(out.stat().st_size / 1024)

        if open_after:
            _open_file(out)

        progress.append(ok(f"Dashboard saved", f"{out.name} ({size_kb:,} KB)"))

        result = {
            "success": True,
            "op": "generate_dashboard",
            "output_path": str(out.resolve()),
            "output_name": out.name,
            "dashboard_title": dashboard_title,
            "charts_included": charts,
            "kpi_columns": numeric_cols[:6],
            "filter_columns": cat_cols[:5],
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

        # Auto-detect charts
        detected = []
        if numeric_cols and cat_cols:
            detected.append("bar")
        if datetime_cols and numeric_cols:
            detected.append("time_series")
        if len(numeric_cols) >= 2:
            detected.append("scatter")
        if cat_cols:
            detected.append("pie")
        if geo_file_path:
            detected.append("geo")

        charts = chart_types if chart_types else detected

        # Build interactive HTML dashboard
        h = []
        h.append("""<!DOCTYPE html>
<html><head><meta charset='utf-8'><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
:root{--bg:#0d1117;--surface:#161b22;--border:#21262d;--text:#c9d1d9;--text-muted:#8b949e;--accent:#58a6ff;--green:#3fb950;--orange:#f0883e;--red:#f85149}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;overflow-x:hidden;max-width:100vw}
::-webkit-scrollbar{width:8px}::-webkit-scrollbar-track{background:var(--bg)}::-webkit-scrollbar-thumb{background:#30363d;border-radius:4px}
header{background:var(--surface);border-bottom:1px solid var(--border);padding:20px 30px;display:flex;justify-content:space-between;align-items:center}
header h1{color:var(--accent);font-size:22px;font-weight:600}
header .meta{color:var(--text-muted);font-size:13px}
.filters{background:var(--surface);border-bottom:1px solid var(--border);padding:16px 30px;display:flex;flex-wrap:wrap;gap:12px;align-items:center}
.filters label{color:var(--text-muted);font-size:12px;text-transform:uppercase;font-weight:600}
.filters select{background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:6px 12px;font-size:13px;min-width:150px}
.kpi-row{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:14px;padding:20px 30px}
.kpi-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:18px;text-align:center}
.kpi-card .num{font-size:26px;font-weight:700;color:var(--accent)}
.kpi-card .label{font-size:11px;color:var(--text-muted);margin-top:4px;text-transform:uppercase}
.charts{padding:20px 30px;display:grid;grid-template-columns:repeat(auto-fill,minmax(480px,1fr));gap:16px}
.chart-box{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px;overflow:hidden;min-width:0}
.chart-box h3{color:var(--text);font-size:13px;margin-bottom:8px;padding-left:4px;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.full-width{grid-column:1/-1}
@media(max-width:1100px){.charts{grid-template-columns:1fr}}
@media(max-width:600px){header,.filters,.kpi-row,.charts{padding-left:16px;padding-right:16px}.kpi-row{grid-template-columns:repeat(2,1fr)}.charts{grid-template-columns:1fr}}
@media(max-width:600px){header,.filters,.kpi-row,.charts{padding-left:16px;padding-right:16px}.kpi-row{grid-template-columns:repeat(2,1fr)}.charts{grid-template-columns:1fr}}
</style></head><body>""")

        # Header
        h.append(f"""<header><h1>:bar_chart: {dashboard_title}</h1>
<p class="meta">{len(df):,} rows x {len(df.columns)} columns | {len(numeric_cols)} numeric, {len(cat_cols)} categorical</p></header>""")

        # Filters
        if cat_cols:
            h.append('<div class="filters"><label>Filters:</label>')
            for fc in cat_cols[:5]:
                unique_vals = df[fc].dropna().unique().tolist()
                if len(unique_vals) <= 50:
                    options = "".join(
                        f'<option value="{v}">{v}</option>' for v in unique_vals[:50]
                    )
                    h.append(
                        f'<div><label>{fc}</label><select id="filter-{fc}" multiple size="1" onchange="applyFilters()">{options}</select></div>'
                    )
            h.append("</div>")

        # KPI cards
        h.append('<div class="kpi-row">')
        for nc in numeric_cols[:6]:
            val = df[nc].sum()
            h.append(
                f'<div class="kpi-card"><div class="num">{val:,.0f}</div><div class="label">{nc}</div></div>'
            )
        h.append("</div>")

        # Charts grid
        h.append('<div class="charts">')

        # Bar charts
        if "bar" in charts and cat_cols and numeric_cols:
            for cc in cat_cols[:3]:
                for nc in numeric_cols[:2]:
                    agg_df = (
                        df.groupby(cc, as_index=False)[nc]
                        .sum()
                        .sort_values(nc, ascending=False)
                        .head(20)
                    )
                    chart_id = f"bar-{cc}-{nc}"
                    h.append(
                        f'<div class="chart-box"><h3>Total {nc} by {cc}</h3><div id="{chart_id}" style="height:380px"></div></div>'
                    )
                    h.append(f"""<script>
(function() {{
    var data = [{{
        x: {agg_df[cc].tolist()},
        y: {agg_df[nc].tolist()},
        type: 'bar',
        marker: {{color: '#58a6ff'}},
        text: {agg_df[nc].apply(lambda x: f"{{x:,.0f}}").tolist()},
        textposition: 'outside'
    }}];
    var layout = {{
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
        height: 380, margin: {{l: 20, r: 20, t: 10, b: 60}},
        xaxis: {{tickangle: -45}}
    }};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d']}});
}})();
</script>""")

        # Pie charts
        if "pie" in charts and cat_cols:
            for cc in cat_cols[:4]:
                val_counts = df[cc].value_counts().head(15)
                chart_id = f"pie-{cc}"
                h.append(
                    f'<div class="chart-box"><h3>{cc} Distribution</h3><div id="{chart_id}" style="height:380px"></div></div>'
                )
                h.append(f"""<script>
(function() {{
    var data = [{{
        values: {val_counts.values.tolist()},
        labels: {val_counts.index.tolist()},
        type: 'pie',
        hole: 0.4,
        marker: {{colors: ['#58a6ff', '#3fb950', '#f0883e', '#f85149', '#bc8cff', '#79c0ff', '#7ee787', '#ffa657', '#ff7b72', '#d2a8ff', '#a5d6ff', '#aff5b4', '#ffd6a5', '#ffabab', '#e0b0ff']}},
        textinfo: 'label+percent',
        textfont: {{size: 12}}
    }}];
    var layout = {{
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
        height: 380, margin: {{l: 20, r: 20, t: 10, b: 20}}
    }};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d']}});
}})();
</script>""")

        # Scatter plots
        if "scatter" in charts and len(numeric_cols) >= 2:
            for i in range(min(3, len(numeric_cols))):
                for j in range(i + 1, min(4, len(numeric_cols))):
                    nc1, nc2 = numeric_cols[i], numeric_cols[j]
                    chart_id = f"scatter-{nc1}-{nc2}"
                    h.append(
                        f'<div class="chart-box"><h3>{nc1} vs {nc2}</h3><div id="{chart_id}" style="height:380px"></div></div>'
                    )
                    h.append(f"""<script>
(function() {{
    var data = [{{
        x: {df[nc1].dropna().tolist()},
        y: {df[nc2].dropna().tolist()},
        type: 'scatter', mode: 'markers',
        marker: {{color: '#58a6ff', opacity: 0.6, size: 6}},
        text: ['{nc1}: ' + {df[nc1].dropna().apply(lambda x: f"{{x:,.1f}}").tolist()} + '<br>{nc2}: ' + {df[nc2].dropna().apply(lambda x: f"{{x:,.1f}}").tolist()}],
        hoverinfo: 'text'
    }}];
    var layout = {{
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
        height: 380, margin: {{l: 20, r: 20, t: 10, b: 40}},
        xaxis: {{title: '{nc1}'}},
        yaxis: {{title: '{nc2}'}}
    }};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d']}});
}})();
</script>""")

        # Correlation heatmap
        if len(numeric_cols) >= 2:
            corr = df[numeric_cols].corr()
            chart_id = "corr-heatmap"
            h.append(
                f'<div class="chart-box full-width"><h3>Correlation Matrix</h3><div id="{chart_id}" style="height:500px"></div></div>'
            )
            h.append(f"""<script>
(function() {{
    var z = {corr.values.tolist()};
    var x = {corr.columns.tolist()};
    var data = [{{z: z, x: x, y: x, type: 'heatmap', colorscale: 'RdBu_r', zmid: 0,
        text: z.map(function(r) {{ return r.map(function(v) {{ return v.toFixed(2); }}); }}),
        texttemplate: '%{{text}}', textfont: {{size: 11}}}}];
    var layout = {{
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
        height: 500, margin: {{l: 120, r: 20, t: 10, b: 120}}
    }};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: false}});
}})();
</script>""")

        # Time series
        if "time_series" in charts and datetime_cols and numeric_cols:
            for dc in datetime_cols[:2]:
                for nc in numeric_cols[:3]:
                    try:
                        ts_df = df.copy()
                        ts_df[dc] = pd.to_datetime(ts_df[dc])
                        ts_df = (
                            ts_df.set_index(dc).resample("ME")[nc].sum().reset_index()
                        )
                        chart_id = f"ts-{dc}-{nc}"
                        h.append(
                            f'<div class="chart-box"><h3>{nc} Over Time (Monthly)</h3><div id="{chart_id}" style="height:380px"></div></div>'
                        )
                        h.append(f"""<script>
(function() {{
    var data = [{{
        x: {ts_df[dc].astype(str).tolist()},
        y: {ts_df[nc].tolist()},
        type: 'scatter', mode: 'lines+markers',
        line: {{color: '#3fb950', width: 2}},
        marker: {{size: 4, color: '#3fb950'}}
    }}];
    var layout = {{
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
        height: 380, margin: {{l: 20, r: 20, t: 10, b: 60}}
    }};
    Plotly.newPlot('{chart_id}', data, layout, {{responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d']}});
}})();
</script>""")
                    except:
                        pass

        # Distribution charts
        for nc in numeric_cols[:6]:
            clean_data = df[nc].dropna().tolist()
            chart_id = f"dist-{nc}"
            h.append(
                f'<div class="chart-box"><h3>{nc} Distribution</h3><div id="{chart_id}" style="height:350px"></div></div>'
            )
            h.append(f"""<script>
(function() {{
    var d = {clean_data};
    var trace1 = {{x: d, type: 'histogram', nbinsx: 50, marker: {{color: '#58a6ff', opacity: 0.7}}, yaxis: 'y'}};
    var trace2 = {{y: d, type: 'box', marker: {{color: '#f0883e'}}, xaxis: 'x2', yaxis: 'y2'}};
    var layout = {{
        paper_bgcolor: '#161b22', plot_bgcolor: '#161b22',
        font: {{color: '#c9d1d9'}},
        grid: {{rows: 1, columns: 2, pattern: 'independent'}},
        height: 350, margin: {{l: 50, r: 20, t: 10, b: 30}},
        yaxis: {{title: 'Count'}},
        yaxis2: {{title: ''}}
    }};
    Plotly.newPlot('{chart_id}', [trace1, trace2], layout, {{responsive: true, displayModeBar: true, modeBarButtonsToRemove: ['lasso2d', 'select2d']}});
}})();
</script>""")

        h.append("</div></body></html>")

        html_content = "\n".join(h)

        if output_path:
            out = Path(output_path)
        else:
            out = path.parent / f"{path.stem}_dashboard.html"

        out.write_text(html_content, encoding="utf-8")
        size_kb = round(out.stat().st_size / 1024)

        if open_after:
            _open_file(out)

        progress.append(ok(f"Dashboard saved", f"{out.name} ({size_kb:,} KB)"))

        result = {
            "success": True,
            "op": "generate_dashboard",
            "output_path": str(out.resolve()),
            "output_name": out.name,
            "dashboard_title": dashboard_title,
            "charts_included": charts,
            "kpi_columns": numeric_cols[:6],
            "filter_columns": cat_cols[:5],
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

        df = _read_csv(str(path))
        dashboard_title = title if title else path.stem

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

        # Auto-detect charts
        detected = []
        if numeric_cols and cat_cols:
            detected.append("bar")
        if datetime_cols and numeric_cols:
            detected.append("time_series")
        if len(numeric_cols) >= 2:
            detected.append("scatter")
        if cat_cols:
            detected.append("pie")
        if geo_file_path:
            detected.append("geo")

        charts = chart_types if chart_types else detected

        if dry_run:
            progress.append(info("Dry run — no file written", path.name))
            result = {
                "success": True,
                "dry_run": True,
                "op": "generate_dashboard",
                "would_generate": {
                    "title": dashboard_title,
                    "charts": charts,
                    "kpi_columns": numeric_cols[:5],
                    "filter_columns": cat_cols[:3],
                    "run_command": "streamlit run app.py",
                },
                "progress": progress,
            }
            result["token_estimate"] = _token_estimate(result)
            return result

        # Generate app.py - comprehensive auto-dashboard
        abs_path = str(path.resolve())
        abs_geo = str(Path(geo_file_path).resolve()) if geo_file_path else ""

        filter_cols = cat_cols[:5]
        kpi_cols = numeric_cols[:6]

        # Build comprehensive dashboard
        app_code = f'''"""Auto-generated comprehensive dashboard for {dashboard_title}."""
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from pathlib import Path

st.set_page_config(page_title="{dashboard_title}", layout="wide", page_icon=":bar_chart:")

# Custom CSS
st.markdown("""
<style>
    .metric-card {{background-color: #0d1117; border: 1px solid #21262d; border-radius: 10px; padding: 18px; text-align: center;}}
    .metric-card .num {{font-size: 28px; font-weight: 700; color: #58a6ff;}}
    .metric-card .label {{font-size: 11px; color: #8b949e; text-transform: uppercase; margin-top: 4px;}}
    [data-testid="stSidebar"] {{background-color: #161b22;}}
    [data-testid="stSidebar"] * {{color: #c9d1d9;}}
</style>
""", unsafe_allow_html=True)

# Load data
df = pd.read_csv(r"{abs_path}")

# Auto-detect column types
numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
cat_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()
datetime_cols = df.select_dtypes(include=["datetime"]).columns.tolist()

# Title
st.title(":bar_chart: {dashboard_title}")
st.caption(f"{len(df):,} rows x {len(df.columns)} columns | {len(numeric_cols)} numeric, {len(cat_cols)} categorical")

# Sidebar
with st.sidebar:
    st.header(":gear: Filters")
    for fc in {filter_cols}:
        if fc in df.columns:
            unique_vals = df[fc].dropna().unique().tolist()
            if len(unique_vals) <= 50:
                selected = st.multiselect(fc, options=unique_vals, default=unique_vals)
                df = df[df[fc].isin(selected)]
            else:
                selected = st.selectbox(fc, options=unique_vals)
                df = df[df[fc] == selected]

    st.divider()
    st.caption(f"Showing {{len(df):,}} of {{len(pd.read_csv(r'{abs_path}')):,}} rows")

# KPI Cards
st.subheader(":rocket: Key Metrics")
kpi_cols_list = numeric_cols[:6]
cols = st.columns(min(len(kpi_cols_list), 6))
for i, kc in enumerate(kpi_cols_list):
    if kc in df.columns:
        with cols[i % 6]:
            val = df[kc].sum()
            st.metric(label=kc, value=f"{{val:,.0f}}")

# Tabs
tab_overview, tab_multi, tab_analysis, tab_trends, tab_distribution, tab_data = st.tabs([
    ":chart_with_upwards_trend: Overview",
    ":chart_with_upwards_trend: Multi-Condition",
    ":mag: Analysis",
    ":calendar: Trends",
    ":bar_chart: Distribution",
    ":table: Data"
])

with tab_overview:
    # Bar charts for categorical vs numeric
    if cat_cols and numeric_cols:
        for cc in cat_cols[:3]:
            for nc in numeric_cols[:2]:
                if cc in df.columns and nc in df.columns:
                    agg_df = df.groupby(cc, as_index=False)[nc].sum().sort_values(nc, ascending=False)
                    fig = px.bar(agg_df.head(20), x=cc, y=nc, title=f"Total {{nc}} by {{cc}}",
                                 color=nc, color_continuous_scale="Blues", template="plotly_dark")
                    fig.update_layout(height=400, margin=dict(l=20, r=20, t=40, b=60))
                    st.plotly_chart(fig, use_container_width=True)

    # Pie charts for categorical distribution
    if cat_cols:
        for cc in cat_cols[:4]:
            if cc in df.columns:
                val_counts = df[cc].value_counts().head(15)
                fig = px.pie(values=val_counts.values, names=val_counts.index,
                             title=f"{{cc}} Distribution", template="plotly_dark", hole=0.4)
                fig.update_layout(height=400, margin=dict(l=20, r=20, t=40, b=20))
                st.plotly_chart(fig, use_container_width=True)

with tab_multi:
    st.subheader("Multi-Condition Charts")

    # Stacked bar chart: category x value, colored by another category
    if len(cat_cols) >= 2 and numeric_cols:
        cc1, cc2 = cat_cols[0], cat_cols[1]
        nc = numeric_cols[0]
        if all(c in df.columns for c in [cc1, cc2, nc]):
            agg_df = df.groupby([cc1, cc2], as_index=False)[nc].sum()
            fig = px.bar(agg_df, x=cc1, y=nc, color=cc2, barmode="group",
                         title=f"{{nc}} by {{cc1}}, grouped by {{cc2}}", template="plotly_dark")
            fig.update_layout(height=450, margin=dict(l=20, r=20, t=40, b=80))
            st.plotly_chart(fig, use_container_width=True)

            # Stacked version
            fig2 = px.bar(agg_df, x=cc1, y=nc, color=cc2, barmode="stack",
                          title=f"{{nc}} by {{cc1}}, stacked by {{cc2}}", template="plotly_dark")
            fig2.update_layout(height=450, margin=dict(l=20, r=20, t=40, b=80))
            st.plotly_chart(fig2, use_container_width=True)

    # 100% stacked bar chart
    if len(cat_cols) >= 2 and numeric_cols:
        cc1, cc2 = cat_cols[0], cat_cols[1]
        nc = numeric_cols[0]
        if all(c in df.columns for c in [cc1, cc2, nc]):
            agg_df = df.groupby([cc1, cc2], as_index=False)[nc].sum()
            agg_df["pct"] = agg_df.groupby(cc1)[nc].transform(lambda x: x / x.sum() * 100)
            fig = px.bar(agg_df, x=cc1, y="pct", color=cc2, barmode="stack",
                         title=f"% {{nc}} by {{cc1}}, stacked by {{cc2}}", template="plotly_dark",
                         labels={{"pct": "Percentage (%)"}})
            fig.update_layout(height=450, margin=dict(l=20, r=20, t=40, b=80), yaxis_ticksuffix="%")
            st.plotly_chart(fig, use_container_width=True)

    # Multi-line time series: multiple metrics over time
    if datetime_cols and len(numeric_cols) >= 2:
        dc = datetime_cols[0]
        if dc in df.columns:
            ts_df = df.copy()
            ts_df[dc] = pd.to_datetime(ts_df[dc])
            ts_df = ts_df.set_index(dc).resample("ME").agg({{nc: "sum" for nc in numeric_cols[:4]}}).reset_index()
            fig = go.Figure()
            for nc in numeric_cols[:4]:
                if nc in ts_df.columns:
                    fig.add_trace(go.Scatter(x=ts_df[dc], y=ts_df[nc], mode="lines+markers",
                                             name=nc, line=dict(width=2)))
            fig.update_layout(title="Multiple Metrics Over Time (Monthly)", template="plotly_dark",
                              height=450, margin=dict(l=20, r=20, t=40, b=60),
                              xaxis_title="Date", yaxis_title="Value")
            st.plotly_chart(fig, use_container_width=True)

    # Colored scatter plot: two numeric columns, colored by category, sized by another
    if len(numeric_cols) >= 2 and cat_cols:
        nc1, nc2 = numeric_cols[0], numeric_cols[1]
        cc = cat_cols[0]
        if all(c in df.columns for c in [nc1, nc2, cc]):
            size_col = numeric_cols[2] if len(numeric_cols) > 2 else None
            fig = px.scatter(df, x=nc1, y=nc2, color=cc,
                             size=size_col if size_col else None,
                             title=f"{{nc1}} vs {{nc2}}, colored by {{cc}}",
                             template="plotly_dark", hover_data=df.columns[:6].tolist())
            fig.update_layout(height=450, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig, use_container_width=True)

    # Grouped box plot: numeric distribution by category
    if numeric_cols and cat_cols:
        nc = numeric_cols[0]
        cc = cat_cols[0]
        if nc in df.columns and cc in df.columns:
            fig = px.box(df, x=cc, y=nc, color=cc, title=f"{{nc}} distribution by {{cc}}",
                         template="plotly_dark", points="outliers")
            fig.update_layout(height=450, margin=dict(l=20, r=20, t=40, b=80), showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    # Aggregation heatmap: category x category with numeric aggregation
    if len(cat_cols) >= 2 and numeric_cols:
        cc1, cc2 = cat_cols[0], cat_cols[1]
        nc = numeric_cols[0]
        if all(c in df.columns for c in [cc1, cc2, nc]):
            pivot = df.pivot_table(index=cc1, columns=cc2, values=nc, aggfunc="sum", fill_value=0)
            fig = px.imshow(pivot, text_auto=".0f", color_continuous_scale="YlOrRd",
                            title=f"Sum {{nc}}: {{cc1}} x {{cc2}}", template="plotly_dark")
            fig.update_layout(height=500, margin=dict(l=120, r=20, t=40, b=120))
            st.plotly_chart(fig, use_container_width=True)

with tab_analysis:
    # Scatter plots for numeric pairs
    if len(numeric_cols) >= 2:
        for i in range(min(3, len(numeric_cols))):
            for j in range(i+1, min(4, len(numeric_cols))):
                nc1, nc2 = numeric_cols[i], numeric_cols[j]
                fig = px.scatter(df, x=nc1, y=nc2, title=f"{{nc1}} vs {{nc2}}",
                                 template="plotly_dark", trendline="ols")
                fig.update_layout(height=400, margin=dict(l=20, r=20, t=40, b=20))
                st.plotly_chart(fig, use_container_width=True)

    # Correlation heatmap
    if len(numeric_cols) >= 2:
        corr = df[numeric_cols].corr()
        fig = px.imshow(corr, text_auto=".2f", color_continuous_scale="RdBu_r", zmid=0,
                        title="Correlation Matrix", template="plotly_dark")
        fig.update_layout(height=500, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)

with tab_trends:
    if datetime_cols and numeric_cols:
        for dc in datetime_cols[:2]:
            for nc in numeric_cols[:3]:
                if dc in df.columns and nc in df.columns:
                    try:
                        ts_df = df.copy()
                        ts_df[dc] = pd.to_datetime(ts_df[dc])
                        ts_df = ts_df.set_index(dc).resample("ME")[nc].sum().reset_index()
                        fig = px.line(ts_df, x=dc, y=nc, title=f"{{nc}} Over Time (Monthly)",
                                      template="plotly_dark", markers=True)
                        fig.update_layout(height=400, margin=dict(l=20, r=20, t=40, b=60))
                        st.plotly_chart(fig, use_container_width=True)
                    except:
                        pass
    else:
        st.info("No datetime columns detected for trend analysis.")

with tab_distribution:
    for nc in numeric_cols[:6]:
        if nc in df.columns:
            fig = make_subplots(rows=1, cols=2, subplot_titles=[f"{{nc}} Distribution", f"{{nc}} Box Plot"])
            fig.add_trace(go.Histogram(x=df[nc].dropna(), nbinsx=50, marker_color="#58a6ff"), row=1, col=1)
            fig.add_trace(go.Box(y=df[nc].dropna(), marker_color="#f0883e"), row=1, col=2)
            fig.update_layout(height=350, template="plotly_dark", margin=dict(l=20, r=20, t=40, b=20),
                              showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

with tab_data:
    st.subheader("Raw Data")
    st.dataframe(df, use_container_width=True, height=600)

    # Data summary
    st.subheader("Data Summary")
    st.dataframe(df.describe(), use_container_width=True)
'''

        if output_path:
            out = Path(output_path)
        else:
            out = path.parent / "app.py"

        out.write_text(app_code)

        # Validate generated code
        import py_compile

        try:
            py_compile.compile(str(out), doraise=True)
        except py_compile.PyCompileError as e:
            progress.append(warn("Generated code has syntax issues", str(e)))

        progress.append(
            ok(f"Dashboard generated", f"{out.name} — run: streamlit run {out.name}")
        )

        result = {
            "success": True,
            "op": "generate_dashboard",
            "output_path": out.name,
            "dashboard_title": dashboard_title,
            "charts_included": charts,
            "kpi_columns": kpi_cols,
            "filter_columns": filter_cols,
            "run_command": f"streamlit run {out.name}",
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


# ---------------------------------------------------------------------------
# generate_auto_profile - comprehensive EDA rivaling sweetviz/ydata-profiling
# ---------------------------------------------------------------------------
