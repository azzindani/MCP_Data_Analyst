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
            "hint": "Check file_path and column types.",
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

        fig = px.scatter_matrix(
            df[cols_to_plot],
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

        fig = px.scatter_matrix(
            df[cols_to_plot],
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
    """Multi-variable bar or line chart. Compares 2+ metrics on same axis."""
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
                fig.add_trace(
                    go.Bar(
                        x=x_vals,
                        y=grouped[vc],
                        name=vc,
                    )
                )

        elif chart_type == "multi_line":
            df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
            df = df.dropna(subset=[date_column])
            df["period"] = df[date_column].dt.to_period("M").astype(str)
            grouped = df.groupby("period", as_index=False)[value_columns].agg(agg_func)
            x_vals = grouped["period"]

            for vc in value_columns:
                fig.add_trace(
                    go.Scatter(
                        x=x_vals,
                        y=grouped[vc],
                        name=vc,
                        mode="lines+markers",
                    )
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
            ok(
                f"Multi-chart saved",
                f"{out.name} — {len(value_columns)} metrics",
            )
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
# generate_dashboard (kept unchanged)
# ---------------------------------------------------------------------------


def generate_dashboard(
    file_path: str,
    output_path: str = "",
    title: str = "",
    chart_types: list[str] = None,
    geo_file_path: str = "",
    theme: str = "plotly_dark",
    dry_run: bool = False,
) -> dict:
    """Generate Streamlit dashboard app.py from dataset. Run separately."""
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

        # Generate app.py
        abs_path = str(path.resolve())
        abs_geo = str(Path(geo_file_path).resolve()) if geo_file_path else ""

        filter_cols = cat_cols[:3]
        kpi_cols = numeric_cols[:5]

        chart_code_lines = []
        if "bar" in charts and numeric_cols and cat_cols:
            chart_code_lines.append(
                textwrap.dedent(f"""
                st.subheader("Bar Chart")
                chart_df = df.groupby('{cat_cols[0]}', as_index=False)['{numeric_cols[0]}'].sum()
                chart_df = chart_df.sort_values(by='{numeric_cols[0]}', ascending=False)
                fig = px.bar(chart_df, x='{cat_cols[0]}', y='{numeric_cols[0]}',
                             title="Total {numeric_cols[0]} by {cat_cols[0]}", template='{theme}')
                st.plotly_chart(fig, use_container_width=True)
            """)
            )
        if "time_series" in charts and datetime_cols and numeric_cols:
            chart_code_lines.append(
                textwrap.dedent(f"""
                st.subheader("Time Series")
                ts_df = df.copy()
                ts_df['{datetime_cols[0]}'] = pd.to_datetime(ts_df['{datetime_cols[0]}'])
                ts_df = ts_df.groupby(ts_df['{datetime_cols[0]}'].dt.to_period('M').astype(str), as_index=False)['{numeric_cols[0]}'].sum()
                fig = px.line(ts_df, x='{datetime_cols[0]}', y='{numeric_cols[0]}',
                              title="{numeric_cols[0]} Over Time", template='{theme}', markers=True)
                st.plotly_chart(fig, use_container_width=True)
            """)
            )
        if "scatter" in charts and len(numeric_cols) >= 2:
            chart_code_lines.append(
                textwrap.dedent(f"""
                st.subheader("Scatter Plot")
                fig = px.scatter(df, x='{numeric_cols[0]}', y='{numeric_cols[1]}',
                                 title="{numeric_cols[0]} vs {numeric_cols[1]}", template='{theme}')
                st.plotly_chart(fig, use_container_width=True)
            """)
            )
        if "pie" in charts and cat_cols:
            chart_code_lines.append(
                textwrap.dedent(f"""
                st.subheader("Pie Chart")
                pie_df = df.groupby('{cat_cols[0]}', as_index=False).size()
                fig = px.pie(pie_df, names='{cat_cols[0]}', values='size',
                             title="{cat_cols[0]} Distribution", template='{theme}', hole=0.5)
                st.plotly_chart(fig, use_container_width=True)
            """)
            )
        if "geo" in charts and geo_file_path:
            chart_code_lines.append(
                textwrap.dedent(f"""
                st.subheader("Map")
                gdf = gpd.read_file('{abs_geo}')
                fig = px.choropleth_mapbox(gdf, geojson=gdf.geometry, locations=gdf.index,
                                           color='name', title="Geographic View",
                                           template='{theme}', mapbox_style="carto-positron",
                                           center={{"lat": 37.09, "lon": -73.94}}, zoom=3)
                st.plotly_chart(fig, use_container_width=True)
            """)
            )

        chart_sections = "\n".join(chart_code_lines)

        filter_code = ""
        for fc in filter_cols:
            filter_code += textwrap.dedent(f"""
                {fc}_filter = st.sidebar.multiselect('{fc}', options=df['{fc}'].unique().tolist())
                if {fc}_filter:
                    df = df[df['{fc}'].isin({fc}_filter)]
            """)

        # Build KPI code
        kpi_lines = []
        for kc in kpi_cols:
            kpi_lines.append(
                f"    st.metric(label='{kc}', value=f\"{{df['{kc}'].sum():,.0f}}\")"
            )
        kpi_code = "\n".join(kpi_lines)

        kpi_section = (
            "# KPI metrics\ncols = st.columns("
            + str(len(kpi_cols))
            + ")\nfor i, col in enumerate(cols):\n    with col:\n"
        )
        for kc in kpi_cols:
            kpi_section += f"        st.metric(label='{kc}', value=f\"{{df['{kc}'].sum():,.0f}}\")\n"

        app_code = f'''"""Auto-generated Streamlit dashboard for {dashboard_title}."""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from pathlib import Path

try:
    import geopandas as gpd
except ImportError:
    gpd = None

st.set_page_config(page_title="{dashboard_title}", layout="wide")
st.title("{dashboard_title}")

df = pd.read_csv(r"{abs_path}")

# Sidebar filters
{filter_code}
{kpi_section}

# Charts
{chart_sections}
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
# generate_auto_profile — fast comprehensive EDA using pandas + plotly only
# ---------------------------------------------------------------------------


def generate_auto_profile(
    file_path: str,
    output_path: str = "",
    open_after: bool = True,
) -> dict:
    """Fast auto-profile: overview, distributions, correlations, outliers, insights. Opens HTML."""
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

        # Classify columns
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        cat_cols = [c for c in df.columns if df[c].dtype == "object"]
        datetime_cols = [
            c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])
        ]

        # --- Build HTML report ---
        html_parts = []
        html_parts.append("""<!DOCTYPE html>
<html><head><meta charset='utf-8'>
<title>Auto Profile Report</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
body{font-family:system-ui,sans-serif;background:#0d1117;color:#c9d1d9;padding:20px;margin:0}
h1{color:#58a6ff;border-bottom:2px solid #21262d;padding-bottom:10px}
h2{color:#58a6ff;border-bottom:1px solid #21262d;padding-bottom:5px;margin-top:30px}
h3{color:#8b949e;margin-top:20px}
table{border-collapse:collapse;width:100%;margin:10px 0;font-size:14px}
th,td{padding:8px 12px;text-align:left;border:1px solid #21262d}
th{background:#161b22;color:#58a6ff}
tr:nth-child(even){background:#0d1117}
tr:nth-child(odd){background:#161b22}
.card{background:#161b22;padding:15px;border-radius:8px;margin:10px 5px;display:inline-block;min-width:150px;text-align:center}
.card .num{font-size:28px;font-weight:bold;color:#58a6ff}
.card .label{font-size:12px;color:#8b949e}
.grid{display:flex;flex-wrap:wrap}
.warn{color:#f0883e}.good{color:#3fb950}.bad{color:#f85149}
.chart{margin:20px 0}
</style></head><body>""")

        # Title
        html_parts.append(f"<h1>Auto Profile: {path.name}</h1>")

        # Overview cards
        html_parts.append('<div class="grid">')
        html_parts.append(
            f'<div class="card"><div class="num">{rows:,}</div><div class="label">Rows</div></div>'
        )
        html_parts.append(
            f'<div class="card"><div class="num">{cols}</div><div class="label">Columns</div></div>'
        )
        html_parts.append(
            f'<div class="card"><div class="num">{len(numeric_cols)}</div><div class="label">Numeric</div></div>'
        )
        html_parts.append(
            f'<div class="card"><div class="num">{len(cat_cols)}</div><div class="label">Categorical</div></div>'
        )
        html_parts.append(
            f'<div class="card"><div class="num">{len(datetime_cols)}</div><div class="label">Datetime</div></div>'
        )
        total_nulls = int(df.isna().sum().sum())
        null_pct = round(total_nulls / (rows * cols) * 100, 1) if rows * cols > 0 else 0
        cls = "good" if null_pct < 5 else "warn" if null_pct < 20 else "bad"
        html_parts.append(
            f'<div class="card"><div class="num {cls}">{total_nulls:,}</div><div class="label">Nulls ({null_pct}%)</div></div>'
        )
        html_parts.append("</div>")

        # Column summary table
        html_parts.append("<h2>Column Summary</h2>")
        html_parts.append(
            "<table><tr><th>Column</th><th>Type</th><th>Non-Null</th><th>Null %</th><th>Unique</th><th>Mean</th><th>Std</th><th>Min</th><th>Max</th></tr>"
        )
        for c in df.columns:
            nn = int(df[c].notna().sum())
            nc = int(df[c].isna().sum())
            npct = round(nc / rows * 100, 1) if rows > 0 else 0
            uniq = int(df[c].nunique())
            dtype = _dtype_label(df[c])
            mean_v = std_v = min_v = max_v = ""
            if c in numeric_cols:
                mean_v = round(float(df[c].mean()), 2)
                std_v = round(float(df[c].std()), 2)
                min_v = round(float(df[c].min()), 2)
                max_v = round(float(df[c].max()), 2)
            html_parts.append(
                f"<tr><td>{c}</td><td>{dtype}</td><td>{nn:,}</td><td>{npct}%</td><td>{uniq}</td><td>{mean_v}</td><td>{std_v}</td><td>{min_v}</td><td>{max_v}</td></tr>"
            )
        html_parts.append("</table>")

        # Correlation heatmap
        if len(numeric_cols) >= 2:
            html_parts.append("<h2>Correlation Heatmap</h2>")
            html_parts.append('<div id="corr-heatmap" class="chart"></div>')
            corr = df[numeric_cols].corr()
            corr_html = f"""<script>
var z = {corr.values.tolist()};
var x = {corr.columns.tolist()};
var data = [{{z: z, x: x, y: x, type: 'heatmap', colorscale: 'RdBu', zmid: 0}}];
var layout = {{paper_bgcolor: '#0d1117', plot_bgcolor: '#0d1117', font: {{color: '#c9d1d9'}}, margin: {{l: 80, r: 20, t: 20, b: 80}}}};
Plotly.newPlot('corr-heatmap', data, layout, {{responsive: true}});
</script>"""
            html_parts.append(corr_html)

        # Distribution plots for numeric columns (max 8)
        plot_cols = numeric_cols[:8]
        if plot_cols:
            html_parts.append("<h2>Distributions</h2>")
            n = len(plot_cols)
            rows_grid = (n + 1) // 2
            html_parts.append(f'<div id="dist-plots" class="chart"></div>')
            fig = make_subplots(
                rows=rows_grid,
                cols=2,
                subplot_titles=plot_cols + [""] * (rows_grid * 2 - n),
            )
            for i, c in enumerate(plot_cols):
                r, col = divmod(i, 2)
                fig.add_trace(
                    go.Histogram(x=df[c].dropna(), nbinsx=30, name=c, showlegend=False),
                    row=r + 1,
                    col=col + 1,
                )
            fig.update_layout(
                height=250 * rows_grid,
                width=900,
                paper_bgcolor="#0d1117",
                plot_bgcolor="#0d1117",
                font=dict(color="#c9d1d9"),
                showlegend=False,
            )
            dist_html = fig.to_html(
                full_html=False, include_plotlyjs=False, div_id="dist-plots-inner"
            )
            # Replace the div_id
            dist_html = dist_html.replace("dist-plots-inner", "dist-plots")
            html_parts.append(dist_html)

        # Outlier analysis
        outlier_info = []
        for c in numeric_cols:
            q1, q3 = df[c].quantile(0.25), df[c].quantile(0.75)
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            count = int(((df[c] < lower) | (df[c] > upper)).sum())
            if count > 0:
                outlier_info.append(
                    {"column": c, "count": count, "pct": round(count / rows * 100, 1)}
                )

        if outlier_info:
            html_parts.append("<h2>Outlier Analysis (IQR)</h2>")
            html_parts.append(
                "<table><tr><th>Column</th><th>Outliers</th><th>%</th></tr>"
            )
            for o in sorted(outlier_info, key=lambda x: -x["count"]):
                cls = "warn" if o["pct"] < 10 else "bad"
                html_parts.append(
                    f"<tr><td>{o['column']}</td><td class='{cls}'>{o['count']:,}</td><td>{o['pct']}%</td></tr>"
                )
            html_parts.append("</table>")

        # Key insights
        html_parts.append("<h2>Key Insights</h2><ul>")
        # High null columns
        for c in df.columns:
            nc = int(df[c].isna().sum())
            if nc > 0:
                pct = round(nc / rows * 100, 1)
                if pct > 50:
                    html_parts.append(
                        f'<li class="bad"><b>{c}</b>: {pct}% null values — consider dropping</li>'
                    )
                elif pct > 5:
                    html_parts.append(
                        f'<li class="warn"><b>{c}</b>: {pct}% null values — consider imputation</li>'
                    )
        # High cardinality categoricals
        for c in cat_cols:
            uniq = df[c].nunique()
            if uniq > rows * 0.5 and uniq > 10:
                html_parts.append(
                    f'<li class="warn"><b>{c}</b>: high cardinality ({uniq} unique) — may be an ID column</li>'
                )
        # Strong correlations
        if len(numeric_cols) >= 2:
            corr = df[numeric_cols].corr()
            for i in range(len(numeric_cols)):
                for j in range(i + 1, len(numeric_cols)):
                    val = corr.iloc[i, j]
                    if abs(val) > 0.8:
                        html_parts.append(
                            f"<li><b>{numeric_cols[i]}</b> ↔ <b>{numeric_cols[j]}</b>: r={val:.3f} (strong {'positive' if val > 0 else 'negative'} correlation)</li>"
                        )
        # Skewed numeric columns
        for c in numeric_cols:
            skew = df[c].skew()
            if abs(skew) > 2:
                html_parts.append(
                    f'<li class="warn"><b>{c}</b>: highly skewed (skewness={skew:.2f})</li>'
                )
        html_parts.append("</ul>")

        html_parts.append("</body></html>")

        html_content = "\n".join(html_parts)

        if output_path:
            out = Path(output_path)
        else:
            out = path.parent / f"{path.stem}_auto_profile.html"

        out.write_text(html_content, encoding="utf-8")
        size_kb = round(out.stat().st_size / 1024)

        if open_after:
            _open_file(out)

        progress.append(
            ok(
                f"Auto profile saved",
                f"{out.name} ({size_kb:,} KB) — {rows:,} rows × {cols} columns",
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
            "outlier_columns": len(outlier_info),
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
# generate_auto_profile — fast comprehensive EDA using pandas + plotly only
# ---------------------------------------------------------------------------


def generate_auto_profile(
    file_path: str,
    output_path: str = "",
    open_after: bool = True,
) -> dict:
    """Fast auto-profile: overview, distributions, correlations, outliers, insights. Opens HTML."""
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

        # Classify columns
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        cat_cols = [c for c in df.columns if df[c].dtype == "object"]
        datetime_cols = [
            c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])
        ]

        # --- Build HTML report ---
        html_parts = []
        html_parts.append("""<!DOCTYPE html>
<html><head><meta charset='utf-8'>
<title>Auto Profile Report</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
body{font-family:system-ui,sans-serif;background:#0d1117;color:#c9d1d9;padding:20px;margin:0}
h1{color:#58a6ff;border-bottom:2px solid #21262d;padding-bottom:10px}
h2{color:#58a6ff;border-bottom:1px solid #21262d;padding-bottom:5px;margin-top:30px}
h3{color:#8b949e;margin-top:20px}
table{border-collapse:collapse;width:100%;margin:10px 0;font-size:14px}
th,td{padding:8px 12px;text-align:left;border:1px solid #21262d}
th{background:#161b22;color:#58a6ff}
tr:nth-child(even){background:#0d1117}
tr:nth-child(odd){background:#161b22}
.card{background:#161b22;padding:15px;border-radius:8px;margin:10px 5px;display:inline-block;min-width:150px;text-align:center}
.card .num{font-size:28px;font-weight:bold;color:#58a6ff}
.card .label{font-size:12px;color:#8b949e}
.grid{display:flex;flex-wrap:wrap}
.warn{color:#f0883e}.good{color:#3fb950}.bad{color:#f85149}
.chart{margin:20px 0}
</style></head><body>""")

        # Title
        html_parts.append(f"<h1>Auto Profile: {path.name}</h1>")

        # Overview cards
        html_parts.append('<div class="grid">')
        html_parts.append(
            f'<div class="card"><div class="num">{rows:,}</div><div class="label">Rows</div></div>'
        )
        html_parts.append(
            f'<div class="card"><div class="num">{cols}</div><div class="label">Columns</div></div>'
        )
        html_parts.append(
            f'<div class="card"><div class="num">{len(numeric_cols)}</div><div class="label">Numeric</div></div>'
        )
        html_parts.append(
            f'<div class="card"><div class="num">{len(cat_cols)}</div><div class="label">Categorical</div></div>'
        )
        html_parts.append(
            f'<div class="card"><div class="num">{len(datetime_cols)}</div><div class="label">Datetime</div></div>'
        )
        total_nulls = int(df.isna().sum().sum())
        null_pct = round(total_nulls / (rows * cols) * 100, 1) if rows * cols > 0 else 0
        cls = "good" if null_pct < 5 else "warn" if null_pct < 20 else "bad"
        html_parts.append(
            f'<div class="card"><div class="num {cls}">{total_nulls:,}</div><div class="label">Nulls ({null_pct}%)</div></div>'
        )
        html_parts.append("</div>")

        # Column summary table
        html_parts.append("<h2>Column Summary</h2>")
        html_parts.append(
            "<table><tr><th>Column</th><th>Type</th><th>Non-Null</th><th>Null %</th><th>Unique</th><th>Mean</th><th>Std</th><th>Min</th><th>Max</th></tr>"
        )
        for c in df.columns:
            nn = int(df[c].notna().sum())
            nc = int(df[c].isna().sum())
            npct = round(nc / rows * 100, 1) if rows > 0 else 0
            uniq = int(df[c].nunique())
            dtype = _dtype_label(df[c])
            mean_v = std_v = min_v = max_v = ""
            if c in numeric_cols:
                mean_v = round(float(df[c].mean()), 2)
                std_v = round(float(df[c].std()), 2)
                min_v = round(float(df[c].min()), 2)
                max_v = round(float(df[c].max()), 2)
            html_parts.append(
                f"<tr><td>{c}</td><td>{dtype}</td><td>{nn:,}</td><td>{npct}%</td><td>{uniq}</td><td>{mean_v}</td><td>{std_v}</td><td>{min_v}</td><td>{max_v}</td></tr>"
            )
        html_parts.append("</table>")

        # Correlation heatmap
        if len(numeric_cols) >= 2:
            html_parts.append("<h2>Correlation Heatmap</h2>")
            html_parts.append('<div id="corr-heatmap" class="chart"></div>')
            corr = df[numeric_cols].corr()
            corr_html = f"""<script>
var z = {corr.values.tolist()};
var x = {corr.columns.tolist()};
var data = [{{z: z, x: x, y: x, type: 'heatmap', colorscale: 'RdBu', zmid: 0}}];
var layout = {{paper_bgcolor: '#0d1117', plot_bgcolor: '#0d1117', font: {{color: '#c9d1d9'}}, margin: {{l: 80, r: 20, t: 20, b: 80}}}};
Plotly.newPlot('corr-heatmap', data, layout, {{responsive: true}});
</script>"""
            html_parts.append(corr_html)

        # Distribution plots for numeric columns (max 8)
        plot_cols = numeric_cols[:8]
        if plot_cols:
            html_parts.append("<h2>Distributions</h2>")
            n = len(plot_cols)
            rows_grid = (n + 1) // 2
            html_parts.append(f'<div id="dist-plots" class="chart"></div>')
            fig = make_subplots(
                rows=rows_grid,
                cols=2,
                subplot_titles=plot_cols + [""] * (rows_grid * 2 - n),
            )
            for i, c in enumerate(plot_cols):
                r, col = divmod(i, 2)
                fig.add_trace(
                    go.Histogram(x=df[c].dropna(), nbinsx=30, name=c, showlegend=False),
                    row=r + 1,
                    col=col + 1,
                )
            fig.update_layout(
                height=250 * rows_grid,
                width=900,
                paper_bgcolor="#0d1117",
                plot_bgcolor="#0d1117",
                font=dict(color="#c9d1d9"),
                showlegend=False,
            )
            dist_html = fig.to_html(
                full_html=False, include_plotlyjs=False, div_id="dist-plots-inner"
            )
            # Replace the div_id
            dist_html = dist_html.replace("dist-plots-inner", "dist-plots")
            html_parts.append(dist_html)

        # Outlier analysis
        outlier_info = []
        for c in numeric_cols:
            q1, q3 = df[c].quantile(0.25), df[c].quantile(0.75)
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            count = int(((df[c] < lower) | (df[c] > upper)).sum())
            if count > 0:
                outlier_info.append(
                    {"column": c, "count": count, "pct": round(count / rows * 100, 1)}
                )

        if outlier_info:
            html_parts.append("<h2>Outlier Analysis (IQR)</h2>")
            html_parts.append(
                "<table><tr><th>Column</th><th>Outliers</th><th>%</th></tr>"
            )
            for o in sorted(outlier_info, key=lambda x: -x["count"]):
                cls = "warn" if o["pct"] < 10 else "bad"
                html_parts.append(
                    f"<tr><td>{o['column']}</td><td class='{cls}'>{o['count']:,}</td><td>{o['pct']}%</td></tr>"
                )
            html_parts.append("</table>")

        # Key insights
        html_parts.append("<h2>Key Insights</h2><ul>")
        # High null columns
        for c in df.columns:
            nc = int(df[c].isna().sum())
            if nc > 0:
                pct = round(nc / rows * 100, 1)
                if pct > 50:
                    html_parts.append(
                        f'<li class="bad"><b>{c}</b>: {pct}% null values — consider dropping</li>'
                    )
                elif pct > 5:
                    html_parts.append(
                        f'<li class="warn"><b>{c}</b>: {pct}% null values — consider imputation</li>'
                    )
        # High cardinality categoricals
        for c in cat_cols:
            uniq = df[c].nunique()
            if uniq > rows * 0.5 and uniq > 10:
                html_parts.append(
                    f'<li class="warn"><b>{c}</b>: high cardinality ({uniq} unique) — may be an ID column</li>'
                )
        # Strong correlations
        if len(numeric_cols) >= 2:
            corr = df[numeric_cols].corr()
            for i in range(len(numeric_cols)):
                for j in range(i + 1, len(numeric_cols)):
                    val = corr.iloc[i, j]
                    if abs(val) > 0.8:
                        html_parts.append(
                            f"<li><b>{numeric_cols[i]}</b> ↔ <b>{numeric_cols[j]}</b>: r={val:.3f} (strong {'positive' if val > 0 else 'negative'} correlation)</li>"
                        )
        # Skewed numeric columns
        for c in numeric_cols:
            skew = df[c].skew()
            if abs(skew) > 2:
                html_parts.append(
                    f'<li class="warn"><b>{c}</b>: highly skewed (skewness={skew:.2f})</li>'
                )
        html_parts.append("</ul>")

        html_parts.append("</body></html>")

        html_content = "\n".join(html_parts)

        if output_path:
            out = Path(output_path)
        else:
            out = path.parent / f"{path.stem}_auto_profile.html"

        out.write_text(html_content, encoding="utf-8")
        size_kb = round(out.stat().st_size / 1024)

        if open_after:
            _open_file(out)

        progress.append(
            ok(
                f"Auto profile saved",
                f"{out.name} ({size_kb:,} KB) — {rows:,} rows × {cols} columns",
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
            "outlier_columns": len(outlier_info),
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
