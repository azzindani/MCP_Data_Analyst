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
            "hint": "Check file_path and column names.",
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
