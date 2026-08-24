"""Charts sub-module: distribution, correlation, pairwise, multi, export. No MCP imports."""

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
    _open_file,
    _read_csv,
    _save_chart,
    _token_estimate,
    calc_chart_height,
    fail,
    is_numeric_col,
    ok,
    plotly_template,
)

from shared.file_utils import embed_content, hint_for_error, resolve_path
from shared.progress import info, warn
from shared.small_sample import MIN_N_CORRELATION, MIN_N_IQR
from shared.version_control import snapshot_if_exists

logger = logging.getLogger(__name__)


def generate_distribution_plot(
    file_path: str,
    columns: list[str] = None,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "device",
    return_content: bool = False,
) -> dict:
    """Histogram + box plot for numeric columns. Opens HTML file."""
    progress = []
    try:
        try:
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
        numeric_cols = [c for c in df.columns if is_numeric_col(df[c])]

        if columns:
            cols_to_plot = [c for c in columns if c in numeric_cols]
        else:
            cols_to_plot = numeric_cols[:6]

        if not cols_to_plot:
            return {
                "success": False,
                "error": "No numeric columns found to plot",
                "hint": f"Available numeric columns: {', '.join(numeric_cols)}",
                "progress": [fail("No numeric columns", "")],
                "token_estimate": 20,
            }

        n = len(cols_to_plot)
        # make_subplots consumes subplot_titles row-major: (r1c1, r1c2, r2c1...).
        # Listing every histogram title and then every box-plot title handed that
        # row-major reader a column-major list, so each panel was captioned with
        # another panel's name -- "impressions — Histogram" sat over a box plot
        # of spends, and four of six panels in a three-column report were wrong.
        # With one column the two orders coincide, which is why nothing caught
        # it. Each row is one column's pair, so the titles pair up the same way.
        fig = make_subplots(
            rows=n,
            cols=2,
            subplot_titles=[title for c in cols_to_plot for title in (f"{c} — Histogram", f"{c} — Box Plot")],
            vertical_spacing=0.3 / n,
        )

        for i, c in enumerate(cols_to_plot):
            fig.add_trace(
                go.Histogram(x=df[c], nbinsx=30, name=c, showlegend=False),
                row=i + 1,
                col=1,
            )
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

        # A histogram of one value is one bar, and a box plot of one value draws
        # its quartiles and both whiskers on top of each other. Both render, and
        # both look like a distribution to anyone who does not read the axis --
        # so the count each panel was drawn from is worth saying out loud.
        value_counts = {c: int(df[c].notna().sum()) for c in cols_to_plot}
        thin = {c: k for c, k in value_counts.items() if k < MIN_N_IQR}
        if thin:
            progress.append(
                warn(
                    "Too few values to show a distribution",
                    ", ".join(f"{c}: {k} value(s)" for c, k in thin.items()),
                )
            )

        abs_p, fname = _save_chart(fig, output_path, "distributions", path, open_after, theme, progress)
        progress.append(ok("Distribution plots saved", f"{fname} — {n} columns"))

        result = {
            "success": True,
            "op": "generate_distribution_plot",
            "file_path": str(path),
            "output_path": abs_p,
            "output_name": fname,
            "columns_plotted": cols_to_plot,
            "values_plotted": value_counts,
            "columns_too_few_values": sorted(thin),
            "chart_count": n * 2,
            "progress": progress,
        }
        if thin:
            result["hint"] = (
                f"{len(thin)} of {n} column(s) have fewer than {MIN_N_IQR} values, so their box plots draw "
                "every quartile on the same line. The chart is accurate; it is not a distribution."
            )
        embed_content(result, Path(abs_p), return_content)
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_distribution_plot error")
        return {
            "success": False,
            "error": str(exc),
            "hint": hint_for_error(exc, "Check file_path is absolute and the file is a valid CSV."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


def generate_correlation_heatmap(
    file_path: str,
    method: str = "pearson",
    output_path: str = "",
    open_after: bool = True,
    theme: str = "device",
    return_content: bool = False,
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
        numeric_cols = [c for c in df.columns if is_numeric_col(df[c])]

        if len(numeric_cols) < 2:
            return {
                "success": False,
                "error": "Need at least 2 numeric columns",
                "hint": f"Only found {len(numeric_cols)} numeric columns",
                "progress": [fail("Insufficient numeric columns", "")],
                "token_estimate": 20,
            }

        corr = df[numeric_cols].corr(method=method)
        # Pin the scale to the full correlation range. Auto-ranging over the
        # observed values paints the weakest positive correlation in the palette's
        # "strong negative" colour, which reads as the opposite result.
        fig = px.imshow(
            corr,
            text_auto=".2f",
            color_continuous_scale="RdBu_r",
            zmin=-1,
            zmax=1,
            color_continuous_midpoint=0,
            title=f"Correlation Matrix ({method})",
            aspect="auto",
        )
        fig.update_layout(
            template=plotly_template(theme),
            height=300 + 50 * len(numeric_cols),
        )

        # A chart that depicts a relationship needs enough points to have one.
        # Below MIN_N_CORRELATION every pair lies on a line, and the rendered
        # grid comes out blank or zero-valued -- which reads as a measured
        # absence of correlation rather than as nothing having been measured.
        # generate_distribution_plot got this warning in the same round; its
        # two siblings in this file did not, and the re-run caught the gap.
        rows_used = int(len(df))
        if rows_used < MIN_N_CORRELATION:
            progress.append(
                warn("Too few rows for a correlation", f"{rows_used} row(s); every coefficient is undefined")
            )

        abs_p, fname = _save_chart(fig, output_path, "correlation_heatmap", path, open_after, theme, progress)
        progress.append(ok("Correlation heatmap saved", f"{fname} — {len(numeric_cols)} columns"))

        result = {
            "success": True,
            "op": "generate_correlation_heatmap",
            "file_path": str(path),
            "output_path": abs_p,
            "output_name": fname,
            "columns": numeric_cols,
            "method": method,
            "rows_used": rows_used,
            "progress": progress,
        }
        if rows_used < MIN_N_CORRELATION:
            result["hint"] = (
                f"Drawn from {rows_used} row(s). A correlation needs at least {MIN_N_CORRELATION} pairs to "
                "mean anything, so every cell in this grid is undefined -- the chart is not showing weak "
                "correlation, it is showing none that could be computed."
            )
        embed_content(result, Path(abs_p), return_content)
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_correlation_heatmap error")
        return {
            "success": False,
            "error": str(exc),
            "hint": hint_for_error(exc, "Check file_path is absolute and the file is a valid CSV."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


def generate_pairwise_plot(
    file_path: str,
    columns: list[str] = None,
    max_cols: int = 6,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "device",
    return_content: bool = False,
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
        numeric_cols = [c for c in df.columns if is_numeric_col(df[c])]

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
        fig.update_layout(
            autosize=True,
            height=calc_chart_height(len(cols_to_plot), mode="subplot"),
        )

        rows_used = int(len(df))
        if rows_used < MIN_N_CORRELATION:
            progress.append(
                warn("Too few rows for a pairwise plot", f"{rows_used} row(s); no panel can show a relationship")
            )

        abs_p, fname = _save_chart(fig, output_path, "pairwise", path, open_after, theme, progress)
        progress.append(ok("Pairwise plot saved", f"{fname} — {len(cols_to_plot)} columns"))

        result = {
            "success": True,
            "op": "generate_pairwise_plot",
            "file_path": str(path),
            "output_path": abs_p,
            "output_name": fname,
            "columns_plotted": cols_to_plot,
            "rows_used": rows_used,
            "progress": progress,
        }
        if rows_used < MIN_N_CORRELATION:
            result["hint"] = (
                f"Drawn from {rows_used} row(s). Any two points lie on a line, so no panel here shows a "
                f"relationship; {MIN_N_CORRELATION}+ rows are needed before the scatter means anything."
            )
        embed_content(result, Path(abs_p), return_content)
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_pairwise_plot error")
        return {
            "success": False,
            "error": str(exc),
            "hint": hint_for_error(exc, "Check file_path and column names."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


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
    theme: str = "device",
    return_content: bool = False,
) -> dict:
    """Multi-metric bar/line chart. bar needs category_column, line needs date_column."""
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

        chart_type = {"bar": "multi_bar", "line": "multi_line"}.get(chart_type, chart_type)
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

        missing_vals = [c for c in value_columns if c not in df.columns]
        if missing_vals:
            return {
                "success": False,
                "error": f"value_columns not found: {missing_vals}",
                "hint": f"Available columns: {list(df.columns)}",
                "progress": [fail("Column not found", str(missing_vals))],
                "token_estimate": 30,
            }
        if category_column and category_column not in df.columns:
            return {
                "success": False,
                "error": f"category_column '{category_column}' not found.",
                "hint": f"Available columns: {list(df.columns)}",
                "progress": [fail("Column not found", category_column)],
                "token_estimate": 30,
            }

        if chart_type == "multi_line" and not date_column:
            return {
                "success": False,
                "error": "multi_line requires date_column",
                "hint": "Provide date_column for time-based multi-line chart.",
                "progress": [fail("Missing param", "date_column")],
                "token_estimate": 30,
            }

        # multi_line has needed its date_column since it was written; multi_bar
        # had no equivalent guard and fell through to x_vals = range(len(df)).
        # plotly refuses a range object -- "Invalid value of type
        # 'builtins.range' received for the 'x' property of bar" -- so calling
        # this tool with exactly the three arguments its schema marks required
        # failed, under the hint "Check file_path, column names, and chart_type",
        # which names the three that were already correct. Even if plotly took
        # it, one bar per row is not a comparison of metrics: the categories are
        # what the metrics get compared across. generate_chart was given the
        # same guard for the same reason.
        if chart_type == "multi_bar" and not category_column:
            suggestion = next((c for c in df.columns if not is_numeric_col(df[c])), "category")
            return {
                "success": False,
                "error": "multi_bar requires category_column",
                "hint": (
                    f"Name the column to compare the metrics across, e.g. category_column='{suggestion}'. "
                    f"Call inspect_dataset('{path.name}') to list the columns."
                ),
                "progress": [fail("Missing params", "category_column")],
                "token_estimate": 30,
            }

        chart_title = title if title else f"Multi-{chart_type.replace('_', ' ').title()}"
        fig = go.Figure()

        if chart_type == "multi_bar":
            if category_column:
                grouped = df.groupby(category_column)[value_columns].agg(agg_func).reset_index()
                x_vals = grouped[category_column]
            else:
                x_vals = range(len(df))
                grouped = df
            for vc in value_columns:
                fig.add_trace(go.Bar(x=x_vals, y=grouped[vc], name=vc))
        elif chart_type == "multi_line":
            df[date_column] = pd.to_datetime(df[date_column], format="mixed", dayfirst=False, errors="coerce")
            df = df.dropna(subset=[date_column])
            df["period"] = df[date_column].dt.to_period("M").astype(str)
            grouped = df.groupby("period")[value_columns].agg(agg_func).reset_index()
            x_vals = grouped["period"]
            for vc in value_columns:
                fig.add_trace(go.Scatter(x=x_vals, y=grouped[vc], name=vc, mode="lines+markers"))

        fig.update_layout(
            title=chart_title,
            template=plotly_template(theme),
            xaxis_title=category_column or "Period",
            yaxis_title=agg_func.title(),
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
            margin=dict(l=20, r=20, t=40, b=20),
            height=calc_chart_height(len(value_columns), mode="subplot"),
        )

        abs_p, fname = _save_chart(fig, output_path, f"multi_{chart_type}", path, open_after, theme, progress)
        progress.append(ok("Multi-chart saved", f"{fname} - {len(value_columns)} metrics"))

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
        embed_content(result, Path(abs_p), return_content)
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_multi_chart error")
        return {
            "success": False,
            "error": str(exc),
            "hint": hint_for_error(exc, "Check file_path, column names, and chart_type."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


def export_data(
    file_path: str,
    output_path: str = "",
    format: str = "csv",
    encoding: str = "utf-8",
    separator: str = ",",
    open_after: bool = True,
    return_content: bool = False,
    output_format: str = "",
) -> dict:
    """Export dataset to CSV, Excel, or JSON format."""
    progress = []
    # convert_file, the other tool that changes a file's format, calls this
    # output_format.
    if output_format:
        format = output_format
        progress.append(info("Argument alias", "Read format from an accepted alternative spelling"))
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
            out = resolve_path(output_path)
        else:
            ext_map = {"csv": ".csv", "json": ".json", "excel": ".xlsx"}
            out = path.parent / f"{path.stem}_export{ext_map[format]}"

        # An export lands wherever the caller points it, including at another
        # dataset. Without this it overwrote one and recorded nothing:
        # export_data(file_path="d.csv", output_path="precious.csv") returned
        # success: true with .mcp_versions empty.
        snapshot_if_exists(out)

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
            if open_after:
                _open_file(out)

        size_kb = round(out.stat().st_size / 1024)
        progress.append(ok("Data exported", f"{out.name} ({size_kb:,} KB, {len(df)} rows)"))

        result = {
            "success": True,
            "op": "export_data",
            "output_path": str(out),
            "output_name": out.name,
            "format": format,
            "rows": len(df),
            "columns": len(df.columns),
            "file_size_kb": size_kb,
            "progress": progress,
        }
        embed_content(result, out, return_content)
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("export_data error")
        return {
            "success": False,
            "error": str(exc),
            "hint": hint_for_error(exc, "Check file_path and format."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
