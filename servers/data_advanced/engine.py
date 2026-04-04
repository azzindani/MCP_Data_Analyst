"""Tier 3 engine — profiling reports, charts, dashboards. Zero MCP imports."""

from __future__ import annotations

import contextlib
import io
import logging
import sys
import textwrap
from pathlib import Path

import pandas as pd

# Shared utilities
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
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


def _dtype_label(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "int64"
    if pd.api.types.is_float_dtype(series):
        return "float64"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime64"
    return "object"


# ---------------------------------------------------------------------------
# generate_profile_report
# ---------------------------------------------------------------------------


def generate_profile_report(
    file_path: str,
    output_path: str = "",
    title: str = "",
    description: str = "",
    correlations: bool = True,
    minimal: bool = False,
) -> dict:
    progress = []
    try:
        try:
            from ydata_profiling import ProfileReport
        except ImportError:
            return {
                "success": False,
                "error": "ydata-profiling not installed",
                "hint": "Install: uv add ydata-profiling",
                "progress": [fail("Missing dependency", "ydata-profiling")],
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

        # Memory check
        try:
            import psutil

            avail = psutil.virtual_memory().available / (1024**3)  # GB
            if avail < 2:
                progress.append(
                    warn(
                        "Low memory",
                        f"Only {avail:.1f} GB available; consider minimal=True",
                    )
                )
                minimal = True
        except ImportError:
            pass

        df = _read_csv(str(path))
        report_title = title if title else path.stem

        # Auto-sample large datasets to avoid timeout
        DEFAULT_MAX_ROWS = 5000
        if len(df) > DEFAULT_MAX_ROWS:
            df = df.sample(n=DEFAULT_MAX_ROWS, random_state=42)
            progress.append(
                warn(
                    "Large dataset sampled",
                    f"Analyzing {DEFAULT_MAX_ROWS} of {len(_read_csv(str(path)))} rows",
                )
            )

        # Build config for ydata-profiling
        config_kwargs = {"title": report_title, "minimal": minimal}
        if correlations:
            config_kwargs["correlations"] = {
                "pearson": {"calculate": True},
                "spearman": {"calculate": True},
                "kendall": {"calculate": True},
                "phi_k": {"calculate": True},
                "cramers": {"calculate": True},
            }

        # Redirect ydata stdout to stderr
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            report = ProfileReport(df, **config_kwargs)

            if output_path:
                out = Path(output_path)
            else:
                out = path.parent / f"{path.stem}_profile.html"

            report.to_file(str(out))

        size_kb = round(out.stat().st_size / 1024)
        progress.append(ok(f"Profile report saved", f"{out.name} ({size_kb:,} KB)"))

        result = {
            "success": True,
            "op": "generate_profile_report",
            "report_path": out.name,
            "report_size_kb": size_kb,
            "columns_profiled": len(df.columns),
            "rows": len(df),
            "correlations_included": correlations,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_profile_report error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# generate_sweetviz_report
# ---------------------------------------------------------------------------


def generate_sweetviz_report(
    file_path: str,
    output_path: str = "",
    target_column: str = "",
) -> dict:
    progress = []
    try:
        try:
            import numpy as np

            # Patch numpy 2.x compatibility for sweetviz
            if not hasattr(np, "VisibleDeprecationWarning"):
                np.VisibleDeprecationWarning = np.exceptions.VisibleDeprecationWarning
            import sweetviz as sv
        except ImportError:
            return {
                "success": False,
                "error": "sweetviz not installed",
                "hint": "Install: uv add sweetviz",
                "progress": [fail("Missing dependency", "sweetviz")],
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

        # Auto-sample large datasets to avoid timeout
        DEFAULT_MAX_ROWS = 5000
        if len(df) > DEFAULT_MAX_ROWS:
            df = df.sample(n=DEFAULT_MAX_ROWS, random_state=42)
            progress.append(
                warn(
                    "Large dataset sampled",
                    f"Analyzing {DEFAULT_MAX_ROWS} of {len(_read_csv(str(path)))} rows",
                )
            )

        if target_column and target_column not in df.columns:
            return {
                "success": False,
                "error": f"Target column not found: {target_column}",
                "hint": f"Available: {', '.join(df.columns)}",
                "progress": [fail("Column not found", target_column)],
                "token_estimate": 30,
            }

        kwargs = {}
        if target_column:
            kwargs["target_feat"] = target_column

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            with contextlib.redirect_stderr(buf):
                report = sv.analyze(df, **kwargs)

                if output_path:
                    out = Path(output_path)
                else:
                    out = path.parent / f"{path.stem}_sweetviz.html"

                report.show_html(
                    str(out),
                    open_browser=False,
                    layout="widescreen",
                )

        size_kb = round(out.stat().st_size / 1024)
        progress.append(ok(f"SweetViz report saved", f"{out.name} ({size_kb:,} KB)"))

        result = {
            "success": True,
            "op": "generate_sweetviz_report",
            "report_path": out.name,
            "report_size_kb": size_kb,
            "columns_analysed": len(df.columns),
            "target_column": target_column,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_sweetviz_report error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# generate_autoviz_report
# ---------------------------------------------------------------------------


def generate_autoviz_report(
    file_path: str,
    output_dir: str = "",
    chart_format: str = "html",
    max_rows_analyzed: int = 0,
) -> dict:
    progress = []
    try:
        try:
            from autoviz import AutoViz_Class
        except ImportError:
            return {
                "success": False,
                "error": "autoviz not installed",
                "hint": "Install: uv add autoviz",
                "progress": [fail("Missing dependency", "autoviz")],
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
        rows_analyzed = len(df)

        # Auto-sample large datasets to avoid timeout
        DEFAULT_MAX_ROWS = 5000
        effective_max = max_rows_analyzed if max_rows_analyzed > 0 else DEFAULT_MAX_ROWS
        if len(df) > effective_max:
            df = df.sample(n=effective_max, random_state=42)
            rows_analyzed = effective_max
            progress.append(
                warn(
                    "Large dataset sampled",
                    f"Analyzing {effective_max} of {len(_read_csv(str(path)))} rows",
                )
            )

        if output_dir:
            out = Path(output_dir)
        else:
            out = path.parent / "autoviz_output"
        out.mkdir(parents=True, exist_ok=True)

        # AutoViz needs a file path, not a DataFrame
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            with contextlib.redirect_stderr(buf):
                try:
                    av = AutoViz_Class().AutoViz(
                        str(path),
                        chart_format=chart_format,
                        depVar="",
                        verbose=0,
                    )
                except Exception:
                    if chart_format != "html":
                        progress.append(
                            warn(
                                f"{chart_format} failed", "Falling back to html format"
                            )
                        )
                        av = AutoViz_Class().AutoViz(
                            str(path),
                            chart_format="html",
                            depVar="",
                            verbose=0,
                        )
                    else:
                        raise

        chart_files = [f.name for f in out.iterdir() if f.is_file()]
        progress.append(
            ok(f"AutoViz charts saved", f"{len(chart_files)} files in {out.name}/")
        )

        result = {
            "success": True,
            "op": "generate_autoviz_report",
            "output_dir": out.name,
            "chart_files": chart_files,
            "chart_count": len(chart_files),
            "rows_analyzed": rows_analyzed,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_autoviz_report error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path is absolute and the file is a valid CSV.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


# ---------------------------------------------------------------------------
# generate_chart
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
) -> dict:
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

        progress.append(ok(f"Chart saved", f"{out.name} ({rows_plotted} rows)"))

        result = {
            "success": True,
            "op": "generate_chart",
            "chart_type": chart_type,
            "output_path": out.name,
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
# generate_dashboard
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
