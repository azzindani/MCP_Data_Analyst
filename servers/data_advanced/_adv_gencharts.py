"""generate_chart, generate_geo_map, generate_3d_chart. No MCP imports."""

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
    _detect_location_mode,
    _find_geo_cols,
    _read_csv,
    _save_chart,
    _token_estimate,
    calc_chart_height,
    fail,
    info,
    is_numeric_col,
    ok,
    plotly_template,
)

from shared.file_utils import resolve_path

logger = logging.getLogger(__name__)

_VALID_CHART_TYPES = {
    "bar",
    "pie",
    "line",
    "scatter",
    "geo",
    "treemap",
    "time_series",
    "radius",
    "sunburst",
    "waterfall",
    "funnel",
    "parallel_coords",
    "sankey",
}


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

        if chart_type not in _VALID_CHART_TYPES:
            return {
                "success": False,
                "error": f"Invalid chart_type: {chart_type}",
                "hint": f"Valid types: {', '.join(sorted(_VALID_CHART_TYPES))}",
                "progress": [fail("Invalid chart type", chart_type)],
                "token_estimate": 30,
            }

        df = _read_csv(str(path))
        tmpl = plotly_template(theme)
        chart_title = title if title else f"{agg_func} of {value_column}"
        if category_column and chart_type not in (
            "treemap",
            "sunburst",
            "parallel_coords",
        ):
            chart_title += f" by {category_column}"

        # Validate required params per type
        if chart_type == "geo":
            if not geo_file_path or not geo_join_column:
                return {
                    "success": False,
                    "error": "geo chart requires geo_file_path and geo_join_column",
                    "hint": "Provide both geo_file_path and geo_join_column.",
                    "progress": [fail("Missing params", "geo_file_path, geo_join_column")],
                    "token_estimate": 30,
                }
        if chart_type in ("treemap", "sunburst") and not hierarchy_columns:
            return {
                "success": False,
                "error": f"{chart_type} requires hierarchy_columns",
                "hint": "Provide hierarchy_columns list.",
                "progress": [fail("Missing params", "hierarchy_columns")],
                "token_estimate": 30,
            }
        if chart_type == "time_series" and not date_column:
            return {
                "success": False,
                "error": "time_series requires date_column",
                "hint": "Provide date_column parameter.",
                "progress": [fail("Missing params", "date_column")],
                "token_estimate": 30,
            }
        if chart_type == "sankey" and not color_column:
            return {
                "success": False,
                "error": "sankey requires color_column (target column)",
                "hint": "Provide color_column as the target node column.",
                "progress": [fail("Missing params", "color_column")],
                "token_estimate": 30,
            }

        # Build chart_df
        if chart_type in ("bar", "pie", "line", "scatter"):
            if category_column:
                grouped = df.groupby(category_column, as_index=False)[value_column].agg(agg_func)
                grouped = grouped.sort_values(by=value_column, ascending=False)
                chart_df = grouped
            else:
                chart_df = df
        elif chart_type == "time_series":
            df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
            df = df.dropna(subset=[date_column])
            df["period"] = df[date_column].dt.to_period(period).astype(str)
            chart_df = df.groupby("period", as_index=False)[value_column].agg(agg_func)
        elif chart_type in ("treemap", "sunburst", "radius"):
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
            grouped = df.groupby(category_column, as_index=False)[value_column].agg(agg_func)
            chart_df = gdf.merge(grouped, left_on=geo_join_column, right_on=category_column, how="left")
        elif chart_type in ("waterfall", "funnel"):
            if category_column:
                chart_df = df.groupby(category_column, as_index=False)[value_column].agg(agg_func)
                chart_df = chart_df.sort_values(by=value_column, ascending=False)
            else:
                chart_df = df
        elif chart_type == "parallel_coords":
            chart_df = df
        elif chart_type == "sankey":
            chart_df = df
        else:
            chart_df = df

        # Generate figure
        fig = _dispatch_chart(
            chart_type,
            chart_df,
            df,
            value_column,
            category_column,
            color_column,
            date_column,
            hierarchy_columns,
            chart_title,
            tmpl,
            go,
            px,
        )

        if fig is None:
            return {
                "success": False,
                "error": f"Failed to create {chart_type} chart",
                "hint": "Check column names and chart type.",
                "progress": [fail("Chart creation failed", chart_type)],
                "token_estimate": 20,
            }

        fig.update_layout(
            margin=dict(l=20, r=20, t=40, b=20),
            height=calc_chart_height(1, mode="subplot"),
        )
        rows_plotted = len(chart_df)

        abs_p, fname = _save_chart(fig, output_path, chart_type, path, open_after, theme)
        progress.append(ok("Chart saved", f"{fname} ({rows_plotted} rows)"))

        result = {
            "success": True,
            "op": "generate_chart",
            "file_path": str(path),
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


def _dispatch_chart(
    chart_type,
    chart_df,
    df,
    value_column,
    category_column,
    color_column,
    date_column,
    hierarchy_columns,
    chart_title,
    tmpl,
    go,
    px,
):
    """Build and return a plotly Figure for the given chart_type."""
    if chart_type == "bar":
        return px.bar(
            chart_df,
            x=category_column,
            y=value_column,
            title=chart_title,
            template=tmpl,
            color=color_column if color_column else None,
        )
    if chart_type == "pie":
        return px.pie(
            chart_df,
            names=category_column,
            values=value_column,
            title=chart_title,
            template=tmpl,
            hole=0.5,
        )
    if chart_type == "line":
        return px.line(
            chart_df,
            x=category_column,
            y=value_column,
            title=chart_title,
            template=tmpl,
            color=color_column if color_column else None,
        )
    if chart_type == "scatter":
        return px.scatter(
            chart_df,
            x=category_column,
            y=value_column,
            title=chart_title,
            template=tmpl,
            color=color_column if color_column else None,
        )
    if chart_type == "geo":
        return px.choropleth_mapbox(
            chart_df,
            geojson=chart_df.geometry,
            locations=chart_df.index,
            color=value_column,
            title=chart_title,
            template=tmpl,
            mapbox_style="carto-positron",
            center={"lat": 37.09, "lon": -73.94},
            zoom=3,
        )
    if chart_type == "treemap":
        return px.treemap(
            chart_df,
            path=hierarchy_columns,
            values=value_column,
            title=chart_title,
            template=tmpl,
        )
    if chart_type == "time_series":
        fig = px.line(
            chart_df,
            x="period",
            y=value_column,
            title=chart_title,
            template=tmpl,
            markers=True,
        )
        fig.update_xaxes(title_text="Period")
        return fig
    if chart_type == "radius":
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
            template=tmpl,
        )
        return fig
    if chart_type == "sunburst":
        return px.sunburst(
            chart_df,
            path=hierarchy_columns,
            values=value_column,
            title=chart_title,
            template=tmpl,
        )
    if chart_type == "waterfall":
        labels = chart_df[category_column].tolist() if category_column else list(range(len(chart_df)))
        values = chart_df[value_column].tolist()
        fig = go.Figure(
            go.Waterfall(
                x=labels,
                y=values,
                measure=["relative"] * len(values),
                connector={"line": {"color": "rgb(63,63,63)"}},
            )
        )
        fig.update_layout(title=chart_title, template=tmpl)
        return fig
    if chart_type == "funnel":
        labels = chart_df[category_column].tolist() if category_column else list(range(len(chart_df)))
        values = chart_df[value_column].tolist()
        return px.funnel(
            chart_df,
            x=value_column,
            y=category_column,
            title=chart_title,
            template=tmpl,
        )
    if chart_type == "parallel_coords":
        numeric_cols = [c for c in df.columns if is_numeric_col(df[c])][:10]
        color_col = value_column if value_column in numeric_cols else (numeric_cols[0] if numeric_cols else None)
        return px.parallel_coordinates(
            df[numeric_cols].dropna(),
            color=color_col,
            template=tmpl,
            title=chart_title,
        )
    if chart_type == "sankey":
        return _build_sankey(df, category_column, color_column, value_column, chart_title, tmpl, go)
    return None


def _build_sankey(df, source_col, target_col, value_col, chart_title, tmpl, go):
    """Build a Sankey figure."""
    grouped = df.groupby([source_col, target_col], as_index=False)[value_col].sum()
    all_nodes = list(set(grouped[source_col].tolist() + grouped[target_col].tolist()))
    node_idx = {n: i for i, n in enumerate(all_nodes)}
    src_idx = [node_idx[v] for v in grouped[source_col]]
    tgt_idx = [node_idx[v] for v in grouped[target_col]]
    vals = grouped[value_col].tolist()
    fig = go.Figure(
        go.Sankey(
            node=dict(label=all_nodes, pad=15, thickness=20),
            link=dict(source=src_idx, target=tgt_idx, value=vals),
        )
    )
    fig.update_layout(title=chart_title, template=tmpl)
    return fig


def generate_geo_map(
    file_path: str,
    lat_column: str = "",
    lon_column: str = "",
    location_column: str = "",
    value_column: str = "",
    location_mode: str = "",
    color_column: str = "",
    title: str = "",
    output_path: str = "",
    theme: str = "dark",
    open_after: bool = True,
) -> dict:
    """Geo map: scatter (lat/lon) or choropleth (country/state). Auto-detects columns."""
    progress = []
    try:
        try:
            import plotly.express as px  # noqa: F401
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

        auto_lat, auto_lon, auto_loc = _find_geo_cols(df)
        lat_col = lat_column or auto_lat
        lon_col = lon_column or auto_lon
        loc_col = location_column or auto_loc

        use_scatter = bool(lat_col and lon_col)
        use_choro = bool(loc_col and not use_scatter)

        if not use_scatter and not use_choro:
            avail = list(df.columns[:10])
            return {
                "success": False,
                "error": "No geographic columns detected.",
                "hint": (
                    "Provide lat_column+lon_column for a scatter map, or "
                    "location_column for a choropleth. "
                    f"Available columns: {avail}"
                ),
                "progress": [fail("No geo columns", str(avail))],
                "token_estimate": 30,
            }

        if value_column and value_column not in df.columns:
            return {
                "success": False,
                "error": f"value_column '{value_column}' not found.",
                "hint": f"Available numeric columns: {[c for c in df.columns if is_numeric_col(df[c])]}",
                "progress": [fail("Column not found", value_column)],
                "token_estimate": 20,
            }

        tmpl = plotly_template(theme)
        chart_title = title

        if use_scatter:
            for col, label in ((lat_col, "lat"), (lon_col, "lon")):
                if col not in df.columns:
                    return {
                        "success": False,
                        "error": f"{label} column '{col}' not found.",
                        "hint": f"Available columns: {list(df.columns[:15])}",
                        "progress": [fail("Column not found", col)],
                        "token_estimate": 20,
                    }

            plot_df = df[[lat_col, lon_col]].copy()
            for c in (lat_col, lon_col):
                plot_df[c] = pd.to_numeric(plot_df[c], errors="coerce")
            plot_df = plot_df.dropna(subset=[lat_col, lon_col])

            if value_column:
                plot_df[value_column] = pd.to_numeric(df.loc[plot_df.index, value_column], errors="coerce")
            if color_column and color_column in df.columns:
                plot_df[color_column] = df.loc[plot_df.index, color_column].values

            if not chart_title:
                chart_title = f"Geographic Distribution — {path.stem}"

            fig = px.scatter_geo(
                plot_df,
                lat=lat_col,
                lon=lon_col,
                size=value_column if value_column else None,
                color=color_column if color_column else (value_column if value_column else None),
                title=chart_title,
                template=tmpl,
                projection="natural earth",
            )
            fig.update_traces(marker={"opacity": 0.75, "sizemin": 3})
            map_type = "scatter_geo"
            rows_plotted = len(plot_df)
            progress.append(info("Map type", f"scatter_geo ({rows_plotted:,} points)"))

        else:
            if loc_col not in df.columns:
                return {
                    "success": False,
                    "error": f"location_column '{loc_col}' not found.",
                    "hint": f"Available columns: {list(df.columns[:15])}",
                    "progress": [fail("Column not found", loc_col)],
                    "token_estimate": 20,
                }

            _LOC_MODE_ALIASES = {
                "state": "USA-states",
                "usa-states": "USA-states",
                "usa_states": "USA-states",
                "iso3": "ISO-3",
                "iso-3": "ISO-3",
                "country": "country names",
                "country names": "country names",
                "countries": "country names",
            }
            loc_mode = _LOC_MODE_ALIASES.get(
                (location_mode or "").lower(), location_mode or None
            ) or _detect_location_mode(df, loc_col)

            if value_column:
                agg_col = value_column
                grouped = df.groupby(loc_col, as_index=False)[agg_col].sum()
            else:
                grouped = df.groupby(loc_col, as_index=False).size()
                grouped = grouped.rename(columns={"size": "_count"})
                agg_col = "_count"

            if not chart_title:
                nc_label = value_column or "Count"
                chart_title = f"{nc_label} by {loc_col} — {path.stem}"

            fig = px.choropleth(
                grouped,
                locations=loc_col,
                color=agg_col,
                locationmode=loc_mode,
                title=chart_title,
                template=tmpl,
                color_continuous_scale="YlOrRd",
            )
            fig.update_layout(geo={"showframe": False, "showcoastlines": True})
            map_type = f"choropleth ({loc_mode})"
            rows_plotted = len(grouped)
            progress.append(info("Map type", f"choropleth, mode={loc_mode}, {rows_plotted} locations"))

        fig.update_layout(
            margin={"l": 0, "r": 0, "t": 40, "b": 0},
            autosize=True,
            height=calc_chart_height(1, mode="subplot"),
        )
        abs_p, fname = _save_chart(fig, output_path, "geo_map", path, open_after, theme)
        progress.append(ok("Map saved", fname))

        result = {
            "success": True,
            "op": "generate_geo_map",
            "file_path": str(path),
            "map_type": map_type,
            "output_path": abs_p,
            "output_name": fname,
            "rows_plotted": rows_plotted,
            "lat_column": lat_col,
            "lon_column": lon_col,
            "location_column": loc_col,
            "value_column": value_column,
            "color_column": color_column,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_geo_map error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path, column names, and that columns contain valid geo data.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }


def generate_3d_chart(
    file_path: str,
    chart_type: str,
    x_column: str,
    y_column: str,
    z_column: str,
    color_column: str = "",
    title: str = "",
    output_path: str = "",
    theme: str = "dark",
    open_after: bool = True,
) -> dict:
    """3D scatter or surface chart. type: scatter_3d surface. Opens HTML."""
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

        valid_3d = {"scatter_3d", "surface"}
        if chart_type not in valid_3d:
            return {
                "success": False,
                "error": f"Invalid chart_type: {chart_type}",
                "hint": f"Valid types: {', '.join(sorted(valid_3d))}",
                "progress": [fail("Invalid chart type", chart_type)],
                "token_estimate": 30,
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

        for col in (x_column, y_column, z_column):
            if col not in df.columns:
                return {
                    "success": False,
                    "error": f"Column '{col}' not found.",
                    "hint": f"Available columns: {list(df.columns[:20])}",
                    "progress": [fail("Column not found", col)],
                    "token_estimate": 20,
                }

        tmpl = plotly_template(theme)
        chart_title = (
            title if title else f"3D {chart_type.replace('_', ' ').title()}: {x_column} × {y_column} × {z_column}"
        )

        if chart_type == "scatter_3d":
            plot_df = df
            if len(plot_df) > 5000:
                plot_df = plot_df.sample(5000, random_state=42)
            fig = px.scatter_3d(
                plot_df,
                x=x_column,
                y=y_column,
                z=z_column,
                color=color_column if color_column and color_column in df.columns else None,
                title=chart_title,
                template=tmpl,
            )
            rows_plotted = len(plot_df)

        else:  # surface
            for col in (x_column, y_column, z_column):
                if not pd.api.types.is_numeric_dtype(df[col]):
                    return {
                        "success": False,
                        "error": f"surface chart requires numeric columns; '{col}' is not numeric.",
                        "hint": "All three columns (x, y, z) must be numeric for surface chart.",
                        "progress": [fail("Non-numeric column", col)],
                        "token_estimate": 20,
                    }
            try:
                grid = df.pivot_table(
                    index=y_column,
                    columns=x_column,
                    values=z_column,
                    aggfunc="mean",
                )
            except Exception as e:
                return {
                    "success": False,
                    "error": f"Cannot build surface grid: {e}",
                    "hint": "Ensure x_column and y_column have discrete values suitable for a pivot.",
                    "progress": [fail("Pivot failed", str(e))],
                    "token_estimate": 20,
                }
            if grid.shape[0] > 100 or grid.shape[1] > 100:
                return {
                    "success": False,
                    "error": f"Surface grid too large: {grid.shape[0]}×{grid.shape[1]} (max 100×100).",
                    "hint": "Use columns with fewer unique values or pre-aggregate data.",
                    "progress": [fail("Grid too large", str(grid.shape))],
                    "token_estimate": 20,
                }
            fig = go.Figure(
                go.Surface(
                    z=grid.values,
                    x=grid.columns.tolist(),
                    y=grid.index.tolist(),
                    colorscale="Viridis",
                )
            )
            fig.update_layout(title=chart_title, template=tmpl)
            rows_plotted = len(df)

        fig.update_layout(
            margin=dict(l=20, r=20, t=40, b=20),
            height=calc_chart_height(1, mode="subplot"),
        )
        abs_p, fname = _save_chart(fig, output_path, chart_type, path, open_after, theme)
        progress.append(ok("3D chart saved", f"{fname} ({rows_plotted} rows)"))

        result = {
            "success": True,
            "op": "generate_3d_chart",
            "file_path": str(path),
            "chart_type": chart_type,
            "output_path": abs_p,
            "output_name": fname,
            "title": chart_title,
            "rows_plotted": rows_plotted,
            "x_column": x_column,
            "y_column": y_column,
            "z_column": z_column,
            "progress": progress,
        }
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_3d_chart error")
        return {
            "success": False,
            "error": str(exc),
            "hint": "Check file_path, column names, and chart_type.",
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
