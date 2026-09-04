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
    BASEMAP_NOTE,
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
    remote_basemap_traces,
    warn,
)

from shared.column_utils import date_note, parse_dates
from shared.file_utils import embed_content, error_text, hint_for_error, resolve_path
from shared.geo_names import unrecognised_locations
from shared.html_layout import discriminated_suffix

logger = logging.getLogger(__name__)

# Plotly's locationmode strings do not read as English in a sentence.
_LOC_MODE_LABELS = {
    "country names": "country name",
    "ISO-3": "ISO-3 country code",
    "USA-states": "US state code",
}

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

# Types that index category_column unconditionally -- px.bar(x=...), px.pie(names=...),
# px.funnel(y=...), Scatterpolar(theta=...), and geo's groupby. The schema gives
# category_column a "" default, so a caller who supplies only the three required
# arguments reaches plotly with x="" and gets ValueError: Value of 'x' is not the
# name of a column in 'data_frame' -- naming a parameter this tool does not have.
# radius was worse: chart_df[""] raises KeyError(""), whose str() is "''", so the
# response carried an empty error string. Guarded here instead, in the same block
# that already covers geo, treemap, time_series and sankey.
_NEEDS_CATEGORY = ("bar", "pie", "line", "scatter", "funnel", "radius", "geo")

# These read category_column as one level of a hierarchy or as one axis among
# several, so "by <column>" would not describe what the chart shows.
_CATEGORY_IS_NOT_THE_BREAKDOWN = ("treemap", "sunburst", "parallel_coords")


def _coords_out_of_range(frame: pd.DataFrame, lat_column: str, lon_column: str) -> dict | None:
    """Return details of the first column holding values no coordinate can take."""
    for column, kind, low, high in (
        (lat_column, "latitude", -90.0, 90.0),
        (lon_column, "longitude", -180.0, 180.0),
    ):
        values = frame[column]
        if values.empty:
            continue
        bad = int(((values < low) | (values > high)).sum())
        if bad:
            return {
                "column": column,
                "kind": kind,
                "count": bad,
                "low": low,
                "high": high,
                "min": float(values.min()),
                "max": float(values.max()),
            }
    return None


def _sort_along_x(frame: pd.DataFrame, x_column: str) -> pd.DataFrame:
    """Order rows along the x axis for line charts.

    A line joins its points in row order, so the value-descending sort that is
    right for bars turns a trend into a zigzag. Dates are parsed before sorting
    so 2019-11-02 follows 2019-11-01 rather than sorting as text.
    """
    if not x_column or x_column not in frame.columns:
        return frame
    column = frame[x_column]
    if pd.api.types.is_numeric_dtype(column) or pd.api.types.is_datetime64_any_dtype(column):
        return frame.sort_values(by=x_column)
    parsed = pd.to_datetime(column, format="mixed", dayfirst=False, errors="coerce")
    if parsed.notna().all():
        return frame.assign(_x_order=parsed).sort_values("_x_order").drop(columns="_x_order")
    return frame.sort_values(by=x_column)


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
    theme: str = "device",
    open_after: bool = True,
    return_content: bool = False,
    dayfirst: str = "auto",
) -> dict:
    """Generate Plotly chart. bar/pie/line/scatter/funnel/radius/geo need category_column."""
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
        # The " by <column>" suffix finishes the *generated* title -- "sum of
        # spends" alone does not say what it is broken down by. Appending it to
        # a title the caller wrote says it twice: a sweep asked for "Total Ad
        # Spend by Campaign Platform" and the page heading read "Total Ad Spend
        # by Campaign Platform by campaign_platform". generate_geo_map already
        # guards its own generated title with `if not chart_title`.
        chart_title = title or f"{agg_func} of {value_column}"
        if not title and category_column and chart_type not in _CATEGORY_IS_NOT_THE_BREAKDOWN:
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
        if chart_type in _NEEDS_CATEGORY and not category_column:
            return {
                "success": False,
                "error": f"{chart_type} requires category_column",
                "hint": (
                    f"Name the column to plot along the category axis, e.g. "
                    f"category_column='campaign_platform'. Call inspect_dataset('{path.name}') "
                    f"to list the columns."
                ),
                "progress": [fail("Missing params", "category_column")],
                "token_estimate": 30,
            }

        # Build chart_df
        if chart_type in ("bar", "pie", "line", "scatter"):
            if category_column:
                grouped = df.groupby(category_column, as_index=False)[value_column].agg(agg_func)
                if chart_type == "line":
                    grouped = _sort_along_x(grouped, category_column)
                else:
                    grouped = grouped.sort_values(by=value_column, ascending=False)
                chart_df = grouped
            else:
                chart_df = _sort_along_x(df, category_column) if chart_type == "line" else df
        elif chart_type == "time_series":
            df[date_column], _fmt = parse_dates(df[date_column], dayfirst)
            progress.append(date_note(_fmt, date_column))
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

        # No height here on purpose. A single chart gets its own page, and the
        # page's CSS sizes it to the viewport (.plotly-graph-div min-height
        # clamp(20rem,60vh,50rem)) via autosize. Pinning it in the layout
        # overrode that and left every standalone chart 300px tall in whatever
        # window it was opened in. Multi-panel figures still set an explicit
        # height, because those genuinely have to grow with their row count.
        fig.update_layout(margin=dict(l=20, r=20, t=40, b=20), autosize=True)
        rows_plotted = len(chart_df)

        # Named for what is IN it, not just its type. `chart_type` alone gave
        # every bar chart on one dataset the same default filename, so four of
        # them -- different columns, different aggregations -- became one file,
        # each silently replacing the last.
        stem = discriminated_suffix(chart_type, agg_func, value_column, category_column)
        abs_p, fname = _save_chart(fig, output_path, stem, path, open_after, theme, progress)
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
        embed_content(result, Path(abs_p), return_content)
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_chart error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path, column names, and chart_type."),
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
    theme: str = "device",
    open_after: bool = True,
    return_content: bool = False,
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

            # Plotly wraps out-of-range coordinates onto the globe instead of
            # rejecting them, so plotting a spend column as latitude yields a
            # convincing-looking world map of nothing. Refuse rather than invent.
            out_of_range = _coords_out_of_range(plot_df, lat_col, lon_col)
            if out_of_range:
                return {
                    "success": False,
                    "error": f"'{out_of_range['column']}' is not a {out_of_range['kind']}: "
                    f"{out_of_range['count']:,} of {len(plot_df):,} values fall outside "
                    f"{out_of_range['low']}..{out_of_range['high']} "
                    f"(observed {out_of_range['min']:.4g} to {out_of_range['max']:.4g}).",
                    "hint": (
                        "Pass real coordinate columns as lat_column/lon_column, or use "
                        "location_column with country or state names for a choropleth."
                    ),
                    "progress": [fail("Not a coordinate column", out_of_range["column"])],
                    "token_estimate": 40,
                }

            if value_column:
                plot_df[value_column] = pd.to_numeric(df.loc[plot_df.index, value_column], errors="coerce")
            if color_column and color_column in df.columns:
                plot_df[color_column] = df.loc[plot_df.index, color_column].values

            # value_column drives marker size, and a marker cannot be smaller
            # than nothing. Without this check plotly raised its own error --
            # "Invalid element(s) received for the 'size' property of
            # scattergeo.marker" with two raw floats -- and the generic hint
            # sent the caller to check the geo columns, which were fine. Two
            # negative values in 200 rows were enough. lat/lon are already
            # range-checked a few lines above; this is the same check for the
            # third column the map reads.
            if value_column and value_column in plot_df.columns:
                negatives = plot_df[value_column] < 0
                n_negative = int(negatives.sum())
                if n_negative:
                    worst = float(plot_df.loc[negatives, value_column].min())
                    return {
                        "success": False,
                        "op": "generate_geo_map",
                        "error": (
                            f"value_column '{value_column}' has {n_negative} negative value(s) "
                            f"(lowest {worst:.4g}); it sets marker size on a scatter map, which "
                            f"cannot be negative."
                        ),
                        "hint": (
                            f"Pass a non-negative column as value_column, or map '{value_column}' to "
                            f"colour instead with color_column='{value_column}' and no value_column. "
                            f"To size by magnitude, add an absolute-value column first with "
                            f"apply_patch() op=abs_values on '{value_column}'."
                        ),
                        "progress": [*progress, fail("Negative value_column", f"{value_column}: {n_negative} row(s)")],
                        "token_estimate": 60,
                    }

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

            # px.choropleth drops every location it cannot resolve, without
            # saying so: "Google Ads"/"Facebook Ads" as country names produced a
            # complete figure with a colour bar over the real spend range and
            # not one country shaded. rows_plotted counted the distinct values,
            # so the response said "2 locations" about an empty map and a sweep
            # recorded it as a PASS. Nothing downstream can be asked what
            # matched -- plotly.js resolves names in the browser -- so check the
            # names here before drawing.
            place_names = [str(v) for v in grouped[loc_col].tolist()]
            unplaceable = unrecognised_locations(place_names, loc_mode or "")
            mode_label = _LOC_MODE_LABELS.get(loc_mode or "", str(loc_mode))
            if unplaceable and len(unplaceable) == len(place_names):
                sample = ", ".join(repr(v) for v in unplaceable[:3])
                return {
                    "success": False,
                    "error": (
                        f"No value in '{loc_col}' is a recognisable {mode_label}: {sample}"
                        f"{' ...' if len(unplaceable) > 3 else ''}"
                    ),
                    "hint": (
                        f"A choropleth can only shade places. '{loc_col}' holds categories, so "
                        "the map would be blank. Use generate_chart with chart_type=bar to "
                        "compare them, or pass location_column with country or state names."
                    ),
                    "progress": [fail("Not a location column", loc_col)],
                    "token_estimate": 40,
                }
            if unplaceable:
                progress.append(
                    warn(
                        f"{len(unplaceable)} of {len(place_names)} values are not a {mode_label}",
                        f"dropped from the map: {', '.join(str(v) for v in unplaceable[:5])}",
                    )
                )

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

        fig.update_layout(margin={"l": 0, "r": 0, "t": 40, "b": 0}, autosize=True)
        # Every other chart this server writes is complete once it is on disk.
        # A map is not: plotly fetches its country outlines from cdn.plot.ly and
        # tiled maps fetch tiles, so opening one offline gives a colour bar
        # beside an empty rectangle under success: true. The geometry cannot be
        # carried in the page without vendoring a world dataset, so what changes
        # is that the caller is told before they open it.
        basemap = remote_basemap_traces(fig)
        if basemap:
            progress.append(warn("Map needs a network connection to draw", BASEMAP_NOTE))
        abs_p, fname = _save_chart(fig, output_path, "geo_map", path, open_after, theme, progress)
        progress.append(ok("Map saved", fname))

        result = {
            "success": True,
            "op": "generate_geo_map",
            "self_contained": not basemap,
            "needs_network_for": basemap,
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
        if basemap:
            result["hint"] = BASEMAP_NOTE
        embed_content(result, Path(abs_p), return_content)
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_geo_map error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path, column names, and that columns contain valid geo data."),
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
    theme: str = "device",
    open_after: bool = True,
    return_content: bool = False,
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
        # chart_type already carries the dimension ("scatter_3d"), so prefixing
        # "3D " onto its titled form read "3D Scatter 3D: ...".
        kind = chart_type.replace("_3d", "").replace("_", " ").title()
        chart_title = title if title else f"3D {kind}: {x_column} × {y_column} × {z_column}"

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

        fig.update_layout(margin=dict(l=20, r=20, t=40, b=20), autosize=True)
        abs_p, fname = _save_chart(fig, output_path, chart_type, path, open_after, theme, progress)
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
        embed_content(result, Path(abs_p), return_content)
        result["token_estimate"] = _token_estimate(result)
        return result

    except Exception as exc:
        logger.exception("generate_3d_chart error")
        return {
            "success": False,
            "error": error_text(exc),
            "hint": hint_for_error(exc, "Check file_path, column names, and chart_type."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
