"""T4 data_visual MCP server — thin wrapper only. Zero domain logic."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

_root = str(Path(__file__).resolve().parents[2])
if _root not in sys.path:
    sys.path.insert(0, _root)

from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

from shared.arg_errors import contract_errors
from shared.deploy_auth import build_auth, build_oauth_bridge
from shared.json_safe import sanitize_responses
from shared.token_estimate import measure_responses
from shared.tool_annotations import CREATES

try:
    from . import engine
except ImportError:
    import engine

_VERSION = "0.2.2"  # keep in sync with pyproject.toml [project].version

_oauth_bridge = build_oauth_bridge(
    "DA", state_dir=os.environ.get("DA_VISUAL_OAUTH_STATE_DIR", "/tmp/data-visual-oauth-state")
)
_public_origin = os.environ.get("DA_PUBLIC_URL", "").rstrip("/")
_base_url = f"{_public_origin}/visual" if _public_origin else None
_HOST = os.environ.get("DATA_VISUAL_HOST", "127.0.0.1")
_PORT = int(os.environ.get("DATA_VISUAL_PORT", "8815"))
_token_verifier, _auth_settings = build_auth("DA", _base_url, _oauth_bridge)

mcp = FastMCP(
    "data_visual",
    host=_HOST,
    port=_PORT,
    token_verifier=_token_verifier,
    auth=_auth_settings,
)
if _oauth_bridge is not None:
    _oauth_bridge.register_routes(mcp)


@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> JSONResponse:
    """Liveness check. Unauthenticated."""
    return JSONResponse({"status": "ok", "version": _VERSION})


@mcp.custom_route("/version", methods=["GET"])
async def version(request: Request) -> JSONResponse:
    """Report running version. Unauthenticated."""
    return JSONResponse({"current": _VERSION})


@mcp.tool(annotations=CREATES)
def run_eda(
    file_path: str,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "device",
    return_content: bool = False,
    mode: str = "standard",
    sample_n: int = 0,
    include: dict = None,
) -> dict:
    """EDA summary. mode=minimal|standard|full, sample_n, include. Saves HTML."""
    # The long version lives on engine.run_eda, which is not a tool. This string
    # IS the MCP tool description: every client pays for it on every tools/list,
    # which is why the gate caps it at 80 characters.
    return engine.run_eda(file_path, output_path, open_after, theme, return_content, mode, sample_n, include)


@mcp.tool(annotations=CREATES)
def generate_auto_profile(
    file_path: str,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "device",
    return_content: bool = False,
) -> dict:
    """Full column profile: stats charts correlations outliers insights."""
    return engine.generate_auto_profile(file_path, output_path, open_after, theme, return_content)


@mcp.tool(annotations=CREATES)
def generate_distribution_plot(
    file_path: str,
    columns: list[str] = None,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "device",
    return_content: bool = False,
) -> dict:
    """Histogram + box plot for numeric columns. Saves HTML."""
    return engine.generate_distribution_plot(file_path, columns, output_path, open_after, theme, return_content)


@mcp.tool(annotations=CREATES)
def generate_correlation_heatmap(
    file_path: str,
    method: str = "pearson",
    output_path: str = "",
    open_after: bool = True,
    theme: str = "device",
    return_content: bool = False,
) -> dict:
    """Interactive correlation heatmap for numeric columns. Saves HTML."""
    return engine.generate_correlation_heatmap(file_path, method, output_path, open_after, theme, return_content)


@mcp.tool(annotations=CREATES)
def generate_pairwise_plot(
    file_path: str,
    columns: list[str] = None,
    max_cols: int = 6,
    output_path: str = "",
    open_after: bool = True,
    theme: str = "device",
    return_content: bool = False,
) -> dict:
    """Pairwise scatter + histogram matrix for numeric columns. Saves HTML."""
    return engine.generate_pairwise_plot(file_path, columns, max_cols, output_path, open_after, theme, return_content)


@mcp.tool(annotations=CREATES)
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
    return engine.generate_multi_chart(
        file_path,
        chart_type,
        value_columns,
        category_column,
        date_column,
        agg_func,
        output_path,
        title,
        open_after,
        theme,
        return_content,
    )


@mcp.tool(annotations=CREATES)
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
) -> dict:
    """Generate chart. bar/pie/line/scatter/funnel/radius/geo need category_column."""
    return engine.generate_chart(
        file_path,
        chart_type,
        value_column,
        category_column,
        agg_func,
        color_column,
        date_column,
        period,
        hierarchy_columns,
        geo_file_path,
        geo_join_column,
        output_path,
        title,
        theme,
        open_after,
        return_content,
    )


@mcp.tool(annotations=CREATES)
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
    """Geo map: scatter (lat/lon) or choropleth (country/state). Auto-detects."""
    return engine.generate_geo_map(
        file_path,
        lat_column,
        lon_column,
        location_column,
        value_column,
        location_mode,
        color_column,
        title,
        output_path,
        theme,
        open_after,
        return_content,
    )


@mcp.tool(annotations=CREATES)
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
    """3D scatter or surface chart. type: scatter_3d surface. Saves HTML."""
    return engine.generate_3d_chart(
        file_path,
        chart_type,
        x_column,
        y_column,
        z_column,
        color_column,
        title,
        output_path,
        theme,
        open_after,
        return_content,
    )


@mcp.tool(annotations=CREATES)
def generate_dashboard(
    file_path: str,
    output_path: str = "",
    title: str = "",
    chart_types: list[str] = None,
    agg_overrides: list[str] = None,
    geo_file_path: str = "",
    theme: str = "device",
    dry_run: bool = False,
    open_after: bool = True,
    return_content: bool = False,
) -> dict:
    """Interactive HTML dashboard with auto-detected charts. Saves HTML."""
    return engine.generate_dashboard(
        file_path,
        output_path,
        title,
        chart_types,
        agg_overrides,
        geo_file_path,
        theme,
        dry_run,
        open_after,
        return_content,
    )


@mcp.tool(annotations=CREATES)
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
    return engine.export_data(
        file_path, output_path, format, encoding, separator, open_after, return_content, output_format
    )


@mcp.tool(annotations=CREATES)
def customize_chart(
    chart_path: str,
    title: str = "",
    x_label: str = "",
    y_label: str = "",
    z_label: str = "",
    color_scheme: list[str] = None,
    sort_bars: str = "",
    highlight: list[str] = None,
    annotations: list[dict] = None,
    show_value_labels: bool = False,
    width: int = 0,
    height: int = 0,
    output_path: str = "",
    return_content: bool = False,
) -> dict:
    """Customize existing chart. changes: title labels colors annotations."""
    # Keyword, not positional. Inserting z_label between y_label and
    # color_scheme silently rebinds every argument after it when the call is
    # positional -- the chart would have been "coloured" with the z label.
    return engine.customize_chart(
        chart_path,
        title,
        x_label=x_label,
        y_label=y_label,
        z_label=z_label,
        color_scheme=color_scheme,
        sort_bars=sort_bars,
        highlight=highlight,
        annotations=annotations,
        show_value_labels=show_value_labels,
        width=width,
        height=height,
        output_path=output_path,
        return_content=return_content,
    )


# Every tool above reports what its response actually costs; see
# shared/token_estimate.py for why this is a choke point and not 325 edits.
# Infinity and NaN are not JSON; strip them before the estimate is taken
# so the number describes what actually goes on the wire.
sanitize_responses(mcp)
measure_responses(mcp)
# A known argument with the WRONG TYPE is rejected by pydantic before any of
# this runs, and used to escape as a raw dump with no success/hint/token_estimate
# and a pydantic.dev URL. Give it the fleet's failure shape instead.
contract_errors(mcp)


def main() -> None:
    parser = argparse.ArgumentParser(description="data_visual MCP Server")
    parser.add_argument(
        "--transport", choices=["stdio", "http"], default=os.environ.get("DA_VISUAL_TRANSPORT", "stdio")
    )
    args = parser.parse_args()

    if args.transport == "http":
        mcp.run(transport="streamable-http")
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
