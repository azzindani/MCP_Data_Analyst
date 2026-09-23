"""The whole data surface as eight domain tools -- one endpoint, `action` plus `args`.

The seven tier servers list 68 tools between them; a model connected to all of
them reads 68 names on every turn. This endpoint lists eight, one per job, and
each tool's `action` is one of those 68 tools by its own name. Schemas,
validation, wrappers and answers are the tiers' own: see shared/domain_tools.py.
The tier endpoints keep serving unchanged, for small local models and for
every client already connected to one.
"""

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

from mcp.server.fastmcp import FastMCP  # noqa: E402
from starlette.requests import Request  # noqa: E402
from starlette.responses import JSONResponse  # noqa: E402

from servers.data_basic.server import mcp as basic  # noqa: E402
from servers.data_ingest.server import mcp as ingest  # noqa: E402
from servers.data_medium.server import mcp as medium  # noqa: E402
from servers.data_statistics.server import mcp as statistics  # noqa: E402
from servers.data_transform.server import mcp as transform  # noqa: E402
from servers.data_visual.server import mcp as visual  # noqa: E402
from servers.data_workspace.server import mcp as workspace  # noqa: E402
from shared.arg_errors import contract_errors  # noqa: E402
from shared.deploy_auth import build_auth, build_oauth_bridge  # noqa: E402
from shared.domain_tools import register_domains  # noqa: E402
from shared.strict_args import enforce_known_arguments  # noqa: E402

_VERSION = "0.3.0"  # keep in sync with pyproject.toml [project].version

_oauth_bridge = build_oauth_bridge(
    "DA", state_dir=os.environ.get("DA_DOMAIN_OAUTH_STATE_DIR", "/tmp/data-domain-oauth-state")
)
_public_origin = os.environ.get("DA_PUBLIC_URL", "").rstrip("/")
_HOST = os.environ.get("DATA_DOMAIN_HOST", "127.0.0.1")
_PORT = int(os.environ.get("DATA_DOMAIN_PORT", "8817"))
_token_verifier, _auth_settings = build_auth("DA", _public_origin or None, _oauth_bridge)

mcp = FastMCP("data", host=_HOST, port=_PORT, token_verifier=_token_verifier, auth=_auth_settings)
if _oauth_bridge is not None:
    _oauth_bridge.register_routes(mcp)

# Each domain: what it is for, then its actions -- each an existing tier tool.
DOMAINS = {
    "data_inspect": (
        "Look at a dataset without changing it: load, schema, column stats, search, sample, validate, history.",
        [
            (basic, "load_dataset"),
            (basic, "load_geo_dataset"),
            (basic, "inspect_dataset"),
            (basic, "read_column_stats"),
            (basic, "search_columns"),
            (medium, "sample_data"),
            (statistics, "auto_detect_schema"),
            (statistics, "validate_dataset"),
            (statistics, "scan_nulls_zeros"),
            (basic, "read_receipt"),
        ],
    ),
    "data_edit": (
        "Change a dataset's rows and columns: patch ops, cleaning, imputing, derived columns, filtering, undo.",
        [
            (basic, "apply_patch"),
            (basic, "list_patch_ops"),
            (transform, "run_cleaning_pipeline"),
            (transform, "smart_impute"),
            (transform, "feature_engineering"),
            (transform, "list_derive_ops"),
            (transform, "filter_dataset"),
            (transform, "enrich_with_geo"),
            (basic, "restore_version"),
        ],
    ),
    "data_reshape": (
        "Make a new table from one or more: reshape, aggregate, pivot, merge, concat, resample, export.",
        [
            (transform, "reshape_dataset"),
            (transform, "aggregate_dataset"),
            (medium, "pivot_table"),
            (transform, "merge_datasets"),
            (transform, "concat_datasets"),
            (transform, "resample_timeseries"),
            (visual, "export_data"),
        ],
    ),
    "data_stats": (
        "Measure and test: distributions, tests, outliers, correlation, regression, time series, cohorts, anomalies.",
        [
            (statistics, "extended_stats"),
            (statistics, "statistical_test"),
            (statistics, "check_outliers"),
            (statistics, "correlation_analysis"),
            (statistics, "lag_correlation"),
            (statistics, "regression_analysis"),
            (statistics, "time_series_analysis"),
            (statistics, "period_comparison"),
            (statistics, "cohort_analysis"),
            (medium, "detect_anomalies"),
            (medium, "analyze_text_column"),
            (medium, "compare_datasets"),
        ],
    ),
    "data_chart": (
        "Draw a chart as an HTML page: bar, line, distribution, heatmap, pairwise, map, 3D, crosstab, frequencies.",
        [
            (visual, "generate_chart"),
            (visual, "generate_distribution_plot"),
            (visual, "generate_correlation_heatmap"),
            (visual, "generate_pairwise_plot"),
            (visual, "generate_multi_chart"),
            (visual, "generate_geo_map"),
            (visual, "generate_3d_chart"),
            (visual, "customize_chart"),
            (medium, "cross_tabulate"),
            (medium, "value_counts"),
        ],
    ),
    "data_report": (
        "Whole-dataset reports as HTML: EDA, auto profile, dashboard.",
        [
            (visual, "run_eda"),
            (visual, "generate_auto_profile"),
            (visual, "generate_dashboard"),
            (visual, "customize_dashboard"),
        ],
    ),
    "data_ingest": (
        "Get a table out of a spreadsheet or file: sheets, detected tables, headers, merged cells, conversion.",
        [
            (ingest, "list_sheets"),
            (ingest, "extract_sheet"),
            (ingest, "extract_all_sheets"),
            (ingest, "detect_tables"),
            (ingest, "extract_table"),
            (ingest, "normalize_headers"),
            (ingest, "trim_empty"),
            (ingest, "promote_header"),
            (ingest, "flatten_merged_cells"),
            (ingest, "convert_file"),
        ],
    ),
    "data_workspace": (
        "Keep a project's files and saved pipelines together, and rerun a pipeline.",
        [
            (workspace, "create_workspace"),
            (workspace, "open_workspace"),
            (workspace, "register_workspace_file"),
            (workspace, "list_workspace_files"),
            (workspace, "save_workspace_pipeline"),
            (workspace, "run_workspace_pipeline"),
        ],
    ),
}
register_domains(mcp, DOMAINS)


@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> JSONResponse:
    """Liveness check. Unauthenticated."""
    return JSONResponse({"status": "ok", "version": _VERSION, "tools": len(DOMAINS)})


# A wrong-typed `args` or an unknown top-level key gets the fleet's failure
# shape, as on every tier; per-action arguments are checked by the dispatcher.
contract_errors(mcp)
enforce_known_arguments(mcp)


def main() -> None:
    parser = argparse.ArgumentParser(description="data domain MCP Server")
    parser.add_argument(
        "--transport", choices=["stdio", "http"], default=os.environ.get("DA_DOMAIN_TRANSPORT", "stdio")
    )
    args = parser.parse_args()
    mcp.run(transport="streamable-http" if args.transport == "http" else "stdio")


if __name__ == "__main__":
    main()
