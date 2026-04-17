"""Universal handover protocol for multi-MCP-server tool call loops.

Workflow steps (server-agnostic, in order):
  COLLECT -> INSPECT -> CLEAN -> PREPARE -> TRAIN -> EVALUATE -> REPORT

Domain routing (which MCP server handles which domain):
  data    -> MCP_Data_Analyst
  ml      -> MCP_Machine_Learning
  office  -> MCP_Office (future)
  fs      -> MCP_FileSystem (future)
  search  -> MCP_Search (future)

Every tool response should include two fields:
  context  = make_context(op, summary, artifacts)
  handover = make_handover(workflow_step, suggested_next, carry_forward)
"""

from __future__ import annotations

from datetime import UTC, datetime

WORKFLOW_STEPS: list[str] = ["COLLECT", "INSPECT", "CLEAN", "PREPARE", "TRAIN", "EVALUATE", "REPORT"]

DOMAIN_SERVERS: dict[str, str] = {
    "data": "MCP_Data_Analyst",
    "ml": "MCP_Machine_Learning",
    "office": "MCP_Office",
    "fs": "MCP_FileSystem",
    "search": "MCP_Search",
}

STEP_TOOLS: dict[str, list[str]] = {
    "COLLECT": ["load_dataset", "create_workspace", "open_workspace"],
    "INSPECT": [
        "inspect_dataset",
        "read_column_stats",
        "search_columns",
        "auto_detect_schema",
        "scan_nulls_zeros",
        "check_outliers",
    ],
    "CLEAN": [
        "apply_patch",
        "smart_impute",
        "run_cleaning_pipeline",
        "run_preprocessing",
        "filter_dataset",
        "feature_engineering",
    ],
    "PREPARE": [
        "merge_datasets",
        "concat_datasets",
        "reshape_dataset",
        "aggregate_dataset",
        "feature_engineering",
        "run_preprocessing",
    ],
    "TRAIN": [
        "train_classifier",
        "train_regressor",
        "train_with_cv",
        "compare_models",
        "run_clustering",
        "tune_hyperparameters",
    ],
    "EVALUATE": [
        "evaluate_model",
        "get_predictions",
        "read_model_report",
        "plot_roc_curve",
        "plot_learning_curve",
        "correlation_analysis",
        "statistical_test",
        "regression_analysis",
    ],
    "REPORT": [
        "generate_eda_report",
        "generate_chart",
        "run_eda",
        "generate_cluster_report",
        "plot_predictions_vs_actual",
    ],
}


def _normalize_step(step: str) -> str:
    normalized = step.upper()
    if normalized in WORKFLOW_STEPS:
        return normalized
    return normalized


def _next_step(step: str) -> str:
    try:
        idx = WORKFLOW_STEPS.index(step)
        return WORKFLOW_STEPS[idx + 1] if idx + 1 < len(WORKFLOW_STEPS) else ""
    except ValueError:
        return ""


def make_context(
    op: str,
    summary: str,
    artifacts: list[dict] | None = None,
) -> dict:
    """Return a context dict capturing what this tool just did.

    op        : tool/operation name (e.g. "merge_datasets")
    summary   : plain-English description of what happened and the result
    artifacts : list of {"type": str, "path": str, "role": str, ...} dicts
    """
    return {
        "op": op,
        "summary": summary,
        "artifacts": artifacts or [],
        "timestamp": datetime.now(UTC).isoformat(),
    }


def make_handover(
    workflow_step: str,
    suggested_next: list[dict],
    carry_forward: dict | None = None,
) -> dict:
    """Return a handover dict for inclusion in every tool response.

    workflow_step   : current step (COLLECT/INSPECT/CLEAN/PREPARE/TRAIN/EVALUATE/REPORT)
    suggested_next  : list of {"tool": str, "server": str, "domain": str, "reason": str}
    carry_forward   : exact params the LLM should pass to the next tool call
    """
    step = _normalize_step(workflow_step)
    return {
        "workflow_step": step,
        "workflow_next": _next_step(step),
        "suggested_next": [
            {
                "tool": s.get("tool", ""),
                "server": s.get("server", ""),
                "domain": s.get("domain", "data"),
                "reason": s.get("reason", ""),
            }
            for s in suggested_next
        ],
        "carry_forward": carry_forward or {},
    }


__all__ = [
    "WORKFLOW_STEPS",
    "DOMAIN_SERVERS",
    "STEP_TOOLS",
    "make_context",
    "make_handover",
]
