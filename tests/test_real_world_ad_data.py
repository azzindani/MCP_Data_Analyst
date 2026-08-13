"""Integration tests against the full real (not synthetic) Ad_Data.csv dataset.

Reproduces the exact workflow used during the 2026-08-12 comprehensive
real-world tool sweep, so the scenarios that surfaced real bugs stay
covered going forward instead of only having been checked once by hand.
See ad_data_full_csv fixture in conftest.py and the project memory
entries reference_ad_data_test_source / project_comprehensive_connector_test.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from servers.data_basic.engine import apply_patch
from servers.data_workspace.engine import (
    create_workspace,
    register_workspace_file,
    run_workspace_pipeline,
    save_workspace_pipeline,
)


@pytest.fixture()
def project_base(tmp_path) -> Path:
    """Isolated workspace directory for each test."""
    base = tmp_path / "mcp_projects"
    base.mkdir()
    return base


class TestRealWorldCleaningPipeline:
    """The fill_nulls + column_math(ctr_pct) + drop_duplicates pipeline run
    against the real dataset during the sweep — including the real
    impressions=0 rows that make ctr_pct contain +inf."""

    def test_apply_patch_real_pipeline(self, ad_data_full_csv):
        r = apply_patch(
            str(ad_data_full_csv),
            ops=[
                {"op": "fill_nulls", "column": "link_clicks", "strategy": "median"},
                {"op": "column_math", "formula": "clicks / impressions * 100", "target_column": "ctr_pct"},
                {"op": "drop_duplicates", "keep": "first"},
            ],
        )
        assert r["success"] is True
        df = pd.read_csv(ad_data_full_csv)
        assert df["link_clicks"].isna().sum() == 0
        assert "ctr_pct" in df.columns
        # the 4 real impressions=0/clicks>0 rows must survive as +inf, not crash the op
        assert (df["ctr_pct"] == float("inf")).sum() == 4


class TestRealWorldWorkspacePipeline:
    """Regression test for the run_workspace_pipeline bug (fixed 2026-08-12)
    where a real pipeline run mutated the registered raw input file in
    place instead of only writing the new output alias."""

    def test_run_workspace_pipeline_leaves_raw_untouched(self, ad_data_full_csv, project_base):
        original_bytes = ad_data_full_csv.read_bytes()
        create_workspace("real_ad_data", base_dir=str(project_base))
        register_workspace_file(
            "real_ad_data", str(ad_data_full_csv), alias="raw", stage="raw", base_dir=str(project_base)
        )
        ops = [
            {"op": "fill_nulls", "column": "link_clicks", "strategy": "median"},
            {"op": "column_math", "formula": "clicks / impressions * 100", "target_column": "ctr_pct"},
        ]
        save_workspace_pipeline("real_ad_data", "clean", ops, base_dir=str(project_base))

        r = run_workspace_pipeline(
            "real_ad_data", "clean", input_alias="raw", output_alias="clean_v1", base_dir=str(project_base)
        )
        assert r["success"] is True
        assert ad_data_full_csv.read_bytes() == original_bytes, "raw input must not be mutated"

        out_df = pd.read_csv(r["output_path"])
        assert "ctr_pct" in out_df.columns
