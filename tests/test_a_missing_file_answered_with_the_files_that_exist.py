"""A missing file is answered with the nearest files that exist.

"File not found: ad_data.csv", hint "Check file_path is absolute and the file
exists". Both halves were wrong for a remote caller: a relative path is read
from the data folder, and the caller cannot look -- it shares no filesystem
with the server -- so it guesses again. `Ad_Data.csv` one case-fold away was
the answer, and the server was the only thing that could see it.

Every tool reports a missing file, so this is asserted through the registered
tools themselves, not only through the helper. And because a suggestion lists
real file names, it is also asserted never to reach outside the served folders.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.missing_file import suggest  # noqa: E402

TIERS = [
    "data_basic",
    "data_medium",
    "data_ingest",
    "data_statistics",
    "data_transform",
    "data_visual",
    "data_workspace",
]


def _tool(tier: str, name: str):
    server = importlib.import_module(f"servers.{tier}.server")
    return server.mcp._tool_manager._tools[name].fn


@pytest.fixture
def served(tmp_path, monkeypatch):
    data = tmp_path / "data"
    (data / "archive").mkdir(parents=True)
    monkeypatch.setenv("MCP_CONFINE_PATHS", "1")
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(data))
    monkeypatch.setenv("MCP_WORKSPACE_DIR", str(tmp_path / "ws"))
    monkeypatch.delenv("MCP_DATA_ROOT", raising=False)
    monkeypatch.delenv("MCP_ALLOWED_ROOTS", raising=False)
    pd.DataFrame({"clicks": [1, 2, 3]}).to_csv(data / "Ad_Data.csv", index=False)
    pd.DataFrame({"x": [1]}).to_csv(data / "archive" / "orders_2023.csv", index=False)
    (data / ".hidden.csv").write_text("x\n1\n")
    return data


class TestThroughTheTool:
    def test_a_case_fold_away_is_named_first(self, served):
        if (served / "ad_data.csv").exists():
            pytest.skip("case-insensitive filesystem: the file is found, nothing to suggest")
        r = _tool("data_basic", "inspect_dataset")(file_path="ad_data.csv")
        assert r["success"] is False
        assert r["did_you_mean"][0] == "Ad_Data.csv"
        assert "Ad_Data.csv" in r["hint"]
        assert "absolute" not in r["hint"], "a relative path is exactly what the caller should pass"

    def test_a_misremembered_extension_finds_the_real_one(self, served):
        r = _tool("data_basic", "inspect_dataset")(file_path="Ad_Data.xlsx")
        assert r["did_you_mean"][0] == "Ad_Data.csv"

    def test_a_file_in_a_subfolder_is_named_by_the_path_to_pass(self, served):
        r = _tool("data_basic", "inspect_dataset")(file_path="orders_2024.csv")
        assert str(Path("archive") / "orders_2023.csv") in r["did_you_mean"]

    def test_nothing_close_lists_what_the_folder_holds(self, served):
        r = _tool("data_basic", "inspect_dataset")(file_path="zzqx.parquet")
        assert "did_you_mean" not in r
        assert "Ad_Data.csv" in r["hint"]
        assert ".hidden.csv" not in r["hint"]

    def test_the_token_estimate_counts_the_suggestion(self, served):
        r = _tool("data_basic", "inspect_dataset")(file_path="Ad_Data.xlsx")
        assert r["token_estimate"] >= len(str(r["did_you_mean"])) // 4

    @pytest.mark.parametrize("tier", TIERS)
    def test_every_tier_installs_it(self, tier):
        server = importlib.import_module(f"servers.{tier}.server")
        tools = server.mcp._tool_manager._tools.values()
        assert tools
        assert all(getattr(t.fn, "__suggests_missing_files__", False) for t in tools)


class TestNeverOutside:
    def test_a_folder_outside_the_served_ones_is_never_listed(self, served, tmp_path):
        outside = tmp_path / "private"
        outside.mkdir()
        (outside / "secrets.csv").write_text("k\n1\n")
        r = suggest(
            {"success": False, "error": "File not found: secret.csv", "hint": "h"},
            {"file_path": str(outside / "secret.csv")},
        )
        assert "secrets.csv" not in str(r)

    def test_a_confined_server_with_nothing_to_search_leaves_the_answer_alone(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MCP_CONFINE_PATHS", "1")
        monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path / "absent"))
        original = {"success": False, "error": "File not found: a.csv", "hint": "h"}
        assert suggest(dict(original), {"file_path": "a.csv"}) == original


class TestLocal:
    def test_the_named_folder_is_searched(self, tmp_path, monkeypatch):
        monkeypatch.delenv("MCP_CONFINE_PATHS", raising=False)
        monkeypatch.delenv("MCP_DATA_ROOT", raising=False)
        (tmp_path / "report_2023.csv").write_text("a\n1\n")
        r = suggest(
            {"success": False, "error": "File not found: report_2024.csv", "hint": "h"},
            {"file_path": str(tmp_path / "report_2024.csv")},
        )
        assert r["did_you_mean"] == [str((tmp_path / "report_2023.csv").resolve())]


class TestLeftAlone:
    @pytest.mark.parametrize(
        "result",
        [
            {"success": True, "error": "File not found: a.csv"},
            {"success": False, "error": "Column not found: 'a.csv'"},
            {"success": False, "error": "Columns not found: ['x']"},
        ],
    )
    def test_only_a_missing_file_is_touched(self, served, result):
        assert suggest(dict(result), {"file_path": "Ad_Data.csv"}) == result
