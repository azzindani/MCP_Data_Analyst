"""customize_chart knew about 3D scenes and still only labelled two axes.

2902ae0 taught customize_chart where a 3D figure's axis titles have to go --
layout.scene.xaxis, never the top-level layout.xaxis, because plotly reads them
from nowhere else. It then wrote exactly two of them:

    for key, label in (("xaxis", x_label), ("yaxis", y_label)):

A 3D figure has three axes. generate_3d_chart *requires* z_column, so the z
axis is never optional in the data -- but there was no z_label parameter at all,
and layout.scene.zaxis.title could not be set by any call. The fix that taught
the tool about 3D carried two thirds of it.

Found by round 17, whose report noted only: "removed z_label param (not
supported)". The verdict column said the handover was fine; the note is where
the defect was.

Three separate places had to learn the new parameter, which is what makes this
worth a test rather than a one-line change:

  * the block is gated on `if x_label or y_label` -- z_label alone skipped it
    entirely and fell through to "No customization parameters provided"
  * that refusal's hint enumerates the accepted parameters and would otherwise
    have taught a vocabulary the tool no longer matched
  * server.py passed its arguments POSITIONALLY, so inserting z_label between
    y_label and color_scheme silently rebound every argument after it -- the
    chart would have been "coloured" with the z label. It now passes by keyword.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from servers.data_visual import server as v


def tool_fn(mod, name: str):
    """The callable a client actually reaches, via the tool registry.

    Under fastmcp 2.x the module-level name WAS the registry entry, so
    `mod.some_tool.fn` and a client's path were the same object. The official
    MCP SDK's @mcp.tool returns the plain undecorated function, so the
    module-level name now bypasses every wrapper installed on the registry --
    sanitize_responses, measure_responses, contract_errors.

    Going through _tools keeps these tests on the path a request takes. A test
    calling the bare function would pass while the thing it guards sat switched
    off, which is the one failure mode those guards exist to prevent.
    """
    return mod.mcp._tool_manager._tools[name].fn


@pytest.fixture()
def data(tmp_path: Path) -> str:
    csv = tmp_path / "t.csv"
    csv.write_text("cat,a,b,c\nx,1,2,3\ny,4,5,6\nz,7,8,9\n")
    return str(csv)


def _chart_3d(data: str, tmp_path: Path) -> str:
    r = tool_fn(v, "generate_3d_chart")(
        file_path=data,
        chart_type="scatter_3d",
        x_column="a",
        y_column="b",
        z_column="c",
        output_path=str(tmp_path / "c3d.html"),
        open_after=False,
    )
    assert r["success"], r
    return r["output_path"]


def _chart_2d(data: str, tmp_path: Path) -> str:
    r = tool_fn(v, "generate_chart")(
        file_path=data,
        chart_type="bar",
        value_column="a",
        category_column="cat",
        output_path=str(tmp_path / "bar.html"),
        open_after=False,
    )
    assert r["success"], r
    return r["output_path"]


def _has_title(html: str, axis: str) -> bool:
    return bool(re.search(rf'"{axis}":\s*\{{[^}}]*"title"', html))


class TestTheZAxisCanBeLabelled:
    def test_z_label_alone_is_applied(self, data: str, tmp_path: Path) -> None:
        out = tmp_path / "z.html"
        r = tool_fn(v, "customize_chart")(chart_path=_chart_3d(data, tmp_path), z_label="Depth", output_path=str(out))
        assert r["success"], r
        assert _has_title(out.read_text(errors="replace"), "zaxis")

    def test_all_three_land_under_the_scene(self, data: str, tmp_path: Path) -> None:
        """plotly reads a 3D figure's titles from layout.scene and nowhere else."""
        out = tmp_path / "xyz.html"
        r = tool_fn(v, "customize_chart")(
            chart_path=_chart_3d(data, tmp_path), x_label="A", y_label="B", z_label="C", output_path=str(out)
        )
        assert r["success"], r
        html = out.read_text(errors="replace")
        scene = re.search(r'"scene":\s*\{.*?\n', html, re.S)
        assert scene, "no scene block"
        for axis in ("xaxis", "yaxis", "zaxis"):
            assert _has_title(html, axis), f"{axis} has no title"

    def test_the_reply_names_the_z_change(self, data: str, tmp_path: Path) -> None:
        r = tool_fn(v, "customize_chart")(
            chart_path=_chart_3d(data, tmp_path), z_label="Depth", output_path=str(tmp_path / "z2.html")
        )
        assert any("z-axis" in c for c in r["changes_applied"]), r["changes_applied"]


class TestItRefusesWhereThereIsNoZAxis:
    """Applying it anyway would be the lie the x/y branch already guards against."""

    def test_a_2d_chart_is_refused_by_name(self, data: str, tmp_path: Path) -> None:
        r = tool_fn(v, "customize_chart")(
            chart_path=_chart_2d(data, tmp_path), z_label="Depth", output_path=str(tmp_path / "b2.html")
        )
        assert r["success"] is False
        assert "z axis" in r["error"], r["error"]
        assert "generate_3d_chart" in r["hint"], r["hint"]

    def test_x_and_y_still_work_on_a_2d_chart(self, data: str, tmp_path: Path) -> None:
        """The new refusal must not swallow the case that always worked."""
        out = tmp_path / "b3.html"
        r = tool_fn(v, "customize_chart")(
            chart_path=_chart_2d(data, tmp_path), x_label="A", y_label="B", output_path=str(out)
        )
        assert r["success"], r


class TestTheParametersStayBound:
    def test_the_hint_lists_z_label(self, data: str, tmp_path: Path) -> None:
        """The 'nothing to change' hint enumerates what the tool accepts."""
        r = tool_fn(v, "customize_chart")(chart_path=_chart_2d(data, tmp_path))
        assert r["success"] is False
        assert "z_label" in r["hint"], r["hint"]

    def test_colour_is_not_bound_to_the_z_label(self, data: str, tmp_path: Path) -> None:
        """server.py passed positionally; z_label sits where color_scheme did.

        If the wrapper ever reverts to positional arguments this fails, because
        color_scheme would receive the z label and the recoloured chart would
        silently not be recoloured.
        """
        out = tmp_path / "col.html"
        r = tool_fn(v, "customize_chart")(
            chart_path=_chart_2d(data, tmp_path), color_scheme=["#ff0000"], output_path=str(out)
        )
        assert r["success"], r
        assert "ff0000" in out.read_text(errors="replace").lower()
