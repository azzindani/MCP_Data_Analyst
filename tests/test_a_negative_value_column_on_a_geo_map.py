"""generate_geo_map handed plotly's own error to the caller.

`value_column` sets marker size on a scatter map, and a marker cannot be
smaller than nothing. Two negative values in a 200-row column produced:

    Invalid element(s) received for the 'size' property of scattergeo.marker
        Invalid elements include: [-1.1167625047232148, -0.1042252984258587]

under a hint reading "Check file_path, column names, and that columns contain
valid geo data" -- the geo columns were valid, and the real cause was a
different argument. lat/lon were already range-checked; this is the same check
for the third column the map reads.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced._adv_gencharts import generate_geo_map


@pytest.fixture
def coords(tmp_path):
    rng = np.random.default_rng(3)
    n = 60
    df = pd.DataFrame(
        {
            "lat": rng.uniform(-60, 60, n),
            "lon": rng.uniform(-160, 160, n),
            "positive": rng.uniform(1, 10, n),
            # Non-negative base, so the negative count below is exactly two --
            # the real case was 2 bad values in 200 good ones.
            "signed": rng.uniform(1, 10, n),
        }
    )
    df.loc[0, "signed"] = -2.5
    df.loc[1, "signed"] = -0.25
    path = tmp_path / "coords.csv"
    df.to_csv(path, index=False)
    return str(path)


def test_a_negative_value_column_fails_before_plotly_does(coords):
    r = generate_geo_map(coords, lat_column="lat", lon_column="lon", value_column="signed", open_after=False)

    assert r["success"] is False
    assert "scattergeo" not in r["error"], "plotly's own error text reached the caller"
    assert "negative" in r["error"]
    assert "signed" in r["error"]


def test_the_error_counts_the_offending_rows(coords):
    r = generate_geo_map(coords, lat_column="lat", lon_column="lon", value_column="signed", open_after=False)
    assert "2 negative value(s)" in r["error"]


def test_the_hint_names_the_argument_that_is_wrong(coords):
    r = generate_geo_map(coords, lat_column="lat", lon_column="lon", value_column="signed", open_after=False)

    hint = r["hint"]
    # The old hint sent the reader to the geo columns, which were never at fault.
    assert "color_column" in hint
    assert "abs_values" in hint
    assert "signed" in hint


def test_a_non_negative_value_column_still_works(coords, tmp_path):
    r = generate_geo_map(
        coords,
        lat_column="lat",
        lon_column="lon",
        value_column="positive",
        output_path=str(tmp_path / "map.html"),
        open_after=False,
    )
    assert r["success"] is True
    assert r["rows_plotted"] == 60


def test_no_value_column_at_all_still_works(coords, tmp_path):
    r = generate_geo_map(
        coords,
        lat_column="lat",
        lon_column="lon",
        output_path=str(tmp_path / "map2.html"),
        open_after=False,
    )
    assert r["success"] is True


def test_zero_is_not_negative(coords, tmp_path):
    # A size of 0 is legal; the check must not reject a column of zeros.
    df = pd.read_csv(coords)
    df["zeros"] = 0.0
    p = tmp_path / "zeros.csv"
    df.to_csv(p, index=False)

    r = generate_geo_map(
        str(p),
        lat_column="lat",
        lon_column="lon",
        value_column="zeros",
        output_path=str(tmp_path / "map3.html"),
        open_after=False,
    )
    assert r["success"] is True
