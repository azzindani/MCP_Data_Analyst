"""Two tools that quietly did less than asked, and now say so.

**feature_engineering(features=["one_hot"])** had two limits stacked and
reported neither: text columns with more than ten distinct values were skipped,
and of whatever survived only the first five were encoded. On the reference
dataset that is 5 of 12 text columns encoded, a response reading "8 new
columns", and nothing whatsoever about the seven it declined. A caller who
asked for one-hot encoding got a partly encoded frame and no way to find out.

Both limits are right -- one-hot on 16,834 distinct creative names is not what
anyone means -- so both stay. What changes is that the tool names what it
skipped and why.

**load_geo_dataset(rename_column=...)** renames a column literally called
"name", which the schema does not say and the docstring never mentions:
rename_column is the NEW name, and the old one is hardcoded. On a GeoJSON whose
label column is called anything else the argument did nothing at all, in
silence -- a sweep renaming "site" got a file byte-identical to the no-argument
call, and success both times. Its `keep_columns` had the same shape: names that
were not in the file were dropped from the keep list without comment.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_basic")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from servers.data_basic.engine import load_geo_dataset  # noqa: E402
from servers.data_medium._med_transform import _ONE_HOT_MAX_COLUMNS, _ONE_HOT_MAX_LEVELS  # noqa: E402
from servers.data_transform.engine import feature_engineering  # noqa: E402

# --- one_hot names what it left alone ---------------------------------------


@pytest.fixture
def wide(tmp_path):
    """Eight low-cardinality text columns and one high-cardinality one."""
    f = tmp_path / "wide.csv"
    rows = 40
    data = {f"cat{i}": [f"v{j % 3}" for j in range(rows)] for i in range(8)}
    data["many"] = [f"u{j}" for j in range(rows)]  # 40 distinct
    data["n"] = list(range(rows))
    pd.DataFrame(data).to_csv(f, index=False)
    return f


def test_one_hot_reports_the_columns_it_encoded(tmp_path, wide):
    r = feature_engineering(str(wide), features=["one_hot"], output_path=str(tmp_path / "o.csv"))
    assert r["success"] is True, r.get("error")
    assert len(r["one_hot_encoded"]) == _ONE_HOT_MAX_COLUMNS
    assert r["one_hot_encoded"] == [f"cat{i}" for i in range(_ONE_HOT_MAX_COLUMNS)]


def test_one_hot_reports_the_columns_it_skipped_and_why(tmp_path, wide):
    r = feature_engineering(str(wide), features=["one_hot"], output_path=str(tmp_path / "o.csv"))
    skipped = r["one_hot_skipped"]
    # The high-cardinality one, for a different reason than the over-cap ones.
    assert "many" in skipped
    assert str(_ONE_HOT_MAX_LEVELS) in skipped["many"]
    assert "40 distinct" in skipped["many"]
    # And the ones past the per-call cap.
    for i in range(_ONE_HOT_MAX_COLUMNS, 8):
        assert f"cat{i}" in skipped, f"cat{i} was skipped without being reported"
        assert str(_ONE_HOT_MAX_COLUMNS) in skipped[f"cat{i}"]


def test_the_skipping_is_warned_about_not_only_recorded(tmp_path, wide):
    """A field in the response is easy to miss; a warn is not."""
    r = feature_engineering(str(wide), features=["one_hot"], output_path=str(tmp_path / "o.csv"))
    warnings = [p for p in r["progress"] if p.get("status") == "warn"]
    assert any("not one-hot encoded" in p["message"] for p in warnings), r["progress"]


def test_nothing_skipped_means_nothing_reported(tmp_path):
    f = tmp_path / "narrow.csv"
    pd.DataFrame({"a": ["x", "y", "x"], "b": ["p", "q", "p"], "n": [1, 2, 3]}).to_csv(f, index=False)
    r = feature_engineering(str(f), features=["one_hot"], output_path=str(tmp_path / "o.csv"))
    assert r["success"] is True
    assert r["one_hot_skipped"] == {}
    assert sorted(r["one_hot_encoded"]) == ["a", "b"]
    assert not [p for p in r["progress"] if p.get("status") == "warn" and "one-hot" in p.get("message", "")]


def test_the_encoding_itself_is_unchanged(tmp_path):
    """The limits are reported, not relaxed: the same columns get the same dummies."""
    f = tmp_path / "n.csv"
    pd.DataFrame({"a": ["x", "y", "x"], "n": [1, 2, 3]}).to_csv(f, index=False)
    out = tmp_path / "o.csv"
    feature_engineering(str(f), features=["one_hot"], output_path=str(out))
    written = pd.read_csv(out)
    assert {"a_x", "a_y"} <= set(written.columns)
    assert list(written["a_x"]) == [1, 0, 1]


# --- load_geo_dataset says when an argument did nothing ---------------------


def _geojson(path: Path, label_key: str) -> Path:
    path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {label_key: "Empire State", "pop": 5},
                        "geometry": {"type": "Point", "coordinates": [-73.9, 40.7]},
                    }
                ],
            }
        )
    )
    return path


pytest.importorskip("geopandas")


def test_renaming_a_column_that_is_not_there_says_so(tmp_path):
    r = load_geo_dataset(str(_geojson(tmp_path / "site.geojson", "site")), rename_column="geom")
    assert r["success"] is True
    assert r["renamed_from"] == "", "nothing was renamed, so nothing should be claimed"
    assert "geom" not in r["columns"]
    warnings = [p for p in r["progress"] if p.get("status") == "warn"]
    assert any("nothing to rename" in p["message"] for p in warnings), r["progress"]
    # The warning has to name the columns that ARE there, or it only says no.
    assert any("site" in str(p.get("detail", "")) for p in warnings)


def test_renaming_a_column_that_is_there_still_works(tmp_path):
    r = load_geo_dataset(str(_geojson(tmp_path / "name.geojson", "name")), rename_column="geom")
    assert r["success"] is True
    assert r["renamed_from"] == "name"
    assert "geom" in r["columns"]
    assert "name" not in r["columns"]


def test_keeping_a_column_that_is_not_there_says_so(tmp_path):
    r = load_geo_dataset(str(_geojson(tmp_path / "k.geojson", "name")), keep_columns=["name", "nope"])
    assert r["success"] is True
    assert r["columns_not_found"] == ["nope"]
    assert "name" in r["columns"]
    warnings = [p for p in r["progress"] if p.get("status") == "warn"]
    assert any("nope" in str(p.get("detail", "")) for p in warnings), r["progress"]


def test_a_clean_call_reports_no_surprises(tmp_path):
    r = load_geo_dataset(str(_geojson(tmp_path / "c.geojson", "name")), keep_columns=["name"])
    assert r["success"] is True
    assert r["columns_not_found"] == []
    assert r["renamed_from"] == ""
