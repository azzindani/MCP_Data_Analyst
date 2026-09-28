"""A placeholder is a missing value in disguise, and the alerts say so.

The sweep's ad data holds "'-" in 15,101 of 16,834 audience_type rows, "device"
in 1,733 rows of the device column (a header repeated inside the data), and
"Undetermined" for age. None of it is null, so the EDA report showed
"Top: '-:15101" with no alert, the dashboard offered "'-" as a filter, and
validate_dataset passed the columns. Placeholders are now an alert in the EDA
report and the dashboard, and an issue in validate_dataset.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from servers.data_medium._med_inspect import validate_dataset  # noqa: E402
from shared.data_alerts import alerts_for_frame, placeholder_counts  # noqa: E402

N = 100
FRAME = pd.DataFrame(
    {
        "audience_type": ["'-"] * 90 + ["Lookalike"] * 10,
        "device": ["device"] * 10 + ["Mobile"] * 45 + ["Desktop"] * 45,
        "age": ["Undetermined"] * 30 + ["25-34"] * 70,
        "platform": ["Google"] * 50 + ["Facebook"] * 50,
        "spend": range(N),
    }
)


def _placeholders(df: pd.DataFrame) -> dict[str, dict]:
    cats = [c for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]
    alerts = alerts_for_frame(df, ["spend"], cats)
    return {a["col"]: a for a in alerts if a["type"] == "PLACEHOLDER"}


def test_each_disguise_is_an_alert_with_its_weight():
    found = _placeholders(FRAME)
    assert set(found) == {"audience_type", "device", "age"}
    assert found["audience_type"]["sev"] == "error"  # 90%
    assert found["age"]["sev"] == "warning"  # 30%
    assert found["device"]["sev"] == "warning"  # 10%, but a header row inside the data
    assert "90 of 100 values (90.0%)" in found["audience_type"]["msg"]
    assert '"\'-" 90' in found["audience_type"]["msg"]


def test_a_real_category_is_not_a_placeholder():
    assert placeholder_counts(FRAME["platform"], "platform") == {}
    assert placeholder_counts(pd.Series(["Unknown Pleasures", "N/A-rated"]), "album") == {}


@pytest.mark.parametrize("value", ["-", "'-", "N/A", " n/a ", "Unknown", "UNDETERMINED", "(not set)", "—"])
def test_the_spellings(value):
    assert placeholder_counts(pd.Series([value, "x"]), "col") == {value: 1}


def test_validate_dataset_counts_them_against_the_null_limit(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
    FRAME.to_csv(tmp_path / "ads.csv", index=False)
    r = validate_dataset(str(tmp_path / "ads.csv"), max_null_pct=20.0)
    assert r["success"] is True
    assert r["placeholder_summary"] == {
        "audience_type": {"'-": 90},
        "device": {"device": 10},
        "age": {"Undetermined": 30},
    }
    by_column = {i["column"]: i for i in r["issues"] if "placeholders" in i["issue"]}
    assert by_column["audience_type"]["severity"] == "error"
    assert by_column["device"]["severity"] == "warning"
    assert r["passed"] is False
