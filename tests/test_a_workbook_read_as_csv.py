"""A workbook handed to a CSV tool is refused by its format, not parsed as text.

pandas reads any bytes as CSV. The sweep handed inspect_dataset an .xlsx that
export_data had written: success, 89 rows, one column named `PK\\x03\\x04\\x14` --
the zip signature. A larger workbook failed as "Error tokenizing data. C error:
Buffer overflow caught", with a hint about banner lines above the header. Only
load_dataset checked the extension. The check now sits in read_csv, which every
CSV read goes through, and looks at the bytes as well as the name.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_basic"), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from servers.data_advanced._adv_eda import run_eda  # noqa: E402
from servers.data_basic.engine import inspect_dataset, read_column_stats  # noqa: E402
from servers.data_statistics.engine import extended_stats, statistical_test  # noqa: E402
from shared.file_utils import NotATextTableError, read_csv  # noqa: E402

FRAME = pd.DataFrame({"region": ["a", "b", "a", "b"] * 5, "units": range(20), "price": [1.5, 2.5] * 10})


@pytest.fixture
def files(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    xlsx = tmp_path / "sales.xlsx"
    FRAME.to_excel(xlsx, index=False)
    disguised = tmp_path / "renamed.csv"
    disguised.write_bytes(xlsx.read_bytes())
    parquet = tmp_path / "sales.parquet"
    FRAME.to_parquet(parquet, index=False)
    csv = tmp_path / "sales.csv"
    FRAME.to_csv(csv, index=False)
    return {"xlsx": str(xlsx), "disguised": str(disguised), "parquet": str(parquet), "csv": str(csv)}


CALLS = [
    ("inspect_dataset", lambda f: inspect_dataset(f)),
    ("read_column_stats", lambda f: read_column_stats(f, "units")),
    ("extended_stats", lambda f: extended_stats(f)),
    ("statistical_test", lambda f: statistical_test(f, test="t_test", column_a="units", group_column="region")),
    ("run_eda", lambda f: run_eda(f, open_after=False)),
]


@pytest.mark.parametrize(("name", "call"), CALLS, ids=[c[0] for c in CALLS])
@pytest.mark.parametrize("kind", ["xlsx", "disguised", "parquet"])
def test_refused_by_format_with_the_way_through(files, name, call, kind):
    r = call(files[kind])
    assert r["success"] is False, f"{name} parsed a {kind} as CSV: {r}"
    assert "not CSV text" in r["error"]
    assert "convert_file" in r["hint"]


def test_the_zip_signature_wins_over_a_csv_name(files):
    with pytest.raises(NotATextTableError, match="renamed.csv is a zip container"):
        read_csv(files["disguised"])


@pytest.mark.parametrize(("name", "call"), CALLS, ids=[c[0] for c in CALLS])
def test_a_real_csv_still_reads(files, name, call):
    assert call(files["csv"])["success"] is True
