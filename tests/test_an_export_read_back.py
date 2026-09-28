"""An exported workbook reads back as its data, by this server and by pandas.

export_data(format="excel") put its README sheet first. pd.read_excel(file)
read the README (6 rows of metadata), and convert_file turned the export back
into a 6-row CSV, ignoring the data sheet. The data is now the first sheet and
the README is the one the workbook opens on.
"""

from __future__ import annotations

import openpyxl
import pandas as pd

from servers.data_advanced._adv_charts import export_data
from servers.data_ingest.engine import convert_file


def test_the_round_trip(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
    frame = pd.DataFrame({"region": ["a", "b", "c"], "units": [1, 2, 3]})
    csv = tmp_path / "sales.csv"
    frame.to_csv(csv, index=False)
    out = tmp_path / "sales.xlsx"
    assert export_data(str(csv), output_path=str(out), format="excel", open_after=False)["success"] is True

    assert pd.read_excel(out).equals(frame), "pandas' default sheet is the data"
    back = convert_file(str(out), "csv", output_path=str(tmp_path / "back.csv"))
    assert (back["success"], back["rows"], back["sheet"]) == (True, 3, "Data")
    assert openpyxl.load_workbook(out).active.title == "README"
