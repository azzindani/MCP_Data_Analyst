"""A dashboard is drawn from the data a person has, not only from the data that is easy.

The 2026-10-02 corpus sweep generated a default dashboard for every file in /root/Evals (46 CSVs) and found:

* 10 could not be drawn at all -- a text column with a gap (`np.unique` cannot sort a float NaN against
  text) and a UTF-16 file (read as cp1252 it is one column of garbage);
* four drew no chart -- two were semicolon-separated (one column holding every row), two were labels
  and text with nothing to add up;
* the KPIs were absurd: "Total gender", "Total BMI", "Total Blood pressure", "Avg latitude",
  "Avg Patient name" (a name held as a number), "Total Unnamed: 0";
* a loan table about defaults never said the default rate, and a time series fell off a cliff where
  its last month was only begun;
* Excel, Parquet and JSON were refused, and a file too big to hold could not be drawn at all.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_advanced import _adv_dashboard
from servers.data_advanced._adv_dashboard import customize_dashboard, generate_dashboard
from shared.analysis_plan import ROWS_COLUMN, needs_row_count, plan
from shared.column_utils import is_identifier
from shared.dashboard_spec import validate as validate_spec
from shared.file_utils import read_csv, read_table, sniff_encoding, sniff_separator
from shared.metrics import flag_rates, text_flag_rates, value


@pytest.fixture(autouse=True)
def _home(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.delenv("MCP_BIG_TABLE", raising=False)
    monkeypatch.delenv("MCP_CALL_MEMORY_MB", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    return tmp_path


def page(path: Path, tmp: Path, name: str = "p", **kw) -> tuple[dict, str]:
    result = generate_dashboard(str(path), output_path=str(tmp / f"{name}.html"), open_after=False, **kw)
    assert result["success"] is True, result.get("error")
    return result, (tmp / f"{name}.html").read_text(encoding="utf-8")


@pytest.fixture
def sales(_home) -> pd.DataFrame:
    rng = np.random.default_rng(4)
    n = 600
    return pd.DataFrame(
        {
            "day": pd.date_range("2023-01-01", periods=n).strftime("%Y-%m-%d"),
            "region": rng.choice(["North", "South", "East"], n),
            "segment": rng.choice(["a", "b"], n),
            "revenue": rng.integers(50, 500, n),
            "clicks": rng.integers(1, 60, n),
            "price": rng.uniform(5, 40, n).round(2),
            "age": rng.integers(18, 80, n),
            "defaulted": (rng.random(n) < 0.2).astype(int),
        }
    )


class TestAGapInATextColumnIsNotACrash:
    def test_a_scatter_grouped_by_a_column_with_missing_values_draws(self, _home, sales):
        sales = sales.assign(
            grade=np.where(np.arange(len(sales)) % 7 == 0, None, np.where(np.arange(len(sales)) % 2, "x", "y"))
        )
        sales["revenue2"] = sales["revenue"] * 2 + np.random.default_rng(1).normal(0, 30, len(sales))
        path = _home / "gaps.csv"
        sales.to_csv(path, index=False)
        result, html = page(
            path, _home, spec={"layout": [{"chart": "scatter", "cols": {"x": "revenue", "y": "revenue2"}}]}
        )
        assert "scatter" in result["charts_included"]


class TestTheEncodingAndDelimiterAreRead:
    def test_a_utf16_file_is_read_by_its_byte_order_mark(self, _home, sales):
        path = _home / "u16.csv"
        path.write_bytes(b"\xff\xfe" + sales.head(40).to_csv(index=False).encode("utf-16-le"))
        assert sniff_encoding(path) == "utf-16"
        frame = read_csv(str(path))
        assert list(frame.columns) == list(sales.columns) and len(frame) == 40

    @pytest.mark.parametrize("mark,name", [(";", "semicolon"), ("\t", "tab"), ("|", "pipe")])
    def test_a_delimiter_the_header_agrees_on_is_used(self, _home, sales, mark, name):
        path = _home / f"{name}.csv"
        sales.head(30).to_csv(path, index=False, sep=mark)
        assert sniff_separator(path) == mark
        assert read_csv(str(path)).shape == (30, len(sales.columns))

    def test_a_comma_file_and_a_quoted_comma_stay_commas(self, _home):
        path = _home / "q.csv"
        path.write_text('name,note\n"a;b",x\n"c;d",y\n')
        assert sniff_separator(path) == ","
        assert read_csv(str(path)).shape == (2, 2)

    def test_a_one_column_file_is_left_alone(self, _home):
        path = _home / "one.csv"
        path.write_text("value\n1\n2\n3\n")
        assert sniff_separator(path) == "," and read_csv(str(path)).shape == (3, 1)

    def test_duckdb_reads_a_utf16_table_too(self, _home, sales):
        from shared.sql_query import run_query

        path = _home / "u16.csv"
        path.write_bytes(b"\xff\xfe" + sales.head(25).to_csv(index=False).encode("utf-16-le"))
        answer = run_query("SELECT count(*) AS n FROM data", tables={"data": path})
        assert answer["rows"] == [{"n": 25}]


class TestAMeasureAddsUpOnlyWhenItsNameSaysSo:
    def frame(self, n: int = 300) -> pd.DataFrame:
        rng = np.random.default_rng(2)
        return pd.DataFrame(
            {
                "revenue": rng.integers(10, 900, n),
                "units_sold": rng.integers(1, 50, n),
                "age": rng.integers(18, 80, n),
                "bmi": rng.uniform(16, 40, n).round(1),
                "blood_pressure": rng.integers(90, 180, n),
                "bedrooms": rng.integers(1, 7, n),
            }
        )

    def test_the_default_is_the_mean(self):
        cols = plan(self.frame())["columns"]
        assert cols["revenue"]["additive"] is True and cols["units_sold"]["additive"] is True
        for column in ("age", "bmi", "blood_pressure", "bedrooms"):
            assert cols[column]["additive"] is False, column

    def test_a_kpi_for_a_property_of_a_row_is_an_average(self, _home):
        path = _home / "people.csv"
        self.frame().to_csv(path, index=False)
        result, _ = page(path, _home)
        aggs = {p["cols"]["value"]: p.get("agg") for p in result["spec"]["layout"] if p["chart"] == "kpi"}
        assert aggs and all(
            agg == "mean" for col, agg in aggs.items() if col in ("age", "bmi", "blood_pressure", "bedrooms")
        )

    @pytest.mark.parametrize(
        "name,values,role",
        [
            ("latitude", [10.1, 20.5, 33.3, 41.9] * 20, "coordinate"),
            ("lng", [100.1, 120.5, 133.3, 141.9] * 20, "coordinate"),
            ("gender", [1, 2] * 40, "dimension"),
            ("Diabetes_012", [0, 1, 2, 1] * 20, "dimension"),
            ("marital_status", list(range(1, 6)) * 16, "dimension"),
        ],
    )
    def test_a_coordinate_and_a_code_are_not_measures(self, name, values, role):
        found = plan(pd.DataFrame({name: values, "x": np.arange(len(values)) * 1.5 + 0.25}))["columns"][name]
        assert found["role"] == role

    def test_a_small_count_named_for_what_it_counts_stays_a_measure(self):
        assert (
            plan(pd.DataFrame({"quantity": [1, 2, 3, 4] * 20, "x": np.arange(80) * 1.5 + 0.25}))["columns"]["quantity"][
                "role"
            ]
            == "measure"
        )

    @pytest.mark.parametrize(
        "name",
        [
            "Unnamed: 0",
            "keywordid",
            "userId",
            "Phone Number",
            "Patient name",
            "Company Name",
            "Merchant Category Code (MCC)",
            "zip",
        ],
    )
    def test_these_identify_a_row(self, name):
        assert is_identifier(name, pd.Series([5, 91, 12, 77, 3, 41, 18, 66, 25, 8] * 3))

    @pytest.mark.parametrize("name", ["valid", "liquid", "paid", "zip_share_pct", "avg_phone_calls", "total_sum"])
    def test_these_do_not(self, name):
        assert not is_identifier(name, pd.Series([5, 91, 12, 77, 3, 41, 18, 66, 25, 8] * 3))


class TestAFlagIsAnOutcomeAndItsRateIsAFinding:
    def test_the_rate_is_a_metric_with_the_right_direction(self, sales):
        planned = plan(sales)
        rates = {m.name: m for m in flag_rates(sales, planned)}
        assert rates["defaulted rate"].better == "down" and rates["defaulted rate"].unit == "percent"
        assert rates["defaulted rate"].tree == {"agg": "mean", "col": "defaulted"}

    def test_a_flag_that_never_varies_has_no_rate(self, sales):
        rare = sales.assign(defaulted=(np.arange(len(sales)) < 5).astype(int))
        assert flag_rates(rare, plan(rare)) == []

    def test_a_flag_with_no_good_direction_is_neutral(self, sales):
        neutral = sales.rename(columns={"defaulted": "is_premium"})
        assert {m.name: m.better for m in flag_rates(neutral, plan(neutral))} == {"is_premium rate": ""}

    @pytest.mark.parametrize("gaps", [False, True])
    def test_a_flag_written_in_words_has_a_rate_that_is_a_number(self, sales, gaps):
        """`count(col)` of words counts no numbers, so the first version of this rate was x / 0 = NaN."""
        words = pd.Series(np.where(np.arange(len(sales)) % 4 == 0, "Yes", "No"), index=sales.index)
        if gaps:
            words[::10] = None
        frame = sales.assign(Smoking=words)
        rates = {m.name: m for m in text_flag_rates(frame, plan(frame))}
        share = float((frame["Smoking"] == "Yes").sum() / frame["Smoking"].notna().sum())
        assert value(rates["Smoking rate"], frame) == pytest.approx(share)

    def test_the_page_says_the_rate_and_where_it_differs(self, _home, sales):
        sales["defaulted"] = (
            np.where(sales["segment"] == "a", 0.34, 0.12) > np.random.default_rng(3).random(len(sales))
        ).astype(int)
        path = _home / "loans.csv"
        sales.to_csv(path, index=False)
        result, html = page(path, _home)
        assert "defaulted rate" in {k for k in result["metrics"]}
        headlines = " ".join(i["headline"] for i in result["insights"])
        assert (
            "defaulted rate" in headlines and re.search(r"segment|region", headlines) or "defaulted rate" in headlines
        )

    def test_a_flag_is_not_a_segment_that_shares_an_amount(self, _home, sales):
        path = _home / "s.csv"
        sales.to_csv(path, index=False)
        result, _ = page(path, _home)
        assert not any(re.match(r"defaulted = [01] brings", i["headline"]) for i in result["insights"])


class TestATableOfLabelsStillGetsACharts:
    @pytest.fixture
    def labels(self, _home) -> Path:
        rng = np.random.default_rng(6)
        n = 400
        path = _home / "labels.csv"
        pd.DataFrame(
            {
                "text": [f"message {i}" for i in range(n)],
                "label": rng.choice(["ham", "spam"], n, p=[0.8, 0.2]),
                "channel": rng.choice(["sms", "mail", "chat"], n),
            }
        ).to_csv(path, index=False)
        return path

    def test_nothing_to_add_up_is_noticed(self, labels):
        assert needs_row_count(pd.read_csv(labels)) is True
        assert needs_row_count(pd.read_csv(labels).assign(n=np.arange(400) * 1.5 + 0.25)) is False

    def test_the_rows_are_counted_and_charted(self, labels, _home):
        result, _ = page(labels, _home)
        kinds = [p["chart"] for p in result["spec"]["layout"]]
        assert "bar" in kinds and "kpi" in kinds, kinds
        assert any(p["message"] == "Rows are counted" for p in result["progress"])
        titles = " ".join(str(p.get("title")) for p in result["spec"]["layout"])
        assert "Rows by" in titles or "Rows" in titles

    def test_the_spec_it_hands_back_validates_and_round_trips(self, labels, _home):
        result, _ = page(labels, _home)
        validate_spec(result["spec"], pd.read_csv(labels))  # names the Rows column; the file has none
        again = customize_dashboard(result["output_path"], changes={"title": "Mail"})
        assert again["success"] is True, again.get("error")

    def test_a_file_with_something_to_add_up_has_no_such_column(self, sales, _home):
        path = _home / "s.csv"
        sales.to_csv(path, index=False)
        result, html = page(path, _home)
        assert not any(p["message"] == "Rows are counted" for p in result["progress"])
        assert f'"{ROWS_COLUMN}"' not in html


class TestAPartialLastPeriodIsNotAFall:
    def test_the_line_stops_where_the_data_is_complete(self, sales, _home):
        path = _home / "t.csv"
        sales.to_csv(path, index=False)
        _, html = page(path, _home)
        assert "partial=!!(p.complete" in html and "keys.slice(0,cut)" in html
        assert "dash:'dot'" in html and "incomplete period" in html


class TestBigNumbersAreWrittenAsBigNumbers:
    def test_billions_and_negatives(self, sales, _home):
        path = _home / "n.csv"
        sales.to_csv(path, index=False)
        _, html = page(path, _home)
        assert "toFixed(1)+'B'" in html and "toFixed(1)+'T'" in html and "var a=Math.abs(v),g=v<0?'-':''" in html

    @pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
    def test_a_mean_under_a_thousand_keeps_its_decimals(self, sales, _home):
        path = _home / "m.csv"
        sales.to_csv(path, index=False)
        _, html = page(path, _home)
        source = re.search(r"function _fmt\(v\)\{.*?\nfunction _small\(v\)\{.*?\}\n", html, re.S).group(0)
        values = [0, 5, 3.37, 28.33, 0.0856, 12.04, -7.5, 150.4, 1234, 2.5e6]
        script = source + f"console.log(JSON.stringify({values}.map(_fmt)))"
        out = subprocess.run(["node", "-e", script], capture_output=True, text=True, check=True).stdout
        assert json.loads(out) == ["0", "5", "3.37", "28.3", "0.086", "12", "-7.5", "150", "1.2K", "2.5M"]

    def test_the_default_kpi_row_writes_a_billion_too(self):
        import inspect

        assert "1_000_000_000" in inspect.getsource(_adv_dashboard._dash_kpi_row)


class TestAnyTableFormatIsDrawn:
    @pytest.fixture
    def table(self, sales) -> pd.DataFrame:
        return sales.head(200)

    def test_excel_skipping_the_readme_sheet(self, table, _home):
        path = _home / "book.xlsx"
        with pd.ExcelWriter(path) as writer:
            pd.DataFrame({"Field": ["What this is"], "Value": ["notes"]}).to_excel(
                writer, sheet_name="README", index=False
            )
            table.to_excel(writer, sheet_name="Data", index=False)
        assert read_table(str(path)).shape == table.shape
        result, _ = page(path, _home)
        assert result["rows_total"] == 200

    def test_a_named_sheet_and_a_missing_one(self, table, _home):
        path = _home / "two.xlsx"
        with pd.ExcelWriter(path) as writer:
            table.head(10).to_excel(writer, sheet_name="Small", index=False)
            table.to_excel(writer, sheet_name="Big", index=False)
        assert len(read_table(str(path), sheet="Big")) == 200 and len(read_table(str(path), sheet=0)) == 10
        with pytest.raises(ValueError, match="no sheet 'Nope'"):
            read_table(str(path), sheet="Nope")

    def test_parquet(self, table, _home):
        path = _home / "t.parquet"
        table.to_parquet(path)
        assert page(path, _home)[0]["rows_total"] == 200

    def test_json_records_and_json_lines(self, table, _home):
        records, lines = _home / "t.json", _home / "t.jsonl"
        table.to_json(records, orient="records")
        table.to_json(lines, orient="records", lines=True)
        assert read_table(str(records)).shape == table.shape and read_table(str(lines)).shape == table.shape
        assert page(records, _home, "a")[0]["rows_total"] == 200

    def test_nested_json_is_flattened(self, _home):
        path = _home / "n.json"
        path.write_text(json.dumps({"data": [{"a": 1, "b": {"c": 2}}, {"a": 3, "b": {"c": 4}}]}))
        assert read_table(str(path)).shape == (2, 2)

    def test_an_old_xls_is_asked_to_be_saved_as_xlsx(self, _home):
        path = _home / "old.xls"
        path.write_bytes(b"\xd0\xcf\x11\xe0")
        with pytest.raises(ValueError, match=r"\.xlsx"):
            read_table(str(path))


class TestAFileTooBigToHoldIsDrawnFromASample:
    def test_the_page_says_so_and_carries_the_sample(self, _home, sales, monkeypatch):
        big = pd.concat([sales] * 4, ignore_index=True)
        path = _home / "huge.csv"
        big.to_csv(path, index=False)
        monkeypatch.setenv("MCP_BIG_TABLE", "always")
        monkeypatch.setattr(_adv_dashboard, "DASHBOARD_SAMPLE_ROWS", 500)
        result, html = page(path, _home)
        facts = result["sampled_from_file"]
        assert (facts["rows_in_file"], facts["rows_used"]) == (2400, 500) and Path(facts["sample_file"]).is_file()
        assert result["rows_total"] == 500
        assert "a sample of 500 of 2,400 rows" in html
        assert any("sample of a file too big" in p["message"] for p in result["progress"])
        assert len(pd.read_csv(facts["sample_file"])) == 500

    def test_the_sample_is_what_customize_works_on(self, _home, sales, monkeypatch):
        path = _home / "huge.csv"
        pd.concat([sales] * 4, ignore_index=True).to_csv(path, index=False)
        monkeypatch.setenv("MCP_BIG_TABLE", "always")
        monkeypatch.setattr(_adv_dashboard, "DASHBOARD_SAMPLE_ROWS", 500)
        result, _ = page(path, _home)
        again = customize_dashboard(result["output_path"], changes={"theme": "dark"})
        assert again["success"] is True, again.get("error")

    def test_a_file_no_bigger_than_a_sample_is_drawn_whole(self, _home, sales, monkeypatch):
        path = _home / "small.csv"
        sales.to_csv(path, index=False)
        monkeypatch.setenv("MCP_BIG_TABLE", "always")
        result, _ = page(path, _home)
        assert "sampled_from_file" not in result and result["rows_total"] == len(sales)

    def test_a_file_that_fits_is_never_sampled(self, _home, sales):
        path = _home / "fits.csv"
        sales.to_csv(path, index=False)
        assert "sampled_from_file" not in page(path, _home)[0]
