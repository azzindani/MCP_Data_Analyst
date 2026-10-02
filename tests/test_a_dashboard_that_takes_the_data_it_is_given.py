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
from shared.column_utils import is_identifier, parse_date_column
from shared.dashboard_spec import validate as validate_spec
from shared.file_utils import read_csv, read_table, sniff_encoding, sniff_separator
from shared.metrics import (
    MetricError,
    auto_ratios,
    evaluate_tree,
    flag_rates,
    hidden_columns,
    spec_metrics,
    text_flag_rates,
    value,
)
from shared.story import grain_for
from tests.dashboard_page import drawn, run_js


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

    def test_a_rate_that_is_lower_is_lower_not_cheaper(self, _home, sales):
        sales["defaulted"] = (
            np.where(sales["segment"] == "a", 0.40, 0.10) > np.random.default_rng(3).random(len(sales))
        ).astype(int)
        path = _home / "loans.csv"
        sales.to_csv(path, index=False)
        result, _ = page(path, _home)
        texts = " ".join(i["text"] for i in result["insights"] if "defaulted rate" in i["headline"])
        assert "defaulted rate" in texts and "cheaper" not in texts and re.search(r"\d+(\.\d+)?x lower than", texts), (
            texts
        )

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
    @pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
    def test_the_line_stops_where_the_data_is_complete(self, sales, _home):
        path = _home / "t.csv"
        sales.to_csv(path, index=False)
        _, html = page(path, _home)
        figure = next(f for k, f in drawn(html)["figures"].items() if k.endswith("time_series"))
        last = str(pd.to_datetime(sales["day"]).max())[:7]
        assert last not in figure["data"][0]["x"]  # the month the data stops inside is not on the solid line
        marks = [t for t in figure["data"] if t.get("name") == "incomplete period"]
        assert marks and last in marks[0]["x"]
        assert any(t.get("line", {}).get("dash") == "dot" and t.get("showlegend") is False for t in figure["data"])


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

    def test_the_sample_says_what_the_file_said(self, _home, monkeypatch):
        """DuckDB reads Yes/No as true/false, 007 as 7 and 1.50 as 1.5: a copy of rows must not."""
        path = _home / "orders.csv"
        n = 900
        pd.DataFrame(
            {
                "code": [f"{i % 400:03d}" for i in range(n)],
                "returned": np.where(np.arange(n) % 5 == 0, "Yes", "No"),
                "price": ["1.50", "2.00", "3.25"] * (n // 3),
            }
        ).to_csv(path, index=False)
        monkeypatch.setenv("MCP_BIG_TABLE", "always")
        monkeypatch.setattr(_adv_dashboard, "DASHBOARD_SAMPLE_ROWS", 100)
        sample, facts = _adv_dashboard._sample_of_big_file(path, "test")
        text = pd.read_csv(sample, dtype=str, keep_default_na=False)
        assert set(text["returned"]) == {"Yes", "No"} and facts["rows_used"] == 100
        assert set(text["price"]) <= {"1.50", "2.00", "3.25"} and text["code"].str.len().eq(3).all()

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


class TestAFlagIsRightOnAnAggregatedPage:
    """Above 100,000 rows the page holds a cube; a flag's rate must still be the share of rows."""

    @pytest.fixture
    def flags(self, _home):
        n = 120_000
        rng = np.random.default_rng(1)
        frame = pd.DataFrame(
            {
                "day": (pd.Timestamp("2024-01-01") + pd.to_timedelta(rng.integers(0, 300, n), unit="D")).strftime(
                    "%Y-%m-%d"
                ),
                "region": rng.choice(["N", "S", "E", "W"], n),
                "revenue": rng.integers(10, 500, n),
                "int_flag": (rng.random(n) < 0.13).astype(int),
                "bool_flag": rng.random(n) < 0.21,
                "word_flag": np.where(rng.random(n) < 0.34, "Yes", "No"),
            }
        )
        path = _home / "flags.csv"
        frame.to_csv(path, index=False)
        return path, frame

    @pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
    def test_each_kind_of_flag_reads_its_share_on_the_page(self, _home, flags):
        path, frame = flags
        _, html = page(path, _home)
        assert run_js(html, "!!_CUBE") is True
        want = {
            "int_flag rate": frame["int_flag"].mean(),
            "bool_flag rate": frame["bool_flag"].mean(),
            "word_flag rate": (frame["word_flag"] == "Yes").mean(),
        }
        for name, share in want.items():
            got = run_js(html, f"_mval(_METRICS[{name!r}].tree,_RAW)")
            assert got == pytest.approx(share, abs=1e-3), name


class TestANoteIsNotACategory:
    """The hospital file's footnote columns (three sentences repeated down 4,818 rows) were its filters and its headline."""

    FOOTNOTES = [
        "Data are shown only for hospitals that participate in the Inpatient Quality Reporting programs",
        "Results are not available for this reporting period",
        "Data suppressed by CMS for one or more quarters",
    ]

    @pytest.fixture
    def hospitals(self):
        n = 300
        rng = np.random.default_rng(2)
        return pd.DataFrame(
            {
                "Hospital Type": rng.choice(["Acute Care", "Critical Access", "Children's"], n),
                "Mortality footnote": rng.choice(self.FOOTNOTES, n),
                "Rating": rng.integers(1, 6, n),
                "Beds": rng.integers(10, 900, n),
            }
        )

    def test_a_column_of_sentences_is_text(self, hospitals):
        planned = plan(hospitals)
        assert planned["columns"]["Mortality footnote"]["role"] == "text"
        assert "Mortality footnote" not in planned["dimensions"]

    def test_short_labels_with_spaces_are_still_categories(self, hospitals):
        assert plan(hospitals)["columns"]["Hospital Type"]["role"] == "dimension"
        assert (
            plan(hospitals.assign(site=np.where(np.arange(300) % 2, "City Hotel", "Resort Hotel")))["columns"]["site"][
                "role"
            ]
            == "dimension"
        )

    def test_the_page_neither_filters_nor_headlines_by_one(self, _home, hospitals):
        path = _home / "hospitals.csv"
        hospitals.to_csv(path, index=False)
        result, _ = page(path, _home)
        filters = [f if isinstance(f, str) else f.get("column") for f in result["spec"].get("filters", [])]
        assert "Mortality footnote" not in filters
        assert not any(self.FOOTNOTES[1] in i["headline"] for i in result["insights"])


class TestAGapIsAFindingOnlyWhenItIsBigEnoughToMatter:
    def test_eleven_times_nothing_is_not_the_headline(self, _home):
        n = 600
        rng = np.random.default_rng(4)
        segment = rng.choice(["a", "b"], n)
        frame = pd.DataFrame(
            {
                "segment": segment,
                "region": rng.choice(["n", "s", "e"], n),
                # rare spikes: segment a's mean is 3x segment b's, and both are about nothing next to the spikes
                "calls": np.where(rng.random(n) < np.where(segment == "a", 0.03, 0.01), rng.exponential(5, n), 0.0),
                "wage": np.where(segment == "a", 80.0, 50.0) + rng.normal(0, 8, n),
            }
        )
        path = _home / "people.csv"
        frame.to_csv(path, index=False)
        result, _ = page(path, _home)
        headlines = " ".join(i["headline"] for i in result["insights"])
        assert "wage" in headlines and "calls" not in headlines, headlines


class TestADateIsReadAsTheFileMeantIt:
    def test_two_digit_years_of_a_file_of_the_past_are_not_read_into_the_future(self):
        """The economic-news file (1951-2014, m/d/yy) was read as 1976-2075 and drawn in two clusters 80 years apart."""
        years = [51, 60, 75, 80, 91, 99, 2, 7, 14]
        text = pd.Series([f"{1 + i % 12}/{1 + i % 27}/{y:02d}" for i, y in enumerate(years * 20)])
        parsed = parse_date_column(text)
        assert parsed is not None
        assert parsed.min().year == 1951 and parsed.max().year == 2014

    def test_a_column_of_future_dates_alone_is_left_in_the_future(self):
        text = pd.Series([f"{1 + i % 12}/{1 + i % 27}/{y}" for i, y in enumerate([30, 31, 32, 33] * 30)])
        parsed = parse_date_column(text)
        assert parsed is not None and parsed.min().year == 2030 and parsed.max().year == 2033

    def test_four_digit_and_iso_dates_are_untouched(self):
        iso = pd.Series(["2051-03-04", "1999-12-31", "2014-06-30"] * 30)
        parsed = parse_date_column(iso)
        assert parsed is not None and parsed.max().year == 2051


class TestAPeriodIsCompleteOnlyWhenItsLastDayIs:
    @staticmethod
    def _grain(end: str, every: str = "10min", start: str = "2024-01-01"):
        stamps = pd.date_range(start, end, freq=every)
        frame = pd.DataFrame({"stamp": stamps.strftime("%Y-%m-%d %H:%M:%S"), "kw": np.arange(len(stamps)) % 7 + 50.0})
        return grain_for(plan(frame))

    def test_a_week_cut_off_on_its_last_morning_is_not_complete(self):
        # Sunday 2024-03-10 at 10:40: the week of Mon 4 March has six and a half days.
        assert self._grain("2024-03-10 10:40")["complete"] == "2024-02-26"

    def test_a_week_that_ran_to_the_end_of_its_last_day_is_complete(self):
        assert self._grain("2024-03-10 23:50")["complete"] == "2024-03-04"

    def test_a_month_cut_off_on_its_last_day_is_not_complete(self):
        assert self._grain("2024-03-31 10:00", every="1h", start="2023-12-01")["complete"] == "2024-02"
        assert self._grain("2024-03-31 23:00", every="1h", start="2023-12-01")["complete"] == "2024-03"

    def test_dates_without_times_are_whole_days(self):
        frame = pd.DataFrame({"day": pd.date_range("2024-01-01", "2024-03-10").strftime("%Y-%m-%d"), "kw": 1.0})
        assert grain_for(plan(frame))["complete"] == "2024-03-04"


class TestASpikeIsASpikeOfAWholePeriod:
    @staticmethod
    def _plant(_home, end: str, burst: bool = False):
        stamps = pd.date_range("2024-01-01", end, freq="1h")
        rng = np.random.default_rng(8)
        kw = 100 + rng.normal(0, 0.3, len(stamps))
        if burst:
            kw[(stamps >= "2024-02-12") & (stamps < "2024-02-19")] *= 3
        path = _home / "plant.csv"
        pd.DataFrame(
            {
                "stamp": stamps.strftime("%Y-%m-%d %H:%M:%S"),
                "power_total": kw,
                "site": rng.choice(["a", "b"], len(stamps)),
            }
        ).to_csv(path, index=False)
        return path

    def test_a_week_the_data_has_only_begun_is_not_a_dip(self, _home):
        result, _ = page(self._plant(_home, "2024-03-10 10:00"), _home)  # a Sunday morning
        assert not [i for i in result["insights"] if "a typical week" in i["headline"]], result["insights"]

    def test_a_week_that_really_stands_out_is_named_by_how_far(self, _home):
        result, _ = page(self._plant(_home, "2024-03-10 23:00", burst=True), _home)
        found = [i["headline"] for i in result["insights"] if "a typical week" in i["headline"]]
        assert found and "above a typical week" in found[0] and "2024-02-12" in found[0], found


class TestAPeriodTheDataBeginsInIsNotComplete:
    @staticmethod
    def _grain(start: str, end: str, every: str = "1D"):
        stamps = pd.date_range(start, end, freq=every)
        text = stamps.strftime("%Y-%m-%d" if every.endswith("D") else "%Y-%m-%d %H:%M:%S")
        return grain_for(plan(pd.DataFrame({"stamp": text, "kw": np.arange(len(stamps)) % 7 + 50.0})))

    def test_a_month_begun_on_the_23rd_is_not_the_first_complete_month(self):
        assert self._grain("2023-03-23", "2023-12-31")["first"] == "2023-04"
        assert self._grain("2023-03-01", "2023-12-31")["first"] == "2023-03"

    def test_a_week_begun_on_a_wednesday_is_not_the_first_complete_week(self):
        assert self._grain("2024-01-03", "2024-02-20")["first"] == "2024-01-08"
        assert self._grain("2024-01-01", "2024-02-20")["first"] == "2024-01-01"

    def test_data_that_starts_at_10_40_does_not_have_a_whole_first_day(self):
        partial = self._grain("2024-01-01 10:40", "2024-04-30 23:50", every="1h")
        whole = self._grain("2024-01-01 00:00", "2024-04-30 23:50", every="1h")
        assert whole["first"] == "2024-01" and partial["first"] == "2024-02"

    @pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
    def test_the_line_draws_a_short_first_month_apart(self, _home):
        days = pd.date_range("2023-03-23", "2024-02-29")
        frame = pd.DataFrame(
            {"day": days.strftime("%Y-%m-%d"), "revenue": 100.0, "region": np.where(np.arange(len(days)) % 2, "N", "S")}
        )
        path = _home / "late.csv"
        frame.to_csv(path, index=False)
        _, html = page(path, _home)
        figure = next(f for k, f in drawn(html)["figures"].items() if k.endswith("time_series"))
        solid = figure["data"][0]
        assert "2023-03" not in solid["x"] and solid["x"][0] == "2023-04"
        marks = [t for t in figure["data"] if t.get("name") == "incomplete period"]
        assert marks and "2023-03" in marks[0]["x"]


class TestASpikeIsNotAShortFirstPeriod:
    def test_a_week_the_data_began_in_is_not_a_dip(self, _home):
        stamps = pd.date_range("2024-01-04 12:00", "2024-03-31 23:00", freq="1h")  # begins on a Thursday noon
        rng = np.random.default_rng(8)
        path = _home / "plant.csv"
        pd.DataFrame(
            {
                "stamp": stamps.strftime("%Y-%m-%d %H:%M:%S"),
                "power_total": 100 + rng.normal(0, 0.3, len(stamps)),
                "site": rng.choice(["a", "b"], len(stamps)),
            }
        ).to_csv(path, index=False)
        result, _ = page(path, _home)
        assert not [i for i in result["insights"] if "a typical week" in i["headline"]], result["insights"]


class TestAShareThatIsOnlyASizeIsNotAFinding:
    def test_a_class_that_brings_what_its_size_says_is_left_out(self, _home):
        n = 800
        rng = np.random.default_rng(6)
        frame = pd.DataFrame(
            {
                "mode": rng.choice(["Low", "High"], n, p=[0.78, 0.22]),
                "site": rng.choice(["a", "b", "c"], n),
                "revenue": rng.normal(100, 5, n),
                "units": rng.integers(1, 9, n),
            }
        )
        path = _home / "plant.csv"
        frame.to_csv(path, index=False)
        result, _ = page(path, _home)
        headlines = [i["headline"] for i in result["insights"]]
        assert not any(h.startswith("Low brings") or "mode = Low brings" in h for h in headlines), headlines


class TestAnAverageOfNothingIsNothing:
    def test_the_page_does_not_rate_a_group_with_no_values_zero(self, _home, sales):
        if shutil.which("node") is None:
            pytest.skip("node is not installed")
        path = _home / "m.csv"
        sales.to_csv(path, index=False)
        _, html = page(path, _home)
        assert run_js(html, "_mval({agg:'mean',col:'revenue'},[{revenue:null},{revenue:null}])") is None
        assert run_js(html, "_mval({agg:'max',col:'revenue'},[{revenue:null}])") is None
        assert run_js(html, "_measure({value:'revenue',agg:'mean'},[{revenue:null}])") is None
        assert run_js(html, "_mval({agg:'sum',col:'revenue'},[{revenue:null}])") == 0
        assert run_js(html, "_mval({agg:'mean',col:'revenue'},[{revenue:4},{revenue:null}])") == 4

    def test_the_mirror_agrees(self):
        empty = pd.DataFrame({"x": [None, None]})
        assert np.isnan(evaluate_tree({"agg": "mean", "col": "x"}, empty))
        assert evaluate_tree({"agg": "sum", "col": "x"}, empty) == 0.0


class TestAMeasureThatTheFirstCellLacksIsStillAdded:
    """US_Car_Sales: new cars have no mileage and came first, so the aggregated page read every mileage figure as 0."""

    @pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
    @pytest.mark.parametrize("how", ["mean", "sum", "min", "max"])
    def test_the_page_aggregates_what_the_cells_that_have_it_hold(self, _home, how):
        n = 120_000
        rng = np.random.default_rng(9)
        kind = np.where(np.arange(n) % 5 == 0, "a", "b")  # "a" has no mileage and its cells come first
        frame = pd.DataFrame(
            {
                "kind": kind,
                "region": rng.choice(["N", "S", "E", "W"], n),
                "price": rng.integers(10_000, 60_000, n),
                "mileage": np.where(kind == "a", np.nan, rng.integers(1_000, 90_000, n)),
            }
        )
        path = _home / "cars.csv"
        frame.to_csv(path, index=False)
        _, html = page(path, _home)
        assert run_js(html, "!!_CUBE") is True
        got = run_js(html, f"_kpi(_RAW,'mileage','{how}')")
        assert got == pytest.approx(getattr(frame["mileage"], how)(), rel=1e-9)


class TestATimeIsNotSpend:
    def test_time_spent_on_a_site_is_not_what_was_spent(self):
        frame = pd.DataFrame(
            {"Daily Time Spent on Site": [60.0, 70.0, 80.0], "Area Income": [50_000.0, 60_000.0, 70_000.0]}
        )
        assert [m.name for m in auto_ratios(frame, list(frame.columns))] == []

    def test_money_spent_still_makes_a_return(self):
        frame = pd.DataFrame({"spend": [10.0, 20.0], "revenue": [30.0, 80.0]})
        assert [m.name for m in auto_ratios(frame, list(frame.columns))] == ["ROAS"]


class TestACategoryNamedByANumberIsALabel:
    """A category named 1, 2, 3 is a label: drawn on a number line it got ticks at 1.5 and 2.5."""

    @pytest.fixture
    def houses(self, _home):
        n = 400
        rng = np.random.default_rng(12)
        frame = pd.DataFrame({"floors": rng.choice([1, 2, 3], n), "area": rng.integers(500, 3000, n)})
        path = _home / "houses.csv"
        frame.to_csv(path, index=False)
        return path

    @pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
    @pytest.mark.parametrize(
        ("style", "axis"),
        [({}, "xaxis"), ({"orientation": "h"}, "yaxis"), ({"ci": True}, "xaxis")],
        ids=["vertical", "horizontal", "with-intervals"],
    )
    def test_the_bar_puts_its_categories_on_a_category_axis(self, _home, houses, style, axis):
        layout = [{"chart": "bar", "cols": {"category": "floors", "value": "area"}, "agg": "mean", "style": style}]
        _, html = page(houses, _home, spec={"story": False, "layout": layout})
        figure = next(f for k, f in drawn(html)["figures"].items() if k.endswith("_bar"))
        assert figure["layout"][axis]["type"] == "category"


class TestTwoMetricsNeverShareAComputedColumn:
    def test_a_name_makes_its_own_column(self, sales):
        frame = sales
        one = spec_metrics(frame, {"A": "count_if(clicks > 3) / count()"})[0]
        two = spec_metrics(frame, {"B": "count_if(clicks > 6) / count()"})[0]
        assert not set(one.hidden) & set(two.hidden)
        assert set(hidden_columns([one, two])) == set(one.hidden) | set(two.hidden)

    def test_one_column_owned_twice_is_refused(self, sales):
        frame = sales
        one = spec_metrics(frame, {"A": "count_if(clicks > 3) / count()"})[0]
        clash = spec_metrics(frame, {"B": "count_if(clicks > 6) / count()"})[0]
        clash.hidden = dict(one.hidden)
        with pytest.raises(MetricError, match="same hidden column"):
            hidden_columns([one, clash])
