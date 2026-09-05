"""Every generated page says which rows it was drawn from.

The user review's cross-cutting note on the nine HTML artifacts:

    every HTML needs `rows_plotted / rows_total / was_sampled / data_hash` in
    header JSON

The dashboard and the EDA report carried that block. The seven single-chart
pages did not -- which is exactly backwards, because a single chart is the
artifact most likely to travel alone, and a chart drawn from a sample looks
identical to one drawn from everything.

The distributions page is the one the review singled out for sampling (6.9 MB
for three columns), and it is where the two halves have to be held apart:

    AGI: downsample to 5k points, note sampling; print skew/kurtosis on chart
    (income 31.07) for vision models.

**The points are sampled; the statistics are not.** Skew 31.07 on a lognormal
tail is precisely the figure sampling makes unstable, so computing it from the
5,000 drawn rows would have printed an estimate in the shape of an exact number.
The captions therefore report `n` as the full column count, and the tests below
pin that: a caption whose `n` matched the plotted rows would mean the statistic
came from the sample.
"""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_visual import engine as dv
from shared.plotly_payload import load_figure
from shared.provenance import read_provenance

ROWS = 12_000


@pytest.fixture()
def skewed(tmp_path: Path) -> Path:
    rng = np.random.default_rng(0)
    csv = tmp_path / "Credit_Risk.csv"
    pd.DataFrame(
        {
            "income": rng.lognormal(11, 1.4, ROWS),
            "loan_amount": rng.integers(500, 35_000, ROWS),
            "int_rate": rng.random(ROWS) * 20,
        }
    ).to_csv(csv, index=False)
    return csv


def plot(skewed: Path, tmp_path: Path, **kwargs):
    out = tmp_path / "dist.html"
    result = dv.generate_distribution_plot(str(skewed), output_path=str(out), open_after=False, **kwargs)
    assert result["success"] is True, result.get("error")
    return result, out.read_text(encoding="utf-8")


def captions(html: str) -> list[str]:
    _data, layout = load_figure(html)
    return [a["text"] for a in layout.get("annotations", []) if str(a.get("xref", "")).endswith("domain")]


class TestThePageCarriesItsProvenance:
    def test_the_header_is_in_the_page(self, skewed, tmp_path):
        _result, html = plot(skewed, tmp_path)
        header = read_provenance(html)
        assert header, "a chart with no provenance cannot be checked against its source"

    def test_it_names_all_four_fields_the_review_asked_for(self, skewed, tmp_path):
        _result, html = plot(skewed, tmp_path)
        header = read_provenance(html)
        assert set(header) >= {"rows_plotted", "rows_total", "was_sampled", "data_hash"}

    def test_was_sampled_is_derived_from_the_two_counts(self, skewed, tmp_path):
        _result, html = plot(skewed, tmp_path)
        header = read_provenance(html)
        assert header["was_sampled"] == (header["rows_plotted"] < header["rows_total"])

    def test_the_response_repeats_what_the_page_says(self, skewed, tmp_path):
        result, html = plot(skewed, tmp_path)
        header = read_provenance(html)
        for field in ("rows_plotted", "rows_total", "was_sampled"):
            assert result[field] == header[field]

    def test_it_names_the_tool_and_the_source(self, skewed, tmp_path):
        _result, html = plot(skewed, tmp_path)
        header = read_provenance(html)
        assert header["tool"] == "generate_distribution_plot"
        assert header["source"] == skewed.name

    def test_the_hash_identifies_the_rows_that_were_drawn(self, skewed, tmp_path):
        """Two different samples of the same file must not share a hash."""
        a, html_a = plot(skewed, tmp_path, max_points=2000)
        b, html_b = plot(skewed, tmp_path, max_points=4000)
        assert read_provenance(html_a)["data_hash"] != read_provenance(html_b)["data_hash"]
        assert a["rows_plotted"] == 2000 and b["rows_plotted"] == 4000


class TestTheOtherChartPagesCarryItToo:
    """save_chart is the shared path; adding the header there covers all of them."""

    def test_the_shared_writer_accepts_a_header(self):
        import inspect

        from shared.html_theme import save_chart

        assert "header" in inspect.signature(save_chart).parameters

    def test_a_page_written_without_one_is_still_valid_html(self, skewed, tmp_path):
        out = tmp_path / "corr.html"
        result = dv.generate_correlation_heatmap(str(skewed), output_path=str(out), open_after=False)
        assert result["success"] is True, result.get("error")
        html = out.read_text(encoding="utf-8")
        assert html.rstrip().endswith("</html>")
        assert read_provenance(html) == {}


class TestThePointsAreSampledAndTheStatisticsAreNot:
    def test_a_large_file_is_downsampled_by_default(self, skewed, tmp_path):
        result, _html = plot(skewed, tmp_path)
        assert result["rows_plotted"] == 5000
        assert result["rows_total"] == ROWS
        assert result["was_sampled"] is True

    def test_the_statistics_come_from_every_row(self, skewed, tmp_path):
        result, _html = plot(skewed, tmp_path)
        for column, stats in result["shape_stats"].items():
            assert stats["n"] == ROWS, f"{column} reported n={stats['n']}, so its skew came from the sample"

    def test_the_skew_matches_the_whole_column(self, skewed, tmp_path):
        result, _html = plot(skewed, tmp_path)
        truth = pd.read_csv(skewed)
        for column, stats in result["shape_stats"].items():
            assert math.isclose(stats["skew"], round(float(truth[column].skew()), 4), rel_tol=1e-6)

    def test_the_caption_on_the_chart_says_the_same_n(self, skewed, tmp_path):
        """A vision model reads the panel, not the response body."""
        _result, html = plot(skewed, tmp_path)
        drawn = captions(html)
        assert len(drawn) == 3
        for text in drawn:
            assert f"n {ROWS:,}" in text, text

    def test_the_caption_carries_both_shape_numbers(self, skewed, tmp_path):
        _result, html = plot(skewed, tmp_path)
        for text in captions(html):
            assert "skew" in text and "kurt" in text

    def test_sampling_is_reported_in_progress(self, skewed, tmp_path):
        result, _html = plot(skewed, tmp_path)
        joined = " ".join(str(p) for p in result["progress"])
        assert "Downsampled" in joined
        assert "from all rows" in joined


class TestTheCapIsAskable:
    def test_zero_means_every_point(self, skewed, tmp_path):
        result, _html = plot(skewed, tmp_path, max_points=0)
        assert result["rows_plotted"] == ROWS
        assert result["was_sampled"] is False

    def test_a_small_file_is_never_sampled(self, tmp_path):
        csv = tmp_path / "small.csv"
        pd.DataFrame({"x": range(100)}).to_csv(csv, index=False)
        result, _html = plot(csv, tmp_path)
        assert result["was_sampled"] is False
        assert result["shape_stats"]["x"]["n"] == 100

    def test_sampling_shrinks_the_page(self, skewed, tmp_path):
        """The point of the exercise, measured rather than assumed."""
        _sampled, small = plot(skewed, tmp_path, max_points=1000)
        _full, large = plot(skewed, tmp_path, max_points=0)
        assert len(small) < len(large)


class TestAStatisticWithNoValueSaysSo:
    def test_a_constant_column_reports_none_rather_than_nan(self, tmp_path):
        """`float('nan')` serialises to the bare token NaN, which is not JSON."""
        csv = tmp_path / "flat.csv"
        pd.DataFrame({"same": [7] * 200}).to_csv(csv, index=False)
        result, _html = plot(csv, tmp_path)
        stats = result["shape_stats"]["same"]
        assert stats["skew"] is None
        assert stats["n"] == 200

    def test_a_column_with_no_shape_gets_no_caption_numbers(self, tmp_path):
        csv = tmp_path / "flat.csv"
        pd.DataFrame({"same": [7] * 200}).to_csv(csv, index=False)
        _result, html = plot(csv, tmp_path)
        assert captions(html) == []
