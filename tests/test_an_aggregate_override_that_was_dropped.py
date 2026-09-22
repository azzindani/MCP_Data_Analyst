"""An aggregate the caller asked for must be honoured or refused -- never dropped.

generate_dashboard(agg_overrides=[...]) is how a caller corrects the detected
aggregate ("units should be averaged, not summed"). The parser kept only the
entries it recognised and discarded the rest without a word:

    ["units:count", "units:median", "revenue=mean", "nosuchcol:sum"]
    -> {"nosuchcol": "sum"}

So a caller who asked for a count got the detected sum, drawn on the page under
success: true, and the one entry that survived named a column that does not
exist. Every problem is now refused by name, all at once.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_advanced")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _adv_dashboard import generate_dashboard  # noqa: E402

from shared.column_utils import OVERRIDE_AGGS, parse_agg_overrides  # noqa: E402


class TestTheParser:
    def test_valid_entries_are_kept(self):
        assert parse_agg_overrides(["revenue:sum", " rate : MEAN "]) == {"revenue": "sum", "rate": "mean"}

    def test_nothing_means_nothing(self):
        assert parse_agg_overrides(None) == {}
        assert parse_agg_overrides([]) == {}

    def test_aliases_and_an_equals_sign_are_understood(self):
        got = parse_agg_overrides(["a=avg", "b:average", "c:total", "d:maximum", "e:minimum"])
        assert got == {"a": "mean", "b": "mean", "c": "sum", "d": "max", "e": "min"}

    @pytest.mark.parametrize("item", ["units:count", "units:median"])
    def test_an_aggregate_it_cannot_draw_is_refused_with_the_ones_it_can(self, item):
        with pytest.raises(ValueError) as exc:
            parse_agg_overrides([item])
        message = str(exc.value)
        assert item in message
        for agg in OVERRIDE_AGGS:
            assert agg in message

    def test_an_entry_with_no_separator_is_refused(self):
        with pytest.raises(ValueError, match="has no ':'"):
            parse_agg_overrides(["revenue mean"])

    def test_a_non_string_entry_is_refused(self):
        with pytest.raises(ValueError, match="not a 'column:agg' string"):
            parse_agg_overrides([{"revenue": "mean"}])  # type: ignore[list-item]

    def test_an_unknown_column_is_refused_with_the_real_ones(self):
        with pytest.raises(ValueError) as exc:
            parse_agg_overrides(["nosuchcol:sum"], ["revenue", "units"])
        assert "nosuchcol" in str(exc.value)
        assert "revenue, units" in str(exc.value)

    def test_every_problem_is_named_in_one_refusal(self):
        with pytest.raises(ValueError) as exc:
            parse_agg_overrides(["units:count", "revenue mean", "nosuchcol:sum"], ["revenue", "units"])
        message = str(exc.value)
        assert "units:count" in message and "revenue mean" in message and "nosuchcol" in message


@pytest.fixture
def sales(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(3)
    n = 120
    path = tmp_path / "sales.csv"
    pd.DataFrame(
        {
            "region": rng.choice(["North", "South", "East"], n),
            "revenue": rng.normal(1000, 200, n).round(2),
            "units": rng.integers(1, 50, n),
        }
    ).to_csv(path, index=False)
    return path


class TestTheDashboard:
    def test_a_refused_override_writes_nothing(self, sales, tmp_path):
        out = tmp_path / "dash.html"
        r = generate_dashboard(str(sales), output_path=str(out), open_after=False, agg_overrides=["units:median"])
        assert r["success"] is False
        assert "median" in r["error"]
        assert not out.exists()

    def test_an_honoured_override_reaches_the_page(self, sales, tmp_path):
        # Asserted on the page, not on the response: the KPI card's label is
        # what a reader sees.
        out = tmp_path / "dash.html"
        r = generate_dashboard(str(sales), output_path=str(out), open_after=False, agg_overrides=["revenue:mean"])
        assert r["success"] is True, r
        html = out.read_text(encoding="utf-8")
        assert "Avg revenue" in html
        assert "Total revenue" not in html
