"""A dashboard must know what a column is before it decides what to do with it.

Two defects, both from treating the column's storage type as its meaning:

**Every integer was a quantity.** customer_id, zip and a pandas row counter all
became KPI cards ("Total customer_id") and chart values, because each is a
number. An identifier is now recognised by the last word of its name or by
being a row counter, and is never summed, averaged or charted as a value.

**No CSV date was ever a date.** The loader leaves "2024-01-05" as text, so the
dashboard never drew a time series from a CSV, and a column of 90 dates became
a pie chart and a filter. Date columns are now read as dates.

What each column was taken to be comes back as `column_roles`, so a wrong
guess is visible in the response and fixed with one override.
"""

from __future__ import annotations

import re
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

from shared.column_utils import is_identifier, parse_date_column  # noqa: E402


@pytest.fixture
def orders(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    rng = np.random.default_rng(8)
    n = 120
    path = tmp_path / "orders.csv"
    pd.DataFrame(
        {
            "Unnamed: 0": np.arange(n),
            "customer_id": rng.integers(10_000, 99_999, n),
            "zip": rng.choice([10115, 20095, 80331], n),
            "order_date": pd.date_range("2024-01-01", periods=n).strftime("%Y-%m-%d"),
            "region": rng.choice(["North", "South"], n),
            "revenue": rng.normal(500, 80, n).round(2),
            "units": rng.integers(1, 20, n),
        }
    ).to_csv(path, index=False)
    return path


def kpi_labels(html: str) -> list[str]:
    return re.findall(r'<div class="kpi-lbl">([^<]+)</div>', html)


def cards(html: str) -> list[str]:
    return re.findall(r'<div id="([^"]+)" style="width:100%;height:100%"></div>', html)


def build(path, **kw) -> tuple[dict, str]:
    r = generate_dashboard(str(path), open_after=False, **kw)
    assert r["success"] is True, r
    return r, Path(r["output_path"]).read_text(encoding="utf-8")


class TestIdentifiers:
    def test_no_identifier_becomes_a_kpi_or_a_value(self, orders):
        r, html = build(orders)
        labels = " ".join(kpi_labels(html))
        for ident in ("customer_id", "zip", "Unnamed: 0"):
            assert ident not in labels, f"{ident} became a KPI"
            assert ident not in r["kpi_columns"]
        assert "Total revenue" in labels and "units" in labels

    def test_roles_come_back(self, orders):
        r, _ = build(orders)
        roles = r["column_roles"]
        assert roles["customer_id"] == roles["zip"] == roles["Unnamed: 0"] == "identifier"
        assert roles["revenue"] == roles["units"] == "measure"
        assert roles["order_date"] == "date"
        assert roles["region"] == "dimension"

    def test_an_override_says_a_column_is_a_quantity_after_all(self, orders):
        r, html = build(orders, agg_overrides=["zip:sum"])
        assert r["column_roles"]["zip"] == "measure"
        assert "Total zip" in kpi_labels(html)

    def test_a_spec_may_still_name_an_identifier(self, orders):
        _, html = build(orders, spec={"kpis": ["customer_id"]})
        assert kpi_labels(html) == ["Quality Score", "Total customer_id"]

    @pytest.mark.parametrize("name", ["customer_id", "zip_code", "orderKey", "sku", "Account ID"])
    def test_identifier_names(self, name):
        assert is_identifier(name, pd.Series([3, 9, 4, 12]))

    @pytest.mark.parametrize("name", ["paid", "valid_count", "key_accounts", "zip_share_pct", "revenue"])
    def test_names_that_are_not_identifiers(self, name):
        assert not is_identifier(name, pd.Series([3, 9, 4, 12]))

    def test_a_row_counter_is_an_identifier_without_a_telling_name(self):
        assert is_identifier("row", pd.Series(range(50)))
        assert not is_identifier("row", pd.Series(list(range(49)) + [60])), "a gap is not a counter"
        assert not is_identifier("n", pd.Series(range(5))), "too few rows to tell"
        assert not is_identifier("name", pd.Series(["a", "b"])), "text is never a numeric identifier"


class TestDates:
    def test_a_csv_date_gets_a_time_series(self, orders):
        _, html = build(orders)
        assert any(c.startswith("ts_order_date_") for c in cards(html))
        assert "pie_order_date" not in cards(html), "a date is not a category"

    @pytest.mark.parametrize(
        "values",
        [
            ["2024-01-05", "2024-02-11", "2024-03-30"],
            ["01/05/2024", "02/11/2024", "03/30/2024"],
            ["5 Jan 2024", "11 Feb 2024", "30 Mar 2024"],
            ["2024-01-05 10:30:00", "2024-02-11 08:00:00", "2024-03-30 23:59:59"],
        ],
    )
    def test_date_text_is_read_as_dates(self, values):
        parsed = parse_date_column(pd.Series(values * 10))
        assert parsed is not None and pd.api.types.is_datetime64_any_dtype(parsed)

    @pytest.mark.parametrize(
        "values",
        [
            ["North", "South", "East"],
            ["1.2.3", "2.0.1", "10.4.7"],
            ["192.168.1.100", "10.0.0.1", "172.16.4.20"],
            ["SKU-A1", "SKU-B2", "SKU-C3"],
        ],
    )
    def test_text_that_is_not_a_date_is_left_alone(self, values):
        assert parse_date_column(pd.Series(values * 10)) is None

    def test_a_column_that_is_mostly_not_dates_is_left_alone(self):
        values = ["2024-01-05"] * 5 + ["n/a", "unknown", "tbd", "later", "soon"] * 3
        assert parse_date_column(pd.Series(values)) is None

    def test_numbers_are_never_dates(self):
        assert parse_date_column(pd.Series([20240101, 20240102])) is None
