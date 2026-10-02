"""Growth year on year, where the trend is heading, whether customers come back, and what one costs.

A founder pitching puts four numbers in front of an investor that the sweep's
dashboard had no way to draw: growth against the same month a year before
(not against last month, which the season moves), a forecast with an honest
range, retention by cohort, and the cost of acquiring a customer. Each is
checked here against the arithmetic it claims.
"""

from __future__ import annotations

import shutil
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

from shared.metrics import auto_ratios  # noqa: E402
from shared.story import cohorts, noun  # noqa: E402
from tests.dashboard_page import drawn, run_js  # noqa: E402

needs_node = pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")


def _monthly(months: int = 20) -> pd.DataFrame:
    """One order a day for `months` months, revenue growing 5% a month, ending mid-month."""
    days = pd.date_range("2023-01-01", periods=months * 30 + 14, freq="D")
    rng = np.random.default_rng(2)
    return pd.DataFrame(
        {
            "order_date": days.strftime("%Y-%m-%d"),
            "channel": rng.choice(["web", "app"], len(days)),
            "revenue": (100 * 1.05 ** (np.arange(len(days)) / 30) + rng.normal(0, 5, len(days))).round(2),
        }
    )


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.delenv("MCP_CONSTRAINED_MODE", raising=False)
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    return tmp_path


def _page(home, df, name="d", **kw):
    df.to_csv(home / f"{name}.csv", index=False)
    r = generate_dashboard(str(home / f"{name}.csv"), output_path=str(home / f"{name}.html"), open_after=False, **kw)
    assert r["success"] is True, r.get("error")
    return r, Path(r["output_path"]).read_text(encoding="utf-8")


def test_growth_is_against_the_same_month_a_year_before(home):
    df = _monthly()
    r, _ = _page(home, df)
    yoy = next(i for i in r["insights"] if "year on year" in i["headline"])
    month = pd.to_datetime(df["order_date"]).dt.strftime("%Y-%m")
    cur = sorted(m for m in month.unique() if m < month.max())[-1]
    prior = f"{int(cur[:4]) - 1}{cur[4:]}"
    a, b = df.loc[month == cur, "revenue"].sum(), df.loc[month == prior, "revenue"].sum()
    assert yoy["headline"] == f"revenue up {abs(a - b) / b:.0%} year on year in {cur}"


@needs_node
def test_the_forecast_extends_the_complete_months_with_its_range(home):
    df = _monthly()
    r, html = _page(home, df)
    ts = next(p for p in r["spec"]["layout"] if p["chart"] == "time_series")
    assert ts["style"]["forecast"] == 3
    fig = next(f for cid, f in drawn(html)["figures"].items() if cid.endswith("time_series"))
    fc = next(t for t in fig["data"] if t.get("name") == "forecast")
    month = pd.to_datetime(df["order_date"]).dt.strftime("%Y-%m")
    series = df.groupby(month)["revenue"].sum()
    complete = series.iloc[:-1].iloc[-24:]
    slope, intercept = np.polyfit(np.arange(len(complete)), complete.values, 1)
    want = [intercept + slope * (len(complete) - 1 + h) for h in (1, 2, 3)]
    assert fc["y"][1:] == pytest.approx(want, rel=1e-6)
    assert fc["x"][0] == complete.index[-1] and len(fc["x"]) == 4
    band = [t for t in fig["data"] if t.get("line", {}).get("width") == 0]
    lo, hi = band[0]["y"][1:], band[1]["y"][1:]
    assert all(a < m < b for a, m, b in zip(lo, fc["y"][1:], hi, strict=True))


@needs_node
@pytest.mark.parametrize(("dip", "floored"), [(0, True), (-50, False)], ids=["spend", "profit"])
def test_a_series_never_below_zero_is_not_forecast_below_it(home, dip, floored):
    _, html = _page(home, _monthly(8))
    past = [100, 600, 650, 620, 400, 10, 8, dip]
    months = [f"2024-{m:02d}" for m in range(1, 9)]
    fc = run_js(html, f"_forecast({months},{past},'month',3,null).map(function(t){{return t.y.slice(1);}})")
    slope, intercept = np.polyfit(np.arange(8), past, 1)
    line = [intercept + slope * (7 + h) for h in (1, 2, 3)]
    assert max(line) < 0
    assert fc[2] == pytest.approx([max(0, f) for f in line] if floored else line)
    assert min(fc[0]) >= 0 if floored else min(fc[0]) < 0


@needs_node
@pytest.mark.parametrize(
    ("last", "grain", "want"),
    [
        ("2023-11", "month", ["2023-12", "2024-01", "2024-02"]),
        ("2023-Q4", "quarter", ["2024-Q1", "2024-Q2", "2024-Q3"]),
        ("2024-02-26", "week", ["2024-03-04", "2024-03-11", "2024-03-18"]),
        ("2023", "year", ["2024", "2025", "2026"]),
    ],
)
def test_the_periods_after_the_last_are_labelled_as_the_page_labels_them(home, last, grain, want):
    _, html = _page(home, _monthly(8))
    assert run_js(html, f"_after({last!r},{grain!r},3)") == want


def test_retention_is_counted_by_first_month():
    rows = [
        ("a", "2024-01-05"), ("a", "2024-02-10"), ("a", "2024-03-01"),
        ("b", "2024-01-20"), ("b", "2024-03-03"),
        ("c", "2024-02-02"), ("c", "2024-03-09"),
        ("d", "2024-02-11"),
    ]  # fmt: skip
    df = pd.DataFrame(rows, columns=["customer_id", "day"])
    got = cohorts(df, "customer_id", "day")
    assert got["cohorts"] == ["2024-01", "2024-02"] and got["sizes"] == [2, 2]
    assert got["matrix"][0][:3] == [1.0, 0.5, 1.0], "a came back in February; a and b in March"
    assert got["matrix"][1][:3] == [1.0, 0.5, None], "February's cohort has no third month yet"
    assert got["noun"] == "customer" and noun("Account ID") == "Account"


def test_a_storyline_with_returning_customers_shows_their_retention(home):
    rng = np.random.default_rng(4)
    rows = []
    for c in range(60):
        start = pd.Timestamp("2024-01-01") + pd.Timedelta(days=int(rng.integers(0, 120)))
        for k in range(int(rng.integers(2, 6))):
            rows.append(
                {
                    "user_id": f"U{c:03d}",
                    "day": (start + pd.Timedelta(days=30 * k)).strftime("%Y-%m-%d"),
                    "amount": 10.0,
                }
            )
    r, html = _page(home, pd.DataFrame(rows), name="users")
    panel = next(p for p in r["spec"]["layout"] if p["chart"] == "cohort")
    assert panel["cols"] == {"id": "user_id", "date": "day"} and panel["title"] == "Retention of users by first month"
    assert '<div class="cohort-wrap">' in html


def test_a_cohort_panel_needs_who_and_when(home):
    df = pd.DataFrame({"user_id": ["a", "b"] * 5, "day": pd.date_range("2024-01-01", periods=10).strftime("%Y-%m-%d")})
    df.to_csv(home / "c.csv", index=False)
    r = generate_dashboard(
        str(home / "c.csv"), spec={"layout": [{"chart": "cohort", "cols": {"id": "user_id"}}]}, open_after=False
    )
    assert r["success"] is False and "cohort chart and needs cols for: date" in r["error"]


def test_the_cost_of_a_new_customer_is_spend_over_new_customers():
    df = pd.DataFrame({"spend": [100.0, 300.0], "new_customers": [4, 6]})
    (cac,) = [m for m in auto_ratios(df, ["spend", "new_customers"]) if m.name == "CAC"]
    assert cac.formula == "sum(spend) / sum(new_customers)" and cac.better == "down"


def test_a_forecast_is_up_to_twelve_periods(home):
    df = _monthly(8)
    df.to_csv(home / "f.csv", index=False)
    layout = [{"chart": "line", "cols": {"date": "order_date", "value": "revenue"}, "style": {"forecast": 13}}]
    r = generate_dashboard(str(home / "f.csv"), spec={"layout": layout}, open_after=False)
    assert r["success"] is False and "forecast" in r["error"] and "0 to 12" in r["error"]
