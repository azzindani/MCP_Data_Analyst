"""Aggregate Hotel_Bookings_Demand.csv into data.json, the summary every mockup draws from.

    python design/mockups/build_data.py /path/to/Hotel_Bookings_Demand.csv

Definitions, stated once and used by every mockup:
  booking        one row
  canceled       is_canceled == 1 (no-shows included)
  stayed         is_canceled == 0 (reservation_status == Check-Out)
  room nights    stays_in_weekend_nights + stays_in_week_nights, stayed bookings
  room revenue   adr x room nights, stayed bookings, in euros
  realized ADR   room revenue / room nights
  month          arrival month (arrival_date_year + arrival_date_month)
  YTD            Jan-Aug 2017 against Jan-Aug 2016; the data ends 31 Aug 2017
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

OUT = Path(__file__).parent / "data.json"
MONTHS = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]
LEAD_ORDER = ["0-7 d", "8-30 d", "31-90 d", "91-180 d", "181-365 d", "365+ d"]
WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def r(x, d=1):
    return None if pd.isna(x) else round(float(x), d)


def lead_bucket(days: int) -> str:
    for hi, label in [(7, "0-7 d"), (30, "8-30 d"), (90, "31-90 d"), (180, "91-180 d"), (365, "181-365 d")]:
        if days <= hi:
            return label
    return "365+ d"


def load(src: str) -> pd.DataFrame:
    h = pd.read_csv(src)
    h["m"] = h.arrival_date_month.map({m: i + 1 for i, m in enumerate(MONTHS)})
    h["arr"] = pd.to_datetime(dict(year=h.arrival_date_year, month=h.m, day=h.arrival_date_day_of_month))
    h["ym"] = h.arr.dt.to_period("M").astype(str)
    h["nights"] = h.stays_in_weekend_nights + h.stays_in_week_nights
    h["stayed"] = (h.is_canceled == 0).astype(int)
    h["rev"] = np.where(h.stayed == 1, h.adr * h.nights, 0.0)
    h["rn"] = np.where(h.stayed == 1, h.nights, 0)
    h["country"] = h.country.replace({"CN": "CHN"})  # the one non-ISO-3 code in the column
    h["lead_b"] = h.lead_time.map(lead_bucket)
    return h


def summary(df: pd.DataFrame) -> dict:
    stayed = df[df.stayed == 1]
    rn = stayed.nights.sum()
    return {
        "bookings": int(len(df)),
        "canceled": int(df.is_canceled.sum()),
        "cancel_rate": r(df.is_canceled.mean() * 100, 1),
        "stayed": int(len(stayed)),
        "no_show": int((df.reservation_status == "No-Show").sum()),
        "canceled_status": int((df.reservation_status == "Canceled").sum()),
        "room_nights": int(rn),
        "revenue": r(stayed.rev.sum(), 0),
        "adr": r(stayed.rev.sum() / rn, 2) if rn else None,
        "avg_lead": r(df.lead_time.mean(), 0),
        "avg_stay": r(stayed.nights.mean(), 1),
        "repeat_share": r(stayed.is_repeated_guest.mean() * 100, 1),
        "upgrade_share": r((stayed.reserved_room_type != stayed.assigned_room_type).mean() * 100, 1),
        "requests_avg": r(stayed.total_of_special_requests.mean(), 2),
    }


def by_year(by_month: pd.Series, scale: float = 1.0, digits: int | None = 0) -> dict:
    """One list per arrival year, January to December, None where the data has no such month."""

    def cell(y: int, m: int):
        if (y, m) not in by_month.index:
            return None
        v = by_month[(y, m)] * scale
        return int(v) if digits is None else r(v, digits)

    return {str(y): [cell(y, m) for m in range(1, 13)] for y in (2015, 2016, 2017)}


def slice_data(df: pd.DataFrame) -> dict:
    g = df.groupby("ym")
    monthly = pd.DataFrame(
        {
            "bookings": g.size(),
            "canceled": g.is_canceled.sum(),
            "stayed": g.stayed.sum(),
            "revenue": g.rev.sum(),
            "room_nights": g.rn.sum(),
        }
    ).sort_index()
    monthly["adr"] = (monthly.revenue / monthly.room_nights.replace(0, np.nan)).round(2)
    monthly["cancel_rate"] = (monthly.canceled / monthly.bookings * 100).round(1)
    ytd_cur = df[(df.arr >= "2017-01-01") & (df.arr <= "2017-08-31")]
    ytd_prev = df[(df.arr >= "2016-01-01") & (df.arr <= "2016-08-31")]
    lead = (
        df.groupby("lead_b")
        .agg(bookings=("is_canceled", "size"), cancel_rate=("is_canceled", "mean"))
        .reindex(LEAD_ORDER)
    )
    seg = (
        df[df.market_segment != "Undefined"]
        .groupby("market_segment")
        .agg(
            bookings=("is_canceled", "size"),
            cancel_rate=("is_canceled", "mean"),
            revenue=("rev", "sum"),
            rn=("rn", "sum"),
        )
        .sort_values("bookings", ascending=False)
    )
    seg["adr"] = seg.revenue / seg.rn.replace(0, np.nan)
    dep = (
        df.groupby("deposit_type")
        .agg(bookings=("is_canceled", "size"), cancel_rate=("is_canceled", "mean"))
        .sort_values("bookings", ascending=False)
    )
    cust = df.groupby("customer_type").size().sort_values(ascending=False)
    wd = df.groupby(df.arr.dt.dayofweek).size().reindex(range(7), fill_value=0)
    ctry = (
        df.groupby("country")
        .agg(bookings=("is_canceled", "size"), cancel_rate=("is_canceled", "mean"), revenue=("rev", "sum"))
        .sort_values("bookings", ascending=False)
    )
    ym = [df.arr.dt.year, df.arr.dt.month]
    return {
        "summary": summary(df),
        "ytd": {"current": summary(ytd_cur), "previous": summary(ytd_prev), "label": "Jan-Aug 2017 vs Jan-Aug 2016"},
        "monthly": {"ym": monthly.index.tolist(), **{k: [r(v, 2) for v in monthly[k]] for k in monthly.columns}},
        "lead": {
            "bucket": LEAD_ORDER,
            "bookings": [int(v) for v in lead.bookings],
            "cancel_rate": [r(v * 100, 1) for v in lead.cancel_rate],
        },
        "segments": {
            "name": seg.index.tolist(),
            "bookings": [int(v) for v in seg.bookings],
            "cancel_rate": [r(v * 100, 1) for v in seg.cancel_rate],
            "adr": [r(v, 2) for v in seg.adr],
            "revenue": [r(v, 0) for v in seg.revenue],
        },
        "deposit": {
            "name": dep.index.tolist(),
            "bookings": [int(v) for v in dep.bookings],
            "cancel_rate": [r(v * 100, 1) for v in dep.cancel_rate],
        },
        "customer": {"name": cust.index.tolist(), "bookings": [int(v) for v in cust]},
        "weekday": {"day": WEEKDAYS, "bookings": [int(v) for v in wd]},
        "countries_top": {
            "iso3": ctry.index[:10].tolist(),
            "bookings": [int(v) for v in ctry.bookings[:10]],
            "cancel_rate": [r(v * 100, 1) for v in ctry.cancel_rate[:10]],
            "revenue": [r(v, 0) for v in ctry.revenue[:10]],
        },
        "countries_all": {"iso3": ctry.index.tolist(), "bookings": [int(v) for v in ctry.bookings]},
        "by_year": {
            "bookings": by_year(df.groupby(ym).size(), digits=None),
            "revenue": by_year(df.rev.groupby(ym).sum()),
            "cancel_rate": by_year(df.is_canceled.groupby(ym).mean(), 100, 1),
        },
    }


def main(src: str) -> None:
    h = load(src)
    data = {
        "source": Path(src).name,
        "rows": int(len(h)),
        "period": {"start": str(h.arr.min().date()), "end": str(h.arr.max().date())},
        "currency": "EUR",
        "slices": {
            "all": slice_data(h),
            "City Hotel": slice_data(h[h.hotel == "City Hotel"]),
            "Resort Hotel": slice_data(h[h.hotel == "Resort Hotel"]),
        },
    }
    OUT.write_text(json.dumps(data, separators=(",", ":"), allow_nan=False))
    print(f"wrote {OUT} ({OUT.stat().st_size:,} bytes) from {len(h):,} rows")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
