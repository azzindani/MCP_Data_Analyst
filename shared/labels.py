"""A column's name as a person would say it: `lead_time` is "Lead time", `PoliceReportFiled` "Police report filed".

A dashboard's words came out of the file's header row: "Avg lead_time", "is_canceled rate", "Total
total_of_special_requests by month". The numbers behind them are right; the names are a machine's.
`humanize` says a name the way it would be written on a slide, and `relabel` swaps every column or
metric name in a sentence for that wording, so the code that builds a title or a finding keeps working
with the real names and the page shows the readable ones.

A name that already has spaces and no underscore ("Hospital overall rating") is somebody's wording and
is left alone.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

# Words a header spells in capitals or a fixed way: said that way, not as "Adr" or "Kwh".
_FIXED = {
    "adr": "ADR", "ctr": "CTR", "cpc": "CPC", "cpm": "CPM", "cvr": "CVR", "cpa": "CPA", "cac": "CAC", "roas": "ROAS",
    "aov": "AOV", "kpi": "KPI", "id": "ID", "usd": "USD", "eur": "EUR", "gbp": "GBP", "idr": "IDR", "url": "URL",
    "ip": "IP", "api": "API", "gdp": "GDP", "bmi": "BMI", "ai": "AI", "uk": "UK", "us": "US", "eu": "EU",
    "kw": "kW", "kwh": "kWh", "mwh": "MWh", "gwh": "GWh", "hz": "Hz", "km": "km", "kmh": "km/h", "kph": "km/h",
    "co2": "CO2", "no2": "NO2", "pm10": "PM10", "pm25": "PM2.5", "ph": "pH", "ev": "EV", "vip": "VIP", "ytd": "YTD",
    "yoy": "YoY", "pct": "%", "qty": "quantity", "num": "number", "avg": "average", "amt": "amount",
}  # fmt: skip
_LEADING_FLAG = ("is", "has", "was", "had")
_BOUNDARY = re.compile(r"(?<=[a-z]{2})(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")  # PoliceReport, ADRValue; not kW or 6G
_SPLIT = re.compile(r"[\s_\-.]+")
_WORDS_ALREADY = re.compile(r"[a-z][A-Z]")


def humanize(name: object) -> str:
    """`name` in sentence case, words split and acronyms kept: `lead_time` -> "Lead time"."""
    text = str(name).strip()
    if not text:
        return text
    if " " in text and "_" not in text and not _WORDS_ALREADY.search(text):
        return text  # the file's own wording
    words = [w for w in _SPLIT.split(_BOUNDARY.sub(" ", text)) if w]
    if len(words) > 1 and words[0].lower() in _LEADING_FLAG:
        words = words[1:]  # is_canceled is "Canceled"; its rate is "Canceled rate"
    said = [_FIXED.get(w.lower()) or (w if w[0].isdigit() or (len(w) == 1 and w.isupper()) else w.lower()) for w in words]
    out = " ".join(said)
    return out[:1].upper() + out[1:]


def _starts_sentence(text: str, at: int) -> bool:
    before = text[:at].rstrip(" *_#>-\t")
    return not before or before[-1] in ".!?:\n" or text[max(0, at - 1)] in "\n"


def relabel(text: str, names: Iterable[str]) -> str:
    """`text` with each of `names` (columns, metrics) replaced by its humanized form, whole names only.

    A label that lands mid-sentence loses its capital ("Avg lead time" -> "avg lead time" is not what is
    wanted, so only the label's own first letter is lowered, and only when it is an ordinary word).
    """
    mapping = {n: humanize(n) for n in dict.fromkeys(str(n) for n in names if n) if humanize(n) != n}
    if not mapping or not text:
        return text
    pattern = re.compile(
        r"(?<![A-Za-z0-9_])(" + "|".join(re.escape(n) for n in sorted(mapping, key=len, reverse=True)) + r")(?![A-Za-z0-9_])"
    )

    def swap(m: re.Match[str]) -> str:
        label = mapping[m.group(1)]
        ordinary = len(label) > 1 and label[0].isupper() and label[1].islower()
        if ordinary and not _starts_sentence(m.string, m.start()):
            return label[0].lower() + label[1:]
        return label

    return pattern.sub(swap, text)
