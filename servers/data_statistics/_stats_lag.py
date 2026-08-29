"""Lead-lag cross-correlation between two time series. No MCP imports.

`correlation_analysis` answers "do these two move together", and every one of
its correlations is contemporaneous -- row i against row i. The question it
cannot answer is the one an advertising dataset actually raises: spend today
does not produce clicks today, it produces them over the following days, and a
contemporaneous correlation of nearly zero is exactly what a real three-day
effect looks like through that tool.

The pieces were already here -- `apply_patch` has lag and lead ops, and
`correlation_analysis` runs the correlation -- so a caller could build this by
hand, one lag per pass, reading the peak off by eye. This does the sweep.

Sign convention, which is the whole tool and is stated in the response as well:

    lag = +k   x leads y by k periods -- corr(x[t], y[t+k])
    lag =  0   contemporaneous, what correlation_analysis reports
    lag = -k   y leads x by k periods

so a positive peak lag means x moves first.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
_HERE = str(Path(__file__).resolve().parent)
for _p in (str(_ROOT), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import pandas as pd

from shared.arg_alias import missing, pick
from shared.column_utils import infer_agg, is_numeric_col
from shared.file_utils import read_csv as _read_csv
from shared.file_utils import resolve_path
from shared.platform_utils import get_max_lag
from shared.progress import fail, info, ok, warn

logger = logging.getLogger(__name__)

# Same vocabulary period_comparison accepts, so a caller who learned D/W/M/Q/Y
# there does not have to learn a second spelling here.
_FREQ_MAP = {"H": "h", "D": "D", "W": "W", "M": "ME", "Q": "QE", "Y": "YE"}
_PERIOD_ALIASES = {"DOD": "D", "WOW": "W", "MOM": "M", "QOQ": "Q", "YOY": "Y"}

_VALID_METHODS = ("pearson", "spearman", "kendall")
_VALID_AGGS = ("sum", "mean", "max", "min")

# Below this many overlapping periods a correlation is not worth reporting: at
# n=4 a coefficient of 0.9 is an ordinary result from unrelated noise. The far
# ends of a wide sweep are always the thinnest, and without this floor they are
# where a spurious peak reliably shows up.
_MIN_OVERLAP_FLOOR = 3
_DEFAULT_MIN_OVERLAP = 8


def _err(msg: str, hint: str, what: str, detail: str = "") -> dict:
    return {
        "success": False,
        "op": "lag_correlation",
        "error": msg,
        "hint": hint,
        "progress": [fail(what, detail or msg)],
        "token_estimate": (len(msg) + len(hint)) // 4 + 20,
    }


def _resample(series: pd.Series, freq: str, agg: str) -> pd.Series:
    """One column collapsed onto a regular grid, gaps left as gaps.

    `resample(...).sum()` reports 0 for a period with no rows in it, which is a
    real number the correlation would then treat as an observed zero. A week
    nobody recorded is not a week of zero spend, and a run of them at the head
    or tail of a series will bend a correlation on its own. min_count=1 leaves
    those periods NaN so the per-lag pairwise deletion drops them.
    """
    grouped = series.resample(freq)
    if agg == "sum":
        return grouped.sum(min_count=1)
    return getattr(grouped, agg)()


def _correlate(a: pd.Series, b: pd.Series, method: str) -> tuple[float, float]:
    from scipy import stats as scipy_stats

    if method == "spearman":
        r, p = scipy_stats.spearmanr(a, b)
    elif method == "kendall":
        r, p = scipy_stats.kendalltau(a, b)
    else:
        r, p = scipy_stats.pearsonr(a, b)
    return float(r), float(p)


def lag_correlation(
    file_path: str,
    date_column: str = "",
    x_column: str = "",
    y_column: str = "",
    max_lag: int = 10,
    period_unit: str = "D",
    method: str = "pearson",
    x_agg: str = "",
    y_agg: str = "",
    min_overlap: int = _DEFAULT_MIN_OVERLAP,
    date_col: str = "",
) -> dict:
    """Cross-correlate two columns across lags. Returns the curve and its peak."""
    progress: list = []

    date_column, note = pick("lag_correlation", "date_column", date_column, date_col)
    if not date_column:
        return missing("lag_correlation", "date_column", "date_col")
    if note:
        progress.append(info("Argument alias", note))
    if not x_column or not y_column:
        return _err(
            "lag_correlation needs both x_column and y_column",
            "Pass x_column='spends', y_column='clicks' — the pair to cross-correlate.",
            "Missing argument",
            "x_column/y_column",
        )

    try:
        max_lag = int(max_lag)
        min_overlap = int(min_overlap)
    except TypeError, ValueError:
        return _err(
            "max_lag and min_overlap must be whole numbers.",
            "Pass max_lag=10 to test ten periods either side of zero.",
            "Invalid argument",
        )

    if max_lag < 1:
        return _err(
            f"max_lag must be at least 1, got {max_lag}.",
            "Pass max_lag=10 to test ten periods either side of zero. For lag 0 only, "
            "correlation_analysis() is the tool.",
            "Invalid max_lag",
            str(max_lag),
        )
    cap = get_max_lag()
    lag_capped = max_lag > cap
    if lag_capped:
        progress.append(warn("max_lag capped", f"{max_lag} -> {cap}"))
        max_lag = cap

    if min_overlap < _MIN_OVERLAP_FLOOR:
        return _err(
            f"min_overlap must be at least {_MIN_OVERLAP_FLOOR}, got {min_overlap}.",
            f"A correlation on fewer than {_MIN_OVERLAP_FLOOR} pairs is not defined. "
            f"The default, {_DEFAULT_MIN_OVERLAP}, is the smallest worth reporting.",
            "Invalid min_overlap",
            str(min_overlap),
        )

    method = (method or "pearson").strip().lower()
    if method not in _VALID_METHODS:
        return _err(
            f"Invalid method: {method}",
            f"Valid methods: {', '.join(_VALID_METHODS)}.",
            "Invalid method",
            method,
        )

    unit = (period_unit or "D").strip().upper()
    unit = _PERIOD_ALIASES.get(unit, unit)
    if unit not in _FREQ_MAP:
        return _err(
            f"Invalid period_unit {period_unit!r}.",
            f"Use one of: {sorted(_FREQ_MAP)} (H=hour, D=day, W=week, M=month, Q=quarter, Y=year).",
            "Invalid period_unit",
            str(period_unit),
        )

    for name, value in (("x_agg", x_agg), ("y_agg", y_agg)):
        if value and value.strip().lower() not in _VALID_AGGS:
            return _err(
                f"Invalid {name}: {value}",
                f"Valid aggregations: {', '.join(_VALID_AGGS)}. Leave it empty to infer from the column name.",
                f"Invalid {name}",
                str(value),
            )

    try:
        path = resolve_path(file_path)
        if not path.exists():
            return _err(
                f"File not found: {path.name}",
                "Check file_path is absolute and the file exists.",
                "File not found",
                path.name,
            )

        df = _read_csv(str(path))

        for name, col in (("date_column", date_column), ("x_column", x_column), ("y_column", y_column)):
            if col not in df.columns:
                return _err(
                    f"{name} '{col}' not found.",
                    f"Available: {list(df.columns)}",
                    "Column not found",
                    col,
                )

        for name, col in (("x_column", x_column), ("y_column", y_column)):
            df[col] = pd.to_numeric(df[col], errors="coerce")
            if not is_numeric_col(df[col]) or df[col].notna().sum() == 0:
                return _err(
                    f"{name} '{col}' holds no numeric values.",
                    f"Cross-correlation needs two numeric columns. Use read_column_stats() to see what '{col}' holds.",
                    "Not numeric",
                    col,
                )

        df[date_column] = pd.to_datetime(df[date_column], format="mixed", dayfirst=False, errors="coerce")
        undated = int(df[date_column].isna().sum())
        df = df.dropna(subset=[date_column])
        if df.empty:
            return _err(
                f"No parseable dates in '{date_column}'.",
                "Check the column holds dates. inspect_dataset() shows a sample of its values.",
                "No dates",
                date_column,
            )
        if undated:
            progress.append(warn("Rows without a date dropped", str(undated)))

        x_use = (x_agg or infer_agg(x_column, df[x_column])).strip().lower()
        y_use = (y_agg or infer_agg(y_column, df[y_column])).strip().lower()

        # Both series onto one regular grid. Without this the shift below counts
        # rows, not time: on transaction-level data row i-1 is whatever happened
        # to be logged before row i, so a "lag of 3" is three records, which may
        # be three seconds or three months.
        indexed = df.set_index(date_column).sort_index()
        freq = _FREQ_MAP[unit]
        x_series = _resample(indexed[x_column], freq, x_use)
        y_series = _resample(indexed[y_column], freq, y_use)
        periods = int(len(x_series))
        observed = int((x_series.notna() & y_series.notna()).sum())
        progress.append(ok(f"Resampled to {periods} {unit} periods", f"x={x_use}, y={y_use}"))

        if observed < min_overlap:
            return _err(
                f"Only {observed} periods have both columns; min_overlap is {min_overlap}.",
                f"Use a coarser period_unit than {unit}, or lower min_overlap (never below {_MIN_OVERLAP_FLOOR}).",
                "Too few periods",
                str(observed),
            )

        # Every lag is its own paired sample: shifting drops observations off one
        # end, and the two columns' gaps do not line up, so the overlap has to be
        # recomputed per lag rather than taken once from lag 0.
        rows: list[dict] = []
        skipped: list[int] = []
        for lag in range(-max_lag, max_lag + 1):
            pair = pd.DataFrame({"x": x_series.shift(lag), "y": y_series}).dropna()
            n = int(len(pair))
            if n < min_overlap or pair["x"].nunique() < 2 or pair["y"].nunique() < 2:
                skipped.append(lag)
                continue
            try:
                r, p = _correlate(pair["x"], pair["y"], method)
            except Exception as exc:  # a degenerate window, not a failed call
                logger.debug("lag %s: %s", lag, exc)
                skipped.append(lag)
                continue
            if pd.isna(r):
                skipped.append(lag)
                continue
            rows.append({"lag": lag, "r": round(r, 6), "p_value": float(p), "n": n})

        if not rows:
            return _err(
                f"No lag from -{max_lag} to {max_lag} had {min_overlap} usable pairs.",
                f"The series has {observed} periods with both columns. Use a coarser "
                f"period_unit than {unit}, a smaller max_lag, or a lower min_overlap.",
                "No usable lags",
                f"{len(skipped)} skipped",
            )

        # Peak by strength, not by signedness -- a strong negative lead is as much
        # a finding as a positive one. Ties go to the smaller |lag|, then to the
        # negative one, so the answer does not depend on dict ordering.
        peak = min(rows, key=lambda d: (-abs(d["r"]), abs(d["lag"]), d["lag"]))
        zero = next((d for d in rows if d["lag"] == 0), None)

        # The peak was chosen by looking at every lag, so its p-value is the
        # smallest of len(rows) draws and is optimistic by construction. Reporting
        # it alone is how a sweep like this manufactures significance.
        m = len(rows)
        peak_p_adjusted = min(1.0, peak["p_value"] * m)

        if peak["lag"] > 0:
            reading = f"{x_column} leads {y_column} by {peak['lag']} {unit} period(s)"
        elif peak["lag"] < 0:
            reading = f"{y_column} leads {x_column} by {abs(peak['lag'])} {unit} period(s)"
        else:
            reading = f"{x_column} and {y_column} move together with no lead or lag"

        gain = None
        if zero is not None and zero["lag"] != peak["lag"]:
            gain = round(abs(peak["r"]) - abs(zero["r"]), 6)

        progress.append(ok(f"Peak at lag {peak['lag']}", f"r={peak['r']}, n={peak['n']}"))
        if skipped:
            progress.append(info("Lags skipped for thin overlap", str(len(skipped))))

        response = {
            "success": True,
            "op": "lag_correlation",
            "x_column": x_column,
            "y_column": y_column,
            "method": method,
            "period_unit": unit,
            "aggregation": {"x": x_use, "y": y_use},
            "periods": periods,
            "periods_with_both": observed,
            "lags_tested": m,
            "lags_skipped": skipped,
            "min_overlap": min_overlap,
            "sign_convention": (
                f"lag +k means {x_column} leads {y_column} by k periods: corr(x[t], y[t+k]). "
                "Negative k means the reverse."
            ),
            "peak_lag": peak["lag"],
            "peak_r": peak["r"],
            "peak_p_value": peak["p_value"],
            "peak_p_value_adjusted": peak_p_adjusted,
            "peak_n": peak["n"],
            "reading": reading,
            "contemporaneous_r": zero["r"] if zero else None,
            "gain_over_lag_0": gain,
            "correlations": rows,
            "note": (
                f"peak_p_value_adjusted is Bonferroni over the {m} lags tested; the raw "
                "p-value is the smallest of that many and is optimistic on its own."
            ),
            "progress": progress,
        }
        if lag_capped:
            response["max_lag_capped_to"] = max_lag
        response["token_estimate"] = len(str(response)) // 4
        return response

    except Exception as exc:
        logger.exception("lag_correlation failed")
        return _err(
            f"lag_correlation failed: {exc}",
            "Check the date column parses as dates and both value columns are numeric. "
            "inspect_dataset() shows the dtypes.",
            "Unhandled error",
            type(exc).__name__,
        )
