"""Period comparison and comparative analysis. No MCP imports."""

from __future__ import annotations

import logging
import math
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
# data_medium holds the shared chart saver; engine.py puts it on the path too,
# but this module is also imported directly by the tests.
_MED = str(Path(__file__).resolve().parents[1] / "data_medium")
for _p in (str(_ROOT), _MED):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import numpy as np
import pandas as pd

from shared.arg_alias import missing, pick
from shared.column_utils import date_note, parse_dates
from shared.file_utils import error_text, hint_for_error, resolve_path
from shared.file_utils import read_csv as _read_csv
from shared.progress import fail, info, ok, warn

logger = logging.getLogger(__name__)

_FREQ_MAP = {
    "W": "W",
    "M": "ME",
    "Q": "QE",
    "Y": "YE",
    "D": "D",
    "H": "h",
}

_VALID_PERIOD_UNITS = set(_FREQ_MAP)

# The tool description is "Compare periods: MoM QoQ YoY", and the schema carries
# no enum, so those three strings are the whole vocabulary a caller can see --
# and all three were rejected, because the code wanted the single letters. The
# names are not interchangeable in general (MoM is a comparison, M is a period),
# but for this tool they mean the same thing: month over month is a monthly
# period compared with the one before it, which is what compare_to="previous"
# already does. Matched after the .upper() below, so MoM/mom/MOM all land.
_PERIOD_ALIASES = {"MOM": "M", "QOQ": "Q", "YOY": "Y", "WOW": "W", "DOD": "D"}

# What pandas' to_period() calls each unit. Only the hour differs -- pandas 3
# removed the uppercase "H" alias -- but the mapping is spelled out so the next
# alias to change is a one-line edit rather than another traceback.
_PERIOD_ALIAS = {"D": "D", "W": "W", "M": "M", "Q": "Q", "Y": "Y", "H": "h"}


def _comparison_chart(
    comparisons: list[dict],
    metrics: list[str],
    current_period: str,
    reference_period: str,
    output_path: str,
    input_path: Path,
    theme: str,
    open_after: bool,
    progress: list,
) -> tuple[str, str]:
    """Plot each metric's current value against its reference, side by side.

    Grouped bars rather than the percentage deltas: the deltas are already in
    the response as numbers, and what a chart adds is the magnitudes they came
    from -- a +300% move on a base of 2 reads very differently next to its bar.

    One panel per metric, each with its own y-axis, because that reasoning only
    holds while the metrics share a scale. On a single axis the largest metric
    sets it and the rest collapse into the baseline: comparing impressions,
    link_clicks and clicks over one real month of the ad dataset -- a 170:1
    spread -- drew link_clicks at 0 and 4 pixels beside a 456-pixel bar. The
    caller had named link_clicks as a thing to compare and the chart showed
    nothing for it, under success: true. Measured off the rendered page rather
    than judged by eye.

    One metric is still one panel, which is the chart this always drew.
    """
    try:
        import plotly.graph_objects as go  # type: ignore[import-untyped]
        from _med_helpers import _save_chart  # type: ignore[import]
        from plotly.subplots import make_subplots  # type: ignore[import-untyped]
    except ImportError:
        return "", ""

    # With group_by, one entry per group; label the panel accordingly.
    labels = [f"{c.get('group', '')} {m}".strip() for c in comparisons for m in metrics]
    current = [c[m]["current"] for c in comparisons for m in metrics]
    reference = [c[m]["reference"] for c in comparisons for m in metrics]
    if not labels:
        return "", ""

    cols = min(len(labels), 3)
    rows = math.ceil(len(labels) / cols)
    fig = make_subplots(rows=rows, cols=cols, subplot_titles=labels)

    for i in range(len(labels)):
        row, col = divmod(i, cols)
        for name, value, colour, group in (
            (reference_period, reference[i], "#8b949e", "reference"),
            (current_period, current[i], "#58a6ff", "current"),
        ):
            fig.add_trace(
                go.Bar(
                    name=name,
                    x=[name],
                    y=[value],
                    marker_color=colour,
                    # One legend entry per period, not one per panel.
                    showlegend=i == 0,
                    legendgroup=group,
                ),
                row=row + 1,
                col=col + 1,
            )

    # A period name is "2019-11", which plotly reads as a date the moment it is
    # an x value rather than a series name -- the panels came back with a date
    # axis running Oct 27 to Dec 8 and the two bars placed along it. They are
    # two labels, not two instants.
    fig.update_xaxes(type="category")
    fig.update_layout(
        title=f"{current_period} vs {reference_period}",
        margin=dict(l=20, r=20, t=80, b=20),
        # A panel grid has to grow with its row count; a single row keeps the
        # responsive height every other chart on this server uses.
        autosize=rows == 1,
        height=None if rows == 1 else 260 * rows + 120,
    )
    return _save_chart(fig, output_path, "period_comparison", input_path, open_after, theme, progress)


def period_comparison(
    file_path: str,
    date_col: str = "",
    metrics: list[str] = None,
    period_unit: str = "",
    current_period: str = "",
    compare_to: str = "previous",
    group_by: str = "",
    output_path: str = "",
    theme: str = "device",
    open_after: bool = False,
    date_column: str = "",
    dayfirst: str = "auto",
) -> dict:
    """Compare MoM/QoQ/YoY metrics. Returns delta, pct_change, direction."""
    progress = []
    # time_series_analysis and cohort_analysis, in the same server, say date_column.
    date_col, note = pick("period_comparison", "date_col", date_col, date_column)
    if not date_col:
        return missing("period_comparison", "date_col", "date_column")
    if note:
        progress.append(info("Argument alias", note))
    # A parameter with a default cannot precede one without, so accepting
    # date_column made these two optional in the signature. They are still
    # required in fact, and refusing here keeps the message as clear as
    # pydantic's was.
    if not metrics:
        return {
            "success": False,
            "op": "period_comparison",
            "error": "period_comparison needs metrics",
            "hint": "Pass metrics=['spends'] — the numeric columns to compare across periods.",
            "progress": [fail("Missing argument", "metrics")],
            "token_estimate": 25,
        }
    if not period_unit:
        return {
            "success": False,
            "op": "period_comparison",
            "error": "period_comparison needs period_unit",
            "hint": "Pass period_unit= one of: D, W, M, Q, Y.",
            "progress": [fail("Missing argument", "period_unit")],
            "token_estimate": 25,
        }
    try:
        path = resolve_path(file_path)
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {path.name}",
                "hint": "Check file_path is absolute.",
                "progress": [fail("File not found", path.name)],
                "token_estimate": 20,
            }

        df = _read_csv(str(path))

        if date_col not in df.columns:
            return {
                "success": False,
                "error": f"Date column '{date_col}' not found.",
                "hint": f"Available: {list(df.columns)}",
                "progress": [fail("Column not found", date_col)],
                "token_estimate": 20,
            }

        missing_metrics = [m for m in metrics if m not in df.columns]
        if missing_metrics:
            return {
                "success": False,
                "error": f"Metric columns not found: {missing_metrics}",
                "hint": f"Available: {list(df.columns)}",
                "progress": [fail("Columns not found", str(missing_metrics))],
                "token_estimate": 20,
            }

        period_unit = period_unit.strip().upper()
        period_unit = _PERIOD_ALIASES.get(period_unit, period_unit)
        if period_unit not in _VALID_PERIOD_UNITS:
            return {
                "success": False,
                "error": f"Invalid period_unit {period_unit!r}.",
                "hint": (
                    f"Use one of: {sorted(_VALID_PERIOD_UNITS)} "
                    "(D=day, W=week, M=month, Q=quarter, Y=year, H=hour), "
                    "or MoM / QoQ / YoY."
                ),
                "progress": [fail("Invalid period_unit", period_unit)],
                "token_estimate": 25,
            }

        df[date_col], _fmt = parse_dates(df[date_col], dayfirst)
        progress.append(date_note(_fmt, date_col))
        df = df.dropna(subset=[date_col])

        freq = _FREQ_MAP.get(period_unit, "ME")

        # Group by date period (and optional group_by). to_period wants a period
        # alias, not the resample offset in _FREQ_MAP ("ME" is not a period) and
        # not the raw letter either: pandas 3 dropped uppercase "H" and answers
        # it with 'Invalid frequency: H ... Did you mean h?', nested inside a
        # second copy of itself. period_unit="H" is listed as valid by this
        # tool's own validation hint, so it reached that line and failed there.
        df["__period__"] = df[date_col].dt.to_period(_PERIOD_ALIAS.get(period_unit, period_unit))

        group_cols = ["__period__"]
        if group_by and group_by in df.columns:
            group_cols = [group_by, "__period__"]

        # Aggregate metrics
        agg_dict = {m: "sum" for m in metrics}
        grouped = df.groupby(group_cols).agg(agg_dict).reset_index()

        # Determine current and reference periods
        all_periods = sorted(grouped["__period__"].unique())
        if not all_periods:
            return {
                "success": False,
                "error": "No valid date periods found.",
                "hint": f"Ensure '{date_col}' contains valid dates.",
                "progress": [fail("No periods", date_col)],
                "token_estimate": 20,
            }

        if current_period:
            try:
                cur_pd = pd.Period(current_period, freq=period_unit)
            except Exception:
                return {
                    "success": False,
                    "error": f"Cannot parse current_period '{current_period}'",
                    "hint": "Use format like '2024-Q3' or '2024-01' depending on period_unit.",
                    "progress": [fail("Parse error", current_period)],
                    "token_estimate": 20,
                }
        else:
            cur_pd = all_periods[-1]

        # A period the data only partly covers still produces a clean-looking
        # percentage, and most of that percentage is calendar. On the reference
        # dataset the last month runs 2020-07-01 to 2020-07-07 against a full
        # June, and this tool reported spends "down 78.84%" with nothing to say
        # that seven days were being held against thirty. The arithmetic is
        # right; read as a business decline it is wrong by roughly a factor of
        # four. Round 16 found it by checking where the fixture ends.
        data_start, data_end = df[date_col].min(), df[date_col].max()

        if compare_to == "previous":
            ref_pd = cur_pd - 1
        elif compare_to == "same_last_year":
            ref_pd = cur_pd - 12 if period_unit == "M" else cur_pd - 4 if period_unit == "Q" else cur_pd - 1
        else:
            try:
                ref_pd = pd.Period(compare_to, freq=period_unit)
            except Exception:
                ref_pd = cur_pd - 1

        comparisons: list[dict] = []

        def _compare_rows(cur_row: pd.Series, ref_row: pd.Series, group_label: str = "") -> dict:
            comp: dict = {
                "current_period": str(cur_pd),
                "reference_period": str(ref_pd),
                "compare_to": compare_to,
            }
            if group_label:
                comp["group"] = group_label
            for m in metrics:
                cur_val = float(cur_row[m]) if not pd.isna(cur_row[m]) else None  # type: ignore[arg-type]
                ref_val = float(ref_row[m]) if not pd.isna(ref_row[m]) else None  # type: ignore[arg-type]
                if cur_val is not None and ref_val is not None and ref_val != 0:
                    delta = cur_val - ref_val
                    pct = (delta / abs(ref_val)) * 100
                    direction = "up" if delta > 0 else "down" if delta < 0 else "flat"
                else:
                    delta = None
                    pct = None
                    direction = "no_data"
                comp[m] = {
                    "current": cur_val,
                    "reference": ref_val,
                    "delta": round(delta, 4) if delta is not None else None,
                    "pct_change": round(pct, 2) if pct is not None else None,
                    "direction": direction,
                }
            return comp

        if group_by and group_by in df.columns:
            for grp in grouped[group_by].unique():
                grp_df = grouped[grouped[group_by] == grp].set_index("__period__")
                cur_row = grp_df.loc[cur_pd] if cur_pd in grp_df.index else None
                ref_row = grp_df.loc[ref_pd] if ref_pd in grp_df.index else None
                if cur_row is not None and ref_row is not None:
                    comparisons.append(_compare_rows(cur_row, ref_row, group_label=str(grp)))
        else:
            idx_df = grouped.set_index("__period__")
            cur_row = idx_df.loc[cur_pd] if cur_pd in idx_df.index else None
            ref_row = idx_df.loc[ref_pd] if ref_pd in idx_df.index else None
            if cur_row is not None and ref_row is not None:
                comparisons.append(_compare_rows(cur_row, ref_row))
            elif cur_row is None:
                return {
                    "success": False,
                    "error": f"Current period {cur_pd} not found in data.",
                    "hint": f"Available periods: {[str(p) for p in all_periods[-5:]]}",
                    "progress": [fail("Period not found", str(cur_pd))],
                    "token_estimate": 30,
                }

        progress.append(ok("Period comparison", f"{cur_pd} vs {ref_pd}"))

        # Days the data actually covers in each period, against the days the
        # calendar gives it. Reported for both, because a run starting mid-month
        # produces the same distortion with its sign flipped.
        def _coverage(p) -> dict:
            span_start, span_end = p.start_time, p.end_time
            have_start = max(span_start, data_start)
            have_end = min(span_end, data_end)
            days_in = (span_end.normalize() - span_start.normalize()).days + 1
            days_have = (
                max(0, (have_end.normalize() - have_start.normalize()).days + 1) if have_end >= have_start else 0
            )
            return {
                "period": str(p),
                "days_in_period": int(days_in),
                "days_with_data": int(min(days_have, days_in)),
                "complete": bool(days_have >= days_in),
            }

        cur_cov, ref_cov = _coverage(cur_pd), _coverage(ref_pd)
        partial = [c for c in (cur_cov, ref_cov) if not c["complete"]]

        result = {
            "success": True,
            "op": "period_comparison",
            "current_period": str(cur_pd),
            "reference_period": str(ref_pd),
            "period_unit": period_unit,
            "metrics": metrics,
            "comparisons": comparisons,
            "current_period_coverage": cur_cov,
            "reference_period_coverage": ref_cov,
            "partial_period": bool(partial),
            "all_periods_available": [str(p) for p in all_periods],
            "progress": progress,
        }
        if partial:
            worst = ", ".join(f"{c['period']} has {c['days_with_data']} of {c['days_in_period']} days" for c in partial)
            progress.append(warn("Partial period compared", worst))
            result["hint"] = (
                f"{worst} — so a change of this size is partly calendar, not behaviour. "
                "Compare complete periods (see all_periods_available), or divide each metric by "
                "days_with_data to compare daily rates instead of totals."
            )

        # Same defect as regression_analysis: output_path was accepted and then
        # dropped, so the caller got success:true and no file.
        if output_path:
            chart_path, chart_name = _comparison_chart(
                comparisons, metrics, str(cur_pd), str(ref_pd), output_path, path, theme, open_after, progress
            )
            if chart_path:
                result["output_path"] = chart_path
                result["output_name"] = chart_name
                progress.append(ok("Comparison chart saved", chart_name))
            else:
                progress.append(warn("No chart written", "plotly is unavailable in this environment"))

        result["token_estimate"] = len(str(result)) // 4
        return result

    except Exception as exc:
        logger.exception("period_comparison error")
        return {
            "success": False,
            "error": error_text(exc),
            # The validation hint above lists H as valid; this one used to omit
            # it, so the tool contradicted itself about its own vocabulary.
            "hint": hint_for_error(exc, "Check date_col, metrics, and period_unit (D W M Q Y H, or MoM QoQ YoY)."),
            "progress": [fail("Unexpected error", str(exc))],
            "token_estimate": 20,
        }
