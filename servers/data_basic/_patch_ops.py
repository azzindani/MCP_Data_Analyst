"""Patch operation functions for apply_patch. No MCP imports."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
from shared.progress import fail, ok  # noqa: F401 — re-exported for convenience

# ---------------------------------------------------------------------------
# Existing ops
# ---------------------------------------------------------------------------


def _op_drop_column(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    columns = op["columns"]
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Columns not found: {missing}. Available: {list(df.columns)}")
    remaining = [c for c in df.columns if c not in columns]
    df = df.drop(columns=columns)
    return df, {"op": "drop_column", "dropped": columns, "remaining": len(remaining)}


def _op_clean_text(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    scope = op.get("scope", "both")
    affected = 0
    if scope in ("headers", "both"):
        df.columns = [c.strip().title() for c in df.columns]
        affected = len(df.columns)
    if scope in ("values", "both"):
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].apply(lambda v: v.strip().title() if isinstance(v, str) else v)
            affected += 1
    return df, {"op": "clean_text", "scope": scope, "columns_affected": affected}


def _op_cast_column(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    dtype = op["dtype"]

    # Determine from_dtype label
    if pd.api.types.is_integer_dtype(df[col]):
        from_dtype = "int64"
    elif pd.api.types.is_float_dtype(df[col]):
        from_dtype = "float64"
    elif pd.api.types.is_datetime64_any_dtype(df[col]):
        from_dtype = "datetime64"
    else:
        from_dtype = "object"

    failed = 0

    if dtype == "int":
        converted = pd.to_numeric(df[col], errors="coerce")
        failed = max(0, int(converted.isna().sum()) - int(df[col].isna().sum()))
        df[col] = converted.astype("Int64")
        to_dtype = "Int64"
    elif dtype == "float":
        converted = pd.to_numeric(df[col], errors="coerce")
        failed = max(0, int(converted.isna().sum()) - int(df[col].isna().sum()))
        df[col] = converted
        to_dtype = "float64"
    elif dtype == "str":
        df[col] = df[col].astype(str)
        to_dtype = "object"
    elif dtype == "datetime":
        converted = pd.to_datetime(df[col], errors="coerce")
        failed = max(0, int(converted.isna().sum()) - int(df[col].isna().sum()))
        df[col] = converted
        to_dtype = "datetime64[ns]"
    else:
        raise ValueError(f"Unknown dtype: {dtype}")

    return df, {
        "op": "cast_column",
        "column": col,
        "from": from_dtype,
        "to": to_dtype,
        "failed": failed,
    }


def _op_replace_values(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    mapping = op["mapping"]
    replaced = int(df[col].isin(mapping.keys()).sum())
    df[col] = df[col].replace(mapping)
    return df, {"op": "replace_values", "column": col, "replaced": replaced}


def _parse_expr(expr: str, df: pd.DataFrame) -> pd.Series:
    """Parse simple math expression safely — no eval()."""
    tokens = re.split(r"(\s*[\+\-\*\/]\s*)", expr)
    tokens = [t.strip() for t in tokens]

    def resolve(token: str) -> pd.Series:
        token = token.strip()
        if token in df.columns:
            return df[token].astype(float)
        try:
            return pd.Series([float(token)] * len(df), index=df.index)
        except ValueError:
            raise ValueError(f"Unknown token in expr: '{token}'. Must be a column name or number.")

    if len(tokens) == 1:
        return resolve(tokens[0])

    result = resolve(tokens[0])
    i = 1
    while i < len(tokens):
        op_sym = tokens[i].strip()
        right = resolve(tokens[i + 1])
        if op_sym == "+":
            result = result + right
        elif op_sym == "-":
            result = result - right
        elif op_sym == "*":
            result = result * right
        elif op_sym == "/":
            result = result / right
        else:
            raise ValueError(f"Unsupported operator: '{op_sym}'")
        i += 2

    return result


def _op_add_column(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    name = op["name"]
    mode = op.get("mode", "math")

    if mode == "math":
        expr = op["expr"]
        df[name] = _parse_expr(expr, df)
    elif mode == "threshold":
        source = op["source"]
        if source not in df.columns:
            raise ValueError(f"Source column not found: {source}. Available: {list(df.columns)}")
        threshold = op.get("threshold", 0)
        freq = df[source].value_counts()
        df[name] = df[source].apply(lambda v: v if freq.get(v, 0) >= threshold else "Other")
    else:
        raise ValueError(f"Unknown mode: {mode}")

    if pd.api.types.is_integer_dtype(df[name]):
        dtype = "int64"
    elif pd.api.types.is_float_dtype(df[name]):
        dtype = "float64"
    elif pd.api.types.is_datetime64_any_dtype(df[name]):
        dtype = "datetime64"
    else:
        dtype = "object"

    null_count = int(df[name].isna().sum())
    return df, {
        "op": "add_column",
        "name": name,
        "dtype": dtype,
        "null_count": null_count,
    }


def _op_cap_outliers(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(f"Column '{col}' is not numeric; cannot cap outliers.")

    method = op.get("method", "iqr")
    clean = df[col].dropna()
    result_info: dict = {"op": "cap_outliers", "column": col, "method": method}

    if method == "iqr":
        th1 = op.get("th1", 0.25)
        th3 = op.get("th3", 0.75)
        q1 = float(clean.quantile(th1))
        q3 = float(clean.quantile(th3))
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        capped_lower = int((df[col] < lower).sum())
        capped_upper = int((df[col] > upper).sum())
        df[col] = df[col].clip(lower=lower, upper=upper)
        result_info.update(
            {
                "capped_lower": capped_lower,
                "capped_upper": capped_upper,
                "lower_limit": round(lower, 4),
                "upper_limit": round(upper, 4),
            }
        )
    elif method == "std":
        mean_val = float(clean.mean())
        std_val = float(clean.std())
        lower = mean_val - 3 * std_val
        upper = mean_val + 3 * std_val
        capped_lower = int((df[col] < lower).sum())
        capped_upper = int((df[col] > upper).sum())
        df[col] = df[col].clip(lower=lower, upper=upper)
        result_info.update(
            {
                "capped_lower": capped_lower,
                "capped_upper": capped_upper,
                "lower_limit": round(lower, 4),
                "upper_limit": round(upper, 4),
            }
        )

    return df, result_info


def _op_fill_nulls(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    strategy = op["strategy"]
    fill_zeros = op.get("fill_zeros", False)

    if fill_zeros and pd.api.types.is_numeric_dtype(df[col]):
        df[col] = df[col].replace(0, pd.NA)

    null_before = int(df[col].isna().sum())
    value_used = None

    if strategy == "mean":
        value_used = float(df[col].mean()) if not df[col].dropna().empty else None
        df[col] = df[col].fillna(value_used) if value_used is not None else df[col]
    elif strategy == "median":
        value_used = float(df[col].median()) if not df[col].dropna().empty else None
        df[col] = df[col].fillna(value_used) if value_used is not None else df[col]
    elif strategy == "mode":
        mode_series = df[col].mode()
        if not mode_series.empty:
            value_used = mode_series.iloc[0]
            df[col] = df[col].fillna(value_used)
    elif strategy == "ffill":
        df[col] = df[col].ffill()
    elif strategy == "bfill":
        df[col] = df[col].bfill()
    elif strategy == "drop":
        df = df.dropna(subset=[col])

    null_after = int(df[col].isna().sum()) if col in df.columns else 0
    filled = null_before - null_after

    return df, {
        "op": "fill_nulls",
        "column": col,
        "strategy": strategy,
        "filled": filled,
        "value_used": (
            round(float(value_used), 4)
            if isinstance(value_used, (int, float))
            else str(value_used)
            if value_used is not None
            else None
        ),
    }


def _op_drop_duplicates(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    subset = op.get("subset", None)
    before = len(df)
    df = df.drop_duplicates(subset=subset)
    dropped = before - len(df)
    return df, {
        "op": "drop_duplicates",
        "dropped": dropped,
        "remaining": len(df),
    }


# ---------------------------------------------------------------------------
# New ops
# ---------------------------------------------------------------------------


def _op_normalize(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(f"Column '{col}' is not numeric; normalize requires a numeric column.")
    method = op.get("method", "minmax")
    clean = df[col].dropna()

    if method == "minmax":
        mn = float(clean.min())
        mx = float(clean.max())
        denom = mx - mn
        if denom == 0:
            df[col] = df[col].where(df[col].isna(), 0.0)
        else:
            df[col] = (df[col] - mn) / denom
        return df, {
            "op": "normalize",
            "column": col,
            "method": method,
            "min": round(mn, 4),
            "max": round(mx, 4),
        }
    elif method == "zscore":
        mean_val = float(clean.mean())
        std_val = float(clean.std())
        if std_val == 0:
            df[col] = df[col].where(df[col].isna(), 0.0)
        else:
            df[col] = (df[col] - mean_val) / std_val
        return df, {
            "op": "normalize",
            "column": col,
            "method": method,
            "mean": round(mean_val, 4),
            "std": round(std_val, 4),
        }
    else:
        raise ValueError(f"Unknown normalize method: '{method}'. Valid: minmax, zscore")


def _op_label_encode(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    unique_vals = sorted(df[col].dropna().unique().tolist(), key=str)
    encoding = {str(v): i for i, v in enumerate(unique_vals)}
    df[col] = df[col].map({v: i for i, v in enumerate(unique_vals)})
    return df, {
        "op": "label_encode",
        "column": col,
        "encoding": encoding,
        "unique_count": len(encoding),
    }


def _op_extract_regex(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    pattern_str = op["pattern"]
    new_col = op["new_column"]
    group = op.get("group", 0)

    try:
        compiled = re.compile(pattern_str)
    except re.error as exc:
        raise ValueError(f"Invalid regex pattern: {exc}")

    def _extract(val):
        if not isinstance(val, str):
            return None
        m = compiled.search(val)
        if m is None:
            return None
        try:
            return m.group(group)
        except IndexError:
            return None

    results = df[col].apply(_extract)
    matched = int(results.notna().sum())
    failed = len(df) - matched
    df[new_col] = results
    return df, {
        "op": "extract_regex",
        "column": col,
        "new_column": new_col,
        "matched": matched,
        "failed": failed,
    }


def _op_date_diff(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col_a = op["date_col_a"]
    col_b = op["date_col_b"]
    new_col = op["new_column"]
    unit = op.get("unit", "days")

    for col in (col_a, col_b):
        if col not in df.columns:
            raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")

    da = pd.to_datetime(df[col_a], errors="coerce")
    db = pd.to_datetime(df[col_b], errors="coerce")
    delta_days = (da - db).dt.days

    if unit == "days":
        df[new_col] = delta_days
    elif unit == "months":
        df[new_col] = (delta_days / 30.44).round().astype("Int64")
    elif unit == "years":
        df[new_col] = (delta_days / 365.25).round().astype("Int64")
    else:
        raise ValueError(f"Unknown unit: '{unit}'. Valid: days, months, years")

    null_count = int(df[new_col].isna().sum())
    return df, {
        "op": "date_diff",
        "new_column": new_col,
        "unit": unit,
        "null_count": null_count,
    }


def _op_rank_column(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    new_col = op.get("new_column", col + "_rank")
    ascending = op.get("ascending", True)
    method = op.get("method", "dense")

    _valid_methods = {"average", "min", "max", "first", "dense"}
    if method not in _valid_methods:
        raise ValueError(f"Unknown rank method: '{method}'. Valid: {', '.join(sorted(_valid_methods))}")

    df[new_col] = df[col].rank(method=method, ascending=ascending, na_option="keep")
    return df, {
        "op": "rank_column",
        "column": col,
        "new_column": new_col,
        "ascending": ascending,
    }


# ---------------------------------------------------------------------------
# Filtering & Sorting ops
# ---------------------------------------------------------------------------


def _op_sort(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    by = op["by"]
    ascending = op.get("ascending", True)
    if isinstance(ascending, bool):
        ascending = [ascending] * len(by)
    missing = [c for c in by if c not in df.columns]
    if missing:
        raise ValueError(f"Columns not found: {missing}. Available: {list(df.columns)}")
    df = df.sort_values(by=by, ascending=ascending).reset_index(drop=True)
    return df, {"op": "sort", "by": by, "ascending": ascending, "rows": len(df)}


def _op_filter_isin(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    values = op["values"]
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    before = len(df)
    df = df[df[col].isin(values)].reset_index(drop=True)
    return df, {"op": "filter_isin", "column": col, "removed": before - len(df), "remaining": len(df)}


def _op_filter_not_isin(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    values = op["values"]
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    before = len(df)
    df = df[~df[col].isin(values)].reset_index(drop=True)
    return df, {"op": "filter_not_isin", "column": col, "removed": before - len(df), "remaining": len(df)}


def _op_filter_between(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    min_val = op["min"]
    max_val = op["max"]
    inclusive = op.get("inclusive", "both")
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    before = len(df)
    df = df[df[col].between(min_val, max_val, inclusive=inclusive)].reset_index(drop=True)
    return df, {
        "op": "filter_between",
        "column": col,
        "min": min_val,
        "max": max_val,
        "removed": before - len(df),
        "remaining": len(df),
    }


def _op_filter_date_range(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    start = op.get("start")
    end = op.get("end")
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    series = pd.to_datetime(df[col], errors="coerce")
    before = len(df)
    mask = pd.Series([True] * len(df), index=df.index)
    if start:
        mask &= series >= pd.Timestamp(start)
    if end:
        mask &= series <= pd.Timestamp(end)
    df = df[mask].reset_index(drop=True)
    return df, {
        "op": "filter_date_range",
        "column": col,
        "start": start,
        "end": end,
        "removed": before - len(df),
        "remaining": len(df),
    }


def _op_filter_regex(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    pattern = op["pattern"]
    negate = op.get("negate", False)
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    try:
        re.compile(pattern)
    except re.error as exc:
        raise ValueError(f"Invalid regex pattern: {exc}")
    before = len(df)
    mask = df[col].astype(str).str.contains(pattern, regex=True, na=False)
    if negate:
        mask = ~mask
    df = df[mask].reset_index(drop=True)
    return df, {
        "op": "filter_regex",
        "column": col,
        "pattern": pattern,
        "negate": negate,
        "removed": before - len(df),
        "remaining": len(df),
    }


def _op_filter_quantile(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    min_q = op.get("min_q", 0.0)
    max_q = op.get("max_q", 1.0)
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(f"Column '{col}' must be numeric for quantile filtering.")
    q_low = float(df[col].quantile(min_q))
    q_high = float(df[col].quantile(max_q))
    before = len(df)
    df = df[(df[col] >= q_low) & (df[col] <= q_high)].reset_index(drop=True)
    return df, {
        "op": "filter_quantile",
        "column": col,
        "min_q": min_q,
        "max_q": max_q,
        "q_low": round(q_low, 4),
        "q_high": round(q_high, 4),
        "removed": before - len(df),
        "remaining": len(df),
    }


def _op_filter_top_n(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    n = int(op["n"])
    keep = op.get("keep", "top")
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if keep not in ("top", "bottom"):
        raise ValueError(f"Unknown keep value: '{keep}'. Valid: top, bottom")
    before = len(df)
    if keep == "top":
        df = df.nlargest(n, col).reset_index(drop=True)
    else:
        df = df.nsmallest(n, col).reset_index(drop=True)
    return df, {
        "op": "filter_top_n",
        "column": col,
        "n": n,
        "keep": keep,
        "removed": before - len(df),
        "remaining": len(df),
    }


def _op_dedup_subset(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    columns = op.get("columns")
    keep = op.get("keep", "first")
    if keep not in ("first", "last", False):
        raise ValueError(f"Unknown keep value: '{keep}'. Valid: first, last, false")
    missing = [c for c in (columns or []) if c not in df.columns]
    if missing:
        raise ValueError(f"Columns not found: {missing}. Available: {list(df.columns)}")
    before = len(df)
    df = df.drop_duplicates(subset=columns, keep=keep).reset_index(drop=True)
    return df, {
        "op": "dedup_subset",
        "columns": columns,
        "keep": keep,
        "dropped": before - len(df),
        "remaining": len(df),
    }


# ---------------------------------------------------------------------------
# Numeric transform ops
# ---------------------------------------------------------------------------


def _op_log_transform(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    import numpy as np

    col = op["column"]
    method = op.get("method", "log1p")
    new_col = op.get("new_column", col)
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(f"Column '{col}' must be numeric for log transform.")
    valid = {"log1p", "log2", "log10", "log"}
    if method not in valid:
        raise ValueError(f"Unknown method: '{method}'. Valid: {', '.join(sorted(valid))}")
    if method == "log1p":
        df[new_col] = np.log1p(df[col])
    elif method == "log2":
        df[new_col] = np.log2(df[col])
    elif method == "log10":
        df[new_col] = np.log10(df[col])
    else:
        df[new_col] = np.log(df[col])
    return df, {
        "op": "log_transform",
        "column": col,
        "new_column": new_col,
        "method": method,
        "null_count": int(df[new_col].isna().sum()),
    }


def _op_sqrt_transform(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    import numpy as np

    col = op["column"]
    new_col = op.get("new_column", col)
    safe = op.get("safe", True)
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(f"Column '{col}' must be numeric.")
    df[new_col] = np.sqrt(df[col].abs()) if safe else np.sqrt(df[col])
    return df, {
        "op": "sqrt_transform",
        "column": col,
        "new_column": new_col,
        "safe": safe,
        "null_count": int(df[new_col].isna().sum()),
    }


def _op_robust_scale(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    new_col = op.get("new_column", col)
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(f"Column '{col}' must be numeric.")
    clean = df[col].dropna()
    median_val = float(clean.median())
    q1 = float(clean.quantile(0.25))
    q3 = float(clean.quantile(0.75))
    iqr = q3 - q1
    if iqr == 0:
        df[new_col] = df[col].where(df[col].isna(), 0.0)
    else:
        df[new_col] = (df[col] - median_val) / iqr
    return df, {
        "op": "robust_scale",
        "column": col,
        "new_column": new_col,
        "median": round(median_val, 4),
        "iqr": round(iqr, 4),
    }


def _op_winsorize(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    lower_q = op.get("lower_q", 0.05)
    upper_q = op.get("upper_q", 0.95)
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(f"Column '{col}' must be numeric.")
    clean = df[col].dropna()
    lower = float(clean.quantile(lower_q))
    upper = float(clean.quantile(upper_q))
    clipped_low = int((df[col] < lower).sum())
    clipped_high = int((df[col] > upper).sum())
    df[col] = df[col].clip(lower=lower, upper=upper)
    return df, {
        "op": "winsorize",
        "column": col,
        "lower_q": lower_q,
        "upper_q": upper_q,
        "clipped_low": clipped_low,
        "clipped_high": clipped_high,
        "lower_bound": round(lower, 4),
        "upper_bound": round(upper, 4),
    }


def _op_bin_column(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    bins = op["bins"]
    labels = op.get("labels")
    new_col = op.get("new_column", col + "_bin")
    right = op.get("right", True)
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(f"Column '{col}' must be numeric.")
    df[new_col] = pd.cut(df[col], bins=bins, labels=labels, right=right)
    dist = {str(k): int(v) for k, v in df[new_col].value_counts().items()}
    n_bins = bins if isinstance(bins, int) else len(bins) - 1
    return df, {"op": "bin_column", "column": col, "new_column": new_col, "bins": n_bins, "distribution": dist}


def _op_qbin_column(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    q = int(op["q"])
    labels = op.get("labels")
    new_col = op.get("new_column", col + "_qbin")
    duplicates = op.get("duplicates", "drop")
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(f"Column '{col}' must be numeric.")
    df[new_col] = pd.qcut(df[col], q=q, labels=labels, duplicates=duplicates)
    dist = {str(k): int(v) for k, v in df[new_col].value_counts().items()}
    return df, {"op": "qbin_column", "column": col, "new_column": new_col, "q": q, "distribution": dist}


def _op_clip_values(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    min_val = op.get("min")
    max_val = op.get("max")
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(f"Column '{col}' must be numeric.")
    clipped_low = int((df[col] < min_val).sum()) if min_val is not None else 0
    clipped_high = int((df[col] > max_val).sum()) if max_val is not None else 0
    df[col] = df[col].clip(lower=min_val, upper=max_val)
    return df, {
        "op": "clip_values",
        "column": col,
        "min": min_val,
        "max": max_val,
        "clipped_low": clipped_low,
        "clipped_high": clipped_high,
    }


def _op_round_values(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    decimals = op.get("decimals", 2)
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(f"Column '{col}' must be numeric.")
    df[col] = df[col].round(decimals)
    return df, {"op": "round_values", "column": col, "decimals": decimals}


def _op_abs_values(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    new_col = op.get("new_column", col)
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(f"Column '{col}' must be numeric.")
    df[new_col] = df[col].abs()
    return df, {"op": "abs_values", "column": col, "new_column": new_col}


# ---------------------------------------------------------------------------
# Encoding ops
# ---------------------------------------------------------------------------


def _op_ordinal_encode(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    order = op["order"]
    new_col = op.get("new_column", col)
    unknown_value = op.get("unknown_value", -1)
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    encoding = {str(v): i for i, v in enumerate(order)}
    mapped = df[col].astype(str).map(encoding)
    unmapped_mask = mapped.isna() & df[col].notna()
    unmapped = int(unmapped_mask.sum())
    mapped = mapped.where(~unmapped_mask, other=float(unknown_value))
    df[new_col] = mapped
    return df, {
        "op": "ordinal_encode",
        "column": col,
        "new_column": new_col,
        "encoding": encoding,
        "unmapped": unmapped,
    }


def _op_binary_encode(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    new_col = op.get("new_column", col + "_binary")
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if pd.api.types.is_numeric_dtype(df[col]):
        threshold = op.get("threshold", 0)
        df[new_col] = (df[col] > threshold).astype(int)
        extra = {"threshold": threshold}
    else:
        value = op.get("value")
        if value is None:
            raise ValueError("binary_encode requires 'threshold' for numeric cols or 'value' for categorical cols.")
        df[new_col] = (df[col].astype(str) == str(value)).astype(int)
        extra = {"value": value}
    positive_count = int((df[new_col] == 1).sum())
    return df, {"op": "binary_encode", "column": col, "new_column": new_col, "positive_count": positive_count, **extra}


def _op_frequency_encode(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    new_col = op.get("new_column", col + "_freq")
    normalize = op.get("normalize", False)
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    freq_map = df[col].value_counts(normalize=normalize)
    df[new_col] = df[col].map(freq_map)
    return df, {
        "op": "frequency_encode",
        "column": col,
        "new_column": new_col,
        "normalize": normalize,
        "unique_values": int(freq_map.shape[0]),
    }


# ---------------------------------------------------------------------------
# Temporal ops (lag, diff, rolling, cumulative)
# ---------------------------------------------------------------------------


def _op_lag(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    periods = int(op.get("periods", 1))
    new_col = op.get("new_column", f"{col}_lag{periods}")
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    df[new_col] = df[col].shift(periods)
    return df, {
        "op": "lag",
        "column": col,
        "new_column": new_col,
        "periods": periods,
        "null_count": int(df[new_col].isna().sum()),
    }


def _op_lead(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    periods = int(op.get("periods", 1))
    new_col = op.get("new_column", f"{col}_lead{periods}")
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    df[new_col] = df[col].shift(-periods)
    return df, {
        "op": "lead",
        "column": col,
        "new_column": new_col,
        "periods": periods,
        "null_count": int(df[new_col].isna().sum()),
    }


def _op_diff(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    periods = int(op.get("periods", 1))
    new_col = op.get("new_column", f"{col}_diff{periods}")
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(f"Column '{col}' must be numeric.")
    df[new_col] = df[col].diff(periods=periods)
    return df, {
        "op": "diff",
        "column": col,
        "new_column": new_col,
        "periods": periods,
        "null_count": int(df[new_col].isna().sum()),
    }


def _op_pct_change(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    periods = int(op.get("periods", 1))
    new_col = op.get("new_column", f"{col}_pct{periods}")
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(f"Column '{col}' must be numeric.")
    df[new_col] = df[col].pct_change(periods=periods) * 100
    return df, {
        "op": "pct_change",
        "column": col,
        "new_column": new_col,
        "periods": periods,
        "null_count": int(df[new_col].isna().sum()),
    }


def _op_rolling_agg(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    window = int(op["window"])
    agg = op.get("agg", "mean")
    new_col = op.get("new_column", f"{col}_roll{window}_{agg}")
    min_periods = int(op.get("min_periods", 1))
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(f"Column '{col}' must be numeric.")
    valid_aggs = {"mean", "sum", "std", "min", "max", "count", "median"}
    if agg not in valid_aggs:
        raise ValueError(f"Unknown agg: '{agg}'. Valid: {', '.join(sorted(valid_aggs))}")
    rolling = df[col].rolling(window=window, min_periods=min_periods)
    df[new_col] = getattr(rolling, agg)()
    return df, {
        "op": "rolling_agg",
        "column": col,
        "new_column": new_col,
        "window": window,
        "agg": agg,
        "null_count": int(df[new_col].isna().sum()),
    }


def _op_ewm(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    span = int(op.get("span", 7))
    new_col = op.get("new_column", f"{col}_ewm{span}")
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(f"Column '{col}' must be numeric.")
    df[new_col] = df[col].ewm(span=span, adjust=False).mean()
    return df, {
        "op": "ewm",
        "column": col,
        "new_column": new_col,
        "span": span,
        "null_count": int(df[new_col].isna().sum()),
    }


def _op_cumulative(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    agg = op.get("agg", "sum")
    new_col = op.get("new_column", f"{col}_cum{agg}")
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(f"Column '{col}' must be numeric.")
    valid_aggs = {"sum", "prod", "max", "min"}
    if agg not in valid_aggs:
        raise ValueError(f"Unknown agg: '{agg}'. Valid: {', '.join(sorted(valid_aggs))}")
    df[new_col] = getattr(df[col], f"cum{agg}")()
    return df, {"op": "cumulative", "column": col, "new_column": new_col, "agg": agg}


# ---------------------------------------------------------------------------
# Arithmetic & structural ops
# ---------------------------------------------------------------------------


def _op_column_math(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    """Evaluate formula and write result to target_column (create or overwrite)."""
    formula = op["formula"]
    target_col = op["target_column"]
    df[target_col] = _parse_expr(formula, df)
    return df, {
        "op": "column_math",
        "formula": formula,
        "target_column": target_col,
        "null_count": int(df[target_col].isna().sum()),
    }


def _op_conditional_assign(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    new_col = op["new_column"]
    conditions = op["conditions"]
    default = op.get("default")
    result = pd.Series([default] * len(df), index=df.index, dtype=object)
    # Apply in reverse so first condition in list wins
    for cond in reversed(conditions):
        col = cond["column"]
        if col not in df.columns:
            raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
        cop = cond["op"]
        val = cond["value"]
        label = cond["label"]
        if cop == "equals":
            mask = df[col] == val
        elif cop == "not_equals":
            mask = df[col] != val
        elif cop == "gt":
            mask = df[col] > val
        elif cop == "gte":
            mask = df[col] >= val
        elif cop == "lt":
            mask = df[col] < val
        elif cop == "lte":
            mask = df[col] <= val
        elif cop == "contains":
            mask = df[col].astype(str).str.contains(str(val), na=False)
        elif cop == "isin":
            mask = df[col].isin(val if isinstance(val, list) else [val])
        else:
            raise ValueError(f"Unknown condition op: '{cop}'. Valid: equals not_equals gt gte lt lte contains isin")
        result = result.where(~mask, other=label)
    df[new_col] = result
    dist = {str(k): int(v) for k, v in df[new_col].value_counts().items()}
    return df, {
        "op": "conditional_assign",
        "new_column": new_col,
        "conditions_applied": len(conditions),
        "distribution": dist,
    }


def _op_split_column(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    delimiter = op.get("delimiter", " ")
    new_columns = op.get("new_columns")
    drop_original = op.get("drop_original", False)
    n_splits = int(op.get("n_splits", -1))
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    split_df = df[col].astype(str).str.split(delimiter, n=n_splits if n_splits > 0 else -1, expand=True)
    created = []
    if new_columns:
        for i, nc in enumerate(new_columns):
            if i < split_df.shape[1]:
                df[nc] = split_df.iloc[:, i]
                created.append(nc)
    else:
        for i in range(split_df.shape[1]):
            nc = f"{col}_part{i}"
            df[nc] = split_df.iloc[:, i]
            created.append(nc)
    if drop_original:
        df = df.drop(columns=[col])
    return df, {
        "op": "split_column",
        "column": col,
        "delimiter": delimiter,
        "new_columns": created,
        "parts": split_df.shape[1],
    }


def _op_combine_columns(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    columns = op["columns"]
    delimiter = op.get("delimiter", " ")
    new_col = op.get("new_column", "_".join(columns))
    drop_originals = op.get("drop_originals", False)
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"Columns not found: {missing}. Available: {list(df.columns)}")
    combined = df[columns[0]].astype(str)
    for col in columns[1:]:
        combined = combined + delimiter + df[col].astype(str)
    df[new_col] = combined
    if drop_originals:
        df = df.drop(columns=columns)
    return df, {"op": "combine_columns", "columns": columns, "new_column": new_col, "delimiter": delimiter}


def _op_regex_replace(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    pattern = op["pattern"]
    replacement = op.get("replacement", "")
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    try:
        re.compile(pattern)
    except re.error as exc:
        raise ValueError(f"Invalid regex pattern: {exc}")
    before = df[col].astype(str).copy()
    df[col] = df[col].astype(str).str.replace(pattern, replacement, regex=True)
    changed = int((before != df[col]).sum())
    return df, {
        "op": "regex_replace",
        "column": col,
        "pattern": pattern,
        "replacement": replacement,
        "changed": changed,
    }


def _op_str_slice(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    start = op.get("start", 0)
    end = op.get("end")
    new_col = op.get("new_column", col + "_slice")
    if col not in df.columns:
        raise ValueError(f"Column not found: {col}. Available: {list(df.columns)}")
    df[new_col] = df[col].astype(str).str[start:end]
    return df, {"op": "str_slice", "column": col, "new_column": new_col, "start": start, "end": end}


def _op_concat_file(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    file_path = op["file_path"]
    direction = op.get("direction", "rows")
    fill_missing = op.get("fill_missing", "null")
    add_source = op.get("add_source_column", False)
    other_path = Path(file_path)
    if not other_path.exists():
        raise ValueError(f"File not found: {file_path}. Provide an absolute path.")
    try:
        other_df = pd.read_csv(other_path, encoding="utf-8")
    except UnicodeDecodeError:
        other_df = pd.read_csv(other_path, encoding="latin-1")
    if direction == "rows":
        before = len(df)
        if add_source:
            df = df.copy()
            df["__source"] = "original"
            other_df = other_df.copy()
            other_df["__source"] = other_path.name
        if fill_missing == "drop":
            common = list(set(df.columns) & set(other_df.columns))
            df = pd.concat([df[common], other_df[common]], ignore_index=True)
        else:
            df = pd.concat([df, other_df], ignore_index=True)
        return df, {
            "op": "concat_file",
            "direction": "rows",
            "file_appended": other_path.name,
            "rows_added": len(df) - before,
            "total_rows": len(df),
        }
    elif direction == "columns":
        if len(df) != len(other_df):
            raise ValueError(f"Column concat requires same row count. Got {len(df)} and {len(other_df)}.")
        added = len(other_df.columns)
        df = pd.concat([df.reset_index(drop=True), other_df.reset_index(drop=True)], axis=1)
        return df, {
            "op": "concat_file",
            "direction": "columns",
            "file_appended": other_path.name,
            "columns_added": added,
            "total_columns": len(df.columns),
        }
    else:
        raise ValueError(f"Unknown direction: '{direction}'. Valid: rows, columns")


def _op_melt(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    id_vars = op.get("id_vars")
    value_vars = op.get("value_vars")
    var_name = op.get("var_name", "variable")
    value_name = op.get("value_name", "value")
    for group_label, group in (("id_vars", id_vars or []), ("value_vars", value_vars or [])):
        missing = [c for c in group if c not in df.columns]
        if missing:
            raise ValueError(f"{group_label} columns not found: {missing}. Available: {list(df.columns)}")
    before = list(df.shape)
    df = df.melt(id_vars=id_vars, value_vars=value_vars, var_name=var_name, value_name=value_name)
    return df, {
        "op": "melt",
        "id_vars": id_vars,
        "value_vars": value_vars or "all non-id",
        "var_name": var_name,
        "value_name": value_name,
        "before_shape": before,
        "after_shape": list(df.shape),
    }
