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
        raise ValueError(
            f"Columns not found: {missing}. "
            f"Available: {list(df.columns)}"
        )
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
            df[col] = df[col].apply(
                lambda v: v.strip().title() if isinstance(v, str) else v
            )
            affected += 1
    return df, {"op": "clean_text", "scope": scope, "columns_affected": affected}


def _op_cast_column(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    if col not in df.columns:
        raise ValueError(
            f"Column not found: {col}. Available: {list(df.columns)}"
        )
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
        failed = int(converted.isna().sum() - df[col].isna().sum())
        failed = max(0, failed)
        df[col] = converted.astype("Int64")
        to_dtype = "Int64"
    elif dtype == "float":
        converted = pd.to_numeric(df[col], errors="coerce")
        failed = int(converted.isna().sum() - df[col].isna().sum())
        failed = max(0, failed)
        df[col] = converted
        to_dtype = "float64"
    elif dtype == "str":
        df[col] = df[col].astype(str)
        to_dtype = "object"
    elif dtype == "datetime":
        converted = pd.to_datetime(df[col], errors="coerce")
        failed = int(converted.isna().sum() - df[col].isna().sum())
        failed = max(0, failed)
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
        raise ValueError(
            f"Column not found: {col}. Available: {list(df.columns)}"
        )
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
            raise ValueError(
                f"Unknown token in expr: '{token}'. "
                f"Must be a column name or number."
            )

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
            raise ValueError(
                f"Source column not found: {source}. Available: {list(df.columns)}"
            )
        threshold = op.get("threshold", 0)
        freq = df[source].value_counts()
        df[name] = df[source].apply(
            lambda v: v if freq.get(v, 0) >= threshold else "Other"
        )
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
        raise ValueError(
            f"Column not found: {col}. Available: {list(df.columns)}"
        )
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
        result_info.update({
            "capped_lower": capped_lower,
            "capped_upper": capped_upper,
            "lower_limit": round(lower, 4),
            "upper_limit": round(upper, 4),
        })
    elif method == "std":
        mean_val = float(clean.mean())
        std_val = float(clean.std())
        lower = mean_val - 3 * std_val
        upper = mean_val + 3 * std_val
        capped_lower = int((df[col] < lower).sum())
        capped_upper = int((df[col] > upper).sum())
        df[col] = df[col].clip(lower=lower, upper=upper)
        result_info.update({
            "capped_lower": capped_lower,
            "capped_upper": capped_upper,
            "lower_limit": round(lower, 4),
            "upper_limit": round(upper, 4),
        })

    return df, result_info


def _op_fill_nulls(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    if col not in df.columns:
        raise ValueError(
            f"Column not found: {col}. Available: {list(df.columns)}"
        )
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
            if isinstance(value_used, (int, float)) else
            str(value_used) if value_used is not None else None
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
        raise ValueError(
            f"Column not found: {col}. Available: {list(df.columns)}"
        )
    if not pd.api.types.is_numeric_dtype(df[col]):
        raise ValueError(
            f"Column '{col}' is not numeric; normalize requires a numeric column."
        )
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
        raise ValueError(
            f"Unknown normalize method: '{method}'. Valid: minmax, zscore"
        )


def _op_label_encode(df: pd.DataFrame, op: dict) -> tuple[pd.DataFrame, dict]:
    col = op["column"]
    if col not in df.columns:
        raise ValueError(
            f"Column not found: {col}. Available: {list(df.columns)}"
        )
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
        raise ValueError(
            f"Column not found: {col}. Available: {list(df.columns)}"
        )
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
            raise ValueError(
                f"Column not found: {col}. Available: {list(df.columns)}"
            )

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
        raise ValueError(
            f"Unknown unit: '{unit}'. Valid: days, months, years"
        )

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
        raise ValueError(
            f"Column not found: {col}. Available: {list(df.columns)}"
        )
    new_col = op.get("new_column", col + "_rank")
    ascending = op.get("ascending", True)
    method = op.get("method", "dense")

    _valid_methods = {"average", "min", "max", "first", "dense"}
    if method not in _valid_methods:
        raise ValueError(
            f"Unknown rank method: '{method}'. "
            f"Valid: {', '.join(sorted(_valid_methods))}"
        )

    df[new_col] = df[col].rank(method=method, ascending=ascending, na_option="keep")
    return df, {
        "op": "rank_column",
        "column": col,
        "new_column": new_col,
        "ascending": ascending,
    }
