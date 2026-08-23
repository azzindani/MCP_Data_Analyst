"""Ring-1 pure utility — validates op arrays with no I/O or side effects."""

from __future__ import annotations

# The group_transform vocabulary lives here, not beside the handler, because
# the validator and the handler must never disagree about what is legal --
# _patch_ops imports these back. Reducers collapse a group to one number and
# broadcast it to that group's rows; row-wise aggs give each row its own value
# relative to its group.
GROUP_REDUCERS: frozenset[str] = frozenset({"sum", "mean", "median", "max", "min", "std", "count", "nunique"})
GROUP_ROWWISE: frozenset[str] = frozenset({"share", "rank", "cumsum", "zscore", "diff_from_mean", "pct_of_max"})
GROUP_AGGS: frozenset[str] = GROUP_REDUCERS | GROUP_ROWWISE

VALID_OPS: frozenset[str] = frozenset(
    {
        # original
        "drop_column",
        "clean_text",
        "cast_column",
        "replace_values",
        "add_column",
        "cap_outliers",
        "fill_nulls",
        "drop_duplicates",
        "normalize",
        "label_encode",
        "extract_regex",
        "date_diff",
        "rank_column",
        # filtering & sorting
        "sort",
        "filter_isin",
        "filter_not_isin",
        "filter_between",
        "filter_date_range",
        "filter_regex",
        "filter_quantile",
        "filter_top_n",
        "dedup_subset",
        # numeric transforms
        "log_transform",
        "sqrt_transform",
        "boxcox_transform",
        "yeojohnson_transform",
        "robust_scale",
        "winsorize",
        "bin_column",
        "qbin_column",
        "clip_values",
        "round_values",
        "abs_values",
        # encoding
        "ordinal_encode",
        "binary_encode",
        "frequency_encode",
        # temporal
        "lag",
        "lead",
        "diff",
        "pct_change",
        "rolling_agg",
        "ewm",
        "cumulative",
        "group_transform",
        # arithmetic & structural
        "column_math",
        "conditional_assign",
        "split_column",
        "combine_columns",
        "regex_replace",
        "str_slice",
        "concat_file",
        "melt",
    }
)

_FILL_STRATEGIES = frozenset({"mean", "median", "mode", "ffill", "bfill", "drop"})
_CAST_DTYPES = frozenset({"int", "float", "str", "datetime"})
_CLEAN_SCOPES = frozenset({"headers", "values", "both"})
# clean_text applied strip+title unconditionally and read no vocabulary at
# all, so asking for lowercase headers returned Campaign_Platform.
CLEAN_OPERATIONS = frozenset({"strip", "lower", "upper", "title", "collapse_spaces"})
_CAP_METHODS = frozenset({"iqr", "std"})
_ADD_MODES = frozenset({"math", "threshold"})


def unwrap_params(op: dict) -> dict:
    """Lift a nested `params` dict up onto the op itself.

    list_patch_ops describes every op as {"op": "clean_text", "params": "scope:
    headers|values|both"} -- so a caller reading the catalog writes the same
    shape back, {"op": ..., "params": {...}}, and every field inside it was
    dropped. The refusal was then actively misleading:

        Op 0 (add_column): missing 'name'; math mode requires 'expr'

    naming two fields the caller had in fact supplied. The catalog taught the
    form the tool rejected. Flat keys win when both are present, so an op that
    already works is untouched.
    """
    nested = op.get("params")
    if isinstance(nested, dict):
        for k, v in nested.items():
            op.setdefault(k, v)
    return op


def validate_ops(ops: list[dict]) -> list[str]:
    """Validate op list. Returns list of error strings (empty = valid)."""
    errors: list[str] = []
    for op in ops:
        if isinstance(op, dict):
            unwrap_params(op)
    if not ops:
        errors.append("ops list is empty; at least one op is required.")
        return errors

    for i, op in enumerate(ops):
        prefix = f"Op {i}"
        if not isinstance(op, dict):
            errors.append(f"{prefix}: must be a dict, got {type(op).__name__}")
            continue

        op_name = op.get("op")
        if not op_name:
            errors.append(f"{prefix}: missing 'op' field")
            continue

        if op_name not in VALID_OPS:
            errors.append(f"{prefix}: unknown op '{op_name}'. Valid ops: {', '.join(sorted(VALID_OPS))}")
            continue

        if op_name == "drop_column":
            if "columns" not in op or not isinstance(op["columns"], list):
                errors.append(f"{prefix} (drop_column): 'columns' must be a list of strings")

        elif op_name == "clean_text":
            scope = op.get("scope", "both")
            if scope not in _CLEAN_SCOPES:
                errors.append(
                    f"{prefix} (clean_text): invalid scope '{scope}'. Valid: {', '.join(sorted(_CLEAN_SCOPES))}"
                )
            given = op.get("operations")
            if given is not None:
                given = [given] if isinstance(given, str) else given
                if not isinstance(given, list):
                    errors.append(f"{prefix} (clean_text): 'operations' must be a list of strings")
                else:
                    bad = [o for o in given if o not in CLEAN_OPERATIONS]
                    if bad:
                        errors.append(
                            f"{prefix} (clean_text): invalid operations {bad}. "
                            f"Valid: {', '.join(sorted(CLEAN_OPERATIONS))}"
                        )

        elif op_name == "cast_column":
            if "column" not in op:
                errors.append(f"{prefix} (cast_column): missing 'column'")
            dtype = op.get("dtype")
            if dtype not in _CAST_DTYPES:
                errors.append(
                    f"{prefix} (cast_column): invalid dtype '{dtype}'. Valid: {', '.join(sorted(_CAST_DTYPES))}"
                )

        elif op_name == "replace_values":
            if "column" not in op:
                errors.append(f"{prefix} (replace_values): missing 'column'")
            if "mapping" not in op or not isinstance(op["mapping"], dict):
                errors.append(f"{prefix} (replace_values): 'mapping' must be a dict")

        elif op_name == "add_column":
            if "name" not in op:
                errors.append(f"{prefix} (add_column): missing 'name'")
            mode = op.get("mode", "math")
            if mode not in _ADD_MODES:
                errors.append(f"{prefix} (add_column): invalid mode '{mode}'. Valid: {', '.join(sorted(_ADD_MODES))}")
            if mode == "math" and "expr" not in op:
                errors.append(f"{prefix} (add_column): math mode requires 'expr'")
            if mode == "threshold" and "source" not in op:
                errors.append(f"{prefix} (add_column): threshold mode requires 'source'")

        elif op_name == "cap_outliers":
            if "column" not in op:
                errors.append(f"{prefix} (cap_outliers): missing 'column'")
            method = op.get("method", "iqr")
            if method not in _CAP_METHODS:
                errors.append(
                    f"{prefix} (cap_outliers): invalid method '{method}'. Valid: {', '.join(sorted(_CAP_METHODS))}"
                )

        elif op_name == "fill_nulls":
            if "column" not in op:
                errors.append(f"{prefix} (fill_nulls): missing 'column'")
            if "strategy" not in op:
                # Distinct from an invalid one: reporting a missing key as
                # "invalid strategy 'None'" sends the caller looking for a bad
                # value it never passed.
                errors.append(
                    f"{prefix} (fill_nulls): missing 'strategy'. Valid: {', '.join(sorted(_FILL_STRATEGIES))}"
                )
            elif op["strategy"] not in _FILL_STRATEGIES:
                errors.append(
                    f"{prefix} (fill_nulls): invalid strategy '{op['strategy']}'. "
                    f"Valid: {', '.join(sorted(_FILL_STRATEGIES))}. There is no literal-value fill; "
                    f"'mean' or 'median' on a mostly-zero column is the closest equivalent."
                )

        elif op_name == "normalize":
            if "column" not in op:
                errors.append(f"{prefix} (normalize): missing 'column'")
            method = op.get("method", "minmax")
            if method not in {"minmax", "zscore"}:
                errors.append(f"{prefix} (normalize): invalid method '{method}'. Valid: minmax, zscore")

        elif op_name == "label_encode":
            if "column" not in op:
                errors.append(f"{prefix} (label_encode): missing 'column'")

        elif op_name == "extract_regex":
            if "column" not in op:
                errors.append(f"{prefix} (extract_regex): missing 'column'")
            if "pattern" not in op:
                errors.append(f"{prefix} (extract_regex): missing 'pattern'")
            if "new_column" not in op:
                errors.append(f"{prefix} (extract_regex): missing 'new_column'")

        elif op_name == "date_diff":
            if "date_col_a" not in op:
                errors.append(f"{prefix} (date_diff): missing 'date_col_a'")
            if "date_col_b" not in op:
                errors.append(f"{prefix} (date_diff): missing 'date_col_b'")
            if "new_column" not in op:
                errors.append(f"{prefix} (date_diff): missing 'new_column'")
            unit = op.get("unit", "days")
            if unit not in {"days", "months", "years"}:
                errors.append(f"{prefix} (date_diff): invalid unit '{unit}'. Valid: days, months, years")

        elif op_name == "rank_column":
            if "column" not in op:
                errors.append(f"{prefix} (rank_column): missing 'column'")
            method = op.get("method", "dense")
            if method not in {"average", "min", "max", "first", "dense"}:
                errors.append(
                    f"{prefix} (rank_column): invalid method '{method}'. Valid: average, min, max, first, dense"
                )

        # --- filtering & sorting ---
        elif op_name == "sort":
            if "by" not in op or not isinstance(op["by"], list):
                errors.append(f"{prefix} (sort): 'by' must be a list of column names")

        elif op_name in ("filter_isin", "filter_not_isin"):
            if "column" not in op:
                errors.append(f"{prefix} ({op_name}): missing 'column'")
            if "values" not in op or not isinstance(op["values"], list):
                errors.append(f"{prefix} ({op_name}): 'values' must be a list")

        elif op_name == "filter_between":
            if "column" not in op:
                errors.append(f"{prefix} (filter_between): missing 'column'")
            if "min" not in op or "max" not in op:
                errors.append(f"{prefix} (filter_between): requires 'min' and 'max'")

        elif op_name == "filter_date_range":
            if "column" not in op:
                errors.append(f"{prefix} (filter_date_range): missing 'column'")
            if "start" not in op and "end" not in op:
                errors.append(f"{prefix} (filter_date_range): at least one of 'start' or 'end' is required")

        elif op_name == "filter_regex":
            if "column" not in op:
                errors.append(f"{prefix} (filter_regex): missing 'column'")
            if "pattern" not in op:
                errors.append(f"{prefix} (filter_regex): missing 'pattern'")

        elif op_name == "filter_quantile":
            if "column" not in op:
                errors.append(f"{prefix} (filter_quantile): missing 'column'")

        elif op_name == "filter_top_n":
            if "column" not in op:
                errors.append(f"{prefix} (filter_top_n): missing 'column'")
            if "n" not in op:
                errors.append(f"{prefix} (filter_top_n): missing 'n'")
            keep = op.get("keep", "top")
            if keep not in {"top", "bottom"}:
                errors.append(f"{prefix} (filter_top_n): 'keep' must be top or bottom")

        elif op_name == "dedup_subset":
            pass  # all params optional

        # --- numeric transforms ---
        elif op_name == "log_transform":
            if "column" not in op:
                errors.append(f"{prefix} (log_transform): missing 'column'")
            method = op.get("method", "log1p")
            if method not in {"log1p", "log2", "log10", "log"}:
                errors.append(f"{prefix} (log_transform): invalid method '{method}'. Valid: log1p log2 log10 log")

        elif op_name in ("sqrt_transform", "robust_scale", "abs_values"):
            if "column" not in op:
                errors.append(f"{prefix} ({op_name}): missing 'column'")

        elif op_name == "winsorize":
            if "column" not in op:
                errors.append(f"{prefix} (winsorize): missing 'column'")

        elif op_name == "bin_column":
            if "column" not in op:
                errors.append(f"{prefix} (bin_column): missing 'column'")
            if "bins" not in op:
                errors.append(f"{prefix} (bin_column): missing 'bins'")

        elif op_name == "qbin_column":
            if "column" not in op:
                errors.append(f"{prefix} (qbin_column): missing 'column'")
            if "q" not in op:
                errors.append(f"{prefix} (qbin_column): missing 'q'")

        elif op_name in ("clip_values",):
            if "column" not in op:
                errors.append(f"{prefix} (clip_values): missing 'column'")
            if "min" not in op and "max" not in op:
                errors.append(f"{prefix} (clip_values): at least one of 'min' or 'max' is required")

        elif op_name in ("round_values",):
            if "column" not in op:
                errors.append(f"{prefix} (round_values): missing 'column'")

        # --- encoding ---
        elif op_name == "ordinal_encode":
            if "column" not in op:
                errors.append(f"{prefix} (ordinal_encode): missing 'column'")
            if "order" not in op or not isinstance(op["order"], list):
                errors.append(f"{prefix} (ordinal_encode): 'order' must be a list of values")

        elif op_name == "binary_encode":
            if "column" not in op:
                errors.append(f"{prefix} (binary_encode): missing 'column'")

        elif op_name == "frequency_encode":
            if "column" not in op:
                errors.append(f"{prefix} (frequency_encode): missing 'column'")

        # --- temporal ---
        elif op_name in ("lag", "lead", "diff", "pct_change", "ewm", "cumulative"):
            if "column" not in op:
                errors.append(f"{prefix} ({op_name}): missing 'column'")

        elif op_name == "group_transform":
            if "column" not in op:
                errors.append(f"{prefix} (group_transform): missing 'column'")
            by = op.get("group_by") or op.get("by")
            if not by:
                errors.append(f"{prefix} (group_transform): missing 'group_by' (the column(s) defining each group)")
            elif not isinstance(by, (list, str)):
                errors.append(f"{prefix} (group_transform): 'group_by' must be a column name or a list of them")
            agg = op.get("agg", "mean")
            if agg not in GROUP_AGGS:
                errors.append(f"{prefix} (group_transform): invalid agg '{agg}'. Valid: {' '.join(sorted(GROUP_AGGS))}")

        elif op_name == "rolling_agg":
            if "column" not in op:
                errors.append(f"{prefix} (rolling_agg): missing 'column'")
            if "window" not in op:
                errors.append(f"{prefix} (rolling_agg): missing 'window'")
            agg = op.get("agg", "mean")
            if agg not in {"mean", "sum", "std", "min", "max", "count", "median"}:
                errors.append(f"{prefix} (rolling_agg): invalid agg '{agg}'. Valid: mean sum std min max count median")

        # --- arithmetic & structural ---
        elif op_name == "column_math":
            if "formula" not in op:
                errors.append(f"{prefix} (column_math): missing 'formula'")
            if "target_column" not in op:
                errors.append(f"{prefix} (column_math): missing 'target_column'")

        elif op_name == "conditional_assign":
            if "new_column" not in op:
                errors.append(f"{prefix} (conditional_assign): missing 'new_column'")
            if "conditions" not in op or not isinstance(op["conditions"], list):
                errors.append(f"{prefix} (conditional_assign): 'conditions' must be a list")

        elif op_name == "split_column":
            if "column" not in op:
                errors.append(f"{prefix} (split_column): missing 'column'")

        elif op_name == "combine_columns":
            if "columns" not in op or not isinstance(op["columns"], list) or len(op["columns"]) < 2:
                errors.append(f"{prefix} (combine_columns): 'columns' must be a list of at least 2 column names")

        elif op_name == "regex_replace":
            if "column" not in op:
                errors.append(f"{prefix} (regex_replace): missing 'column'")
            if "pattern" not in op:
                errors.append(f"{prefix} (regex_replace): missing 'pattern'")

        elif op_name == "str_slice":
            if "column" not in op:
                errors.append(f"{prefix} (str_slice): missing 'column'")

        elif op_name == "concat_file":
            if "file_path" not in op:
                errors.append(f"{prefix} (concat_file): missing 'file_path'")
            direction = op.get("direction", "rows")
            if direction not in {"rows", "columns"}:
                errors.append(f"{prefix} (concat_file): 'direction' must be rows or columns")

        elif op_name == "melt":
            pass  # all params are optional

    return errors
