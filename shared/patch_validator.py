from __future__ import annotations

VALID_OPS: frozenset[str] = frozenset(
    {
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
    }
)

_FILL_STRATEGIES = frozenset({"mean", "median", "mode", "ffill", "bfill", "drop"})
_CAST_DTYPES = frozenset({"int", "float", "str", "datetime"})
_CLEAN_SCOPES = frozenset({"headers", "values", "both"})
_CAP_METHODS = frozenset({"iqr", "std"})
_ADD_MODES = frozenset({"math", "threshold"})


def validate_ops(ops: list[dict]) -> list[str]:
    """Validate op list. Returns list of error strings (empty = valid)."""
    errors: list[str] = []
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
            errors.append(
                f"{prefix}: unknown op '{op_name}'. "
                f"Valid ops: {', '.join(sorted(VALID_OPS))}"
            )
            continue

        if op_name == "drop_column":
            if "columns" not in op or not isinstance(op["columns"], list):
                errors.append(
                    f"{prefix} (drop_column): 'columns' must be a list of strings"
                )

        elif op_name == "clean_text":
            scope = op.get("scope", "both")
            if scope not in _CLEAN_SCOPES:
                errors.append(
                    f"{prefix} (clean_text): invalid scope '{scope}'. "
                    f"Valid: {', '.join(sorted(_CLEAN_SCOPES))}"
                )

        elif op_name == "cast_column":
            if "column" not in op:
                errors.append(f"{prefix} (cast_column): missing 'column'")
            dtype = op.get("dtype")
            if dtype not in _CAST_DTYPES:
                errors.append(
                    f"{prefix} (cast_column): invalid dtype '{dtype}'. "
                    f"Valid: {', '.join(sorted(_CAST_DTYPES))}"
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
                errors.append(
                    f"{prefix} (add_column): invalid mode '{mode}'. "
                    f"Valid: {', '.join(sorted(_ADD_MODES))}"
                )
            if mode == "math" and "expr" not in op:
                errors.append(f"{prefix} (add_column): math mode requires 'expr'")
            if mode == "threshold" and "source" not in op:
                errors.append(
                    f"{prefix} (add_column): threshold mode requires 'source'"
                )

        elif op_name == "cap_outliers":
            if "column" not in op:
                errors.append(f"{prefix} (cap_outliers): missing 'column'")
            method = op.get("method", "iqr")
            if method not in _CAP_METHODS:
                errors.append(
                    f"{prefix} (cap_outliers): invalid method '{method}'. "
                    f"Valid: {', '.join(sorted(_CAP_METHODS))}"
                )

        elif op_name == "fill_nulls":
            if "column" not in op:
                errors.append(f"{prefix} (fill_nulls): missing 'column'")
            strategy = op.get("strategy")
            if strategy not in _FILL_STRATEGIES:
                errors.append(
                    f"{prefix} (fill_nulls): invalid strategy '{strategy}'. "
                    f"Valid: {', '.join(sorted(_FILL_STRATEGIES))}"
                )

        elif op_name == "normalize":
            if "column" not in op:
                errors.append(f"{prefix} (normalize): missing 'column'")
            method = op.get("method", "minmax")
            if method not in {"minmax", "zscore"}:
                errors.append(
                    f"{prefix} (normalize): invalid method '{method}'. "
                    f"Valid: minmax, zscore"
                )

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
                errors.append(
                    f"{prefix} (date_diff): invalid unit '{unit}'. "
                    f"Valid: days, months, years"
                )

        elif op_name == "rank_column":
            if "column" not in op:
                errors.append(f"{prefix} (rank_column): missing 'column'")
            method = op.get("method", "dense")
            if method not in {"average", "min", "max", "first", "dense"}:
                errors.append(
                    f"{prefix} (rank_column): invalid method '{method}'. "
                    f"Valid: average, min, max, first, dense"
                )

    return errors
