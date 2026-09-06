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

# conditional_assign's conditions are a list of dicts, one level below where
# validate_ops used to look: it checked that `conditions` was a list and stopped.
# So every key inside was a guess, and a wrong guess came back as
#
#     Op 0 (conditional_assign): 'label'
#
# a bare KeyError naming a field the catalog never mentioned, from an op that
# had already been accepted. The catalog entry now names these four, and the
# aliases below cover the spellings a caller writes when reading `op` and
# `label` as English rather than as a vocabulary.
CONDITION_FIELDS: frozenset[str] = frozenset({"column", "op", "value", "label"})
CONDITION_ALIASES: dict[str, str] = {
    "then": "label",
    "result": "label",
    "assign": "label",
    "value_if_true": "label",
    "comparison": "op",
    "operator": "op",
}
CONDITION_OPS: frozenset[str] = frozenset({"equals", "not_equals", "gt", "gte", "lt", "lte", "contains", "isin"})
# Symbols mean exactly one comparison each, so accepting them costs nothing and
# `>` is what anyone writes first.
CONDITION_OP_ALIASES: dict[str, str] = {
    "==": "equals",
    "=": "equals",
    "eq": "equals",
    "!=": "not_equals",
    "<>": "not_equals",
    "ne": "not_equals",
    ">": "gt",
    ">=": "gte",
    "<": "lt",
    "<=": "lte",
    "in": "isin",
}


def normalize_condition(cond: dict) -> dict:
    """Rewrite one condition's aliased keys and comparison symbols in place."""
    if not isinstance(cond, dict):
        return cond
    for given, canonical in CONDITION_ALIASES.items():
        if canonical not in cond and given in cond:
            cond[canonical] = cond.pop(given)
    given_op = cond.get("op")
    if isinstance(given_op, str):
        cond["op"] = CONDITION_OP_ALIASES.get(given_op.strip().lower(), given_op)
    return cond


def _condition_errors(conditions: list, prefix: str) -> list[str]:
    """Name what is wrong with each condition dict, by its index."""
    errors: list[str] = []
    for j, cond in enumerate(conditions):
        where = f"{prefix} (conditional_assign) condition {j}"
        if not isinstance(cond, dict):
            errors.append(f"{where}: must be a dict, got {type(cond).__name__}")
            continue
        normalize_condition(cond)
        missing_keys = sorted(CONDITION_FIELDS - set(cond))
        if missing_keys:
            errors.append(
                f"{where}: missing {', '.join(missing_keys)} -- "
                f"each condition needs {', '.join(sorted(CONDITION_FIELDS))} "
                f"(label is the value assigned when it matches)"
            )
        unknown_keys = sorted(set(cond) - CONDITION_FIELDS)
        if unknown_keys:
            suggestion = _did_you_mean(unknown_keys[0], sorted(CONDITION_FIELDS))
            lead = f"did you mean {suggestion}? " if suggestion else ""
            errors.append(
                f"{where}: unknown field(s) {', '.join(unknown_keys)} -- "
                f"{lead}a condition accepts: {', '.join(sorted(CONDITION_FIELDS))}"
            )
        cop = cond.get("op")
        if cop is not None and cop not in CONDITION_OPS:
            errors.append(
                f"{where}: invalid op '{cop}'. Valid: {', '.join(sorted(CONDITION_OPS))} "
                f"(symbols {'>'}, {'>='}, {'<'}, {'<='}, ==, != are accepted too)"
            )
    return errors


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

# Every field each op reads off its own dict, derived from the handlers in
# _patch_ops.py and kept honest by a test that walks them both ways.
#
# Until this existed, validate_ops checked required fields and enumerated
# values and nothing else, so any other key was dropped in silence. Round 11
# measured what that costs on a three-row frame, changing one character:
#
#     fill_nulls column=a strategy=mean fill_zeros=True  ->  a = 1.0, 1.0, 1.0
#     fill_nulls column=a strategy=mean fill_zero=True   ->  a = 1.0, 0.0, 0.5
#
# Both `success: true`. The flag decides whether zeros count as missing, so the
# typo does not fail -- it writes different numbers into the caller's dataset
# and reports the same thing either way. `apply_patch` takes ops as a
# list[dict], one level below where strict_args can see, so nothing else was
# ever going to catch it.
_OP_FIELDS: dict[str, frozenset[str]] = {
    "abs_values": frozenset({"column", "new_column"}),
    "add_column": frozenset({"expr", "mode", "name", "source", "threshold"}),
    "bin_column": frozenset({"bins", "column", "labels", "new_column", "right"}),
    "binary_encode": frozenset({"column", "new_column", "threshold", "value"}),
    "boxcox_transform": frozenset({"column", "new_column"}),
    # threshold: the catalog advertised it all along; the handler now reads it
    # (IQR multiplier, or sigma count for method=std) instead of hardcoding
    # 1.5 and 3.
    "cap_outliers": frozenset({"column", "method", "th1", "th3", "threshold"}),
    "cast_column": frozenset({"column", "dtype"}),
    "clean_text": frozenset({"operations", "scope"}),
    "clip_values": frozenset({"column", "max", "min"}),
    "column_math": frozenset({"formula", "target_column"}),
    "combine_columns": frozenset({"columns", "delimiter", "drop_original", "drop_originals", "new_column"}),
    "concat_file": frozenset({"add_source_column", "direction", "file_path", "fill_missing"}),
    "conditional_assign": frozenset({"conditions", "default", "new_column"}),
    "cumulative": frozenset({"agg", "column", "new_column"}),
    "date_diff": frozenset({"date_col_a", "date_col_b", "new_column", "unit"}),
    "dedup_subset": frozenset({"columns", "keep"}),
    "diff": frozenset({"column", "new_column", "periods"}),
    "drop_column": frozenset({"columns"}),
    # keep: advertised as first|last|False and never read, so a request to
    # keep the last of each duplicate group kept the first.
    "drop_duplicates": frozenset({"subset", "keep"}),
    "ewm": frozenset({"column", "new_column", "span"}),
    "extract_regex": frozenset({"column", "group", "new_column", "pattern"}),
    "fill_nulls": frozenset({"column", "fill_zeros", "strategy"}),
    "filter_between": frozenset({"column", "inclusive", "max", "min"}),
    "filter_date_range": frozenset({"column", "end", "start"}),
    "filter_isin": frozenset({"column", "values"}),
    "filter_not_isin": frozenset({"column", "values"}),
    "filter_quantile": frozenset({"column", "max_q", "min_q"}),
    "filter_regex": frozenset({"column", "negate", "pattern"}),
    "filter_top_n": frozenset({"column", "keep", "n"}),
    "frequency_encode": frozenset({"column", "new_column", "normalize"}),
    "group_transform": frozenset({"agg", "by", "column", "descending", "group_by", "method", "new_column"}),
    # new_column: advertised and never read, so the codes were written over
    # the categorical they encode.
    "label_encode": frozenset({"column", "new_column"}),
    "lag": frozenset({"column", "new_column", "periods"}),
    "lead": frozenset({"column", "new_column", "periods"}),
    "log_transform": frozenset({"column", "method", "new_column"}),
    "melt": frozenset({"id_vars", "value_name", "value_vars", "var_name"}),
    "normalize": frozenset({"column", "method"}),
    "ordinal_encode": frozenset({"column", "new_column", "order", "unknown_value"}),
    "pct_change": frozenset({"column", "new_column", "periods"}),
    "qbin_column": frozenset({"column", "duplicates", "labels", "new_column", "q"}),
    "rank_column": frozenset({"ascending", "column", "method", "new_column"}),
    "regex_replace": frozenset({"column", "pattern", "replacement"}),
    "replace_values": frozenset({"column", "mapping"}),
    "robust_scale": frozenset({"column", "new_column"}),
    "rolling_agg": frozenset({"agg", "column", "min_periods", "new_column", "window"}),
    "round_values": frozenset({"column", "decimals"}),
    "sort": frozenset({"ascending", "by"}),
    "split_column": frozenset({"column", "delimiter", "drop_original", "drop_originals", "n_splits", "new_columns"}),
    "sqrt_transform": frozenset({"column", "new_column", "safe"}),
    "str_slice": frozenset({"column", "end", "new_column", "start"}),
    "winsorize": frozenset({"column", "lower_q", "upper_q"}),
    "yeojohnson_transform": frozenset({"column", "new_column"}),
}

# Spellings the op catalog printed that the handlers never read. Aliased rather
# than renamed, and only where the mapping is unambiguous: `new_col` is the
# same field as `new_column` everywhere in the fleet, and `add_source` can only
# mean `add_source_column`. date_diff's `start_col`/`end_col` are deliberately
# NOT aliased -- the handler computes date_col_a minus date_col_b, and guessing
# which of a pair is the start silently flips the sign of every result. Its
# catalog entry now names the fields the handler actually reads instead.
_FIELD_ALIASES: dict[str, dict[str, str]] = {
    "concat_file": {"add_source": "add_source_column"},
    # `drop_column` takes `columns` here and at run_cleaning_pipeline, and
    # `column` at ml-medium's run_preprocessing, which runs the same-named op.
    # Each server refused the other's spelling with a confident correction --
    # "did you mean columns?" one way, "missing required field: 'column'" the
    # other -- and nothing told the caller the two disagreed. `unwrap_params`
    # lifts the singular onto the plural; the list guard below accepts a bare
    # string as the one-element list it obviously means.
    "drop_column": {"column": "columns"},
}
_NEW_COL_OPS = frozenset(op for op, fields in _OP_FIELDS.items() if "new_column" in fields)
for _op in _NEW_COL_OPS:
    _FIELD_ALIASES.setdefault(_op, {})["new_col"] = "new_column"

# `op` names the operation; `params` survives unwrap_params, which lifts its
# contents onto the op and leaves the key in place.
_UNIVERSAL_FIELDS: frozenset[str] = frozenset({"op", "params"})

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


def apply_field_aliases(op: dict) -> dict:
    """Fill a canonical field from the spelling the op catalog printed."""
    for given, canonical in _FIELD_ALIASES.get(op.get("op", ""), {}).items():
        if canonical not in op and given in op:
            op[canonical] = op[given]
    return op


def known_fields(op_name: str) -> list[str]:
    """Every field this op reads, plus the aliases and the two universals."""
    return sorted(_UNIVERSAL_FIELDS | _OP_FIELDS.get(op_name, frozenset()) | set(_FIELD_ALIASES.get(op_name, {})))


def _did_you_mean(unknown: str, known: list[str]) -> str:
    """The closest accepted name, when one is obviously close."""
    import difflib

    # Containment first: fill_zero for fill_zeros and group_by for by are the
    # real misses, where one name is a substring of the other and difflib rates
    # the pair below any useful cutoff.
    for k in known:
        if k in unknown or unknown in k:
            return k
    close = difflib.get_close_matches(unknown, known, n=1, cutoff=0.75)
    return close[0] if close else ""


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

        apply_field_aliases(op)

        # Names before values: a field the op does not read is dropped, and the
        # dropped field decides what gets written. Checked first so a typo'd
        # optional field is reported as itself, not as a complaint about some
        # other field that happens to be missing.
        known = known_fields(op_name)
        unknown = sorted(k for k in op if k not in known)
        if unknown:
            suggestion = _did_you_mean(unknown[0], known)
            lead = f"did you mean {suggestion}? " if suggestion else ""
            errors.append(
                f"{prefix} ({op_name}): unknown field(s) {', '.join(unknown)} -- "
                f"{lead}{op_name} accepts: {', '.join(known)}"
            )
            continue

        if op_name == "drop_column":
            if "columns" not in op:
                errors.append(f"{prefix} (drop_column): 'columns' must be a list of strings")
            elif not isinstance(op["columns"], (list, str)):
                errors.append(f"{prefix} (drop_column): 'columns' must be a list of strings")
            elif isinstance(op["columns"], str):
                # A caller who sent `column="age"` lands here after the alias
                # above. One name is a one-element list; refusing it would be
                # refusing the request over its punctuation.
                op["columns"] = [op["columns"]]

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
            elif not op["conditions"]:
                errors.append(f"{prefix} (conditional_assign): 'conditions' is empty; every row would get 'default'")
            else:
                errors.extend(_condition_errors(op["conditions"], prefix))

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
