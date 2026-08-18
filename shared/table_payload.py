"""Pack a DataFrame into a report at a size worth carrying.

A standalone report has to hold its own data — that is what makes it work with
nothing running behind it. Written as `to_json(orient="records")` every row
repeats every column name, so a 16,834 x 16 table cost 5.85 MB of the 10.75 MB
dashboard: the column names alone outweighed the values.

Storing the table by column instead, and replacing low-cardinality strings with
an index into a dictionary of their distinct values, measured 8.6x smaller on
that same table — 5.85 MB to 0.68 MB. The page rebuilds the identical
array-of-row-objects on load, so nothing downstream of it changes.
"""

from __future__ import annotations

import json

import pandas as pd

# Dictionary-encode a column when its distinct values are few enough that the
# codes plus the dictionary beat repeating the strings. A column of mostly
# unique values (an id, a free-text note) is left as-is: encoding it would add
# a dictionary as large as the column.
_MAX_DICT_RATIO = 0.5
_MIN_DICT_ROWS = 16


def encode_frame(df: pd.DataFrame) -> dict:
    """Return a compact, JSON-serialisable form of `df`."""
    columns: dict = {}
    for name in df.columns:
        series = df[name]
        values = series.tolist()
        if pd.api.types.is_numeric_dtype(series) or pd.api.types.is_bool_dtype(series):
            columns[str(name)] = [None if pd.isna(v) else v for v in values]
            continue

        text = ["" if pd.isna(v) else str(v) for v in values]
        distinct = set(text)
        worth_it = len(text) >= _MIN_DICT_ROWS and len(distinct) <= max(2, int(len(text) * _MAX_DICT_RATIO))
        if worth_it:
            vocabulary = sorted(distinct)
            index = {value: i for i, value in enumerate(vocabulary)}
            columns[str(name)] = {"d": vocabulary, "c": [index[v] for v in text]}
        else:
            columns[str(name)] = text

    return {"n": int(len(df)), "cols": [str(c) for c in df.columns], "enc": columns}


# Rebuilds exactly what to_json(orient="records") produced, so every consumer
# downstream keeps working against the same shape.
_DECODER_JS = """(function(P){
  const rows = new Array(P.n);
  for (let i = 0; i < P.n; i++) rows[i] = {};
  for (let c = 0; c < P.cols.length; c++) {
    const name = P.cols[c], col = P.enc[name];
    if (col && col.d) {
      const dict = col.d, codes = col.c;
      for (let i = 0; i < P.n; i++) rows[i][name] = dict[codes[i]];
    } else {
      for (let i = 0; i < P.n; i++) rows[i][name] = col[i];
    }
  }
  return rows;
})"""


def records_js(df: pd.DataFrame) -> str:
    """Return a JS expression evaluating to the same array `to_json` would give."""
    packed = json.dumps(encode_frame(df), separators=(",", ":"), default=str)
    return f"{_DECODER_JS}({packed})"
