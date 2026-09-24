"""A big file is streamed through a chain's first filters -- and the answer is the whole read's.

run_chain read every file whole before its first filter ran. Over
MCP_CHAIN_LAZY_MB, a load read by one ops step that opens with filters is now
streamed: each chunk is filtered as it arrives, and when the table then feeds
one group_by, scalar, pivot or resample, only the columns those steps name
are kept. The claim is that nothing but memory changes, so every case here
runs the same chain twice -- streamed in 3-row chunks, and read whole -- and
requires the same answer: the same step results, variables, notes, errors and
result table, and byte-identical written files.

Where a chunk sees a column differently from the whole file -- whole numbers
in one chunk and a gap in the next, a stretch of blanks in a text column,
zero-padded ids -- the file is read again with the whole read's types. Where
that cannot be settled -- a line with too many fields, a true/false column
with gaps -- the file is read whole, and these cases check that it was.
"""

from __future__ import annotations

import copy

import numpy as np
import pandas as pd
import pytest

from servers.data_transform import engine

N = 40


@pytest.fixture
def data(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("MCP_CHAIN_CHUNK_ROWS", "3")
    rng = np.random.default_rng(71)
    df = pd.DataFrame(
        {
            "order_id": np.arange(1, N + 1),
            " region ": rng.choice(["North", "South", "East", None], N),
            "status": rng.choice(["ok", "cancelled"], N, p=[0.8, 0.2]),
            "amount": rng.normal(100, 40, N).round(2),
            "units": rng.integers(1, 9, N),
            "note": rng.choice(["a", "b, with comma", 'c "quoted"', None], N),
        }
    )
    for i in range(12):
        df[f"wide_{i}"] = rng.normal(0, 1, N).round(3)
    df.to_csv(tmp_path / "orders.csv", index=False)
    return tmp_path


def _both(data, monkeypatch, steps: list[dict], **kw) -> tuple[dict, dict, dict[str, bytes], dict[str, bytes]]:
    """The chain streamed, then read whole; the tables each run wrote.

    Only the CSVs: the receipt and lineage beside each carry the time it was written.
    """
    runs, files = [], []
    for lazy in ("0", "1000000"):
        monkeypatch.setenv("MCP_CHAIN_LAZY_MB", lazy)
        for old in data.glob("out*"):
            old.unlink()
        runs.append(engine.run_chain(copy.deepcopy(steps), **kw))
        files.append({p.name: p.read_bytes() for p in sorted(data.glob("out*.csv"))})
    return runs[0], runs[1], files[0], files[1]


def _same(streamed: dict, whole: dict) -> None:
    def comparable(r: dict) -> dict:
        r = copy.deepcopy(r)
        r.pop("progress", None)
        r.pop("written", None)
        r.pop("token_estimate", None)  # the streamed run says how it read, so it is longer
        for s in r.get("steps", []):
            s.pop("read", None)
        return r

    assert comparable(streamed) == comparable(whole)


def _load(r: dict) -> dict:
    return next(s for s in r["steps"] if s.get("kind") == "load")


def _streamed(r: dict) -> bool:
    return _load(r).get("read", {}).get("streamed") is True


CLEAN = {
    "id": "clean",
    "from": "orders",
    "ops": [
        {"op": "filter", "where": "status == 'ok'"},
        {"op": "filter", "where": "amount > $floor"},
    ],
}
FLOOR = {"id": "floor", "param": 50}
ORDERS = {"id": "orders", "load": "orders.csv"}
BY_REGION = {"group_by": "region", "agg": {"revenue": "sum(amount)", "orders": "count()", "big": "count_if(units > 4)"}}


class TestTheAnswerIsTheWholeReads:
    def test_filters_then_a_group_by_keep_only_the_columns_named(self, data, monkeypatch):
        steps = [{"id": "floor", "param": 80}, ORDERS, CLEAN, BY_REGION, {"write": "out_by_region.csv"}]
        streamed, whole, a, b = _both(data, monkeypatch, steps)
        assert streamed["success"] is True, streamed.get("error")
        _same(streamed, whole)
        assert a == b and a
        read = _load(streamed)["read"]
        assert read["streamed"] is True and read["columns_read"] == ["region", "status", "amount", "units"]
        df = pd.read_csv(data / "orders.csv")
        assert read["rows_kept_by_filters"] == int(((df["status"] == "ok") & (df["amount"] > 80)).sum())

    def test_filters_then_derive_sort_and_write_keep_every_column(self, data, monkeypatch):
        clean = copy.deepcopy(CLEAN)
        clean["ops"] += [
            {"op": "derive", "name": "net", "expr": "amount * units"},
            {"op": "sort", "by": ["net"], "ascending": False},
        ]
        streamed, whole, a, b = _both(data, monkeypatch, [FLOOR, ORDERS, clean, {"write": "out_net.csv"}])
        _same(streamed, whole)
        assert a == b and a
        assert _streamed(streamed) and "columns_read" not in _load(streamed)["read"]

    def test_a_derived_column_counts_in_the_width_when_columns_are_dropped(self, data, monkeypatch):
        clean = copy.deepcopy(CLEAN)
        clean["ops"] += [{"op": "derive", "name": "net", "expr": "amount * units"}]
        steps = [FLOOR, ORDERS, clean, {"group_by": "region", "agg": {"net": "sum(net)"}}]
        streamed, whole, _, _ = _both(data, monkeypatch, steps)
        _same(streamed, whole)
        assert "columns_read" in _load(streamed)["read"]
        assert streamed["steps"][2]["columns"] == 19

    def test_a_filter_that_keeps_nothing_says_so_the_same_way(self, data, monkeypatch):
        streamed, whole, _, _ = _both(data, monkeypatch, [{"id": "floor", "param": 10_000}, ORDERS, CLEAN])
        _same(streamed, whole)
        assert _streamed(streamed)
        assert any("no row matched 'amount > 10000.0'" in n for n in streamed["steps"][-1]["notes"])

    def test_a_dry_run_samples_the_files_first_rows_with_every_column(self, data, monkeypatch):
        steps = [FLOOR, ORDERS, CLEAN, BY_REGION]
        streamed, whole, _, _ = _both(data, monkeypatch, steps, dry_run=True)
        _same(streamed, whole)
        assert _streamed(streamed) and "columns_read" not in _load(streamed)["read"]
        assert [r["order_id"] for r in _load(streamed)["sample"]] == [1, 2, 3]

    def test_the_pandas_export_is_the_same_script(self, data, monkeypatch):
        steps = [{"id": "floor", "param": 80}, ORDERS, CLEAN, {"group_by": "region", "agg": {"revenue": "sum(amount)"}}]
        streamed, whole, _, _ = _both(data, monkeypatch, steps, export_pandas=True)
        assert _streamed(streamed)
        assert streamed["pandas"] == whole["pandas"]


def _column(data, name: str, values: list) -> None:
    df = pd.read_csv(data / "orders.csv")
    df[name] = values
    df.to_csv(data / "orders.csv", index=False)


class TestWhereAChunkSeesAColumnDifferently:
    @pytest.mark.parametrize(
        ("name", "values"),
        [
            ("gap", [str(i) for i in range(N - 1)] + [""]),  # whole numbers, then one gap: float64
            ("decimal", ["1"] * 7 + ["7.5"] + ["8"] * (N - 8)),  # whole numbers, then a decimal: float64
            ("text", [""] * 6 + ["x"] * (N - 6)),  # two chunks of blanks, then text: str
            ("digits_then_text", ["12"] * 30 + ["unknown"] * (N - 30)),  # numbers, then text: str
        ],
    )
    def test_the_file_is_read_again_with_the_whole_reads_type(self, data, monkeypatch, name, values):
        _column(data, name, values)
        streamed, whole, a, b = _both(data, monkeypatch, [FLOOR, ORDERS, CLEAN, {"write": "out.csv"}])
        _same(streamed, whole)
        assert a == b
        assert _streamed(streamed)

    def test_a_padded_id_stays_text(self, data, monkeypatch):
        _column(data, "zip", ["01234", "20500", "30301", "40401"] * (N // 4))
        streamed, whole, a, b = _both(data, monkeypatch, [FLOOR, ORDERS, CLEAN, {"write": "out.csv"}])
        _same(streamed, whole)
        assert a == b and b"01234" in a["out.csv"]
        assert _streamed(streamed)

    def test_a_true_false_column_with_gaps_is_read_whole(self, data, monkeypatch):
        _column(data, "flag", ["True", "False", ""] * (N // 3) + ["True"] * (N % 3))
        streamed, whole, a, b = _both(data, monkeypatch, [FLOOR, ORDERS, CLEAN, {"write": "out.csv"}])
        _same(streamed, whole)
        assert a == b
        assert not _streamed(streamed), "whole, the gaps make it object; no chunk type says so"


class TestWhereTheFileIsReadWhole:
    @pytest.mark.parametrize("at", [10, 11], ids=["first-in-a-chunk", "inside-a-chunk"])
    @pytest.mark.parametrize("extra", [",x", ",x,y"], ids=["one-field-too-many", "two-too-many"])
    def test_a_line_with_too_many_fields(self, data, monkeypatch, at, extra):
        lines = (data / "orders.csv").read_text(encoding="utf-8").splitlines()
        lines.insert(at, lines[at] + extra)
        (data / "orders.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")
        streamed, whole, a, b = _both(data, monkeypatch, [FLOOR, ORDERS, CLEAN, {"write": "out.csv"}])
        _same(streamed, whole)
        assert a == b and not _streamed(streamed)

    def test_a_first_row_with_one_field_more_than_the_header(self, data, monkeypatch):
        text = (data / "orders.csv").read_text(encoding="utf-8")
        header, rest = text.split("\n", 1)
        (data / "orders.csv").write_text(
            header + "\n" + "\n".join(f"{i},{line}" for i, line in enumerate(rest.splitlines())) + "\n"
        )
        steps = [FLOOR, ORDERS, {**CLEAN, "ops": [{"op": "filter", "where": "status == 'ok'"}]}]
        streamed, whole, _, _ = _both(data, monkeypatch, steps)
        _same(streamed, whole)
        assert not _streamed(streamed)

    def test_a_blank_first_line(self, data, monkeypatch):
        text = (data / "orders.csv").read_text(encoding="utf-8")
        (data / "orders.csv").write_text("\n" + text, encoding="utf-8")
        streamed, whole, a, b = _both(data, monkeypatch, [FLOOR, ORDERS, CLEAN, {"write": "out.csv"}])
        _same(streamed, whole)
        assert a == b and not _streamed(streamed)

    def test_a_small_file(self, data, monkeypatch):
        monkeypatch.setenv("MCP_CHAIN_LAZY_MB", "64")
        assert not _streamed(engine.run_chain([FLOOR, ORDERS, CLEAN]))

    def test_a_load_read_by_two_steps(self, data, monkeypatch):
        monkeypatch.setenv("MCP_CHAIN_LAZY_MB", "0")
        r = engine.run_chain([FLOOR, ORDERS, CLEAN, {"id": "all", "from": "orders", "scalar": "count()"}])
        assert r["success"] is True and not _streamed(r)

    def test_a_run_that_stops_at_the_load(self, data, monkeypatch):
        monkeypatch.setenv("MCP_CHAIN_LAZY_MB", "0")
        r = engine.run_chain([FLOOR, ORDERS, CLEAN], until="orders")
        assert r["success"] is True and not _streamed(r) and r["result"]["step"] == "orders"


class TestAnAwkwardFileStillStreams:
    def test_an_encoding_found_part_way_down(self, data, monkeypatch):
        df = pd.read_csv(data / "orders.csv")
        df.loc[N - 2, "note"] = "café"
        (data / "orders.csv").write_bytes(df.to_csv(index=False).encode("cp1252"))
        streamed, whole, a, b = _both(data, monkeypatch, [FLOOR, ORDERS, CLEAN, {"write": "out.csv"}])
        _same(streamed, whole)
        assert a == b and _streamed(streamed)

    def test_a_short_line_and_a_quoted_newline(self, data, monkeypatch):
        lines = (data / "orders.csv").read_text(encoding="utf-8").splitlines()
        lines[5] = ",".join(lines[5].split(",")[:4])  # a line that stops early: its last columns are empty
        lines[8] = lines[8].replace(",a,", ',"two\nlines",', 1) if ",a," in lines[8] else lines[8]
        (data / "orders.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")
        streamed, whole, a, b = _both(data, monkeypatch, [FLOOR, ORDERS, CLEAN, {"write": "out.csv"}])
        _same(streamed, whole)
        assert a == b and _streamed(streamed)


class TestAFailureAfterAStreamIsTheWholeReads:
    def test_a_step_that_fails_names_every_column_the_file_has(self, data, monkeypatch):
        steps = [FLOOR, ORDERS, CLEAN, {"id": "g", "group_by": "regoin", "agg": {"revenue": "sum(amount)"}}]
        streamed, whole, _, _ = _both(data, monkeypatch, steps)
        assert streamed["success"] is False
        _same(streamed, whole)
        assert len(streamed["columns_available"]) == 18

    def test_a_skipped_step_writes_once_and_says_what_the_whole_read_says(self, data, monkeypatch):
        steps = [
            FLOOR,
            ORDERS,
            CLEAN,
            {"id": "g", "group_by": "region", "agg": {"revenue": "sum(amont)"}, "on_error": "skip"},
            {"id": "keep", "from": "clean", "write": "out_clean.csv"},
        ]
        streamed, whole, a, b = _both(data, monkeypatch, steps)
        assert streamed["success"] is True and streamed["skipped"]
        _same(streamed, whole)
        assert a == b and list(a) == ["out_clean.csv"]
        assert not any("backup" in w for w in streamed["written"]), "written once: nothing to back up"

    def test_a_formula_error_in_a_streamed_filter(self, data, monkeypatch):
        bad = copy.deepcopy(CLEAN)
        bad["ops"][0]["where"] = "stauts == 'ok'"
        streamed, whole, _, _ = _both(data, monkeypatch, [FLOOR, ORDERS, bad])
        assert streamed["success"] is False and streamed["failed_step"] == "clean"
        _same(streamed, whole)
