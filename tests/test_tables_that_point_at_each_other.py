"""A folder of tables says how it joins; the join keeps the grain of the table asked about.

The 2026-10-02 sweep found three multi-table sets in the corpus (a retail chain's five tables, a
logistics database's fourteen with its schema file, a supply chain's six) and the fleet could join
two files given the keys, but not tell which columns were keys, nor how a third and a fourth hang
off them, nor that joining a trip to its fuel purchases multiplies the trip.

`relate_tables` proposes a relationship only when a name says so AND the values agree, orients it
to the table the column is the key OF, and plans the join from a chosen fact table along many-to-one
and one-to-one edges only: one row per fact row, with the tables that point back at it named to be
aggregated first.
"""

from __future__ import annotations

import asyncio
import re
import sys
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from servers.data_domain.server import DOMAINS
from servers.data_transform.engine import relate_tables
from shared.table_graph import table_name

EVALS = Path("/root/Evals/dataframe/Logistic_Operations.zip")


@pytest.fixture(autouse=True)
def _home(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path / "out"))
    (tmp_path / "out").mkdir()
    return tmp_path


def _shop(root: Path, seed: int = 3) -> Path:
    """Seven tables: loads point at customers and routes, trips at loads (one each), drivers and trucks,
    fuel purchases at trips (several each), monthly metrics at drivers -- and two decoys."""
    rng = np.random.default_rng(seed)
    folder = root / "shop"
    folder.mkdir()
    customers = pd.DataFrame({"customer_id": [f"C{i:03d}" for i in range(60)], "segment": rng.choice(list("ab"), 60)})
    routes = pd.DataFrame({"route_id": range(1, 41), "miles": rng.integers(50, 900, 40)})
    drivers = pd.DataFrame({"driver_id": [f"D{i:03d}" for i in range(30)], "tenure": rng.integers(1, 20, 30)})
    trucks = pd.DataFrame({"truck_id": range(1000, 1025), "model_year": rng.integers(2010, 2024, 25)})
    n_loads = 800
    loads = pd.DataFrame(
        {
            "load_id": [f"L{i:05d}" for i in range(n_loads)],
            "customer_id": rng.choice(customers.customer_id, n_loads),
            "route_id": rng.choice(routes.route_id, n_loads),
            "revenue": rng.normal(2000, 300, n_loads).round(2),
        }
    )
    trips = pd.DataFrame(
        {
            "trip_id": [f"T{i:05d}" for i in range(n_loads)],
            "load_id": rng.permutation(loads.load_id),
            "driver_id": rng.choice(drivers.driver_id, n_loads),
            "truck_id": rng.choice(trucks.truck_id, n_loads),
            "miles": rng.integers(40, 900, n_loads),
        }
    )
    fuel = pd.DataFrame(
        {
            "fuel_id": range(2400),
            "trip_id": rng.choice(trips.trip_id, 2400),
            "gallons": rng.normal(60, 10, 2400).round(1),
        }
    )
    metrics = pd.DataFrame(
        {
            "driver_id": np.repeat(drivers.driver_id, 4),
            "month": np.tile([1, 2, 3, 4], 30),
            "score": rng.normal(80, 5, 120).round(1),
        }
    )
    # Decoys: a small-integer column that "matches" routes.route_id by value alone, and a status code.
    config = pd.DataFrame({"setting": [f"s{i}" for i in range(10)], "code": range(1, 11), "status": [1, 2] * 5})
    for name, frame in dict(
        customers=customers,
        routes=routes,
        drivers=drivers,
        trucks=trucks,
        loads=loads,
        trips=trips,
        fuel_purchases=fuel,
        driver_monthly_metrics=metrics,
        config=config,
    ).items():
        frame.to_csv(folder / f"{name}.csv", index=False)
    return folder


TRUTH = {
    ("loads", "customer_id", "customers"),
    ("loads", "route_id", "routes"),
    ("trips", "load_id", "loads"),
    ("trips", "driver_id", "drivers"),
    ("trips", "truck_id", "trucks"),
    ("fuel_purchases", "trip_id", "trips"),
    ("driver_monthly_metrics", "driver_id", "drivers"),
}


def _found(result: dict) -> set[tuple[str, str, str]]:
    return {(r["child"], r["child_column"], r["parent"]) for r in result["relationships"]}


class TestTheRelationshipsAreTheSchemas:
    def test_every_foreign_key_and_nothing_else(self, _home):
        result = relate_tables(source=str(_shop(_home)))
        assert result["success"] is True, result.get("error")
        assert _found(result) == TRUTH

    def test_a_one_to_one_reads_from_the_table_the_column_is_the_key_of(self, _home):
        rel = {(r["child"], r["parent"]): r for r in relate_tables(source=str(_shop(_home)))["relationships"]}
        assert rel[("trips", "loads")]["cardinality"] == "one_to_one"
        assert ("loads", "trips") not in rel, "load_id is loads' key: trips refers to it, not the other way"
        assert rel[("loads", "customers")]["cardinality"] == "many_to_one"

    def test_a_value_match_alone_is_not_a_relationship(self, _home):
        result = relate_tables(source=str(_shop(_home)))
        assert "config" in result["unrelated_tables"]
        assert not [r for r in result["relationships"] if "config" in (r["child"], r["parent"])]

    def test_a_table_with_a_composite_key_says_so(self, _home):
        result = relate_tables(source=str(_shop(_home)))
        assert any("driver_monthly_metrics has no single unique column" in n for n in result["notes"])
        keys = {t["name"]: t["primary_key"] for t in result["tables"]}
        assert keys["trips"] == "trip_id" and keys["loads"] == "load_id" and keys["driver_monthly_metrics"] == ""

    def test_missing_parents_are_counted_and_still_related(self, _home):
        folder = _shop(_home)
        loads = pd.read_csv(folder / "loads.csv")
        loads.loc[:39, "customer_id"] = "C999"  # 5% of loads name a customer that is not there
        loads.to_csv(folder / "loads.csv", index=False)
        result = relate_tables(source=str(folder))
        rel = next(r for r in result["relationships"] if (r["child"], r["child_column"]) == ("loads", "customer_id"))
        assert rel["orphan_rate"] == pytest.approx(0.05)
        assert any("5.0% of loads.customer_id has no match in customers.customer_id" in n for n in result["notes"])

    def test_a_column_that_is_mostly_not_in_the_parent_is_not_a_reference(self, _home):
        folder = _shop(_home)
        loads = pd.read_csv(folder / "loads.csv")
        loads["customer_id"] = [f"X{i}" for i in range(len(loads))]
        loads.to_csv(folder / "loads.csv", index=False)
        assert ("loads", "customer_id", "customers") not in _found(relate_tables(source=str(folder)))

    def test_names_are_matched_whatever_their_case_and_a_role_names_its_target(self, _home):
        folder = _home / "retail"
        folder.mkdir()
        pd.DataFrame({"Store_ID": [f"S{i}" for i in range(8)], "City": list("abcdefgh")}).to_csv(
            folder / "stores.csv", index=False
        )
        pd.DataFrame({"country": ["fr", "de", "es", "it"], "region": list("wwee")}).to_csv(
            folder / "country.csv", index=False
        )
        pd.DataFrame(
            {
                "sale_id": range(40),
                "store_id": [f"S{i % 8}" for i in range(40)],
                "origin_country": ["fr", "de", "es", "it"] * 10,
                "destination_country": ["de", "es", "it", "fr"] * 10,
            }
        ).to_csv(folder / "sales.csv", index=False)
        found = _found(relate_tables(source=str(folder)))
        assert ("sales", "store_id", "stores") in found
        assert ("sales", "origin_country", "country") in found and ("sales", "destination_country", "country") in found


class TestTheJoinKeepsTheGrain:
    @pytest.fixture
    def folder(self, _home) -> Path:
        return _shop(_home)

    def test_one_row_per_fact_row_and_the_values_are_pandas(self, folder, _home):
        out = _home / "out" / "trips_enriched.parquet"
        result = relate_tables(source=str(folder), fact="trips", output_path=str(out))
        assert result["success"] is True, result.get("error")
        assert result["grain_kept"] is True and result["rows_written"] == 800
        joined = pd.read_parquet(out)
        trips, loads, drivers = (pd.read_csv(folder / f"{n}.csv") for n in ("trips", "loads", "drivers"))
        oracle = trips.merge(loads, on="load_id", suffixes=("", "__l")).merge(drivers, on="driver_id", how="left")
        joined = joined.sort_values("trip_id").reset_index(drop=True)
        oracle = oracle.sort_values("trip_id").reset_index(drop=True)
        assert joined["trip_id"].tolist() == oracle["trip_id"].tolist()
        assert joined["revenue"].tolist() == pytest.approx(oracle["revenue"].tolist())
        assert joined["tenure"].tolist() == oracle["tenure"].tolist()

    def test_what_points_back_at_the_fact_is_named_not_joined(self, folder):
        plan = relate_tables(source=str(folder), fact="trips")["join_plan"]
        assert "fuel_purchases" not in plan["tables_used"]
        (child,) = plan["to_aggregate_first"]
        assert child["table"] == "fuel_purchases" and child["rows_per_fact_row"] == 3.0

    def test_children_can_be_aggregated_into_the_row(self, folder, _home):
        out = _home / "out" / "agg.parquet"
        result = relate_tables(source=str(folder), fact="trips", aggregate_children=True, output_path=str(out))
        assert result["success"] is True and result["grain_kept"] is True, result.get("error")
        joined = pd.read_parquet(out).set_index("trip_id")
        fuel = pd.read_csv(folder / "fuel_purchases.csv")
        counts = fuel.groupby("trip_id").size()
        assert joined["fuel_purchases__rows"].fillna(0).astype(int).tolist() == (
            counts.reindex(joined.index).fillna(0).astype(int).tolist()
        )
        assert joined["fuel_purchases__avg_gallons"].dropna().tolist() == pytest.approx(
            fuel.groupby("trip_id").gallons.mean().reindex(joined.index).dropna().tolist()
        )

    def test_a_role_played_twice_is_joined_twice_with_its_own_columns(self, _home):
        folder = _home / "roles"
        folder.mkdir()
        pd.DataFrame({"country": ["fr", "de", "es"], "region": ["w", "c", "s"]}).to_csv(
            folder / "country.csv", index=False
        )
        pd.DataFrame(
            {
                "route_id": range(30),
                "origin_country": ["fr", "de", "es"] * 10,
                "destination_country": ["es", "fr", "de"] * 10,
            }
        ).to_csv(folder / "routes.csv", index=False)
        out = _home / "out" / "r.csv"
        result = relate_tables(source=str(folder), fact="routes", output_path=str(out))
        assert result["success"] is True, result.get("error")
        joined = pd.read_csv(out)
        assert "region" not in joined.columns, "a role-played table's columns each say which role"
        assert joined.loc[0, "origin_country__region"] == "w" and joined.loc[0, "destination_country__region"] == "s"

    def test_the_sql_is_a_call_query_data_accepts(self, folder):
        from servers.data_ingest.engine import query_data

        result = relate_tables(source=str(folder), fact="loads")
        args = result["query_data_args"]
        ran = query_data(**args, max_rows=3)
        assert ran["success"] is True, ran.get("error")
        assert "customer_id" in {c["name"] for c in ran["columns"]}


class TestWhereTablesComeFrom:
    def test_a_zip_is_unpacked_beside_the_output_and_nothing_escapes(self, _home):
        folder = _shop(_home)
        archive = _home / "shop.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            for file in folder.glob("*.csv"):
                zf.write(file, file.name)
            zf.writestr("../escape.csv", "a\n1\n")
            zf.writestr("notes.txt", "not a table")
        result = relate_tables(source=str(archive))
        assert result["success"] is True, result.get("error")
        assert _found(result) == TRUTH
        assert (_home / "out" / "shop" / "trips.csv").exists()
        assert not (_home / "escape.csv").exists() and not (_home / "out" / "notes.txt").exists()

    def test_parquet_and_csv_mix(self, _home):
        folder = _shop(_home)
        pd.read_csv(folder / "customers.csv").to_parquet(folder / "customers.parquet")
        (folder / "customers.csv").unlink()
        assert _found(relate_tables(source=str(folder))) == TRUTH

    def test_named_tables(self, _home):
        folder = _shop(_home)
        result = relate_tables(tables={"o": str(folder / "loads.csv"), "c": str(folder / "customers.csv")})
        assert result["success"] is True and _found(result) == {("o", "customer_id", "c")}

    def test_a_file_name_becomes_a_safe_table_name(self):
        assert table_name(Path("Product Sales (2024).csv")) == "Product_Sales_2024"


class TestRefusals:
    def test_one_table_is_not_a_set(self, _home):
        folder = _home / "one"
        folder.mkdir()
        pd.DataFrame({"a": [1]}).to_csv(folder / "only.csv", index=False)
        result = relate_tables(source=str(folder))
        assert result["success"] is False and "at least two" in result["error"]

    def test_an_unknown_fact_lists_the_tables(self, _home):
        result = relate_tables(source=str(_shop(_home)), fact="nope")
        assert result["success"] is False and "trips" in result["error"]

    def test_output_needs_a_fact(self, _home):
        result = relate_tables(source=str(_shop(_home)), output_path=str(_home / "x.csv"))
        assert result["success"] is False and "fact" in result["hint"]

    def test_nothing_named(self):
        assert relate_tables()["success"] is False

    def test_a_file_is_not_a_folder(self, _home):
        path = _home / "a.csv"
        path.write_text("a\n1\n")
        assert relate_tables(source=str(path))["success"] is False

    def test_two_files_with_one_table_name(self, _home):
        folder = _home / "dup"
        folder.mkdir()
        pd.DataFrame({"a": [1]}).to_csv(folder / "t.csv", index=False)
        pd.DataFrame({"a": [1]}).to_parquet(folder / "t.parquet")
        assert "both the table 't'" in relate_tables(source=str(folder))["error"]


class TestItIsAnActionOfDataReshape:
    def test_registered_where_merge_lives(self):
        assert any(tool == "relate_tables" for _, tool in DOMAINS["data_reshape"][1])

    @pytest.mark.skipif(not sys.platform.startswith("linux"), reason="process isolation is Linux only")
    def test_it_runs_through_the_dispatcher(self, _home):
        from servers.data_domain.server import mcp as domain

        result = asyncio.run(
            domain._tool_manager._tools["data_reshape"].run(
                {"action": "relate_tables", "args": {"source": str(_shop(_home)), "fact": "trips"}}
            )
        )
        assert result["success"] is True and result["join_plan"]["fact"] == "trips"


@pytest.mark.skipif(not EVALS.exists(), reason="the corpus is not on this machine")
class TestAgainstTheLogisticsSchemaFile:
    """DATABASE_SCHEMA.txt in the corpus lists every foreign key: it is the ground truth."""

    def test_every_listed_foreign_key_is_found_and_nothing_else(self, _home):
        with zipfile.ZipFile(EVALS) as zf:
            schema = zf.read("DATABASE_SCHEMA.txt").decode()
        expected = set()
        for block in re.split(r"\n\d+\. ", schema)[1:]:
            table = block.split("\n", 1)[0].split()[0].lower()
            keys = re.search(r"Foreign Keys: (.+)", block)
            if keys and "Composite" not in block:
                expected |= {(table, column.strip()) for column in keys.group(1).split(",")}
        found = {(c, col) for c, col, _ in _found(relate_tables(source=str(EVALS)))}
        assert expected <= found, expected - found
        # the two monthly summary tables have composite keys and no listed foreign key, yet each refers to its entity
        assert found - expected == {("driver_monthly_metrics", "driver_id"), ("truck_utilization_metrics", "truck_id")}
