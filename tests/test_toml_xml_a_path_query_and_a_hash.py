"""TOML and XML in, values read out of JSON by path, and a file's checksum.

data_ingest converted xlsx/ods/csv/json/parquet and nothing else: a TOML or
XML export was refused, a value nested three levels into a JSON file could be
reached only by converting the whole file to a table, and proving a file was
the one expected meant leaving the server. Now, standard library only:

- convert_file reads .toml (its one array of tables is the rows; several are
  refused by name, since picking one would be a guess) and .xml;
- query_json reads values by a JSONPath subset and refuses the rest by name;
- hash_file streams a sha256, md5 or sha1.

convert_file also raised UnboundLocalError on any file it could not read --
malformed JSON included -- because its failure path used a variable set only
after the read.
"""

from __future__ import annotations

import asyncio
import hashlib

import pandas as pd
import pytest

from servers.data_domain.server import DOMAINS
from servers.data_domain.server import mcp as domain
from servers.data_ingest.engine import convert_file, hash_file, query_json

ORDERS_TOML = """\
title = "orders"

[[order]]
id = 1
region = "APAC"
dims = { w = 2, h = 3 }

[[order]]
id = 2
region = "EMEA"
dims = { w = 4, h = 5 }
"""

DOC = {
    "store": {
        "name": "north",
        "opening hours": "9-5",
        "books": [
            {"id": 1, "title": "A", "tags": ["x"]},
            {"id": 2, "title": "B", "tags": []},
            {"id": 3, "title": "C", "author": {"id": 99}},
        ],
    }
}


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_OUTPUT_DIR", str(tmp_path))
    monkeypatch.setenv("MCP_DATA_ROOT", str(tmp_path))
    return tmp_path


@pytest.fixture
def doc(home):
    import json

    f = home / "doc.json"
    f.write_text(json.dumps(DOC), encoding="utf-8")
    return f


class TestConvertReadsTomlAndXml:
    def test_the_array_of_tables_is_the_rows(self, home):
        (home / "orders.toml").write_text(ORDERS_TOML, encoding="utf-8")
        r = convert_file(str(home / "orders.toml"), "csv")
        assert r["success"] is True, r
        assert (r["input_format"], r["rows"]) == ("toml", 2)
        df = pd.read_csv(r["output_path"])
        assert df.to_dict("records") == [
            {"id": 1, "region": "APAC", "dims.w": 2, "dims.h": 3},
            {"id": 2, "region": "EMEA", "dims.w": 4, "dims.h": 5},
        ]
        assert any("$.order" in p.get("detail", "") for p in r["progress"])

    def test_a_document_with_no_array_is_one_row(self, home):
        (home / "cfg.toml").write_text('name = "x"\n[server]\nport = 8080\n', encoding="utf-8")
        df = pd.read_csv(convert_file(str(home / "cfg.toml"), "csv")["output_path"])
        assert df.to_dict("records") == [{"name": "x", "server.port": 8080}]

    def test_several_arrays_are_refused_by_name(self, home):
        (home / "two.toml").write_text(ORDERS_TOML + "\n[[refund]]\nid = 7\n", encoding="utf-8")
        r = convert_file(str(home / "two.toml"), "csv")
        assert r["success"] is False
        assert "2 arrays of tables (order, refund)" in r["error"]
        assert "query_json(path='$.order')" in r["error"]
        assert not (home / "two.csv").exists()

    def test_xml_rows_are_the_roots_children(self, home):
        (home / "people.xml").write_text(
            '<people><person id="1"><name>Ann</name></person><person id="2"><name>Bo</name></person></people>',
            encoding="utf-8",
        )
        r = convert_file(str(home / "people.xml"), "json")
        assert r["success"] is True, r
        assert pd.read_json(r["output_path"]).to_dict("records") == [
            {"id": 1, "name": "Ann"},
            {"id": 2, "name": "Bo"},
        ]

    def test_dry_run_writes_nothing(self, home):
        (home / "orders.toml").write_text(ORDERS_TOML, encoding="utf-8")
        r = convert_file(str(home / "orders.toml"), "csv", dry_run=True)
        assert r["success"] is True and r["dry_run"] is True
        assert not (home / "orders.csv").exists()

    @pytest.mark.parametrize(("name", "text"), [("bad.json", "{bad"), ("bad.toml", "a = "), ("bad.xml", "<a><b></a>")])
    def test_a_file_it_cannot_read_says_why(self, home, name, text):
        (home / name).write_text(text, encoding="utf-8")
        r = convert_file(str(home / name), "csv")
        assert r["success"] is False
        assert "UnboundLocalError" not in r["error"] and "'out'" not in r["error"]


class TestQueryJson:
    @pytest.mark.parametrize(
        ("path", "found"),
        [
            ("$", [("$", DOC)]),
            ("$.store.name", [("$.store.name", "north")]),
            ("$['store']['opening hours']", [("$.store['opening hours']", "9-5")]),
            ("$.store.books[1].title", [("$.store.books[1].title", "B")]),
            ("$.store.books[-1].title", [("$.store.books[2].title", "C")]),
            (
                "$.store.books[*].title",
                [("$.store.books[0].title", "A"), ("$.store.books[1].title", "B"), ("$.store.books[2].title", "C")],
            ),
            (
                "$.store.books.*.id",
                [("$.store.books[0].id", 1), ("$.store.books[1].id", 2), ("$.store.books[2].id", 3)],
            ),
            (
                "$..id",
                [
                    ("$.store.books[0].id", 1),
                    ("$.store.books[1].id", 2),
                    ("$.store.books[2].id", 3),
                    ("$.store.books[2].author.id", 99),
                ],
            ),
            ("$.store.books[7]", []),
            ("$.nope", []),
        ],
    )
    def test_each_step(self, doc, path, found):
        r = query_json(str(doc), path)
        assert r["success"] is True, r
        assert [(m["path"], m["value"]) for m in r["matches"]] == found
        assert (r["returned"], r["total"], r["truncated"]) == (len(found), len(found), False)

    def test_each_path_reads_back(self, doc):
        for m in query_json(str(doc), "$.store.*")["matches"]:
            assert query_json(str(doc), m["path"])["matches"] == [m]

    def test_nothing_matched_says_how_to_look(self, doc):
        assert "'$.*' lists the top level" in query_json(str(doc), "$.nope")["hint"]

    @pytest.mark.parametrize("path", ["$.store.books[?(@.id > 1)]", "$.store.books[0:2]", "store.name", "$.a b"])
    def test_other_syntax_is_refused_by_name(self, doc, path):
        r = query_json(str(doc), path)
        assert r["success"] is False
        assert "$, .key, ['key'], [n], [*] or .*, ..key" in r["hint"]

    def test_toml_is_queried_as_it_parses(self, home):
        (home / "orders.toml").write_text(ORDERS_TOML, encoding="utf-8")
        r = query_json(str(home / "orders.toml"), "$.order[*].dims.w")
        assert [m["value"] for m in r["matches"]] == [2, 4]

    def test_a_table_file_is_pointed_at_convert(self, home):
        (home / "t.csv").write_text("a\n1\n", encoding="utf-8")
        r = query_json(str(home / "t.csv"), "$")
        assert r["success"] is False and "convert_file()" in r["hint"]

    def test_malformed_json_is_an_answer(self, home):
        (home / "bad.json").write_text("{bad", encoding="utf-8")
        assert query_json(str(home / "bad.json"), "$")["success"] is False

    def test_missing_file(self, home):
        assert query_json(str(home / "gone.json"), "$")["success"] is False

    def test_the_answer_is_capped_and_says_so(self, home, monkeypatch):
        import json

        monkeypatch.setenv("MCP_CONSTRAINED_MODE", "1")
        (home / "many.json").write_text(json.dumps({"rows": list(range(40))}), encoding="utf-8")
        r = query_json(str(home / "many.json"), "$.rows[*]")
        assert (r["returned"], r["total"], r["truncated"]) == (10, 40, True)
        assert r["matches"][-1] == {"path": "$.rows[9]", "value": 9}


class TestHashFile:
    @pytest.mark.parametrize("algorithm", ["sha256", "md5", "sha1"])
    def test_the_digest_is_hashlibs(self, home, algorithm):
        data = bytes(range(256)) * 9000  # over one read chunk
        (home / "blob.bin").write_bytes(data)
        r = hash_file(str(home / "blob.bin"), algorithm)
        assert r["success"] is True, r
        assert (r["digest"], r["bytes"]) == (hashlib.new(algorithm, data).hexdigest(), len(data))

    def test_sha256_is_the_default(self, home):
        (home / "a.txt").write_bytes(b"abc")
        assert hash_file(str(home / "a.txt"))["digest"] == hashlib.sha256(b"abc").hexdigest()

    def test_an_unknown_algorithm_names_the_valid_ones(self, home):
        (home / "a.txt").write_bytes(b"abc")
        r = hash_file(str(home / "a.txt"), "crc32")
        assert r["success"] is False and r["hint"] == "Valid: sha256, md5, sha1."

    def test_a_directory_or_missing_file_is_refused(self, home):
        assert hash_file(str(home))["success"] is False
        assert hash_file(str(home / "gone.bin"))["success"] is False


class TestTheyAreIngestActions:
    def test_listed_under_data_ingest(self):
        actions = [name for _, name in DOMAINS["data_ingest"][1]]
        assert actions[-2:] == ["query_json", "hash_file"]

    def test_the_algorithm_enum_is_in_the_schema(self):
        tool = next(t for t in asyncio.run(domain.list_tools()) if t.name == "data_ingest")
        assert tool.inputSchema["properties"]["args"]["properties"]["algorithm"]["enum"] == ["sha256", "md5", "sha1"]
