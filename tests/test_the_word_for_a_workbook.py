"""Two tools, one vocabulary, and fixing one of them left the other refusing.

    export_data(format="xlsx")
      -> "Invalid format: xlsx"  /  "Valid formats: csv, excel, json"

The format is called "excel" and the file it writes is called `.xlsx`. Both
spellings arrive from callers, and the fleet's own messages used both --
data_ingest's refusals said "convert to xlsx first" about a tool that accepted
only "excel".

That was found and fixed in `convert_file` in the morning. `export_data`, on a
different server in the same repo, has the identical vocabulary and went on
refusing `xlsx` until a sweep phase tried it that afternoon. The first fix put
an alias table next to one of the two call sites, which is a second table by
another name.

So the table lives in `shared/file_utils.py` now and both tools read it, and
this walks it: every alias, through every tool that takes a format, so a third
one cannot be added with a copy of its own.

Alias rather than rename, as ever -- "excel" has always worked and every
existing caller passes it.

Found in a round-15 sweep report, one phase after the fix that should have
covered it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from servers.data_advanced import engine as adv
from servers.data_ingest import engine as ing
from shared.file_utils import EXPORT_FORMAT_ALIASES, normalise_export_format

# Every tool in the repo that takes an output format, and how it is spelled
# there. The point of the list is that it is a list.
FORMAT_TOOLS = {
    "export_data": lambda src, fmt, out: adv.export_data(str(src), format=fmt, output_path=str(out)),
    "convert_file": lambda src, fmt, out: ing.convert_file(str(src), output_format=fmt, output_path=str(out)),
}


@pytest.fixture()
def csv_file(tmp_path: Path) -> Path:
    f = tmp_path / "small.csv"
    f.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
    return f


class TestEveryAliasWorksInEveryTool:
    @pytest.mark.parametrize("tool", sorted(FORMAT_TOOLS))
    @pytest.mark.parametrize("alias", sorted(EXPORT_FORMAT_ALIASES))
    def test_the_alias_is_accepted(self, tool: str, alias: str, csv_file: Path, tmp_path: Path) -> None:
        out = tmp_path / f"{tool}_{alias}.xlsx"
        r = FORMAT_TOOLS[tool](csv_file, alias, out)
        assert r["success"] is True, r.get("error")
        assert out.exists()

    @pytest.mark.parametrize("tool", sorted(FORMAT_TOOLS))
    def test_the_canonical_name_still_works(self, tool: str, csv_file: Path, tmp_path: Path) -> None:
        out = tmp_path / f"{tool}_excel.xlsx"
        r = FORMAT_TOOLS[tool](csv_file, "excel", out)
        assert r["success"] is True, r.get("error")

    @pytest.mark.parametrize("tool", sorted(FORMAT_TOOLS))
    def test_a_real_typo_is_still_refused(self, tool: str, csv_file: Path, tmp_path: Path) -> None:
        r = FORMAT_TOOLS[tool](csv_file, "exel", tmp_path / "no.xlsx")
        assert r["success"] is False
        assert "exel" in r["error"], r["error"]

    @pytest.mark.parametrize("tool", sorted(FORMAT_TOOLS))
    def test_the_refusal_mentions_the_alias(self, tool: str, csv_file: Path, tmp_path: Path) -> None:
        """A caller who guessed wrong should not have to guess again."""
        r = FORMAT_TOOLS[tool](csv_file, "exel", tmp_path / "no.xlsx")
        assert "xlsx" in r["hint"], r["hint"]
        assert "excel" in r["hint"], r["hint"]


class TestTheTableItself:
    def test_every_alias_points_at_a_canonical_name(self) -> None:
        assert set(EXPORT_FORMAT_ALIASES.values()) == {"excel"}

    def test_case_and_padding_do_not_matter(self) -> None:
        assert normalise_export_format("  XLSX ") == "excel"

    def test_an_unknown_word_passes_through_unchanged(self) -> None:
        """So the refusal quotes back what the caller actually wrote."""
        assert normalise_export_format("exel") == "exel"

    def test_a_canonical_name_is_left_alone(self) -> None:
        for name in ("csv", "json", "parquet", "excel"):
            assert normalise_export_format(name) == name

    def test_there_is_only_one_table(self) -> None:
        """The defect this file is about: an alias map beside a call site."""
        import subprocess

        root = Path(__file__).resolve().parents[1]
        hits = subprocess.run(
            ["grep", "-rn", "-e", '"xlsx": *"excel"', "--include=*.py", "servers", "shared"],
            cwd=root,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        assert len(hits) == 1, hits
        assert "shared/file_utils.py" in hits[0], hits
