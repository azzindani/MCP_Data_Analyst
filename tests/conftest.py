"""Shared pytest fixtures for Tier 1 tests."""
from __future__ import annotations

import shutil
from pathlib import Path

import pytest


FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture()
def simple_csv(tmp_path) -> Path:
    """Clean 5-row CSV; no nulls."""
    src = FIXTURES_DIR / "simple.csv"
    dest = tmp_path / "simple.csv"
    shutil.copy(src, dest)
    return dest


@pytest.fixture()
def messy_csv(tmp_path) -> Path:
    """CSV with nulls, duplicates, whitespace, bad date."""
    src = FIXTURES_DIR / "messy.csv"
    dest = tmp_path / "messy.csv"
    shutil.copy(src, dest)
    return dest


@pytest.fixture()
def large_csv(tmp_path) -> Path:
    """500-row CSV for truncation / constrained mode tests."""
    src = FIXTURES_DIR / "large.csv"
    dest = tmp_path / "large.csv"
    shutil.copy(src, dest)
    return dest
