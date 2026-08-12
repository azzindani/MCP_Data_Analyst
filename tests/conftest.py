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


@pytest.fixture()
def ad_data_real_sample_csv(tmp_path) -> Path:
    """294-row real (not synthetic) stratified sample of the Ad_Data.csv
    dataset used for the 2026-08-12 comprehensive real-world tool sweep —
    see reference_ad_data_test_source in project memory. Includes 4 genuine
    impressions=0/clicks>0 rows (the real pattern that produces +inf in a
    derived ctr_pct column) and 20 real null link_clicks rows, so real-world
    regression scenarios can be re-run here instead of only manually."""
    src = FIXTURES_DIR / "ad_data_real_sample.csv"
    dest = tmp_path / "ad_data_real_sample.csv"
    shutil.copy(src, dest)
    return dest
