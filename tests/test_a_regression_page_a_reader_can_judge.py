"""A regression page shows the fit a reader can judge, and says when groups were pooled.

The sweep's regression page was one bar chart of raw coefficients: spend in
dollars beside impressions in counts, so the longer bar was the column with the
smaller unit. R2 was only in the JSON, there were no residuals, and the two ad
platforms -- two separate lines in the data -- were pooled into one fit with
nothing to say so. The page now draws standardised effects with their
intervals, the fit statistics and the residual diagnostics, and the response
and the page name a column whose groups follow separate lines.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_statistics"), str(ROOT / "servers" / "data_medium")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _stats_regression import regression_analysis  # type: ignore[import]  # noqa: E402


@pytest.fixture
def ads(tmp_path) -> Path:
    """Two platforms, each on its own line: Facebook buys impressions far cheaper."""
    rng = np.random.default_rng(3)
    n = 400
    platform = np.where(np.arange(n) % 2 == 0, "Facebook", "Google")
    spends = rng.uniform(10, 500, n)
    per_dollar = np.where(platform == "Facebook", 400.0, 15.0)
    impressions = spends * per_dollar * rng.uniform(0.8, 1.2, n)
    clicks = np.where(platform == "Facebook", 0.002 * impressions, 0.05 * impressions) + rng.normal(0, 3, n)
    f = tmp_path / "ads.csv"
    pd.DataFrame(
        {
            "platform": platform,
            "spends": spends,
            "impressions": impressions,
            "clicks": clicks,
            "noise": rng.choice(["a", "b"], n),
        }
    ).to_csv(f, index=False)
    return f


def _fit(path, tmp_path, **kw):
    kw.setdefault("x_cols", ["spends", "impressions"])
    kw.setdefault("y_col", "clicks")
    return regression_analysis(str(path), output_path=str(tmp_path / "reg.html"), open_after=False, **kw)


def _ssr(frame: pd.DataFrame) -> float:
    design = np.column_stack([np.ones(len(frame)), frame["spends"], frame["impressions"]])
    beta, *_ = np.linalg.lstsq(design, frame["clicks"].to_numpy(), rcond=None)
    return float(((frame["clicks"].to_numpy() - design @ beta) ** 2).sum())


class TestPooledGroups:
    def test_the_platform_is_named_with_what_separate_lines_remove(self, ads, tmp_path):
        r = _fit(ads, tmp_path)
        assert r["success"] is True, r.get("error")
        pooled = {p["column"]: p for p in r["pooled_groups"]}
        assert "noise" not in pooled
        frame = pd.read_csv(ads)
        oracle = 1 - sum(_ssr(g) for _, g in frame.groupby("platform")) / _ssr(frame)
        assert pooled["platform"]["unexplained_removed"] == pytest.approx(oracle, abs=1e-4)
        assert pooled["platform"]["chow_p"] < 0.001
        assert any("pooled into one fit" in p["message"] for p in r["progress"])

    def test_a_column_already_in_the_model_is_not_a_candidate(self, ads, tmp_path):
        r = _fit(ads, tmp_path, x_cols=["spends", "impressions", "platform"])
        assert "platform" not in {p["column"] for p in r.get("pooled_groups", [])}

    def test_one_population_pools_nothing(self, tmp_path):
        rng = np.random.default_rng(5)
        n = 300
        x = rng.uniform(0, 10, n)
        f = tmp_path / "one.csv"
        pd.DataFrame({"x": x, "y": 2 * x + rng.normal(0, 1, n), "label": rng.choice(["p", "q", "r"], n)}).to_csv(
            f, index=False
        )
        r = regression_analysis(str(f), "y", ["x"], output_path=str(tmp_path / "one.html"))
        assert r["success"] is True and "pooled_groups" not in r


class TestThePage:
    def test_it_draws_the_effects_the_fit_and_the_residuals(self, ads, tmp_path):
        r = _fit(ads, tmp_path)
        page = Path(r["output_path"]).read_text(encoding="utf-8")
        for text in ("Standardised effect", "Residuals vs fitted", "Actual vs fitted", "Shapiro", "RMSE"):
            assert text in page, text
        assert f"R² {r['r_squared']}" in page
        for name, row in r["coefficients"].items():
            assert name in page and json.dumps(row["std_beta"]) in page, name
        assert "follow separate lines" in page

    def test_a_logistic_page_shows_its_probabilities(self, ads, tmp_path):
        frame = pd.read_csv(ads)
        frame["converted"] = (frame["clicks"] > frame["clicks"].median()).astype(int)
        frame.to_csv(ads, index=False)
        r = _fit(ads, tmp_path, y_col="converted", model_type="logistic")
        assert r["success"] is True, r.get("error")
        page = Path(r["output_path"]).read_text(encoding="utf-8")
        assert "Change in log-odds per SD of x" in page and "Predicted probability by actual class" in page
