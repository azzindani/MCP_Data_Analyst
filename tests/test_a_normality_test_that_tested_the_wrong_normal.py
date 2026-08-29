"""The KS normality test compared every column against N(0, 1).

`scipy.stats.kstest(x, "norm")` tests whether x came from the STANDARD normal
-- mean 0, sd 1 -- not whether it is normal at all. `statistical_test(test="ks")`
called it that way, so 300 draws from N(1000, 100) came back p=0.0, "Reject H0",
and the tool reported "Not normally distributed" for textbook-normal data. Any
column whose mean was not about zero got that verdict, which is every real one:
spend, impressions, clicks.

The medium server's `statistical_tests(test_type="ks")` had it right -- it fits
the normal to the sample's own mean and sd -- so the two servers returned
opposite answers for the same column under the same test name. That is the
tell: a vocabulary written down twice, where the copies drifted.

The medium server had a second, separate problem on the same line. It passed
loc/scale through `args=`, which stops working in scipy 1.18: "norm" resolves
there to `scipy.special.ndtr`, which takes no loc/scale, and the call raises
`TypeError: ndtr() takes from 1 to 2 positional arguments but 3 were given`.
Both now build a frozen distribution and hand kstest its `.cdf`, which is
correct on both scipy versions and says what it means.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]
for _p in (str(ROOT), str(ROOT / "servers" / "data_medium"), str(ROOT / "servers" / "data_statistics")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from _med_analysis import statistical_tests  # noqa: E402
from _stats_tests import statistical_test  # noqa: E402


@pytest.fixture()
def normal_far_from_zero(tmp_path) -> Path:
    """300 draws from N(1000, 100) -- unmistakably normal, nowhere near N(0, 1)."""
    rng = np.random.default_rng(0)
    values = rng.normal(1000, 100, 300)
    f = tmp_path / "normal_far_from_zero.csv"
    f.write_text("v\n" + "\n".join(f"{v:.6f}" for v in values) + "\n", encoding="utf-8")
    return f


@pytest.fixture()
def normal_at_zero(tmp_path) -> Path:
    """The same distribution centred at zero, which the old code got right."""
    rng = np.random.default_rng(0)
    values = rng.normal(0, 1, 300)
    f = tmp_path / "normal_at_zero.csv"
    f.write_text("v\n" + "\n".join(f"{v:.6f}" for v in values) + "\n", encoding="utf-8")
    return f


class TestNormalDataIsCalledNormal:
    def test_the_statistics_server_does_not_reject_a_normal_column(self, normal_far_from_zero):
        r = statistical_test(str(normal_far_from_zero), test="ks", column_a="v")
        assert r["success"] is True, r.get("error")
        assert r["p_value"] > 0.05, (
            f"N(1000, 100) reported as non-normal at p={r['p_value']}; "
            f"the reference normal is being fitted at N(0, 1) again"
        )

    def test_the_medium_server_does_not_reject_a_normal_column(self, normal_far_from_zero):
        r = statistical_tests(str(normal_far_from_zero), test_type="ks", column_a="v")
        assert r["success"] is True, r.get("error")
        assert r["significant"] is False
        assert "NOT" not in r["interpretation"]

    def test_both_servers_agree_on_the_same_column(self, normal_far_from_zero):
        """Same test name, same data, two servers: they must not disagree."""
        med = statistical_tests(str(normal_far_from_zero), test_type="ks", column_a="v")
        sta = statistical_test(str(normal_far_from_zero), test="ks", column_a="v")
        assert (med["p_value"] > 0.05) == (sta["p_value"] > 0.05), (
            f"medium says p={med['p_value']}, statistics says p={sta['p_value']}"
        )

    def test_a_column_already_centred_at_zero_still_works(self, normal_at_zero):
        """The case the old code happened to get right must not regress."""
        r = statistical_test(str(normal_at_zero), test="ks", column_a="v")
        assert r["success"] is True, r.get("error")
        assert r["p_value"] > 0.05


class TestGenuinelyNonNormalDataIsStillRejected:
    def test_a_skewed_column_is_rejected(self, tmp_path):
        """Fitting the normal to the sample must not make everything pass."""
        rng = np.random.default_rng(1)
        values = rng.exponential(50, 300) + 1000
        f = tmp_path / "skewed.csv"
        f.write_text("v\n" + "\n".join(f"{v:.6f}" for v in values) + "\n", encoding="utf-8")

        r = statistical_test(str(f), test="ks", column_a="v")
        assert r["success"] is True, r.get("error")
        assert r["p_value"] < 0.05, "a heavily skewed column passed as normal"

        m = statistical_tests(str(f), test_type="ks", column_a="v")
        assert m["significant"] is True


class TestTheCallSurvivesTheNextScipy:
    def test_neither_server_passes_loc_scale_through_args(self):
        """scipy 1.18 resolves "norm" to ndtr, which has no loc/scale, so
        kstest(x, "norm", args=(mean, sd)) raises TypeError there."""
        for module in ("servers/data_medium/_med_analysis.py", "servers/data_statistics/_stats_tests.py"):
            source = (ROOT / module).read_text(encoding="utf-8")
            assert "kstest(" not in source or "args=(" not in source.split("kstest(")[1][:200], (
                f"{module} still passes args= to kstest"
            )

    def test_the_reference_distribution_is_frozen_not_named(self):
        """A frozen dist's .cdf is the form that works on 1.17 and 1.18 alike."""
        for module in ("servers/data_medium/_med_analysis.py", "servers/data_statistics/_stats_tests.py"):
            source = (ROOT / module).read_text(encoding="utf-8")
            assert "fitted.cdf" in source, f"{module} no longer fits the reference normal"
