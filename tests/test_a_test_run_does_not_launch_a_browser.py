"""A test run must never ask the desktop shell to open a chart.

The chart savers call _open_file() when open_after is set, and on Windows that
was an in-process os.startfile(). In the sibling Office repo the equivalent
call reached the COM layer on the CI runner and killed the interpreter part-way
through the suite:

    Windows fatal exception: code 0x80010108        (RPC_E_DISCONNECTED)
    Windows fatal exception: access violation

with no failing test named and no traceback, because an access violation is not
an exception -- the `except Exception: pass` around it had never been able to
catch it. The job simply reported exit code 1 after ~30% of the tests, and
passed on ubuntu and macos where the same call is a subprocess.

Both savers here now return immediately when PYTEST_CURRENT_TEST is set, and
spawn a child on Windows rather than calling os.startfile() in-process.
"""

from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest

MODULES = [
    "servers.data_medium._med_helpers",
    "servers.data_advanced._adv_helpers",
]


@pytest.mark.parametrize("dotted", MODULES)
class TestOpenFileIsInertUnderPytest:
    def test_it_launches_nothing_while_a_test_is_running(self, dotted, tmp_path, monkeypatch):
        mod = importlib.import_module(dotted)
        called: list = []
        monkeypatch.setattr(mod.subprocess, "Popen", lambda *a, **k: called.append(a))
        # pytest sets this for the duration of every test; assert it rather than
        # trust it, since the guard is worthless if the name ever changes.
        assert os.environ.get("PYTEST_CURRENT_TEST")
        mod._open_file(tmp_path / "chart.html")
        assert called == [], f"{dotted} tried to launch the desktop handler"

    def test_outside_a_test_run_it_still_opens(self, dotted, tmp_path, monkeypatch):
        mod = importlib.import_module(dotted)
        called: list = []
        monkeypatch.setattr(mod.subprocess, "Popen", lambda *a, **k: called.append(a))
        monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
        mod._open_file(tmp_path / "chart.html")
        assert len(called) == 1, "the guard must not disable the feature itself"

    def test_startfile_is_not_called_in_this_process(self, dotted, tmp_path):
        # os.startfile exists only on Windows, so assert on the source: the
        # in-process call is what can take the interpreter down, and no amount
        # of exception handling around it helps.
        mod = importlib.import_module(dotted)
        body = Path(mod.__file__).read_text(encoding="utf-8").split("def _open_file(")[1]
        code = [ln for ln in body.splitlines() if not ln.lstrip().startswith("#")]
        assert "startfile" not in "\n".join(code), f"{dotted} still calls os.startfile in-process"
