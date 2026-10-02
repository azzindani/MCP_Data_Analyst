"""One heavy call must not freeze the server or take it down with it.

The 2026-10-02 sweep ran the fleet's data servers at the live 1 GB limit. A support-vector fit on
95,000 rows ran past twenty minutes and the container went unhealthy with the call still running,
because the MCP SDK runs a synchronous tool inline on the event loop. Four Excel and conversion
calls on a 16 MB file needed more memory than the container had; the kernel killed the whole
server, and every call in flight died with it.

`run_tool` (shared/isolation.py) runs the call in a child process: the loop stays responsive, the
child is stopped before the container is out of memory or past a time limit, and the caller gets a
reply that says which.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import sys
import time

import pytest

from shared import isolation
from shared.domain_tools import register_domain
from shared.isolation import run_tool

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="process isolation is Linux only")

from mcp.server.fastmcp import FastMCP  # noqa: E402

inner = FastMCP("inner")


@inner.tool()
def quick(n: int) -> dict:
    """Answer at once."""
    return {"success": True, "n": n, "pid": os.getpid()}


@inner.tool()
def nap(seconds: float) -> dict:
    """Hold the thread."""
    time.sleep(seconds)
    return {"success": True}


@inner.tool()
def hog(megabytes: int) -> dict:
    """Take memory and keep it."""
    data = bytearray(megabytes * 1024 * 1024)
    for i in range(0, len(data), 4096):
        data[i] = 1
    time.sleep(20)
    return {"success": True, "held": len(data)}


@inner.tool()
def boom() -> dict:
    """Fail."""
    raise ValueError("it broke")


@inner.tool()
def unsendable() -> dict:
    """Return something that cannot cross a process."""
    return {"success": True, "f": lambda: 1}


def tool(name: str):
    return inner._tool_manager._tools[name]


@pytest.fixture(autouse=True)
def _process_mode(monkeypatch):
    for name in (
        "MCP_CALL_ISOLATION",
        "MCP_CALL_MEMORY_MB",
        "MCP_CALL_TIMEOUT_S",
        "MCP_MAX_CALLS",
        "MCP_CONSTRAINED_MODE",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("MCP_CALL_ISOLATION", "process")


class TestTheLoopStaysFree:
    @staticmethod
    def _ticks_during(seconds: float) -> int:
        async def scenario() -> int:
            ticks = 0

            async def ticker() -> None:
                nonlocal ticks
                while True:
                    await asyncio.sleep(0.05)
                    ticks += 1

            task = asyncio.create_task(ticker())
            await run_tool(tool("nap"), {"seconds": seconds})
            task.cancel()
            return ticks

        return asyncio.run(scenario())

    def test_a_slow_call_does_not_stop_the_loop(self):
        assert self._ticks_during(1.0) >= 10

    def test_the_old_behaviour_did(self, monkeypatch):
        monkeypatch.setenv("MCP_CALL_ISOLATION", "inline")
        assert self._ticks_during(1.0) <= 1

    def test_a_thread_does_not_either(self, monkeypatch):
        monkeypatch.setenv("MCP_CALL_ISOLATION", "thread")
        assert self._ticks_during(1.0) >= 10

    def test_the_answer_is_the_tools_own_and_comes_from_another_process(self):
        result = asyncio.run(run_tool(tool("quick"), {"n": 7}))
        assert result["n"] == 7 and result["success"] is True
        assert result["pid"] != os.getpid()


class TestACallThatWantsTooMuch:
    def test_it_is_stopped_with_the_reason_and_the_server_carries_on(self, monkeypatch):
        monkeypatch.setenv("MCP_CALL_MEMORY_MB", "64")

        async def scenario() -> tuple[dict, dict, float]:
            started = time.monotonic()
            refused = await run_tool(tool("hog"), {"megabytes": 400})
            took = time.monotonic() - started
            return refused, await run_tool(tool("quick"), {"n": 1}), took

        refused, after, took = asyncio.run(scenario())
        assert refused["success"] is False
        assert "more memory" in refused["error"] and "64 MB" in refused["error"]
        assert "sample_n" in refused["hint"]
        assert took < 15, "the watchdog should have stopped it long before its 20 s sleep ended"
        assert after["success"] is True
        with pytest.raises(ChildProcessError):
            os.waitpid(-1, os.WNOHANG)  # nothing left behind to reap

    def test_a_call_inside_the_limit_is_untouched(self, monkeypatch):
        monkeypatch.setenv("MCP_CALL_MEMORY_MB", "600")
        monkeypatch.setenv("MCP_CALL_TIMEOUT_S", "30")
        result = asyncio.run(run_tool(tool("nap"), {"seconds": 0.3}))
        assert result["success"] is True


class TestACallThatRunsTooLong:
    def test_it_is_stopped_at_the_limit(self, monkeypatch):
        monkeypatch.setenv("MCP_CALL_TIMEOUT_S", "1")
        started = time.monotonic()
        result = asyncio.run(run_tool(tool("nap"), {"seconds": 30}))
        assert result["success"] is False and "longer than 1 s" in result["error"]
        assert time.monotonic() - started < 8


class TestACallerWhoLeaves:
    def test_cancelling_the_call_stops_the_child(self):
        async def scenario() -> int:
            task = asyncio.create_task(run_tool(tool("nap"), {"seconds": 30}))
            await asyncio.sleep(0.6)
            (pid,) = list(isolation._children)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            return pid

        pid = asyncio.run(scenario())
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
        assert not isolation._children


class TestErrorsKeepTheirWords:
    def test_an_exception_arrives_with_its_message(self):
        with pytest.raises(RuntimeError, match="it broke"):
            asyncio.run(run_tool(tool("boom"), {}))

    def test_an_answer_that_cannot_be_sent_says_so(self):
        with pytest.raises(RuntimeError, match="could not be sent back"):
            asyncio.run(run_tool(tool("unsendable"), {}))


class TestThroughTheDomainTool:
    @pytest.fixture
    def run(self):
        outer = FastMCP("outer")
        register_domain(outer, "work", "Work.", {n: tool(n) for n in ("quick", "boom", "nap")})
        return outer._tool_manager._tools["work"].fn

    def test_a_call_runs_and_answers(self, run):
        result = asyncio.run(run("quick", {"n": 4}))
        assert result["n"] == 4 and result["pid"] != os.getpid()

    def test_a_bad_argument_is_still_a_named_refusal(self, run):
        result = asyncio.run(run("quick", {"n": "not a number"}))
        assert result["success"] is False and "rejected its arguments" in result["error"]
        assert "n:" in result["error"]

    def test_an_unknown_argument_is_refused_before_a_process_is_started(self, run):
        result = asyncio.run(run("quick", {"m": 1}))
        assert result["success"] is False and "does not take m" in result["error"]


class TestHowManyAtOnce:
    @staticmethod
    def _two_naps() -> float:
        async def scenario() -> float:
            started = time.monotonic()
            await asyncio.gather(
                run_tool(tool("nap"), {"seconds": 0.8}),
                run_tool(tool("nap"), {"seconds": 0.8}),
            )
            return time.monotonic() - started

        return asyncio.run(scenario())

    def test_two_at_a_time_by_default(self):
        assert self._two_naps() < 1.45

    def test_one_when_told_so(self, monkeypatch):
        monkeypatch.setenv("MCP_MAX_CALLS", "1")
        assert self._two_naps() >= 1.55

    def test_one_in_constrained_mode(self, monkeypatch):
        monkeypatch.setenv("MCP_CONSTRAINED_MODE", "1")
        assert self._two_naps() >= 1.55

    def test_the_limit_can_still_be_raised_in_constrained_mode(self, monkeypatch):
        monkeypatch.setenv("MCP_CONSTRAINED_MODE", "1")
        monkeypatch.setenv("MCP_MAX_CALLS", "2")
        assert self._two_naps() < 1.45
