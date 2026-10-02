"""Run one tool call off the event loop and, on Linux, in a child process of its own.

The MCP SDK calls a synchronous tool inline on the event loop. Two things followed from that in a
container with 1 GB and one CPU:

* a long call (a support-vector fit on 95,000 rows ran past twenty minutes) answered no health
  check and no other request until it ended, so the container went "unhealthy" with the call
  still running;
* a call that wanted more memory than the container had (an Excel export of 119,390 x 32 cells) was
  killed by the kernel *together with the server*, and every call in flight died with it.

A child process turns both into the answer for that one call. The parent only dispatches, so it
never holds the data, stays responsive, and is never the process the kernel picks. The child is
stopped by a watchdog before the container is out of memory, or at a time limit, and the caller
gets a reply that says which and what to try.

`MCP_CALL_ISOLATION` picks the mode: `process` (the default on Linux), `thread` (the default
elsewhere, where there is no safe fork), or `inline` (what the SDK did). Limits, all optional:
`MCP_MAX_CALLS` (concurrent calls, default 2), `MCP_CALL_TIMEOUT_S` (default 1800),
`MCP_CALL_MEMORY_MB` (cap per call; otherwise the container limit is read from its cgroup).
"""

from __future__ import annotations

import asyncio
import os
import pickle
import signal
import sys
import time
import warnings
import weakref
from pathlib import Path
from typing import Any

POLL_SECONDS = 0.25
# Stop a call when the container's anonymous memory passes this share of its limit: the kernel's
# own OOM kill comes a little later and takes the server too if it picks it.
HEADROOM = 0.92
DEFAULT_TIMEOUT_S = 1800.0

_children: dict[int, _Child] = {}
_gates: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore] = weakref.WeakKeyDictionary()


class _Child:
    """A running call: its pid and, once stopped by us, why."""

    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.stopped = ""


def mode() -> str:
    """`process`, `thread` or `inline`."""
    chosen = os.environ.get("MCP_CALL_ISOLATION", "").strip().lower()
    if chosen in ("thread", "inline"):
        return chosen
    return "process" if _can_fork() else "thread"


def _can_fork() -> bool:
    return sys.platform.startswith("linux") and hasattr(os, "fork")


def _max_calls() -> int:
    try:
        value = int(os.environ.get("MCP_MAX_CALLS", ""))
    except ValueError:
        value = 1 if os.environ.get("MCP_CONSTRAINED_MODE", "0") == "1" else 2
    return max(1, value)


def _timeout() -> float:
    try:
        return float(os.environ.get("MCP_CALL_TIMEOUT_S", DEFAULT_TIMEOUT_S))
    except ValueError:
        return DEFAULT_TIMEOUT_S


def _cap_bytes() -> int | None:
    try:
        return int(float(os.environ["MCP_CALL_MEMORY_MB"]) * 1024 * 1024)
    except KeyError, ValueError:
        return None


def memory_budget_mb(default: int = 1024) -> int:
    """What one call may hold, for a library that takes its own memory limit (DuckDB does).

    MCP_CALL_MEMORY_MB when set (a little under it, the library is not the only thing in the process),
    else half the container's limit, else `default`.
    """
    cap = _cap_bytes()
    if cap:
        return max(64, int(cap * 0.8 / 1024 / 1024))
    limit = _cgroup_limit()
    if limit:
        return max(128, int(limit * 0.5 / 1024 / 1024))
    return default


def worker_threads() -> int:
    """CPUs this process may use, at most four: a query's threads are not worth more than that here."""
    try:
        usable = len(os.sched_getaffinity(0))
    except AttributeError:
        usable = os.cpu_count() or 1
    return max(1, min(4, usable))


def _cgroup_limit() -> int | None:
    """The container's memory limit in bytes, or None when it has none (or this is not a cgroup)."""
    for name in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            text = Path(name).read_text().strip()
        except OSError:
            continue
        if text.isdigit() and int(text) < 1 << 50:
            return int(text)
    return None


def _cgroup_anon() -> int | None:
    """Anonymous (heap) bytes in use in the container: what a runaway call grows, page cache aside."""
    for name, keys in (
        ("/sys/fs/cgroup/memory.stat", ("anon",)),
        ("/sys/fs/cgroup/memory/memory.stat", ("total_rss", "rss")),
    ):
        try:
            lines = Path(name).read_text().splitlines()
        except OSError:
            continue
        found = {k: int(v) for k, v in (line.split()[:2] for line in lines if line.split()) if v.isdigit()}
        for key in keys:
            if key in found:
                return found[key]
    return None


def _private_bytes(pid: int) -> int:
    """Memory only this process holds (its own writes, not the pages it shares with the server)."""
    try:
        text = Path(f"/proc/{pid}/smaps_rollup").read_text()
    except OSError:
        return 0
    total = 0
    for line in text.splitlines():
        if line.startswith(("Private_Dirty:", "Private_Clean:")):
            total += int(line.split()[1]) * 1024
    return total


def _stop_reason(child: _Child, started: float) -> str:
    """Why this child should be stopped now: "timeout", "memory", or "" to let it run."""
    if time.monotonic() - started > _timeout():
        return "timeout"
    cap = _cap_bytes()
    if cap is not None and _private_bytes(child.pid) > cap:
        return "memory"
    limit, used = _cgroup_limit(), _cgroup_anon()
    if limit and used and used > HEADROOM * limit:
        # Several calls may be running; only the one holding the most is stopped, and the others
        # carry on with what that frees.
        biggest = max(_children.values(), key=lambda c: _private_bytes(c.pid), default=child)
        if biggest is child:
            return "memory"
    return ""


def _die_with_parent() -> None:
    """A child must not outlive the server it works for."""
    try:
        import ctypes

        ctypes.CDLL(None).prctl(1, int(signal.SIGKILL))  # PR_SET_PDEATHSIG
    except Exception:
        pass


def _child_main(tool: Any, given: dict, fd: int) -> None:
    """Runs in the forked child: the call, then its answer down the pipe, then exit. Never returns."""
    code = 0
    try:
        try:
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            signal.set_wakeup_fd(-1)  # the server's loop wakeup socket is not ours to write to
        except Exception:
            pass
        _die_with_parent()
        asyncio._set_running_loop(None)  # the copy of the server's loop is still "running" here
        try:
            payload: tuple = ("ok", asyncio.run(tool.run(given)))
        except BaseException as exc:
            payload = ("err", type(exc).__name__, str(exc))
        try:
            data = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as exc:
            data = pickle.dumps(("err", "PicklingError", f"the answer could not be sent back: {exc}"))
        with os.fdopen(fd, "wb") as pipe:
            pipe.write(data)
    except BaseException:
        code = 1
    finally:
        os._exit(code)


def _gate() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    gate = _gates.get(loop)
    if gate is None:
        gate = _gates[loop] = asyncio.Semaphore(_max_calls())
    return gate


def _refusal(name: str, error: str, hint: str) -> dict[str, Any]:
    return {"success": False, "op": name, "error": error, "hint": hint, "progress": [], "token_estimate": 0}


def _megabytes(n: int | None) -> str:
    return f"{(n or 0) / 1024 / 1024:,.0f} MB"


def _stopped(name: str, why: str, started: float, status: int | None) -> dict[str, Any]:
    if why == "memory":
        limit = _cgroup_limit()
        cap = _cap_bytes()
        room = f" of the {_megabytes(cap or limit)} it has" if (cap or limit) else ""
        return _refusal(
            name,
            f"This call needed more memory than the server has{room}, so it was stopped before it could take "
            "the server down. Nothing else was affected and nothing was written by this call.",
            "Narrow it: pass fewer columns, filter or sample the rows (sample_n, preview_rows, filter_rows), "
            "or work on a smaller file; or give the server more memory.",
        )
    if why == "timeout":
        return _refusal(
            name,
            f"This call ran longer than {_timeout():,.0f} s and was stopped; the server is unaffected.",
            "Use a smaller sample or fewer rows, or raise MCP_CALL_TIMEOUT_S.",
        )
    reason = ""
    if status is not None and os.WIFSIGNALED(status):
        if os.WTERMSIG(status) == signal.SIGKILL:  # nothing else here sends it: the kernel ran out of memory
            return _stopped(name, "memory", started, status)
        reason = f" (signal {os.WTERMSIG(status)})"
    return _refusal(
        name,
        f"The process running this call ended without an answer{reason}; the server is unaffected.",
        "Retry it; if it repeats, narrow the input (fewer columns or rows).",
    )


async def _in_child(tool: Any, given: dict) -> Any:
    loop = asyncio.get_running_loop()
    read_fd, write_fd = os.pipe()
    with warnings.catch_warnings():
        # Python warns that fork() in a process with threads (numpy's BLAS pool counts) may deadlock.
        # The parent only dispatches -- it never computes, so none of those threads holds a lock.
        warnings.simplefilter("ignore", DeprecationWarning)
        pid = os.fork()
    if pid == 0:
        os.close(read_fd)
        _child_main(tool, given, write_fd)  # does not return
    os.close(write_fd)
    child = _Child(pid)
    _children[pid] = child
    started = time.monotonic()
    status: int | None = None
    reader = asyncio.StreamReader(limit=1 << 20)
    transport = None
    try:
        transport, _ = await loop.connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(reader), os.fdopen(read_fd, "rb", 0)
        )
        read = asyncio.ensure_future(reader.read(-1))
        while not read.done():
            await asyncio.wait({read}, timeout=POLL_SECONDS)
            if read.done():
                break
            why = _stop_reason(child, started)
            if why:
                child.stopped = why
                os.kill(pid, signal.SIGKILL)
                break
        data = await read
        while status is None:
            done, raw = os.waitpid(pid, os.WNOHANG)
            if done:
                status = raw
            else:
                await asyncio.sleep(0.01)
    finally:
        _children.pop(pid, None)
        if status is None:  # cancelled (the caller went away) or failed mid-way: leave no orphan
            try:
                os.kill(pid, signal.SIGKILL)
                os.waitpid(pid, 0)
            except ProcessLookupError, ChildProcessError:
                pass
        if transport is not None:
            transport.close()
    if child.stopped:
        return _stopped(getattr(tool, "name", "tool"), child.stopped, started, status)
    if not data:
        return _stopped(getattr(tool, "name", "tool"), "", started, status)
    outcome = pickle.loads(data)
    if outcome[0] == "ok":
        return outcome[1]
    raise RuntimeError(outcome[2])


async def run_tool(tool: Any, given: dict) -> Any:
    """`await tool.run(given)`, without blocking the server and, where it can, without risking it."""
    how = mode()
    if how == "inline" or getattr(tool, "is_async", False):
        return await tool.run(given)
    async with _gate():
        if how == "process":
            return await _in_child(tool, given)
        import anyio

        return await anyio.to_thread.run_sync(lambda: asyncio.run(tool.run(given)))
