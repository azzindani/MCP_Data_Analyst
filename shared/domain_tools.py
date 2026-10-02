"""A handful of domain tools, each an `action` plus an `args` object, over the tools this repo already has.

Every tool a model can see costs it attention on every turn, and dozens of
narrow tools make it hunt. A domain tool names one job -- reading a document,
editing a workbook, testing a hypothesis -- and its `action` picks the
operation. The shape is the one the Pipeline server uses: `action` is an
enum, `args` is one object whose every property says which actions take it,
and the description lists each action with its argument names.

Nothing here is a second implementation. Each action IS an existing tool,
registered on its tier: its schema is read from that tool, and a call runs
that tool's own `run()`, so its validation, its wrappers (inline files,
missing-file suggestions, refusals, token estimate) and its answers are
exactly the tier's. The call itself runs off the event loop and, on Linux, in a
child process (shared/isolation.py), so a slow or memory-hungry call cannot
freeze or kill the server. The tiers keep serving their own endpoints unchanged.
"""

from __future__ import annotations

import re
from typing import Any

from shared.arg_errors import explain, looks_like_validation
from shared.isolation import run_tool

_READ_ONLY = "readOnlyHint"
_DESTRUCTIVE = "destructiveHint"


def _first_line(text: str | None) -> str:
    return (text or "").strip().splitlines()[0].strip() if (text or "").strip() else ""


def _signature(tool: Any) -> str:
    schema = tool.parameters or {}
    required = list(schema.get("required") or [])
    optional = [p for p in (schema.get("properties") or {}) if p not in required]
    return ", ".join(required + [f"{p}?" for p in optional])


def _shape(spec: dict[str, Any]) -> dict[str, Any]:
    """A property's schema without the per-tool noise (title, default)."""
    return {k: v for k, v in spec.items() if k not in ("title", "default")}


def _one_value_hint(spec: dict[str, Any]) -> str:
    """How one action reads this argument, in a few words."""
    if "enum" in spec:
        return " | ".join(str(v) for v in spec["enum"])
    kind = spec.get("type")
    if not kind and "anyOf" in spec:
        kinds = [s.get("type") for s in spec["anyOf"] if isinstance(s, dict) and s.get("type") != "null"]
        kind = "/".join(k for k in kinds if k)
    text = str(kind or "value")
    if "default" in spec and spec["default"] not in (None, "", [], {}):
        text += f" · default {spec['default']}"
    return text


def args_schema(actions: dict[str, Any]) -> dict[str, Any]:
    """One `args` object: the union of every action's parameters, each naming the actions that take it."""
    takers: dict[str, list[str]] = {}
    specs: dict[str, dict[str, dict[str, Any]]] = {}
    required_by: dict[str, list[str]] = {}
    for action, tool in actions.items():
        schema = tool.parameters or {}
        for param, spec in (schema.get("properties") or {}).items():
            takers.setdefault(param, []).append(action)
            specs.setdefault(param, {})[action] = spec
        for param in schema.get("required") or []:
            required_by.setdefault(param, []).append(action)
    properties: dict[str, Any] = {}
    for param, names in takers.items():
        shapes = [_shape(specs[param][a]) for a in names]
        same = all(s == shapes[0] for s in shapes)
        who = "every action" if len(names) == len(actions) else ", ".join(names)
        if same:
            prop = dict(shapes[0])
            prop["description"] = f"{who}: {_one_value_hint(specs[param][names[0]])}"
        else:
            prop = {"description": " | ".join(f"{a}: {_one_value_hint(specs[param][a])}" for a in names)}
        needed = required_by.get(param, [])
        if needed:
            prop["description"] += (
                " · required by every action" if len(needed) == len(actions) else f" · required by {', '.join(needed)}"
            )
        properties[param] = prop
    return {
        "type": "object",
        "additionalProperties": False,
        "description": "arguments for the chosen action · each property names the actions that take it",
        "properties": properties,
    }


def describe(summary: str, actions: dict[str, Any]) -> str:
    """The domain tool's description: its job, then each action with its argument names."""
    lines = [summary, "", "Actions:"]
    lines += [f"- {a}: {_first_line(t.description)} ({_signature(t)})" for a, t in actions.items()]
    return "\n".join(lines)


# `filter_rows()` in a hint is how a tier names an action; at the domain tools it is data_edit(...).
_INVOKED = re.compile(r"\b([a-z_][a-z0-9_]{2,})\(\)")


def _retarget(pointer: dict, route: dict[str, str], server: str) -> None:
    """Point `{"tool": "inspect_dataset"}` at the tool a client can call: the domain, with the action named."""
    tool = pointer.get("tool")
    if isinstance(tool, str) and tool in route and not pointer.get("action"):
        pointer["tool"], pointer["action"] = route[tool], tool
        if "server" in pointer:
            pointer["server"] = server


def point_at_domains(result: Any, route: dict[str, str], server: str) -> Any:
    """Rewrite what an answer tells the caller to do next into the vocabulary this server serves.

    Every tier names its own tools -- handover.suggested_next, an insight's action, "use filter_rows()"
    in a hint -- and a client connected to the domain tools has no tool by any of those names:
    `tools/call inspect_dataset` was "Unknown tool", and the pointers carried the pre-trim sub-server
    ("data_basic") as `server`. `route` maps an action to the domain tool that has it.
    """
    if not isinstance(result, dict):
        return result
    handover = result.get("handover")
    if isinstance(handover, dict):
        for pointer in handover.get("suggested_next") or []:
            if isinstance(pointer, dict):
                _retarget(pointer, route, server)
    for insight in result.get("insights") or []:
        if isinstance(insight, dict) and isinstance(insight.get("action"), dict):
            _retarget(insight["action"], route, server)
    hint = result.get("hint")
    if isinstance(hint, str):
        result["hint"] = _INVOKED.sub(lambda m: f"{route[m[1]]}(action='{m[1]}')" if m[1] in route else m[0], hint)
    return result


def _refusal(tool_name: str, error: str, hint: str) -> dict[str, Any]:
    return {"success": False, "op": tool_name, "error": error, "hint": hint, "progress": [], "token_estimate": 0}


def register_domain(
    mcp: Any, name: str, summary: str, actions: dict[str, Any], elsewhere: dict[str, list[str]] | None = None
) -> None:
    """Register domain tool `name` on `mcp`, dispatching each action to the tier tool of that name.

    `elsewhere` maps every action of the OTHER domain tools to the domains
    that have it -- one name can be an action of several, `set_cell` for a
    document and for a workbook -- so an action asked of the wrong tool is
    pointed at every right one.
    """
    elsewhere = elsewhere or {}
    route = {action: homes[0] for action, homes in elsewhere.items() if homes}
    route.update({action: name for action in actions})
    server = getattr(mcp, "name", "") or name

    async def run(action: str, args: dict | None = None) -> dict:
        tool = actions.get(action)
        if tool is None:
            homes = elsewhere.get(action)
            if homes:
                return _refusal(
                    name,
                    f"{action} is an action of {' and '.join(homes)}, not {name}.",
                    "Call " + " or ".join(f"{home}(action={action!r})" for home in homes) + ".",
                )
            return _refusal(name, f"{name} has no action {action!r}.", f"Actions: {', '.join(actions)}.")
        given = dict(args or {})
        known = sorted((tool.parameters or {}).get("properties") or {})
        unknown = sorted(set(given) - set(known))
        if unknown:
            return _refusal(
                name, f"{action} does not take {', '.join(unknown)}.", f"{action} accepts: {', '.join(known)}."
            )
        try:
            return point_at_domains(await run_tool(tool, given), route, server)
        except Exception as exc:
            if not looks_like_validation(str(exc)):
                raise
            required = (tool.parameters or {}).get("required") or []
            # The first line alone is pydantic's header -- "1 validation error
            # for tune_hyperparametersArguments" -- which names neither the
            # argument nor what it should have been.
            problems = explain(str(exc))
            detail = "; ".join(f"{field}: {why}" for field, why in problems) or str(exc).splitlines()[0]
            return _refusal(
                name,
                f"{action} rejected its arguments: {detail}",
                f"{action} takes ({_signature(tool)}); required: {', '.join(required) or 'none'}.",
            )

    run.__name__ = name
    run.__doc__ = summary
    hints = [getattr(t, "annotations", None) for t in actions.values()]
    read_only = all(h is not None and getattr(h, _READ_ONLY, False) for h in hints)
    destructive = any(h is not None and getattr(h, _DESTRUCTIVE, False) for h in hints)
    from mcp.types import ToolAnnotations

    mcp.add_tool(
        run,
        name=name,
        description=describe(summary, actions),
        annotations=ToolAnnotations(readOnlyHint=read_only, destructiveHint=destructive),
    )
    registered = mcp._tool_manager._tools[name]
    registered.parameters = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "action": {"type": "string", "enum": list(actions), "description": "the operation to run"},
            "args": args_schema(actions),
        },
        "required": ["action"],
    }


def register_domains(mcp: Any, domains: dict[str, tuple[str, list[tuple[Any, str]]]]) -> None:
    """Register every domain: {name: (summary, [(tier mcp, tool name), ...])}."""
    resolved: dict[str, dict[str, Any]] = {}
    for name, (_, members) in domains.items():
        resolved[name] = {tool: tier._tool_manager._tools[tool] for tier, tool in members}
    homes: dict[str, list[str]] = {}
    for name, actions in resolved.items():
        for action in actions:
            homes.setdefault(action, []).append(name)
    for name, (summary, _) in domains.items():
        others = {a: [d for d in ds if d != name] for a, ds in homes.items()}
        register_domain(mcp, name, summary, resolved[name], {a: ds for a, ds in others.items() if ds})
