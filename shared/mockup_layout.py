"""How an HTML mockup is arranged: its frame, its density and the width of each kind of card.

`digest_html` (shared/dashboard_looks.py) reads a mockup's colours, type and corner radius. This reads
what is left of its design: whether a rail or a sidebar frames the page, how much air there is between
cards, and the widths its cards take on a twelve-column grid -- a KPI tile a quarter of the row, the trend
chart two thirds, a ranked list a third -- so a dashboard can be dressed *and* set out like it.

A mockup's cards are found by structure, not by name: the children of a grid container (a rule with
`display: grid` and columns) are its cards, a card's width is the `grid-column: span N` of its class or the
share of the track it sits in, and what a card holds is read from its classes and ids (a table, a ranked
list, a chart, a KPI). What cannot be read is reported rather than guessed at. Nothing here runs a script
or fetches anything: the mockup is parsed as text.
"""

from __future__ import annotations

import re
from html.parser import HTMLParser
from typing import Any

GRID_COLUMNS = 12
MAX_SPANS = 24
KINDS = ("kpi", "chart", "ranking", "table")

_VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source", "track", "wbr"}
_SKIP = {"script", "style", "head", "title", "template", "noscript"}
_RULE = re.compile(r"([^{}@]+)\{([^{}]*)\}")
_DECL = re.compile(r"([a-z-]+)\s*:\s*([^;}]+)")
_CARD_WORDS = ("card", "panel", "tile", "widget", "box", "stat")


class _Node:
    def __init__(self, tag: str, attrs: dict[str, str], parent: _Node | None) -> None:
        self.tag, self.attrs, self.parent = tag, attrs, parent
        self.children: list[_Node] = []

    @property
    def classes(self) -> list[str]:
        return self.attrs.get("class", "").split()

    @property
    def tokens(self) -> list[str]:
        """The words in this element's classes and id, lower case."""
        return [t.lower() for t in [*self.classes, self.attrs.get("id", "")] if t]


class _Builder(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = _Node("#root", {}, None)
        self.node = self.root
        self._raw = 0

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag in _SKIP:
            self._raw += 1
            return
        if self._raw:
            return
        child = _Node(tag, {k: (v or "") for k, v in attrs}, self.node)
        self.node.children.append(child)
        if tag not in _VOID:
            self.node = child

    def handle_startendtag(self, tag: str, attrs: list) -> None:
        if tag not in _SKIP and not self._raw:
            self.node.children.append(_Node(tag, {k: (v or "") for k, v in attrs}, self.node))

    def handle_endtag(self, tag: str) -> None:
        if tag in _SKIP:
            self._raw = max(0, self._raw - 1)
            return
        if self._raw:
            return
        walk = self.node
        while walk is not self.root and walk.tag != tag:
            walk = walk.parent  # type: ignore[assignment]
        if walk is not self.root:
            self.node = walk.parent  # type: ignore[assignment]


def _parse(html: str) -> _Node:
    builder = _Builder()
    builder.feed(html)
    return builder.root


def _rules(css: str) -> dict[str, dict[str, str]]:
    """{simple selector: declarations} for the base rules (media queries are the narrow-screen variants)."""
    css = re.sub(r"/\*.*?\*/", "", css, flags=re.S)
    css = re.sub(r"@media[^{]*\{(?:[^{}]*\{[^{}]*\})*[^{}]*\}", "", css, flags=re.S)
    found: dict[str, dict[str, str]] = {}
    for selectors, body in _RULE.findall(css):
        decls = {k: v.strip() for k, v in _DECL.findall(body)}
        for selector in selectors.split(","):
            found.setdefault(selector.strip(), {}).update(decls)
    return found


def _props(node: _Node, rules: dict[str, dict[str, str]]) -> dict[str, str]:
    """Declarations of the rules naming this element by a class, its tag, or its id (a trailing simple selector)."""
    own: dict[str, str] = {}
    wanted = {f".{c}" for c in node.classes} | {node.tag}
    if node.attrs.get("id"):
        wanted.add("#" + node.attrs["id"])
    for selector, decls in rules.items():
        last = re.split(r"[\s>+~]+", selector.strip())[-1]
        parts = re.findall(r"[.#][\w-]+|^[a-z][a-z0-9]*", last)
        if parts and all(p in wanted for p in parts):
            own.update(decls)
    own.update({k: v.strip() for k, v in _DECL.findall(node.attrs.get("style", ""))})
    return own


def _tracks(value: str) -> list[str]:
    """The track list of `grid-template-columns`, each as written ('minmax(0, 1fr)', '300px'), repeat() expanded."""
    value = value.strip()
    match = re.fullmatch(r"repeat\(\s*(\d+)\s*,\s*(.+)\)", value)
    if match:
        return [match.group(2).strip()] * min(int(match.group(1)), GRID_COLUMNS)
    if re.match(r"repeat\(\s*auto-(?:fit|fill)", value):
        size = re.search(r"minmax\(\s*(\d+)px", value)
        width = int(size.group(1)) if size else 260
        return ["1fr"] * max(1, min(GRID_COLUMNS, 1200 // max(width, 1)))
    tracks, depth, current = [], 0, ""
    for char in value:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char.isspace() and depth == 0:
            if current:
                tracks.append(current)
            current = ""
        else:
            current += char
    if current:
        tracks.append(current)
    return tracks


def _weight(track: str) -> tuple[float, bool]:
    """(size, fixed): '7fr' is (7, False), 'minmax(0, 1fr)' is (1, False), '300px' is (300, True)."""
    fr = re.search(r"(\d+(?:\.\d+)?)fr", track)
    if fr:
        return float(fr.group(1)), False
    px = re.search(r"(\d+(?:\.\d+)?)px", track)
    if px:
        return float(px.group(1)), True
    return 1.0, False


def _spans_of_tracks(tracks: list[str]) -> list[int]:
    """Each track as a share of twelve columns: fixed pixel tracks count against a 1200 px page."""
    if len(tracks) == GRID_COLUMNS and all(_weight(t) == (1.0, False) for t in tracks):
        return [1] * GRID_COLUMNS
    sizes = [_weight(t) for t in tracks]
    fixed = sum(size for size, is_fixed in sizes if is_fixed)
    flexible = sum(size for size, is_fixed in sizes if not is_fixed) or 1.0
    room = max(1200 - fixed, 300)
    px = [size if is_fixed else room * size / flexible for size, is_fixed in sizes]
    total = sum(px) or 1.0
    return [max(1, round(GRID_COLUMNS * p / total)) for p in px]


def _own_span(props: dict[str, str]) -> int | None:
    match = re.search(r"span\s+(\d+)", props.get("grid-column", ""))
    return max(1, min(GRID_COLUMNS, int(match.group(1)))) if match else None


def _is_grid(props: dict[str, str]) -> bool:
    return "grid" in props.get("display", "") and bool(props.get("grid-template-columns"))


def _is_card(node: _Node) -> bool:
    """A card says so in its name (`card`, `kpi-card`, `panel`): `cards` and `tiles` are the grids that hold them.

    A surface is not enough to say it: the frame around a page has a background and a shadow too.
    """
    return any(
        token == word or token.startswith(word + "-") or token.endswith("-" + word)
        for token in node.tokens
        for word in _CARD_WORDS
    )


def _descendants(node: _Node, depth: int = 4) -> list[_Node]:
    found: list[_Node] = []
    if depth <= 0:
        return found
    for child in node.children:
        found.append(child)
        found.extend(_descendants(child, depth - 1))
    return found


def kind_of(node: _Node) -> str:
    """What a card holds, from the names and tags in it."""
    inside = _descendants(node)
    tags = {n.tag for n in inside}
    words = " ".join(node.tokens) + " " + " ".join(t for n in inside for t in n.tokens)
    charted = bool(re.search(r"chart|plot|graph|map|ring|gauge|donut|canvas|(^|\s)ch-", words)) or "canvas" in tags
    if "table" in tags and not charted:
        return "table"
    if not charted and any(n.tag in ("ol", "ul") and re.search(r"rank|leader|top|list", " ".join(n.tokens)) for n in inside):
        return "ranking"
    own = " ".join(node.tokens)
    if re.search(r"(^|[\s_-])(kpi|stat|metric)(?![a-z])|(^|\s)k-", own) or re.search(r"kpi-|stat-value|metric-", words):
        return "kpi"
    if charted or "svg" in tags:
        return "chart"
    if re.search(r"(^|\s)(tile|pill|stage|big)\b", own + " " + words):
        return "kpi"
    return "text"


def _cards(grid: _Node, props: dict[str, str], rules: dict[str, dict[str, str]]) -> list[tuple[_Node, int]]:
    shares = _spans_of_tracks(_tracks(props["grid-template-columns"]))
    cards = []
    for i, child in enumerate(c for c in grid.children if c.tag not in ("script", "style")):
        span = _own_span(_props(child, rules))
        if span is None:
            span = shares[i % len(shares)]
        cards.append((child, span))
    return cards


def _rows(node: _Node, rules: dict[str, dict[str, str]], out: list[list[dict[str, Any]]], unread: list[str]) -> None:
    """The rows of cards under `node`, in page order."""
    if node.tag in ("nav", "aside", "footer", "header", "script", "style"):
        return
    props = _props(node, rules)
    # A grid holding the page's own chrome (a nav beside the content) frames the page; it has no cards.
    chrome = any(c.tag in ("nav", "aside", "main") for c in node.children)
    if _is_grid(props) and len(_tracks(props["grid-template-columns"])) >= 2 and not _is_card(node) and not chrome:
        if not node.children:
            unread.append(f"{'.'.join(node.classes) or node.tag} is a grid with no cards in the markup (a script fills it)")
        row: list[dict[str, Any]] = []
        used = 0
        for child, span in _cards(node, props, rules):
            if used + span > GRID_COLUMNS and row:
                out.append(row)
                row, used = [], 0
            row.append({"kind": kind_of(child), "span": span})
            used += span
        if row:
            out.append(row)
        return
    if _is_card(node) and node.tag not in ("main", "body", "html"):
        out.append([{"kind": kind_of(node), "span": GRID_COLUMNS}])
        return
    for child in node.children:
        _rows(child, rules, out, unread)


def _frame(root: _Node, rules: dict[str, dict[str, str]]) -> tuple[str, str]:
    """('rail' | 'sidebar' | 'banner' | 'plain', how it was told)."""
    for node in _descendants(root, 12):
        if node.tag not in ("nav", "aside") and not re.search(r"(^|[\s_-])(rail|sidebar|side)([\s_-]|$)", " ".join(node.tokens)):
            continue
        parent = node.parent
        tracks = _tracks(_props(parent, rules).get("grid-template-columns", "")) if parent is not None else []
        width = _weight(tracks[0])[0] if tracks and _weight(tracks[0])[1] else None
        own = _props(node, rules).get("width", "")
        px = re.search(r"(\d+)px", own)
        width = width or (float(px.group(1)) if px else None)
        named = " ".join(node.tokens)
        if "rail" in named or (width is not None and width <= 120):
            return "rail", f"{node.tag}.{'.'.join(node.classes) or '?'}"
        if "side" in named or (width is not None and width > 120) or node.tag == "aside":
            return "sidebar", f"{node.tag}.{'.'.join(node.classes) or '?'}"
    for node in _descendants(root, 8):
        if node.tag == "header" and re.search(r"banner|hero|masthead", " ".join(node.tokens)):
            return "banner", "header." + ".".join(node.classes)
    return "plain", ""


def _gap_density(rules: dict[str, dict[str, str]], root: _Node) -> str:
    """The gap between cards, as a density."""
    gaps = []
    for node in _descendants(root, 12):
        props = _props(node, rules)
        if _is_grid(props) and (m := re.search(r"(\d+(?:\.\d+)?)(px|rem)", props.get("gap", ""))):
            gaps.append(float(m.group(1)) * (16 if m.group(2) == "rem" else 1))
    if not gaps:
        return ""
    gap = max(gaps)
    return "compact" if gap <= 12 else "airy" if gap >= 24 else "comfortable"


def digest_arrangement(html: str) -> dict[str, Any]:
    """{frame, density, rows, spans, report}: how the mockup is set out, and what could not be read."""
    css = "\n".join(re.findall(r"<style[^>]*>(.*?)</style>", html, flags=re.S | re.I))
    rules = _rules(css)
    root = _parse(html)
    rows: list[list[dict[str, Any]]] = []
    unread: list[str] = []
    _rows(root, rules, rows, unread)
    frame, told = _frame(root, rules)
    spans: dict[str, list[int]] = {}
    for row in rows:
        for card in row:
            spans.setdefault(card["kind"], []).append(card["span"])
    spans = {kind: values[:MAX_SPANS] for kind, values in spans.items() if kind in KINDS}  # a note's width is its own
    notes: list[str] = []
    if not rows:
        notes.append("no grid of cards was found (a card grid is `display: grid` with `grid-template-columns`).")
    if unread:
        notes.extend(unread[:6])
    if "text" in spans:
        notes.append("cards that hold no chart, KPI, list or table (read as text) are laid out too.")
    return {
        "frame": frame,
        "density": _gap_density(rules, root),
        "rows": rows[:40],
        "spans": spans,
        "report": {"frame_from": told or "no nav, aside or banner header", "cards": sum(len(r) for r in rows), "notes": notes},
    }


def validate_arrangement(value: Any) -> dict[str, list[int]]:
    """A style.arrange: {kind: [spans]}, each span a whole number of twelve columns. Raises ValueError."""
    if not isinstance(value, dict) or not value:
        raise ValueError(f"is a dict of kind -> list of spans, kinds from {', '.join(KINDS)}")
    clean: dict[str, list[int]] = {}
    for kind, spans in value.items():
        if kind not in KINDS:
            raise ValueError(f"has unknown kind {kind!r}; the kinds are {', '.join(KINDS)}")
        if not isinstance(spans, list) or not 1 <= len(spans) <= MAX_SPANS:
            raise ValueError(f"{kind} is a list of 1 to {MAX_SPANS} spans")
        for span in spans:
            if isinstance(span, bool) or not isinstance(span, int) or not 1 <= span <= GRID_COLUMNS:
                raise ValueError(f"{kind} span {span!r} is a whole number of columns, 1 to {GRID_COLUMNS}")
        clean[kind] = list(spans)
    return clean


def arrange(layout: list[dict], tabs: list[dict] | None, spans: dict[str, list[int]]) -> int:
    """Give each placed panel the width its kind has in the mockup, in the order the mockup uses them, then
    stretch the last card of any row that falls short so rows end flush. Returns how many panels moved.

    A panel whose kind the mockup does not have keeps its width; a caller's explicit `place` is theirs only
    where the panel was not placed by the storyline, which is every panel this is called on.
    """
    groups = [[int(i) for i in tab.get("slots", [])] for tab in tabs] if tabs else [list(range(len(layout)))]
    moved = 0
    used: dict[str, int] = {}
    for indexes in groups:
        for i in indexes:
            panel = layout[i]
            place = panel.get("place")
            if place is None:
                continue
            kind = _panel_kind(panel)
            wanted = spans.get(kind)
            if not wanted:
                continue
            n = used.get(kind, 0)
            used[kind] = n + 1
            span = wanted[n % len(wanted)]
            if place.get("span") != span:
                place["span"] = span
                moved += 1
        moved += _justify([layout[i] for i in indexes])
    return moved


def _panel_kind(panel: dict) -> str:
    chart = str(panel.get("chart", ""))
    if chart == "kpi":
        return "kpi"
    if chart == "table":
        return "table"
    if chart == "ranking":
        return "ranking"
    if chart in ("markdown", "quality", "insight", "callout", "image", "divider", "section"):
        return "text"  # a note or a finding is as wide as its words need, whatever the mockup's charts are
    return "chart"


def _justify(panels: list[dict]) -> int:
    """Rows that end short are made to end flush: what is left is shared out among the cards of that row."""
    moved = 0
    row: list[dict] = []
    filled = 0

    def close() -> None:
        nonlocal moved, row, filled
        if row and filled < GRID_COLUMNS:
            base, rest = divmod(GRID_COLUMNS - filled, len(row))
            for i, place in enumerate(row):
                extra = base + (1 if i < rest else 0)
                if extra:
                    place["span"] = int(place["span"]) + extra
                    moved += 1
        row, filled = [], 0

    for panel in panels:
        place = panel.get("place")
        if place is None:
            close()
            continue
        span = int(place.get("span") or GRID_COLUMNS // 2)
        if filled + span > GRID_COLUMNS:
            close()
        row.append(place)
        filled += span
        if filled == GRID_COLUMNS:
            row, filled = [], 0
    close()
    return moved
