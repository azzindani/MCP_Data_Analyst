"""Screenshot built previews at desktop and phone width, light and dark, and report what a
screenshot can hide: console errors, charts that drew nothing, horizontal overflow.

    python3 design/mockups/shoot.py lagoon            # light and dark
    python3 design/mockups/shoot.py nocturne dark     # one scheme

Needs playwright with Chromium (the host python3 has it; the repo .venv does not).
"""

from __future__ import annotations

import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

BUILD = Path(__file__).parent / "build"


def shoot(name: str, schemes: list[str]) -> None:
    page_path = BUILD / "preview" / f"{name}.html"
    out = BUILD / "shots"
    out.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        for scheme in schemes:
            for width, height in [(1440, 900), (390, 844)]:
                ctx = browser.new_context(viewport={"width": width, "height": height}, color_scheme=scheme)
                pg = ctx.new_page()
                errors: list[str] = []
                pg.on("console", lambda m: errors.append(m.text) if m.type == "error" else None)
                pg.on("pageerror", lambda e: errors.append(str(e)))
                pg.goto(page_path.as_uri(), wait_until="load", timeout=180_000)
                pg.wait_for_timeout(2500)
                overflow = pg.evaluate("document.documentElement.scrollWidth - document.documentElement.clientWidth")
                empty = pg.evaluate(
                    "[...document.querySelectorAll('.chart')].filter(c => !c.querySelector('.main-svg')).map(c => c.id)"
                )
                shot = out / f"{name}_{scheme}_{width}.png"
                pg.screenshot(path=str(shot), full_page=True)
                print(f"{shot.name}: empty charts={empty} overflow={overflow}px errors={errors[:3]}")
                ctx.close()
        browser.close()


if __name__ == "__main__":
    if not sys.argv[1:]:
        sys.exit(__doc__)
    shoot(sys.argv[1], sys.argv[2].split(",") if len(sys.argv) > 2 else ["light", "dark"])
