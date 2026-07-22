#!/usr/bin/env python3
"""Pad the legacy icon and clip to a squircle — keeps the original artwork."""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw

SIZE = 1024
PADDING_RATIO = 0.12
CORNER_RADIUS_RATIO = 0.223

ROOT = Path(__file__).resolve().parents[1]
ICONS = ROOT / "src-tauri" / "icons"
LEGACY = ICONS / "icon-legacy.png"
OUT = ICONS / "icon-source.png"


def main() -> None:
    padding = int(SIZE * PADDING_RATIO)
    radius = int(SIZE * CORNER_RADIUS_RATIO)
    inner = SIZE - padding * 2

    src = Image.open(LEGACY).convert("RGBA")
    scaled = src.resize((inner, inner), Image.Resampling.LANCZOS)

    canvas = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
    canvas.paste(scaled, (padding, padding), scaled)

    mask = Image.new("L", (SIZE, SIZE), 0)
    draw = ImageDraw.Draw(mask)
    draw.rounded_rectangle((0, 0, SIZE - 1, SIZE - 1), radius=radius, fill=255)

    red, green, blue, alpha = canvas.split()
    alpha = Image.composite(alpha, Image.new("L", (SIZE, SIZE), 0), mask)
    canvas = Image.merge("RGBA", (red, green, blue, alpha))

    canvas.save(OUT)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
