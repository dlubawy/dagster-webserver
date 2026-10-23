"""Inline SVG icons for the admin portal.

Icons are vendored from the Dagster webapp build (``webapp/build/_next/static/media``)
so the admin portal uses the same icon set as the main UI, with no CDN dependency.
Fills are rewritten to ``currentColor`` so icons inherit their surrounding text color.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path

from markupsafe import Markup

_ICONS_DIR = Path(__file__).parent / "icons"

# Canonical icon name -> vendored SVG file
_ICON_FILES: dict[str, str] = {
    "add": "add.13a3b8e2.svg",
    "arrow_back": "arrow_back.737a58dd.svg",
    "cancel": "cancel.e5f3bf84.svg",
    "close": "close.c0a62391.svg",
    "code_location": "code_location.39447bbb.svg",
    "dashboard": "gauge.a269b8a6.svg",
    "delete": "delete.0345d72d.svg",
    "edit": "edit.a13eac80.svg",
    "logo": "dagster_primary.7b92f477.svg",
    "robot": "smart_toy.ae220c48.svg",
    "shield": "shield.4f432a2b.svg",
    "sso": "sso.352d6193.svg",
    "team": "team.accb2cfc.svg",
    "token": "token.8d757929.svg",
    "visibility": "visibility.8aa23689.svg",
}

# Aliases for legacy Font Awesome names used in views/templates
_ALIASES: dict[str, str] = {
    "arrow-left": "arrow_back",
    "ban": "cancel",
    "code": "code_location",
    "dashboard": "dashboard",
    "eye": "visibility",
    "key": "token",
    "plus": "add",
    "robot": "robot",
    "shield-alt": "shield",
    "tachometer-alt": "dashboard",
    "times": "close",
    "trash": "delete",
    "users": "team",
}


def _normalize(name: str) -> str | None:
    """Map a template icon reference (e.g. ``"fas fa-eye"`` or ``"users"``) to a canonical name."""
    name = name.strip()
    if name.startswith("fa-"):
        name = name.split()[-1][3:]
    elif " " in name:
        # e.g. "fas fa-eye"
        parts = name.split()
        name = parts[-1]
        if name.startswith("fa-"):
            name = name[3:]
    name = name.lower()
    if name in _ALIASES:
        return _ALIASES[name]
    if name in _ICON_FILES:
        return name
    return None


@lru_cache(maxsize=None)
def _load_svg(filename: str) -> str:
    svg = (_ICONS_DIR / filename).read_text()
    if "dagster_primary" not in filename:
        # Rewrite explicit fills to currentColor so icons follow text color
        # (the brand logo keeps its own colors)
        svg = re.sub(r'fill="#[0-9a-fA-F]{3,8}"', 'fill="currentColor"', svg)
        # Some icons omit fill entirely (renders black by default — invisible
        # in dark mode); default the root element to currentColor
        if 'fill="' not in svg.split(">", 1)[0]:
            svg = svg.replace("<svg ", '<svg fill="currentColor" ', 1)
    return svg


def icon(name: str, size: int = 16) -> str:
    """Return an inline SVG string for the given icon name.

    Accepts canonical names (``"team"``, ``"token"``), legacy Font Awesome
    short names (``"users"``, ``"eye"``, ``"trash"``), or full FA classes
    (``"fas fa-eye"``). Returns an empty string for unknown icons.

    The result is marked safe: it is a static vendored SVG, and marking it
    also keeps the markup intact when rendered inside <script> blocks.
    """
    canonical = _normalize(name)
    if canonical is None or canonical not in _ICON_FILES:
        return ""
    svg = _load_svg(_ICON_FILES[canonical])
    # Normalize size while keeping the viewBox
    svg = re.sub(r'width="\d+"', f'width="{size}"', svg, count=1)
    svg = re.sub(r'height="\d+"', f'height="{size}"', svg, count=1)
    if 'class="' not in svg:
        svg = svg.replace("<svg ", '<svg class="admin-icon" style="vertical-align: -2px;" ', 1)
    return Markup(svg)
