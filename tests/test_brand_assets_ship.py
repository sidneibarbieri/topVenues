"""The interface loads its logo and favicon from docs/brand at start-up.

A missing file there does not fail an import-level check; it fails when
Streamlit renders the first page, which in the container means a health check
that never turns green. These assertions move that failure into CI.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_every_brand_file_the_interface_loads_exists():
    from web import app

    for path in (app.BRAND_WORDMARK, app.BRAND_MARK, app.BRAND_ICON):
        assert path.is_file(), f"{path.relative_to(ROOT)} is referenced by web/app.py"


def test_the_container_ships_the_brand_directory():
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert re.search(r"^COPY docs/brand/ \./docs/brand/$", dockerfile, flags=re.M)


def test_the_interface_font_is_served_from_the_repository():
    """The theme names Inter; the file must ship, and the image must carry the theme."""
    config = (ROOT / ".streamlit" / "config.toml").read_text(encoding="utf-8")
    url = re.search(r'^url = "app/static/([^"]+)"$', config, flags=re.M)
    assert url, "the theme no longer declares a self-hosted font"
    assert (ROOT / "web" / "static" / url.group(1)).is_file()
    assert re.search(r"^enableStaticServing = true$", config, flags=re.M)
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert re.search(r"^COPY \.streamlit/ \./\.streamlit/$", dockerfile, flags=re.M)
