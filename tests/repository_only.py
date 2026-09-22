"""Marker for tests that audit the repository rather than the running artifact.

Some checks read files that exist only in a checkout: the Dockerfile, the
documentation tree, the release metadata. The container image deliberately
carries only what the tool needs at runtime, so inside it those files are absent
by design. Failing there would report a defect that does not exist; skipping
with a stated reason keeps `docker run ... pytest -q` meaningful.
"""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

# The image never copies its own build recipe, which makes this a reliable
# signal for "this is a checkout, not the built image".
IS_REPOSITORY = (ROOT / "Dockerfile").exists()

REASON = "repository-only check: the container image ships runtime files only"


def skip_unless_repository() -> None:
    if not IS_REPOSITORY:
        pytest.skip(REASON, allow_module_level=True)
