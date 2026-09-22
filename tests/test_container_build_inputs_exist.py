"""Every path the Dockerfile copies must exist, and it must pin what it installs.

`docker build` failed at `COPY data/dataset/papers.db.gz`: that path is not in
the repository, so the container the paper says the artifact packages could not
be built at all. Nothing caught it, because no test read the Dockerfile.

The image also installed the open ranges in requirements.txt, giving a reviewer
who used Docker different versions from one who ran the reproduction script.
"""

from __future__ import annotations

import re
from pathlib import Path

from tests.repository_only import skip_unless_repository

skip_unless_repository()

ROOT = Path(__file__).resolve().parents[1]

DOCKERFILE = (ROOT / "Dockerfile").read_text(encoding="utf-8")

COPY_LINE = re.compile(r"^COPY\s+(?!--from)(.+)$", re.MULTILINE)


def _copied_sources() -> list[str]:
    sources = []
    for line in COPY_LINE.findall(DOCKERFILE):
        parts = line.replace("\\", "").split()
        sources.extend(parts[:-1])  # the last token is the destination
    return sources


def test_every_copied_path_exists():
    missing = [source for source in _copied_sources() if not (ROOT / source).exists()]
    assert not missing, missing


def test_the_image_installs_the_pinned_hash_checked_set():
    assert "requirements-frozen.txt" in DOCKERFILE
    assert "--require-hashes" in DOCKERFILE
    assert "pip install -r requirements.txt" not in DOCKERFILE


def test_the_image_starts_the_interface_through_the_module_launcher():
    """`streamlit run` puts web/ on sys.path, not the repository root."""
    assert '"python", "-m", "streamlit"' in DOCKERFILE


def test_the_image_carries_what_the_interface_reads_at_runtime():
    """The Evidence page raised inside the image: its audit files were excluded.

    The health endpoint still answered 200 while that page was broken, so only
    rendering every page inside the container revealed it.
    """
    ignore = (ROOT / ".dockerignore").read_text(encoding="utf-8")
    assert "!evaluation/security-20-v3" in ignore
    assert "!evaluation/security-20-v4" in ignore
    assert "COPY evaluation/" in DOCKERFILE

    for needed in (
        "evaluation/security-20-v3/manual_abstract_audit_summary.json",
        "evaluation/security-20-v4/audit_transfer.json",
    ):
        assert (ROOT / needed).exists(), needed
