#!/usr/bin/env python3
"""Fetch an archived snapshot from its immutable release and verify its digest."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.profiles import PROFILE_IDS, load_profile  # noqa: E402


def fetch_profile(profile_id: str, root: Path = ROOT) -> Path:
    profile = load_profile(profile_id, root)
    expected = profile.manifest["snapshot"]
    if profile.snapshot_path.is_file():
        if profile.snapshot_path.stat().st_size != expected["gzip_bytes"]:
            raise RuntimeError("local snapshot size does not match the immutable manifest")
        with profile.snapshot_path.open("rb") as snapshot:
            local_digest = hashlib.file_digest(snapshot, "sha256").hexdigest()
        if local_digest != expected["gzip_sha256"]:
            raise RuntimeError("local snapshot SHA-256 does not match the immutable manifest")
        return profile.snapshot_path
    distribution = profile.manifest.get("distribution", {})
    url = distribution.get("url")
    if not url:
        raise RuntimeError(f"profile {profile_id} has no archived distribution URL")

    profile.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{profile.snapshot_path.name}.", dir=profile.snapshot_path.parent
    )
    digest = hashlib.sha256()
    bytes_written = 0
    try:
        with (
            os.fdopen(descriptor, "wb") as target,
            httpx.stream("GET", url, follow_redirects=True, timeout=180.0) as response,
        ):
            response.raise_for_status()
            for chunk in response.iter_bytes():
                target.write(chunk)
                digest.update(chunk)
                bytes_written += len(chunk)
        if bytes_written != expected["gzip_bytes"]:
            raise RuntimeError(
                f"downloaded size mismatch: expected {expected['gzip_bytes']}, got {bytes_written}"
            )
        if digest.hexdigest() != expected["gzip_sha256"]:
            raise RuntimeError("downloaded SHA-256 does not match the immutable manifest")
        os.replace(temporary_name, profile.snapshot_path)
    finally:
        Path(temporary_name).unlink(missing_ok=True)
    return profile.snapshot_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True, choices=PROFILE_IDS)
    args = parser.parse_args()
    path = fetch_profile(args.profile)
    manifest = json.loads((ROOT / "data" / "profiles" / args.profile / "manifest.json").read_text())
    print(f"verified {path} (sha256={manifest['snapshot']['gzip_sha256']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
