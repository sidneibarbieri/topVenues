"""Isolation shared by every test.

`--profile` on the CLI selects a configuration for the whole process through
src.config's module-level manager. A CLI test that pointed it at a temporary
profile left it there, and the interface tests that followed opened that
profile's empty workspace. A clone that had already reproduced security-20
hid the leak; a fresh clone following the README failed on it.
"""

import pytest

from src import config


@pytest.fixture(autouse=True)
def _fresh_configuration(monkeypatch):
    """Start every test from the configuration the environment selects."""
    monkeypatch.setattr(config, "_config_manager", None)
