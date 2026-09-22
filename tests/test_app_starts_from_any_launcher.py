"""The interface must import the way `streamlit run web/app.py` runs it.

That command puts web/ on sys.path, not the repository root. The application
bootstraps the root itself, but the bootstrap once sat below `from web import
charts`, so the import failed while Streamlit's health endpoint still answered
200 and the server log stayed clean: the only symptom was a red page.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WEB_DIRECTORY = ROOT / "web"

# Reproduces what the console script does: the script's own directory leads,
# and the repository root is absent.
IMPORT_AS_STREAMLIT_DOES = """
import sys
sys.path = [sys.argv[1]] + [p for p in sys.path if p not in ("", sys.argv[2])]
import importlib.util
spec = importlib.util.spec_from_file_location("topvenues_app_under_test", sys.argv[3])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
"""


def test_the_application_imports_without_the_repository_root_on_the_path():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            IMPORT_AS_STREAMLIT_DOES,
            str(WEB_DIRECTORY),
            str(ROOT),
            str(WEB_DIRECTORY / "app.py"),
        ],
        cwd=WEB_DIRECTORY,
        capture_output=True,
        text=True,
    )
    assert "ModuleNotFoundError" not in result.stderr, result.stderr
