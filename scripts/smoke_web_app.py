"""Prove the interface renders, not merely that a port answers.

Streamlit serves `/_stcore/health` from the server process, which starts before
and independently of the application script. A health probe therefore returns
200 for an application that cannot even import, and because the script only
runs once a client connects, its traceback never reaches the server log either.
A reviewer would see a green check on an interface that renders nothing.

`AppTest` executes the script the way a browser session does. It also executes
only the page the navigation selects, so every page is visited here: an
exception confined to one of them is invisible from any single render.
"""

from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from pathlib import Path

from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parents[1]
APPLICATION_SCRIPT = ROOT / "web" / "app.py"

# Nothing is added to sys.path here on purpose. `streamlit run web/app.py` puts
# web/ on the path and not the repository root, so an application that needs the
# root added for it is already broken for that command; putting it back here
# would hide exactly that failure.

NAVIGATION_KEY = "page"
SCRIPT_TIMEOUT_SECONDS = 120

# Silences the "missing ScriptRunContext" warning Streamlit documents as safe to
# ignore in bare mode, which AppTest always triggers. Only that logger is
# affected; anything the script raises still arrives through AppTest.exception.
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)


@dataclass(frozen=True)
class PageContract:
    """A navigation entry and the text that proves it rendered its own content.

    Without an anchor a page that silently renders nothing still passes, because
    rendering nothing raises nothing.
    """

    name: str
    anchor: str


PAGE_CONTRACTS = (
    PageContract("Overview", "Start from a research question"),
    PageContract("Search", "Paper details"),
    PageContract("Insights", "Papers by venue"),
    PageContract("Evidence", "Manual abstract audit"),
    PageContract("Dataset lifecycle", "Run the data collection pipeline"),
)


def _rendered_text(session: AppTest) -> str:
    """Every text element a reader would see on the current page."""
    elements = [*session.title, *session.header, *session.subheader, *session.markdown]
    return "\n".join(str(element.value) for element in elements)


def _fail(message: str) -> None:
    raise SystemExit(message)


def _verify_rendered(session: AppTest, contract: PageContract) -> None:
    if session.exception:
        raised = "\n".join(str(item.value) for item in session.exception)
        _fail(f"the {contract.name} page raised while rendering:\n{raised}")
    if contract.anchor not in _rendered_text(session):
        _fail(f"the {contract.name} page rendered without {contract.anchor!r}")


def _verify_contracts_cover(navigation_options: list[str]) -> None:
    """A page added without an anchor would otherwise go unverified."""
    declared = [contract.name for contract in PAGE_CONTRACTS]
    if declared != list(navigation_options):
        _fail(f"navigation offers {list(navigation_options)}, contracts declare {declared}")


def main() -> int:
    if not APPLICATION_SCRIPT.exists():
        _fail(f"application not found at {APPLICATION_SCRIPT}")

    session = AppTest.from_file(str(APPLICATION_SCRIPT), default_timeout=SCRIPT_TIMEOUT_SECONDS)
    session.run()
    if session.exception:
        raised = "\n".join(str(item.value) for item in session.exception)
        _fail(f"the application raised while opening:\n{raised}")

    _verify_contracts_cover(session.radio(key=NAVIGATION_KEY).options)

    for contract in PAGE_CONTRACTS:
        session.radio(key=NAVIGATION_KEY).set_value(contract.name).run()
        _verify_rendered(session, contract)
        print(f"  {contract.name}: rendered")

    print(f"Every page rendered its own content ({len(PAGE_CONTRACTS)} pages).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
