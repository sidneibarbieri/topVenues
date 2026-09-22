"""The Portuguese interface is complete, and no English string reaches it untranslated.

The source strings are read from the interface code itself, so a string added
without a translation, a translation that drops a placeholder, and a catalog
entry nothing uses any more all fail here rather than on a reader's screen.
"""

import ast
import json
import re
import string
from pathlib import Path

from src.reproduction_commands import SUPPORTED
from src.tiers import tier_scope_options

WEB_DIR = Path(__file__).resolve().parents[1] / "web"
INTERFACE_MODULES = (WEB_DIR / "app.py", WEB_DIR / "i18n.py")
CATALOG = json.loads((WEB_DIR / "locales" / "pt.json").read_text(encoding="utf-8"))
TRANSLATION_FUNCTIONS = {"t", "marked"}

# Strings defined outside the interface that it translates on screen.
EXTERNAL_KEYS = {*tier_scope_options(), *(item.platform for item in SUPPORTED)}

# Calls whose text arguments a reader sees, and which keyword arguments carry text.
DISPLAY_CALLS = {
    "_render_header", "HeadlineCard", "PageMovement",
    "markdown", "caption", "subheader", "write", "text", "info", "warning", "error", "success",
    "button", "download_button", "link_button", "form_submit_button",
    "text_input", "text_area", "number_input", "selectbox", "multiselect", "radio",
    "checkbox", "select_slider", "file_uploader", "metric", "spinner", "expander", "tabs",
    "progress", "TextColumn", "NumberColumn", "LinkColumn",
}  # fmt: skip
DISPLAY_KEYWORDS = {"help", "placeholder", "text", "display_text", "label"}
OPTION_WIDGETS = {"selectbox", "multiselect", "radio", "select_slider"}

# Names that read the same in either language.
UNTRANSLATED_TERMS = re.compile(r"\b(?:BibTeX|DOI|URL|TopVenues|arXiv|CSV|JSON)\b")
MARKUP = re.compile(r"<[^>]*>|&[a-z]+;|\\[a-z]+|\{[^}]*\}")
WORD = re.compile(r"[A-Za-z]{2,}")


def _trees() -> list[ast.Module]:
    return [ast.parse(path.read_text(encoding="utf-8")) for path in INTERFACE_MODULES]


def _calls(names: set[str]) -> list[ast.Call]:
    return [
        node
        for tree in _trees()
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and _call_name(node) in names
    ]


def _call_name(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _source_strings() -> set[str]:
    return {
        call.args[0].value
        for call in _calls(TRANSLATION_FUNCTIONS)
        if call.args
        and isinstance(call.args[0], ast.Constant)
        and isinstance(call.args[0].value, str)
    }


def _placeholders(text: str) -> set[str]:
    return {field for _, field, _, _ in string.Formatter().parse(text) if field is not None}


def _visible_words(node: ast.expr) -> bool:
    """Whether a literal argument would show English words on screen."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        pieces = [node.value]
    elif isinstance(node, ast.JoinedStr):
        pieces = [part.value for part in node.values if isinstance(part, ast.Constant)]
    elif isinstance(node, ast.List | ast.Tuple):
        return any(_visible_words(element) for element in node.elts)
    else:
        return False
    text = UNTRANSLATED_TERMS.sub("", MARKUP.sub("", "".join(pieces)))
    return bool(WORD.search(text))


def _displayed_arguments(call: ast.Call) -> list[ast.expr]:
    name = _call_name(call)
    positional = (
        call.args if name in {"_render_header", "HeadlineCard", "PageMovement"} else call.args[:1]
    )
    keywords = [keyword.value for keyword in call.keywords if keyword.arg in DISPLAY_KEYWORDS]
    return [*positional, *keywords]


def test_every_interface_string_has_a_translation():
    missing = sorted((_source_strings() | EXTERNAL_KEYS) - CATALOG.keys())
    assert not missing, f"add these to web/locales/pt.json: {missing}"


def test_no_translation_is_left_unused():
    unused = sorted(CATALOG.keys() - _source_strings() - EXTERNAL_KEYS)
    assert not unused, f"remove these from web/locales/pt.json: {unused}"


def test_every_translation_keeps_its_placeholders():
    drifted = sorted(
        source
        for source, translated in CATALOG.items()
        if _placeholders(translated) != _placeholders(source)
    )
    assert not drifted, f"these translations add or drop a {{field}}: {drifted}"


def test_translation_lookups_use_whole_strings():
    """An f-string or concatenation builds a key the catalog can never hold."""
    built = [
        ast.unparse(call)
        for call in _calls(TRANSLATION_FUNCTIONS)
        if call.args and isinstance(call.args[0], ast.JoinedStr | ast.BinOp)
    ]
    assert not built, f"pass fields to t() instead of formatting the key: {built}"


def test_no_displayed_literal_bypasses_translation():
    bare = [
        f"line {call.lineno}: {ast.unparse(argument)[:80]}"
        for call in _calls(DISPLAY_CALLS)
        for argument in _displayed_arguments(call)
        if _visible_words(argument)
    ]
    assert not bare, "wrap these in t():\n" + "\n".join(bare)


def test_literal_options_are_translated_on_screen():
    """Options stay English as values, so the widget must translate what it shows."""
    untranslated = [
        f"line {call.lineno}: {_call_name(call)}"
        for call in _calls(OPTION_WIDGETS)
        if len(call.args) > 1
        and _visible_words(call.args[1])
        and not any(keyword.arg == "format_func" for keyword in call.keywords)
    ]
    assert not untranslated, f"give these widgets format_func=t: {untranslated}"


def test_every_page_renders_in_portuguese():
    """A page that raises, or shows a catalog string still in English, fails here."""
    import logging

    from streamlit.testing.v1 import AppTest

    logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(
        logging.ERROR
    )

    problems = []
    for page in ("Overview", "Search", "Insights", "Evidence", "Dataset lifecycle"):
        app = AppTest.from_file("web/app.py", default_timeout=300)
        # The widgets' format_func runs outside the script in AppTest, where the
        # language is unknown, so pages are chosen through session state.
        app.session_state["language"] = "pt"
        app.session_state["page"] = page
        app.run()
        problems += [(page, "exception", str(exception.value)) for exception in app.exception]
        shown = [element.value for element in (*app.subheader, *app.caption, *app.markdown)]
        problems += [(page, "English", text) for text in shown if CATALOG.get(text, text) != text]
    assert not problems, problems
