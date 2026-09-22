"""Interface language: English at the call site, translated when a page renders.

Strings are written in English where they are used and looked up in
web/locales/<language>.json. Numbers are formatted here as well, because a
Portuguese reader writes 20.305 and 94,5% where an English one writes 20,305
and 94.5%. tests/test_interface_translations.py fails when a string has no
translation, a translation drops a placeholder, or a key is no longer used.
"""

import datetime
import functools
import json
from enum import StrEnum
from pathlib import Path

import streamlit as st

LOCALES_DIR = Path(__file__).resolve().parent / "locales"
LANGUAGE_KEY = "language"
LANGUAGE_QUERY_PARAMETER = "lang"


class Language(StrEnum):
    ENGLISH = "en"
    PORTUGUESE = "pt"

    @property
    def native_name(self) -> str:
        return NATIVE_NAMES[self]


NATIVE_NAMES = {Language.ENGLISH: "English", Language.PORTUGUESE: "Português"}

DIGIT_SEPARATORS = {
    Language.ENGLISH: str.maketrans({}),
    Language.PORTUGUESE: str.maketrans({",": ".", ".": ","}),
}

# Vega-Lite's number locale, so axis ticks and bar labels follow the reader too.
CHART_NUMBER_LOCALES = {
    Language.ENGLISH: None,
    Language.PORTUGUESE: {
        "decimal": ",",
        "thousands": ".",
        "grouping": [3],
        "currency": ["R$", ""],
    },
}

MONTH_NAMES = {
    Language.ENGLISH: (
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
    ),
    Language.PORTUGUESE: (
        "janeiro", "fevereiro", "março", "abril", "maio", "junho",
        "julho", "agosto", "setembro", "outubro", "novembro", "dezembro",
    ),
}  # fmt: skip


@functools.cache
def catalog(language: Language) -> dict[str, str]:
    """English source string -> its translation. English needs no catalog."""
    if language is Language.ENGLISH:
        return {}
    return json.loads((LOCALES_DIR / f"{language}.json").read_text(encoding="utf-8"))


def active_language() -> Language:
    return Language(st.session_state.get(LANGUAGE_KEY, Language.ENGLISH))


def t(text: str, **fields: object) -> str:
    """The reader's version of an English interface string, with its fields filled in."""
    translated = catalog(active_language()).get(text, text)
    return translated.format(**fields) if fields else translated


def marked(text: str) -> str:
    """Mark a string kept in data, such as an option value; t() translates it on screen."""
    return text


def option_label(option: object) -> str:
    """Selectbox label for an option that may be a marked string, a venue name, or a year."""
    return t(option) if isinstance(option, str) else str(option)


def number(value: float, decimals: int = 0) -> str:
    return f"{value:,.{decimals}f}".translate(DIGIT_SEPARATORS[active_language()])


def percent(ratio: float, decimals: int = 1) -> str:
    return number(ratio * 100, decimals) + "%"


def month_and_year(iso_date: str) -> str:
    captured = datetime.date.fromisoformat(iso_date)
    month = MONTH_NAMES[active_language()][captured.month - 1]
    return t("{month} {year}", month=month, year=captured.year)


def chart_number_locale() -> dict | None:
    return CHART_NUMBER_LOCALES[active_language()]


def _initial_language() -> Language:
    """The language a shared link names, else the browser's, else English."""
    requested = st.query_params.get(LANGUAGE_QUERY_PARAMETER)
    if requested in {language.value for language in Language}:
        return Language(requested)
    browser_locale = (st.context.locale or "").casefold()
    return Language.PORTUGUESE if browser_locale.startswith("pt") else Language.ENGLISH


def choose_language() -> Language:
    """Offer the language switch and keep the page link naming the choice.

    The options are the plain codes, not the enum members, so a value that
    arrives as text, from a link or a test, still selects its option.
    """
    if LANGUAGE_KEY not in st.session_state:
        st.session_state[LANGUAGE_KEY] = _initial_language().value
    code = st.radio(
        t("Language"),
        tuple(language.value for language in Language),
        format_func=lambda option: Language(option).native_name,
        horizontal=True,
        key=LANGUAGE_KEY,
        label_visibility="collapsed",
    )
    st.query_params[LANGUAGE_QUERY_PARAMETER] = code
    return Language(code)
