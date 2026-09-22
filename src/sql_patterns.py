"""Turn reader-supplied text into a SQL LIKE pattern that means what it says."""

from __future__ import annotations

LIKE_ESCAPE = "\\"
LIKE_ESCAPE_CLAUSE = r"ESCAPE '\'"


def contains_pattern(text: str) -> str:
    """A LIKE pattern matching ``text`` literally, anywhere in the column.

    ``%`` and ``_`` are LIKE wildcards. Unescaped, a topic of ``%`` matches every
    record in the corpus and reports it as that topic's trend, and a topic such
    as ``use_after_free`` also matches text that merely resembles it. Either way
    the reader's count silently stops counting what they typed.

    The query must pair this with :data:`LIKE_ESCAPE_CLAUSE`.
    """
    escaped = (
        text.replace(LIKE_ESCAPE, LIKE_ESCAPE * 2)
        .replace("%", LIKE_ESCAPE + "%")
        .replace("_", LIKE_ESCAPE + "_")
    )
    return f"%{escaped}%"
