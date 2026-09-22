"""Small, framework-neutral helpers for extracting selected chart values."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def selected_chart_value(event: Mapping[str, Any], parameter: str, field: str) -> Any | None:
    """Return the first selected value for ``field`` from a Streamlit Vega event."""
    selection = event.get("selection", {})
    if not isinstance(selection, Mapping):
        return None
    return _find_field(selection.get(parameter), field)


def _find_field(value: Any, field: str) -> Any | None:
    if isinstance(value, Mapping):
        if field in value:
            selected = value[field]
            if isinstance(selected, Sequence) and not isinstance(selected, str):
                return selected[0] if selected else None
            return selected
        for nested in value.values():
            found = _find_field(nested, field)
            if found is not None:
                return found
    elif isinstance(value, Sequence) and not isinstance(value, str):
        for nested in value:
            found = _find_field(nested, field)
            if found is not None:
                return found
    return None
