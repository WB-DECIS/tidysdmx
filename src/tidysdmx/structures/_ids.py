"""Identifier and code-ID sanitisation helpers for SDMX artefacts."""

import re

from typeguard import typechecked

_ID_PATTERN = re.compile(r"[^A-Za-z0-9_]+")


def _sanitize_identifier(raw: str) -> str | None:
    """Return an uppercased SDMX-safe identifier, or ``None`` if empty.

    Replaces non-alphanumeric/underscore runs with ``_``, strips leading and
    trailing underscores, and prefixes a leading digit with ``_``. Returns
    ``None`` when nothing usable remains (e.g. a symbol-only input), so callers
    can raise an error tailored to what the string represents.
    """
    cleaned = _ID_PATTERN.sub("_", raw).strip("_")
    if not cleaned:
        return None
    if cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned.upper()


@typechecked
def _to_identifier(raw: str) -> str:
    """Convert a raw string to a valid SDMX identifier (e.g. a component ID)."""
    cleaned = _sanitize_identifier(raw)
    if cleaned is None:
        raise ValueError(
            f"Column name {raw!r} cannot be converted to a valid SDMX identifier."
        )
    return cleaned


@typechecked
def _code_id(raw: str, uppercase: bool = True) -> str:
    """Sanitize a raw cell value into a valid SDMX code ID.

    Raises:
        ValueError: If *raw* contains no character usable in an SDMX identifier
            (e.g. a symbol-only value such as ``"!!!"``).
    """
    cleaned = _sanitize_identifier(str(raw))
    if cleaned is None:
        raise ValueError(f"Value {raw!r} cannot be converted to a valid SDMX code ID.")
    return cleaned if uppercase else cleaned.lower()


@typechecked
def sanitize_variable(value: str, uppercase: bool = True) -> str:
    """Sanitize a raw string value into a valid SDMX code ID.

    Applies the same sanitization used internally by ``create_schema_from_table``
    when building codelist code IDs from DataFrame column values. Use this
    function during your data cleaning phase to ensure that the values in your
    DataFrame will match the code IDs generated in the schema.

    The sanitization rules are:
    - Non-alphanumeric/underscore characters (including dots) are replaced with ``_``.
    - Leading/trailing underscores are stripped.
    - IDs starting with a digit are prefixed with ``_``.
    - Result is uppercased by default (controlled by ``uppercase``).

    Args:
        value: The raw string value to sanitize (e.g. ``"per_allsp.adq_ep_preT_tot"``).
        uppercase: If True (default), the result is uppercased, matching the default
            behaviour of ``create_schema_from_table``. Set to False if you called
            ``create_schema_from_table`` with ``uppercase_code_ids=False``.

    Returns:
        A sanitized SDMX-safe identifier string.

    Examples:
        >>> sanitize_variable("per_allsp.adq_ep_preT_tot")
        'PER_ALLSP_ADQ_EP_PRET_TOT'
        >>> sanitize_variable("per_allsp.adq_ep_preT_tot", uppercase=False)
        'per_allsp_adq_ep_pret_tot'
    """
    return _code_id(value, uppercase=uppercase)
