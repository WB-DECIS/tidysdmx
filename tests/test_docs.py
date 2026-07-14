"""Executable-documentation smoke tests.

These guard against documentation drifting away from the public API: a fenced
``python`` code block that stops running (e.g. after a builder rename) fails CI
instead of silently misleading readers.
"""

import re
from pathlib import Path

import pytest

DOCS_DIR = Path(__file__).resolve().parents[1] / "docs"


def _extract_python_block(markdown: str, after_heading: str) -> str:
    """Return the first fenced ``python`` block following *after_heading*.

    Args:
        markdown: Full markdown document text.
        after_heading: Substring identifying the heading to search after.

    Returns:
        The code inside the first ```python fence after the heading.
    """
    start = markdown.find(after_heading)
    assert start != -1, f"Heading {after_heading!r} not found"
    match = re.search(r"```python\n(.*?)```", markdown[start:], re.DOTALL)
    assert match is not None, f"No python block found after {after_heading!r}"
    return match.group(1)


def test_pysdmx_overview_quick_reference_executes():
    """The 'Building Mapping Objects' quick-reference snippet must run as written."""
    doc = (DOCS_DIR / "pysdmx-overview.md").read_text(encoding="utf-8")
    code = _extract_python_block(doc, "## 9. Building Mapping Objects")
    namespace: dict[str, object] = {}
    try:
        exec(compile(code, "pysdmx-overview.md#section-9", "exec"), namespace)
    except Exception as exc:  # pragma: no cover - failure path is the assertion
        pytest.fail(f"Section-9 snippet failed to execute: {exc!r}")
    # The snippet ends by applying a StructureMap; its result must carry the
    # mapped target columns, proving the documented flow works end to end.
    result_df = namespace["result_df"]
    assert "REF_AREA" in result_df.columns
    assert "FREQ" in result_df.columns
