# tests/fixtures/fxtr_structures.py
from collections.abc import Callable, Sequence
from pathlib import Path

import openpyxl
import pandas as pd
import pytest


@pytest.fixture
def value_map_df_mandatory_cols():
    """Return a DataFrame with 'source' and 'target' columns.

    Function-scoped and rebuilt from its inline definition on every use so
    tests never share (or accidentally mutate) a single instance, and so the
    inputs can never be silently shadowed by a stale on-disk cache.
    """
    return pd.DataFrame(
        {"source": ["regex:^A", "UY", "FR"], "target": ["ARG", "URY", "FRA"]}
    )


@pytest.fixture
def template_workbook_factory(tmp_path) -> Callable[..., Path]:
    """Return a factory that writes an Excel mapping template to ``tmp_path``.

    The factory takes a mapping of sheet name to rows (each row a sequence of
    cell values; the first row is the header) and returns the path to a real
    ``.xlsx`` file. Using a real workbook — rather than passing hand-built
    object-dtype DataFrames straight to the builder — exercises
    :func:`tidysdmx.utils.parse_mapping_template_wb`, so Excel-specific
    behaviour (empty cells, ``dtype="string"`` coercion, ``"NA"`` handling,
    numeric cells) is covered end to end.

    Example:
        >>> path = template_workbook_factory({
        ...     "INFO": [["Key", "Value"], ["dataflow", "AG:DF(1.0)"]],
        ...     "COMP_MAPPING": [["SOURCE", "TARGET", "MAPPING_RULES"],
        ...                      ["S1", "T1", "implicit"]],
        ... })
    """
    counter = {"n": 0}

    def _make(sheets: dict[str, Sequence[Sequence[object]]]) -> Path:
        wb = openpyxl.Workbook()
        wb.remove(wb.active)
        for sheet_name, rows in sheets.items():
            ws = wb.create_sheet(title=sheet_name)
            for row in rows:
                ws.append(list(row))
        counter["n"] += 1
        path = tmp_path / f"template_{counter['n']}.xlsx"
        wb.save(path)
        return path

    return _make
