"""Parity checks: every polars port must match its pandas original.

A faster wrong answer is not a win, so these assertions gate the benchmark
numbers. Run with::

    poetry run pytest benchmarks/parity.py -o python_files=parity.py

or directly (``python -m benchmarks.parity``) for a printed summary.
"""

from __future__ import annotations

import math

import pandas as pd
import polars as pl

from benchmarks import _data
from benchmarks import polars_impls as plimpl
from tidysdmx.mapping import apply_component_map, apply_multi_component_map
from tidysdmx.qa_utils import qa_coerce_numeric, qa_remove_duplicates
from tidysdmx.tidy_raw import filter_rows
from tidysdmx.tidysdmx import (
    vectorized_lookup_ordered_v1,
    vectorized_lookup_ordered_v2,
)

N_PARITY_ROWS = 5_000


def _norm_cell(value: object) -> str | None:
    """Normalise a scalar to ``None`` (for nulls) or a stable string."""
    if value is None:
        return None
    if isinstance(value, float):
        return None if math.isnan(value) else f"{value:.6f}"
    if pd.isna(value):
        return None
    return str(value)


def _norm_series(series: pd.Series | pl.Series) -> list[str | None]:
    """Positional comparison: row order is meaningful and preserved."""
    if isinstance(series, pl.Series):
        series = series.to_pandas()
    return [_norm_cell(v) for v in series.tolist()]


def _norm_df_sorted(df: pd.DataFrame | pl.DataFrame) -> list[tuple]:
    """Order-insensitive comparison of full rows (handles filtered output)."""
    if isinstance(df, pl.DataFrame):
        df = df.to_pandas()
    rows = [
        tuple(_norm_cell(v) for v in row)
        for row in df.itertuples(index=False, name=None)
    ]
    return sorted(rows, key=lambda r: tuple("" if x is None else x for x in r))


def _fixtures():
    pdf = _data.make_sdmx_df(N_PARITY_ROWS)
    return pdf, _data.to_polars(pdf)


def test_qa_coerce_numeric_parity():
    pdf, pldf = _fixtures()
    expected = qa_coerce_numeric(pdf, ["OBS_VALUE"])
    actual = plimpl.qa_coerce_numeric_pl(pldf, ["OBS_VALUE"])
    assert _norm_df_sorted(expected) == _norm_df_sorted(actual)


def test_qa_remove_duplicates_parity():
    pdf, pldf = _fixtures()
    expected = qa_remove_duplicates(pdf)
    actual = plimpl.qa_remove_duplicates_pl(pldf)
    assert _norm_df_sorted(expected) == _norm_df_sorted(actual)


def test_vectorized_lookup_v1_parity():
    pdf, pldf = _fixtures()
    mapping = _data.make_lookup_mapping_df()
    expected = vectorized_lookup_ordered_v1(pdf["AREA"], mapping[["SOURCE", "TARGET"]])
    actual = plimpl.vectorized_lookup_ordered_v1_pl(pldf["AREA"], mapping)
    assert _norm_series(expected) == _norm_series(actual)


def test_vectorized_lookup_v2_parity():
    pdf, pldf = _fixtures()
    mapping = _data.make_lookup_mapping_df()
    expected = vectorized_lookup_ordered_v2(pdf["AREA"], mapping)
    actual = plimpl.vectorized_lookup_ordered_v2_pl(pldf["AREA"], mapping)
    assert _norm_series(expected) == _norm_series(actual)


def test_apply_component_map_parity():
    pdf, pldf = _fixtures()
    cm = _data.make_component_map()
    expected = apply_component_map(pdf, cm)
    actual = plimpl.apply_component_map_pl(pldf, cm)
    assert _norm_df_sorted(expected) == _norm_df_sorted(actual)


def test_apply_multi_component_map_parity():
    pdf, pldf = _fixtures()
    mcm = _data.make_multi_component_map()
    expected = apply_multi_component_map(pdf, mcm)
    actual = plimpl.apply_multi_component_map_pl(pldf, mcm)
    assert _norm_df_sorted(expected) == _norm_df_sorted(actual)


def test_filter_rows_parity():
    pdf, pldf = _fixtures()
    codelist_ids = _data.make_codelist_ids()
    expected = filter_rows(pdf, codelist_ids)
    actual = plimpl.filter_rows_pl(pldf, codelist_ids)
    assert _norm_df_sorted(expected) == _norm_df_sorted(actual)


def run_all() -> dict[str, bool]:
    """Run every parity check and return ``{name: passed}`` (for the writeup)."""
    results: dict[str, bool] = {}
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                results[name] = True
            except AssertionError:
                results[name] = False
    return results


if __name__ == "__main__":
    outcome = run_all()
    for name, passed in outcome.items():
        print(f"{'PASS' if passed else 'FAIL'}  {name}")
    print(f"\n{sum(outcome.values())}/{len(outcome)} parity checks passed")
