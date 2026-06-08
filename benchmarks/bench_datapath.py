"""pytest-benchmark cases pairing each pandas data-path function with its polars port.

Inputs are built *outside* the timed region (in the test body, before the
``benchmark(...)`` call) so we measure the operation, not data construction or
the pandas->polars conversion. Each pandas/polars pair shares a ``benchmark.group``
so the comparison renders side-by-side.

Run with::

    poetry run pytest benchmarks/bench_datapath.py -o python_files=bench_datapath.py \
        --benchmark-only --benchmark-columns=mean,median,ops \
        --benchmark-json=benchmarks/results.json
"""

from __future__ import annotations

import pytest

from benchmarks import _data
from benchmarks import polars_impls as plimpl
from tidysdmx.mapping import apply_component_map, apply_multi_component_map
from tidysdmx.qa_utils import qa_coerce_numeric, qa_remove_duplicates
from tidysdmx.tidy_raw import filter_rows
from tidysdmx.tidysdmx import (
    vectorized_lookup_ordered_v1,
    vectorized_lookup_ordered_v2,
)

SIZES = _data.SIZES
ROWWISE_SIZES = _data.ROWWISE_SIZES


def _id(n: int) -> str:
    return f"{n // 1000}k" if n < 1_000_000 else "1M"


# --------------------------------------------------------------------------- #
# qa_coerce_numeric
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("n", SIZES, ids=_id)
def test_qa_coerce_numeric_pandas(benchmark, n):
    df = _data.make_sdmx_df(n)
    benchmark.group = f"qa_coerce_numeric/{_id(n)}"
    benchmark(qa_coerce_numeric, df, ["OBS_VALUE"])


@pytest.mark.parametrize("n", SIZES, ids=_id)
def test_qa_coerce_numeric_polars(benchmark, n):
    df = _data.to_polars(_data.make_sdmx_df(n))
    benchmark.group = f"qa_coerce_numeric/{_id(n)}"
    benchmark(plimpl.qa_coerce_numeric_pl, df, ["OBS_VALUE"])


# --------------------------------------------------------------------------- #
# qa_remove_duplicates
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("n", SIZES, ids=_id)
def test_qa_remove_duplicates_pandas(benchmark, n):
    df = _data.make_sdmx_df(n)
    benchmark.group = f"qa_remove_duplicates/{_id(n)}"
    benchmark(qa_remove_duplicates, df)


@pytest.mark.parametrize("n", SIZES, ids=_id)
def test_qa_remove_duplicates_polars(benchmark, n):
    df = _data.to_polars(_data.make_sdmx_df(n))
    benchmark.group = f"qa_remove_duplicates/{_id(n)}"
    benchmark(plimpl.qa_remove_duplicates_pl, df)


# --------------------------------------------------------------------------- #
# vectorized_lookup_ordered_v1 (regex-over-series)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("n", SIZES, ids=_id)
def test_vectorized_lookup_v1_pandas(benchmark, n):
    series = _data.make_sdmx_df(n)["AREA"]
    mapping = _data.make_lookup_mapping_df()[["SOURCE", "TARGET"]]
    benchmark.group = f"vectorized_lookup_v1/{_id(n)}"
    benchmark(vectorized_lookup_ordered_v1, series, mapping)


@pytest.mark.parametrize("n", SIZES, ids=_id)
def test_vectorized_lookup_v1_polars(benchmark, n):
    series = _data.to_polars(_data.make_sdmx_df(n))["AREA"]
    mapping = _data.make_lookup_mapping_df()
    benchmark.group = f"vectorized_lookup_v1/{_id(n)}"
    benchmark(plimpl.vectorized_lookup_ordered_v1_pl, series, mapping)


# --------------------------------------------------------------------------- #
# vectorized_lookup_ordered_v2 (regex or exact)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("n", SIZES, ids=_id)
def test_vectorized_lookup_v2_pandas(benchmark, n):
    series = _data.make_sdmx_df(n)["AREA"]
    mapping = _data.make_lookup_mapping_df()
    benchmark.group = f"vectorized_lookup_v2/{_id(n)}"
    benchmark(vectorized_lookup_ordered_v2, series, mapping)


@pytest.mark.parametrize("n", SIZES, ids=_id)
def test_vectorized_lookup_v2_polars(benchmark, n):
    series = _data.to_polars(_data.make_sdmx_df(n))["AREA"]
    mapping = _data.make_lookup_mapping_df()
    benchmark.group = f"vectorized_lookup_v2/{_id(n)}"
    benchmark(plimpl.vectorized_lookup_ordered_v2_pl, series, mapping)


# --------------------------------------------------------------------------- #
# apply_component_map (dict map / join)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("n", SIZES, ids=_id)
def test_apply_component_map_pandas(benchmark, n):
    df = _data.make_sdmx_df(n)
    cm = _data.make_component_map()
    benchmark.group = f"apply_component_map/{_id(n)}"
    benchmark(apply_component_map, df, cm)


@pytest.mark.parametrize("n", SIZES, ids=_id)
def test_apply_component_map_polars(benchmark, n):
    df = _data.to_polars(_data.make_sdmx_df(n))
    cm = _data.make_component_map()
    benchmark.group = f"apply_component_map/{_id(n)}"
    benchmark(plimpl.apply_component_map_pl, df, cm)


# --------------------------------------------------------------------------- #
# apply_multi_component_map (row-wise regex -> vectorised when/then)
# Capped at ROWWISE_SIZES: the pandas apply(axis=1) is too slow to benchmark at 1M.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("n", ROWWISE_SIZES, ids=_id)
def test_apply_multi_component_map_pandas(benchmark, n):
    df = _data.make_sdmx_df(n)
    mcm = _data.make_multi_component_map()
    benchmark.group = f"apply_multi_component_map/{_id(n)}"
    benchmark(apply_multi_component_map, df, mcm)


@pytest.mark.parametrize("n", ROWWISE_SIZES, ids=_id)
def test_apply_multi_component_map_polars(benchmark, n):
    df = _data.to_polars(_data.make_sdmx_df(n))
    mcm = _data.make_multi_component_map()
    benchmark.group = f"apply_multi_component_map/{_id(n)}"
    benchmark(plimpl.apply_multi_component_map_pl, df, mcm)


# --------------------------------------------------------------------------- #
# filter_rows (boolean-mask filtering)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("n", SIZES, ids=_id)
def test_filter_rows_pandas(benchmark, n):
    df = _data.make_sdmx_df(n)
    codelist_ids = _data.make_codelist_ids()
    benchmark.group = f"filter_rows/{_id(n)}"
    benchmark(filter_rows, df, codelist_ids)


@pytest.mark.parametrize("n", SIZES, ids=_id)
def test_filter_rows_polars(benchmark, n):
    df = _data.to_polars(_data.make_sdmx_df(n))
    codelist_ids = _data.make_codelist_ids()
    benchmark.group = f"filter_rows/{_id(n)}"
    benchmark(plimpl.filter_rows_pl, df, codelist_ids)
