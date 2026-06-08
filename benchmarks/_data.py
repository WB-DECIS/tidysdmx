"""Synthetic data generators for the pandas-vs-polars benchmark spike.

Builds SDMX-shaped data at parametrized row counts and the small mapping
artefacts (regex mapping tables, pysdmx ``ComponentMap`` / ``MultiComponentMap``,
codelist-id dicts) consumed by the data-path functions under test.

Everything is deterministic via a fixed seed so the pandas implementation and
its polars port run on byte-identical inputs. Value pools are derived from
``tests/fixtures/data/ifpri_asti_sample.csv`` so the shape is realistic.

This module is a throwaway assessment artefact; it does not touch ``src/``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import polars as pl
from pysdmx.model.map import (
    ComponentMap,
    MultiComponentMap,
    MultiRepresentationMap,
    MultiValueMap,
    RepresentationMap,
    ValueMap,
)

SEED = 1234

# Sizes swept by the benchmark. The row-wise ``apply(axis=1)`` target is too
# slow in pandas at 1M rows to benchmark in reasonable wall-clock time, so it
# uses ROWWISE_SIZES instead (see bench_datapath.py).
SIZES = [1_000, 10_000, 100_000, 1_000_000]
ROWWISE_SIZES = [1_000, 10_000, 100_000]

AREAS = ["SWZ", "GAB", "KEN", "GHA", "NGA", "ZAF", "UGA", "TZA", "ETH", "MWI"]
INDICATORS = [
    "EXP_CAP_TOT_SHRE",
    "EXP_CAP_TOT",
    "RES_FTE_TOT",
    "SPL_TR_AMT_RD",
    "AG_GDP_SHRE",
]
NOTES = ["", "estimate", "provisional", "Shares based on partial data."]


def make_sdmx_df(n_rows: int, seed: int = SEED) -> pd.DataFrame:
    """Build an SDMX-shaped pandas DataFrame of ``n_rows`` rows.

    Columns mirror the IFPRI ASTI fixture (INDICATOR, NOTE, AREA, TIME_PERIOD,
    OBS_VALUE). ~5% of OBS_VALUE cells are non-numeric noise so numeric
    coercion has work to do, and the small value pools guarantee duplicate rows
    at larger sizes so deduplication is exercised.
    """
    rng = np.random.default_rng(seed)
    obs = rng.normal(100, 25, size=n_rows).round(2).astype(object)
    noise_idx = rng.choice(n_rows, size=max(1, n_rows // 20), replace=False)
    obs[noise_idx] = "n/a"
    return pd.DataFrame(
        {
            "INDICATOR": rng.choice(INDICATORS, size=n_rows),
            "NOTE": rng.choice(NOTES, size=n_rows),
            "AREA": rng.choice(AREAS, size=n_rows),
            "TIME_PERIOD": rng.integers(2000, 2023, size=n_rows).astype(str),
            "OBS_VALUE": obs.astype(str),
        }
    )


def to_polars(df: pd.DataFrame) -> pl.DataFrame:
    """Convert a pandas DataFrame to polars (done outside timed regions)."""
    return pl.from_pandas(df)


def make_lookup_mapping_df() -> pd.DataFrame:
    """Mapping table for ``vectorized_lookup_ordered_v1/v2`` (SOURCE/TARGET/IS_REGEX).

    SOURCE values are regex patterns (v1 treats every SOURCE as a regex; v2
    branches on IS_REGEX between regex and exact matching).
    """
    return pd.DataFrame(
        {
            "SOURCE": ["^SW", "GAB", "KEN", "A$", "GHA", "ZAF"],
            "TARGET": ["SW_GRP", "GAB_X", "KEN_X", "ENDS_A", "GHA_X", "ZAF_X"],
            "IS_REGEX": [True, False, False, True, False, False],
        }
    )


def make_component_map() -> ComponentMap:
    """A ComponentMap that maps AREA codes to ``<code>_M`` in a new column."""
    maps = [ValueMap(source=a, target=f"{a}_M") for a in AREAS]
    rep_map = RepresentationMap(
        id="BENCH_RM", agency="TEST", source="AREA", target="AREA_T", maps=maps
    )
    return ComponentMap(source="AREA", target="AREA_T", values=rep_map)


def make_multi_component_map() -> MultiComponentMap:
    """A MultiComponentMap over (INDICATOR, AREA) with ordered regex/exact rules."""
    maps = [
        MultiValueMap(source=["regex:EXP.*", "regex:.*"], target=["EXP_GROUP"]),
        MultiValueMap(source=["RES_FTE_TOT", "KEN"], target=["RES_KEN"]),
        MultiValueMap(source=["regex:.*", "regex:.*"], target=["OTHER"]),
    ]
    mrm = MultiRepresentationMap(
        id="BENCH_MRM",
        agency="TEST",
        source=["INDICATOR", "AREA"],
        target=["GROUP"],
        maps=maps,
    )
    return MultiComponentMap(source=["INDICATOR", "AREA"], target=["GROUP"], values=mrm)


def make_codelist_ids() -> dict[str, list[str]]:
    """Codelist allow-lists for ``filter_rows`` (drops out-of-list AREA/INDICATOR)."""
    return {
        "AREA": AREAS[:7],
        "INDICATOR": INDICATORS[:3],
    }
