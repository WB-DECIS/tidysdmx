"""Deterministic synthetic inputs for benchmarking ``map_structures``.

Every scenario returns a DataFrame and a StructureMap built from pysdmx
constructors only, so the same inputs run unchanged against every tidysdmx
release since 0.8.0, whose builders differ from today's. Source columns use raw
names and targets SDMX ids, so no target overwrites a source: releases up to
0.10.0 feed each map the previous map's output, later ones read every source
from the input, and distinct names make both produce the same columns.
"""

from collections.abc import Callable, Sequence

import numpy as np
import pandas as pd
from pysdmx.model.map import (
    ComponentMap,
    FixedValueMap,
    ImplicitComponentMap,
    MultiComponentMap,
    MultiRepresentationMap,
    MultiValueMap,
    RepresentationMap,
    StructureMap,
    ValueMap,
)

from benchmarks.records import SCENARIOS

CATCH_ALL: str = "regex:.*"
_AGENCY: str = "BENCH"
_HIGH_CARDINALITY_MAP_SIZE: int = 10_000
_WIDE_COLUMNS: int = 20
_WIDE_CODES: int = 50

Inputs = tuple[pd.DataFrame, StructureMap]
_Map = ComponentMap | FixedValueMap | ImplicitComponentMap | MultiComponentMap


def build(scenario: str, rows: int, seed: int = 0) -> Inputs:
    """Build the DataFrame and StructureMap for one benchmark case.

    Args:
        scenario: One of ``benchmarks.records.SCENARIOS``.
        rows: Number of rows in the DataFrame; at least 1.
        seed: Seed for the random draws, so a case is reproducible.

    Returns:
        The DataFrame to map and the StructureMap to map it with.

    Raises:
        ValueError: If ``scenario`` is unknown or ``rows`` is below 1.
    """
    builder = _BUILDERS.get(scenario)
    if builder is None:
        raise ValueError(
            f"Unknown scenario {scenario!r}; expected one of: {', '.join(SCENARIOS)}"
        )
    if rows < 1:
        raise ValueError(f"rows must be at least 1, got {rows}")
    return builder(rows, np.random.default_rng(seed))


def targets(structure_map: StructureMap) -> list[str]:
    """Return the target columns a structure map writes.

    ``benchmarks.case`` fingerprints these columns of the mapped output.

    Args:
        structure_map: A map from ``build``, or any made of the same map types.

    Returns:
        The distinct target column names, sorted.
    """
    names: set[str] = set()
    for component in structure_map.maps:
        if isinstance(component, MultiComponentMap):
            names.update(component.target)
        else:
            names.add(component.target)
    return sorted(names)


def _codes(prefix: str, count: int) -> list[str]:
    return [f"{prefix}{i:06d}" for i in range(count)]


def _draw(rng: np.random.Generator, codes: Sequence[str], rows: int) -> np.ndarray:
    # An object array of Python str, as a CSV reader produces: pandas then
    # applies its default string dtype (object under pandas 2, str under 3).
    return rng.choice(np.asarray(codes, dtype=object), size=rows)


def _code_map(codes: Sequence[str], target_prefix: str) -> list[ValueMap]:
    return [
        ValueMap(source=code, target=f"{target_prefix}{i:06d}")
        for i, code in enumerate(codes)
    ]


def _component_map(
    source: str, target: str, value_maps: Sequence[ValueMap]
) -> ComponentMap:
    return ComponentMap(
        source=source,
        target=target,
        values=RepresentationMap(
            id=f"RM_{target}",
            agency=_AGENCY,
            source="String",
            target="String",
            maps=list(value_maps),
        ),
    )


def _structure_map(maps: Sequence[_Map]) -> StructureMap:
    return StructureMap(
        id="SM_BENCH", name="Benchmark", agency=_AGENCY, maps=list(maps)
    )


def _typical(rows: int, rng: np.random.Generator) -> Inputs:
    countries = _codes("C", 200)
    series = _codes("S", 300)
    sexes = ["F", "M", "_T"]
    ages = _codes("A", 10)
    units = _codes("U", 5)
    years = [str(year) for year in range(1995, 2025)]
    df = pd.DataFrame(
        {
            "country": _draw(rng, countries, rows),
            "series": _draw(rng, series, rows),
            "sex": _draw(rng, sexes, rows),
            "age": _draw(rng, ages, rows),
            "unit": _draw(rng, units, rows),
            "year": _draw(rng, years, rows),
            "value": rng.random(rows),
        }
    )
    # 40 of the 50 age x unit combinations are mapped and the rest are left
    # unmapped, as in real data where a multi-map covers what occurs.
    combinations = [(age, unit) for age in ages for unit in units][:40]
    unit_measure = MultiComponentMap(
        source=["age", "unit"],
        target=["UNIT_MEASURE"],
        values=MultiRepresentationMap(
            id="MRM_UNIT_MEASURE",
            agency=_AGENCY,
            source=["String", "String"],
            target=["String"],
            maps=[
                MultiValueMap(source=[age, unit], target=[f"UM{i:02d}"])
                for i, (age, unit) in enumerate(combinations)
            ],
        ),
    )
    maps: list[_Map] = [
        _component_map("country", "REF_AREA", _code_map(countries, "RA")),
        _component_map("series", "INDICATOR", _code_map(series, "IN")),
        _component_map("sex", "SEX", _code_map(sexes, "SX")),
        ImplicitComponentMap(source="year", target="TIME_PERIOD"),
        ImplicitComponentMap(source="value", target="OBS_VALUE"),
        FixedValueMap(target="FREQ", value="A"),
        FixedValueMap(target="UNIT_MULT", value="0"),
        unit_measure,
    ]
    return df, _structure_map(maps)


def _high_cardinality(rows: int, rng: np.random.Generator) -> Inputs:
    entities = _codes("E", max(1, rows // 10))
    df = pd.DataFrame({"entity": _draw(rng, entities, rows), "value": rng.random(rows)})
    # A fixed-size literal map whatever the row count: at 1M rows it covers a
    # tenth of the 100,000 distinct entities and the catch-all takes the rest.
    value_maps = _code_map(_codes("E", _HIGH_CARDINALITY_MAP_SIZE), "EN")
    value_maps.append(ValueMap(source=CATCH_ALL, target="_O"))
    maps: list[_Map] = [
        _component_map("entity", "ENTITY", value_maps),
        ImplicitComponentMap(source="value", target="OBS_VALUE"),
    ]
    return df, _structure_map(maps)


def _regex_heavy(rows: int, rng: np.random.Generator) -> Inputs:
    known = _codes("K", 500)
    noise = _codes("N", 50)

    def column() -> np.ndarray:
        # 90% known codes, 10% codes that only the catch-all matches.
        return np.where(
            rng.random(rows) < 0.9, _draw(rng, known, rows), _draw(rng, noise, rows)
        )

    df = pd.DataFrame(
        {"code_a": column(), "code_b": column(), "value": rng.random(rows)}
    )
    # Half the known codes are literals. Ten rules split the other half by
    # final digit; ten more never match, as dead rules accumulate in real
    # templates; the catch-all takes the noise codes after trying all twenty.
    value_maps = _code_map(known[:250], "KL")
    value_maps += [
        ValueMap(source=f"regex:K\\d{{5}}{digit}", target=f"KR{digit}")
        for digit in range(10)
    ]
    value_maps += [
        ValueMap(source=f"regex:Q{rule:02d}\\d+", target=f"Q{rule:02d}")
        for rule in range(10)
    ]
    value_maps.append(ValueMap(source=CATCH_ALL, target="_O"))
    maps: list[_Map] = [
        _component_map("code_a", "CODE_A", value_maps),
        _component_map("code_b", "CODE_B", value_maps),
        ImplicitComponentMap(source="value", target="OBS_VALUE"),
    ]
    return df, _structure_map(maps)


def _wide(rows: int, rng: np.random.Generator) -> Inputs:
    data: dict[str, np.ndarray] = {}
    maps: list[_Map] = []
    for column in range(_WIDE_COLUMNS):
        codes = _codes(f"D{column:02d}_", _WIDE_CODES)
        source = f"dim_{column:02d}"
        data[source] = _draw(rng, codes, rows)
        maps.append(
            _component_map(
                source, f"DIM_{column:02d}", _code_map(codes, f"W{column:02d}_")
            )
        )
    data["value"] = rng.random(rows)
    maps.append(ImplicitComponentMap(source="value", target="OBS_VALUE"))
    return pd.DataFrame(data), _structure_map(maps)


_BUILDERS: dict[str, Callable[[int, np.random.Generator], Inputs]] = {
    "typical": _typical,
    "high_cardinality": _high_cardinality,
    "regex_heavy": _regex_heavy,
    "wide": _wide,
}
