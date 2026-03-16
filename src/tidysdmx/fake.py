"""Generate fake SDMX datasets from pysdmx Schema definitions."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd
from pysdmx.model.concept import DataType
from pysdmx.model.dataflow import Schema
from typeguard import typechecked

if TYPE_CHECKING:
    from pysdmx.model.dataflow import Component


def _ensure_pointblank() -> Any:
    """Lazily import pointblank, raising a helpful error if missing.

    Returns:
        The pointblank module.

    Raises:
        ImportError: If pointblank is not installed.
    """
    try:
        import pointblank as pb
    except ImportError as e:
        raise ImportError(
            "pointblank is required for fake dataset generation. "
            "Install it with: pip install pointblank  "
            "or: poetry install -E fake"
        ) from e
    return pb


_INTEGER_TYPES: frozenset[DataType] = frozenset(
    {
        DataType.INTEGER,
        DataType.LONG,
        DataType.SHORT,
        DataType.COUNT,
        DataType.BIG_INTEGER,
    }
)

_FLOAT_TYPES: frozenset[DataType] = frozenset(
    {
        DataType.FLOAT,
        DataType.DOUBLE,
        DataType.DECIMAL,
    }
)

_PERIOD_TYPES: frozenset[DataType] = frozenset(
    {
        DataType.PERIOD,
        DataType.BASIC_TIME_PERIOD,
        DataType.STD_TIME_PERIOD,
        DataType.GREGORIAN_TIME_PERIOD,
        DataType.YEAR,
        DataType.YEAR_MONTH,
        DataType.REP_YEAR,
        DataType.REP_MONTH,
        DataType.REP_QUARTER,
        DataType.REP_SEMESTER,
        DataType.REP_TRIMESTER,
        DataType.REP_WEEK,
        DataType.REP_DAY,
    }
)

_STRING_TYPES: frozenset[DataType] = frozenset(
    {
        DataType.STRING,
        DataType.ALPHA,
        DataType.ALPHA_NUM,
        DataType.NUMERIC,
    }
)

_FREQ_PERIOD_FORMATS: dict[str, str] = {
    "A": "annual",
    "S": "semester",
    "T": "trimester",
    "Q": "quarter",
    "M": "month",
    "W": "week",
    "D": "day",
    "B": "day",
}


def _generate_period_values(
    freq_codes: list[str],
    start_year: int = 2020,
    end_year: int = 2024,
) -> list[str]:
    """Generate a list of valid SDMX period strings for the given frequencies.

    Args:
        freq_codes: Frequency codes (e.g. ``["A"]``, ``["M"]``).
        start_year: First year to include.
        end_year: Last year to include.

    Returns:
        List of SDMX period strings (e.g. ``["2020", "2021"]`` for annual,
        ``["2020-Q1", "2020-Q2", ...]`` for quarterly).
    """
    periods: list[str] = []
    # Use the first known frequency to determine format
    freq = "A"
    for code in freq_codes:
        if code in _FREQ_PERIOD_FORMATS:
            freq = code
            break

    for year in range(start_year, end_year + 1):
        if freq == "A":
            periods.append(str(year))
        elif freq == "S":
            for s in range(1, 3):
                periods.append(f"{year}-S{s}")
        elif freq == "T":
            for t in range(1, 4):
                periods.append(f"{year}-T{t}")
        elif freq == "Q":
            for q in range(1, 5):
                periods.append(f"{year}-Q{q}")
        elif freq == "M":
            for m in range(1, 13):
                periods.append(f"{year}-M{m:02d}")
        elif freq == "W":
            for w in range(1, 53):
                periods.append(f"{year}-W{w:02d}")
        elif freq in ("D", "B"):
            for d in range(1, 366):
                periods.append(f"{year}-D{d:03d}")
        else:
            periods.append(str(year))

    return periods


def _get_freq_codes(schema: Schema) -> list[str]:
    """Extract frequency codes from the FREQ dimension in a schema.

    Args:
        schema: A pysdmx Schema.

    Returns:
        List of frequency code strings, or ``["A"]`` if no FREQ dimension
        or codelist is found.
    """
    for comp in schema.components:
        if comp.id == "FREQ" and comp.local_codes is not None:
            return [code.id for code in comp.local_codes.items]
    return ["A"]


def _component_to_field(
    component: Component,
    freq_codes: list[str],
) -> Any:
    """Map a pysdmx Component to a pointblank Field.

    Args:
        component: A pysdmx Component (dimension, measure, or attribute).
        freq_codes: Frequency codes from the FREQ dimension, used to
            generate valid time period strings.

    Returns:
        A pointblank Field object appropriate for the component's type
        and constraints.
    """
    pb = _ensure_pointblank()

    nullable = not component.required
    null_prob = 0.3 if nullable else 0.0

    # Coded components: sample from codelist values
    codelist = component.enumeration
    if codelist is not None:
        allowed = [code.id for code in codelist.items]
        return pb.string_field(
            allowed=allowed,
            nullable=nullable,
            null_probability=null_prob,
        )

    # Uncoded components: map by data type
    dtype = component.dtype
    facets = component.facets

    if dtype is None:
        return pb.string_field(
            nullable=nullable,
            null_probability=null_prob,
        )

    if dtype in _INTEGER_TYPES:
        kwargs: dict[str, Any] = {
            "nullable": nullable,
            "null_probability": null_prob,
        }
        if facets is not None:
            if facets.min_value is not None:
                kwargs["min_val"] = int(facets.min_value)
            if facets.max_value is not None:
                kwargs["max_val"] = int(facets.max_value)
        return pb.int_field(**kwargs)

    if dtype in _FLOAT_TYPES:
        kwargs = {
            "nullable": nullable,
            "null_probability": null_prob,
        }
        if facets is not None:
            if facets.min_value is not None:
                kwargs["min_val"] = float(facets.min_value)
            if facets.max_value is not None:
                kwargs["max_val"] = float(facets.max_value)
        return pb.float_field(**kwargs)

    if dtype == DataType.BOOLEAN:
        return pb.bool_field(
            nullable=nullable,
            null_probability=null_prob,
        )

    if dtype in _PERIOD_TYPES:
        periods = _generate_period_values(freq_codes)
        return pb.string_field(
            allowed=periods,
            nullable=nullable,
            null_probability=null_prob,
        )

    if dtype == DataType.DATE:
        return pb.date_field(
            nullable=nullable,
            null_probability=null_prob,
        )

    if dtype == DataType.DATE_TIME:
        return pb.datetime_field(
            nullable=nullable,
            null_probability=null_prob,
        )

    # Default: string field with facets
    kwargs = {
        "nullable": nullable,
        "null_probability": null_prob,
    }
    if facets is not None:
        if facets.min_length is not None:
            kwargs["min_length"] = facets.min_length
        if facets.max_length is not None:
            kwargs["max_length"] = facets.max_length
        if facets.pattern is not None:
            kwargs["pattern"] = facets.pattern
    return pb.string_field(**kwargs)


@typechecked
def generate_fake_dataset(
    schema: Schema,
    n: int = 100,
    seed: int | None = None,
) -> pd.DataFrame:
    """Generate a fake pandas DataFrame that conforms to a pysdmx Schema.

    Translates each component in the schema (dimensions, measures, attributes)
    into a pointblank ``Field``, builds a pointblank ``Schema``, and uses
    ``pointblank.generate_dataset()`` to produce synthetic data.

    Coded components sample from their codelist values. Uncoded components
    are generated based on their data type and facet constraints. Time period
    columns infer the period format from the FREQ dimension's codelist.

    Args:
        schema: A pysdmx Schema containing component definitions
            (dimensions, measures, attributes) with codelists and data types.
        n: Number of rows to generate.
        seed: Random seed for reproducibility.

    Returns:
        A pandas DataFrame with columns matching the schema's components
        and values conforming to codelists and data type constraints.

    Raises:
        ImportError: If pointblank is not installed.
    """
    pb = _ensure_pointblank()

    freq_codes = _get_freq_codes(schema)

    fields: dict[str, Any] = {}
    for component in schema.components:
        fields[component.id] = _component_to_field(component, freq_codes)

    pb_schema = pb.Schema(**fields)
    return pb.generate_dataset(pb_schema, n=n, seed=seed, output="pandas")
