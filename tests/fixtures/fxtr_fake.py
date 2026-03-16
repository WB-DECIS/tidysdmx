"""Fixtures for fake dataset generation tests."""

import pytest
from pysdmx.model import (
    Code,
    Codelist,
    Component,
    Components,
    Concept,
    DataType,
    Facets,
    Role,
    Schema,
)

AGENCY = "TEST"


@pytest.fixture
def fake_schema_basic():
    """Schema with coded dimensions, PERIOD time, DOUBLE measure, optional attr."""
    freq_cl = Codelist(
        id="CL_FREQ",
        agency=AGENCY,
        items=[Code(id="A"), Code(id="M"), Code(id="Q")],
    )
    area_cl = Codelist(
        id="CL_AREA",
        agency=AGENCY,
        items=[Code(id="USA"), Code(id="FRA"), Code(id="GBR")],
    )

    components = Components(
        [
            Component(
                "FREQ",
                True,
                Role.DIMENSION,
                Concept("FREQ"),
                DataType.STRING,
                local_codes=freq_cl,
            ),
            Component(
                "REF_AREA",
                True,
                Role.DIMENSION,
                Concept("REF_AREA"),
                DataType.STRING,
                local_codes=area_cl,
            ),
            Component(
                "TIME_PERIOD",
                True,
                Role.DIMENSION,
                Concept("TIME_PERIOD"),
                DataType.PERIOD,
            ),
            Component(
                "OBS_VALUE",
                True,
                Role.MEASURE,
                Concept("OBS_VALUE"),
                DataType.DOUBLE,
            ),
            Component(
                "COMMENT",
                False,
                Role.ATTRIBUTE,
                Concept("COMMENT"),
                DataType.STRING,
                attachment_level="O",
            ),
        ]
    )

    return Schema(
        context="dataflow",
        agency=AGENCY,
        id="FAKE_BASIC",
        components=components,
    )


@pytest.fixture
def fake_schema_annual_only():
    """Schema with FREQ codelist containing only 'A'."""
    freq_cl = Codelist(
        id="CL_FREQ",
        agency=AGENCY,
        items=[Code(id="A")],
    )

    components = Components(
        [
            Component(
                "FREQ",
                True,
                Role.DIMENSION,
                Concept("FREQ"),
                DataType.STRING,
                local_codes=freq_cl,
            ),
            Component(
                "TIME_PERIOD",
                True,
                Role.DIMENSION,
                Concept("TIME_PERIOD"),
                DataType.PERIOD,
            ),
            Component(
                "OBS_VALUE",
                True,
                Role.MEASURE,
                Concept("OBS_VALUE"),
                DataType.DOUBLE,
            ),
        ]
    )

    return Schema(
        context="dataflow",
        agency=AGENCY,
        id="FAKE_ANNUAL",
        components=components,
    )


@pytest.fixture
def fake_schema_monthly():
    """Schema with FREQ codelist containing only 'M'."""
    freq_cl = Codelist(
        id="CL_FREQ",
        agency=AGENCY,
        items=[Code(id="M")],
    )

    components = Components(
        [
            Component(
                "FREQ",
                True,
                Role.DIMENSION,
                Concept("FREQ"),
                DataType.STRING,
                local_codes=freq_cl,
            ),
            Component(
                "TIME_PERIOD",
                True,
                Role.DIMENSION,
                Concept("TIME_PERIOD"),
                DataType.PERIOD,
            ),
            Component(
                "OBS_VALUE",
                True,
                Role.MEASURE,
                Concept("OBS_VALUE"),
                DataType.DOUBLE,
            ),
        ]
    )

    return Schema(
        context="dataflow",
        agency=AGENCY,
        id="FAKE_MONTHLY",
        components=components,
    )


@pytest.fixture
def fake_schema_quarterly():
    """Schema with FREQ codelist containing only 'Q'."""
    freq_cl = Codelist(
        id="CL_FREQ",
        agency=AGENCY,
        items=[Code(id="Q")],
    )

    components = Components(
        [
            Component(
                "FREQ",
                True,
                Role.DIMENSION,
                Concept("FREQ"),
                DataType.STRING,
                local_codes=freq_cl,
            ),
            Component(
                "TIME_PERIOD",
                True,
                Role.DIMENSION,
                Concept("TIME_PERIOD"),
                DataType.PERIOD,
            ),
            Component(
                "OBS_VALUE",
                True,
                Role.MEASURE,
                Concept("OBS_VALUE"),
                DataType.DOUBLE,
            ),
        ]
    )

    return Schema(
        context="dataflow",
        agency=AGENCY,
        id="FAKE_QUARTERLY",
        components=components,
    )


@pytest.fixture
def fake_schema_integer_measure():
    """Schema with an INTEGER measure with facets."""
    freq_cl = Codelist(
        id="CL_FREQ",
        agency=AGENCY,
        items=[Code(id="A")],
    )

    components = Components(
        [
            Component(
                "FREQ",
                True,
                Role.DIMENSION,
                Concept("FREQ"),
                DataType.STRING,
                local_codes=freq_cl,
            ),
            Component(
                "OBS_VALUE",
                True,
                Role.MEASURE,
                Concept("OBS_VALUE"),
                DataType.INTEGER,
                Facets(min_value=0, max_value=1000),
            ),
        ]
    )

    return Schema(
        context="dataflow",
        agency=AGENCY,
        id="FAKE_INT",
        components=components,
    )


@pytest.fixture
def fake_schema_boolean():
    """Schema with a BOOLEAN attribute."""
    freq_cl = Codelist(
        id="CL_FREQ",
        agency=AGENCY,
        items=[Code(id="A")],
    )

    components = Components(
        [
            Component(
                "FREQ",
                True,
                Role.DIMENSION,
                Concept("FREQ"),
                DataType.STRING,
                local_codes=freq_cl,
            ),
            Component(
                "OBS_VALUE",
                True,
                Role.MEASURE,
                Concept("OBS_VALUE"),
                DataType.DOUBLE,
            ),
            Component(
                "IS_ESTIMATED",
                False,
                Role.ATTRIBUTE,
                Concept("IS_ESTIMATED"),
                DataType.BOOLEAN,
                attachment_level="O",
            ),
        ]
    )

    return Schema(
        context="dataflow",
        agency=AGENCY,
        id="FAKE_BOOL",
        components=components,
    )
