# tests/fixtures/fxtr_schemas.py

import pickle as pkl
from pathlib import Path

import pytest
from pysdmx.api import fmr
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

# Directory for cached responses
CACHE_DIR = Path(__file__).parent / "cassettes"
CACHE_DIR.mkdir(exist_ok=True)


@pytest.fixture(scope="session")
def api_params_schema():
    """Fixture for API parameters."""
    return {
        "fmr_url": "https://fmrqa.worldbank.org/FMR/sdmx/v2",
        "raw_structure_agency": "WB",
        "raw_structure_id": "IFPRI_ASTI",
        "raw_structure_version": "1.0",
    }


@pytest.fixture(scope="session")
def ifpri_asti_schema(api_params_schema):
    """Fixture that loads an FMR DSD schema from a local cassette.

    Falls back to a live FMR call if the cassette is missing.
    Skips the test if neither the cassette nor the FMR is available.

    Generate the cassette with:
        python -m tests.fixtures.fxtr_schemas
    """
    cache_file = CACHE_DIR / "ifpri_asti_schema.pkl"

    if cache_file.exists():
        with open(cache_file, "rb") as f:
            schema = pkl.load(f)
        assert isinstance(schema, Schema)
        return schema

    # Cassette missing — try live FMR, skip on failure
    try:
        client = fmr.RegistryClient(api_params_schema["fmr_url"])
        schema = client.get_schema(
            "datastructure",
            agency=api_params_schema["raw_structure_agency"],
            id=api_params_schema["raw_structure_id"],
            version=api_params_schema["raw_structure_version"],
        )
    except Exception as exc:
        pytest.skip(
            f"Cassette {cache_file.name} not found and FMR unavailable: {exc}"
        )

    with open(cache_file, "wb") as f:
        pkl.dump(schema, f)

    return schema


@pytest.fixture
def sdmx_schema():
    agency = "tidysdmx"
    # Define codes and codelist
    c1 = Code(id="IND1", name="Indicator 1")
    c2 = Code(id="IND3", name="Indicator 3")
    codes_ind = [c1, c2]

    cl_indicator = Codelist(
        id="cl_id", name="Valid indicators", agency=agency, items=codes_ind
    )

    # Define components
    f1 = Component(
        "INDICATOR",
        True,
        Role.DIMENSION,
        Concept("INDICATOR", dtype=DataType.STRING),
        DataType.STRING,
        Facets(min_length=2, max_length=4),
        local_codes=cl_indicator,
    )
    f2 = Component(
        "TIME_PERIOD", True, Role.DIMENSION, Concept("TIME_PERIOD"), DataType.PERIOD
    )
    f3 = Component("SEX", True, Role.DIMENSION, Concept("SEX"), DataType.STRING)
    f4 = Component(
        "OBS_VALUE",
        False,
        Role.MEASURE,
        Concept("OBS_VALUE"),
        DataType.INTEGER,
        Facets(min_value=0, start_value=100),
    )

    components = Components([f1, f2, f3, f4])

    # Define schema
    schema = Schema(context="dataflow", agency=agency, id="tx1", components=components)

    return schema


if __name__ == "__main__":
    """Generate all schema cassettes from the live FMR.

    Run from repo root:
        python -m tests.fixtures.fxtr_schemas
    """
    base_url = "https://fmrqa.worldbank.org/FMR/sdmx/v2"
    client = fmr.RegistryClient(base_url)

    cassettes = {
        "ifpri_asti_schema.pkl": {
            "context": "datastructure",
            "agency": "WB",
            "id": "IFPRI_ASTI",
            "version": "1.0",
        },
        "pipeline_dis_schema.pkl": {
            "context": "datastructure",
            "agency": "WB.GGH.HSP",
            "id": "DS_ASPIRE",
            "version": "1.0.0",
        },
    }

    for filename, params in cassettes.items():
        path = CACHE_DIR / filename
        print(f"Fetching {filename} ...")
        schema = client.get_schema(**params)
        with open(path, "wb") as f:
            pkl.dump(schema, f)
        print(f"  Saved to {path}")
