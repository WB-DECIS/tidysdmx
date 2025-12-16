# tests/fixtures/fmr_api.py
import pytest
import pickle as pkl
from pathlib import Path
import pysdmx as px
from datetime import datetime
from pysdmx.model import (
    ArrayBoundaries,
    Component,
    Components,
    Concept,
    DataType,
    Facets,
    Role,
    Schema,
    decoders,
    encoders,
)
from pysdmx.model import Code, Codelist

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
        "raw_structure_version": "1.0"
    }

@pytest.fixture(scope="session")
def ifpri_asti_schema(api_params_schema):
    """Fixture that records the FMR response for a DSD schema on first run and reuses it later."""
    cache_file = CACHE_DIR / "ifpri_asti_schema.pkl"

    if cache_file.exists():
        # Load cached response
        with open(cache_file, "rb") as f:
            schema = pkl.load(f)
        assert isinstance(schema, Schema)

    else:
        # Make real API call using pysdmx
        client = px.api.fmr.RegistryClient(api_params_schema["fmr_url"])
        schema = client.get_schema(
            "datastructure",
            agency=api_params_schema["raw_structure_agency"],
            id=api_params_schema["raw_structure_id"],
            version=api_params_schema["raw_structure_version"]
        )

        # Cache the response
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

    cl_indicator = Codelist(id="cl_id", name="Valid indicators", agency=agency, items = codes_ind)
    
    # Define components
    f1 = Component(
        "INDICATOR",
        True,
        Role.DIMENSION,
        Concept("INDICATOR", dtype=DataType.STRING),
        DataType.STRING,
        Facets(min_length=2, max_length=4),
        local_codes = cl_indicator   
    )
    f2 = Component(
        "TIME_PERIOD", True, Role.DIMENSION, Concept("TIME_PERIOD"), DataType.PERIOD
    )
    f3 = Component(
        "SEX", True, Role.DIMENSION, Concept("SEX"), DataType.STRING
    )
    f4 = Component(
        "OBS_VALUE", False, Role.MEASURE, Concept("OBS_VALUE"), DataType.INTEGER, Facets(min_value=0, start_value=100)
    )

    components = Components([f1, f2, f3, f4])

    # Define schema
    schema = Schema(context = "dataflow", agency = agency, id = "tx1", components = components)
    
    return schema