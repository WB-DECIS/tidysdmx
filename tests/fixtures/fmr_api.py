# tests/fixtures/fmr_api.py
import pytest
import pickle as pkl
from pathlib import Path
import pysdmx as px
from pysdmx.model.dataflow import Schema

# Directory for cached responses
CACHE_DIR = Path(__file__).parent / "cassettes"
CACHE_DIR.mkdir(exist_ok=True)

@pytest.fixture(scope="session")
def api_params():
    """Fixture for API parameters."""
    return {
        "fmr_url": "https://fmrqa.worldbank.org/FMR/sdmx/v2",
        "raw_structure_agency": "WB",
        "raw_structure_id": "IFPRI_ASTI",
        "raw_structure_version": "1.0"
    }

@pytest.fixture(scope="session")
def dsd_schema(api_params):
    """
    Fixture that records the FMR response for a DSD schema on first run and reuses it later.
    """
    cache_file = CACHE_DIR / "dsd_schema.pkl"

    if cache_file.exists():
        # Load cached response
        with open(cache_file, "rb") as f:
            schema = pkl.load(f)
        assert isinstance(schema, Schema)

    else:
        # Make real API call using pysdmx
        client = px.api.fmr.RegistryClient(api_params["fmr_url"])
        schema = client.get_schema(
            "datastructure",
            agency=api_params["raw_structure_agency"],
            id=api_params["raw_structure_id"],
            version=api_params["raw_structure_version"]
        )

        # Cache the response
        with open(cache_file, "wb") as f:
            pkl.dump(schema, f)

    return schema