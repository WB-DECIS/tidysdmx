# tests/fixtures/fmr_api.py
import pytest
import pickle as pkl
from pathlib import Path
import pysdmx as px
from datetime import datetime
from pysdmx.model import (
    StructureMap
)
from pysdmx.model import Code, Codelist

# Directory for cached responses
CACHE_DIR = Path(__file__).parent / "cassettes"
CACHE_DIR.mkdir(exist_ok=True)

@pytest.fixture(scope="session")
def api_params_sm():
    """Fixture0 for API parameters."""
    return {
        "fmr_url": "https://fmrqa.worldbank.org/FMR/sdmx/v2",
        "raw_structure_agency": "WB",
        "raw_structure_map": "SM_IFPRI_ASTI_TO_DATA360"   
    }

@pytest.fixture(scope="session")
def ifpri_asti_sm(api_params_sm):
    """Fixture that records the FMR response for a DSD schema on first run and reuses it later."""
    cache_file = CACHE_DIR / "ifpri_asti_sm.pkl"

    if cache_file.exists():
        # Load cached response
        with open(cache_file, "rb") as f:
            sm = pkl.load(f)
        assert isinstance(sm, StructureMap)

    else:
        # Make real API call using pysdmx
        client = px.api.fmr.RegistryClient(api_params_sm["fmr_url"])
        sm = client.get_mapping(agency=api_params_sm["raw_structure_agency"], 
                                id=api_params_sm["raw_structure_map"])

        # Cache the response
        with open(cache_file, "wb") as f:
            pkl.dump(sm, f)

    return sm
