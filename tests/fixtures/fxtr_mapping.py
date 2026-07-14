# tests/fixtures/fxtr_mapping.py
import pickle as pkl
from pathlib import Path

import pytest
from fixtures.cassette_utils import FMR_TEST_URL, load_pickle_cassette
from pysdmx.api import fmr
from pysdmx.model import StructureMap

# Directory for cached responses
CACHE_DIR = Path(__file__).parent / "cassettes"
CACHE_DIR.mkdir(exist_ok=True)


@pytest.fixture(scope="session")
def api_params_sm():
    """Fixture0 for API parameters."""
    return {
        "fmr_url": FMR_TEST_URL,
        "raw_structure_agency": "WB",
        "raw_structure_map": "SM_IFPRI_ASTI_TO_DATA360",
    }


@pytest.fixture(scope="session")
def ifpri_asti_sm(api_params_sm):
    """Fixture that loads a StructureMap from a local cassette.

    Falls back to a live FMR call if the cassette is missing.
    Skips the test if neither the cassette nor the FMR is available.

    Generate the cassette with:
        python -m tests.fixtures.fxtr_mapping
    """
    cache_file = CACHE_DIR / "ifpri_asti_sm.pkl"

    cached = load_pickle_cassette(
        cache_file, StructureMap, "python -m tests.fixtures.fxtr_mapping"
    )
    if cached is not None:
        return cached

    # Local dev only — try live FMR, skip on failure.
    try:
        client = fmr.RegistryClient(api_params_sm["fmr_url"])
        sm = client.get_mapping(
            agency=api_params_sm["raw_structure_agency"],
            id=api_params_sm["raw_structure_map"],
        )
    except Exception as exc:
        pytest.skip(f"Cassette {cache_file.name} not found and FMR unavailable: {exc}")

    with open(cache_file, "wb") as f:
        pkl.dump(sm, f)

    return sm


if __name__ == "__main__":
    """Generate all mapping cassettes from the live FMR.

    Run from repo root:
        python -m tests.fixtures.fxtr_mapping
    """
    client = fmr.RegistryClient(FMR_TEST_URL)

    cassettes = {
        "ifpri_asti_sm.pkl": {
            "agency": "WB",
            "id": "SM_IFPRI_ASTI_TO_DATA360",
        },
    }

    for filename, params in cassettes.items():
        path = CACHE_DIR / filename
        print(f"Fetching {filename} ...")
        sm = client.get_mapping(**params)
        with open(path, "wb") as f:
            pkl.dump(sm, f)
        print(f"  Saved to {path}")
