# tests/fixtures/fxtr_mapping.py
import os
import pickle as pkl
from pathlib import Path

import pytest
from pysdmx.api import fmr
from pysdmx.model import StructureMap

# Directory for cached responses
CACHE_DIR = Path(__file__).parent / "cassettes"
CACHE_DIR.mkdir(exist_ok=True)


@pytest.fixture(scope="session")
def api_params_sm():
    """Fixture0 for API parameters."""
    return {
        "fmr_url": "https://fmrqa.worldbank.org/FMR/sdmx/v2",
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

    if cache_file.exists():
        with open(cache_file, "rb") as f:
            sm = pkl.load(f)
        assert isinstance(sm, StructureMap)
        return sm

    # Cassette missing. Under CI, fail loudly rather than silently skipping and
    # live-fetching from FMR (which would mask a missing or renamed cassette).
    if os.environ.get("CI"):
        raise RuntimeError(
            f"Cassette {cache_file.name} missing under CI; refusing to live-fetch "
            f"FMR. Regenerate it locally with `python -m tests.fixtures.fxtr_mapping` "
            f"and commit it."
        )

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
    base_url = "https://fmrqa.worldbank.org/FMR/sdmx/v2"
    client = fmr.RegistryClient(base_url)

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
