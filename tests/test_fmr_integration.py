"""Live-FMR integration tests (opt-in).

These require network access to the FMR registry and are deselected from the
default CI lane. Run them on demand with::

    poetry run pytest -m integration

Point them at a different registry via ``TIDYSDMX_TEST_FMR_URL``. Each live test
compares the live response against the committed cassette, so it also detects
FMR-side drift (an artefact renamed, a code retired) and pysdmx parsing drift.
"""

import pytest
from fixtures.cassette_utils import FMR_TEST_URL
from pysdmx.api import fmr

from tidysdmx import fetch_schema

pytestmark = pytest.mark.integration


def test_fetch_schema_live_matches_cassette(ifpri_asti_schema):
    """fetch_schema against the live FMR yields the same components as the cassette."""
    live = fetch_schema(
        base_url=FMR_TEST_URL,
        artefact_id="WB:IFPRI_ASTI(1.0)",
        context="datastructure",
    )
    live_ids = sorted(c.id for c in live.components)
    cassette_ids = sorted(c.id for c in ifpri_asti_schema.components)
    assert live_ids == cassette_ids


def test_get_mapping_live_matches_cassette(ifpri_asti_sm):
    """A live get_mapping returns a StructureMap matching the committed cassette."""
    client = fmr.RegistryClient(FMR_TEST_URL)
    live = client.get_mapping(agency="WB", id="SM_IFPRI_ASTI_TO_DATA360")
    assert len(live.maps) == len(ifpri_asti_sm.maps)
    assert {type(m).__name__ for m in live.maps} == {
        type(m).__name__ for m in ifpri_asti_sm.maps
    }
