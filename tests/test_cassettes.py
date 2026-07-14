"""Hermetic tests for the cassette load/guard mechanism and committed cassettes.

These run in the default lane (no network): they exercise the shared
``load_pickle_cassette`` helper and confirm the committed cassettes still
unpickle to the expected pysdmx types on the installed pysdmx version, so a
version bump that breaks the pickles fails loudly here rather than silently
deserialising a misaligned object.
"""

import pickle
from pathlib import Path

import pytest
from fixtures.cassette_utils import FMR_TEST_URL, load_pickle_cassette
from pysdmx.model import Schema
from pysdmx.model.map import StructureMap

CASSETTE_DIR = Path(__file__).parent / "fixtures" / "cassettes"

# Each committed cassette and the pysdmx type it must unpickle to.
COMMITTED_CASSETTES = [
    ("ifpri_asti_schema.pkl", Schema),
    ("pipeline_dis_schema.pkl", Schema),
    ("ifpri_asti_sm.pkl", StructureMap),
]


@pytest.mark.parametrize("filename, expected_type", COMMITTED_CASSETTES)
def test_committed_cassette_loads_with_expected_type(filename, expected_type):
    """Every committed cassette unpickles to its expected pysdmx type."""
    path = CASSETTE_DIR / filename
    assert path.exists(), f"Missing committed cassette: {filename}"
    obj = load_pickle_cassette(path, expected_type, "regen")
    assert isinstance(obj, expected_type)


def test_committed_schema_cassettes_have_components():
    """Schema cassettes carry components (guards against a hollow unpickle)."""
    schema = load_pickle_cassette(
        CASSETTE_DIR / "ifpri_asti_schema.pkl", Schema, "regen"
    )
    assert len(list(schema.components)) > 0


def test_committed_structure_map_cassette_has_maps():
    """The StructureMap cassette carries mapping rules."""
    sm = load_pickle_cassette(CASSETTE_DIR / "ifpri_asti_sm.pkl", StructureMap, "regen")
    assert len(sm.maps) > 0


def test_load_pickle_cassette_missing_raises_under_ci(monkeypatch, tmp_path):
    """A missing cassette under CI raises rather than silently live-fetching."""
    monkeypatch.setenv("CI", "1")
    with pytest.raises(RuntimeError, match="missing under CI"):
        load_pickle_cassette(tmp_path / "nope.pkl", Schema, "regen cmd")


def test_load_pickle_cassette_missing_returns_none_without_ci(monkeypatch, tmp_path):
    """A missing cassette outside CI returns None so the caller can fall back."""
    monkeypatch.delenv("CI", raising=False)
    assert load_pickle_cassette(tmp_path / "nope.pkl", Schema, "regen") is None


def test_load_pickle_cassette_wrong_type_raises(tmp_path):
    """A cassette that unpickles to the wrong type raises TypeError."""
    bad = tmp_path / "bad.pkl"
    bad.write_bytes(pickle.dumps({"not": "a schema"}))
    with pytest.raises(TypeError, match="expected Schema"):
        load_pickle_cassette(bad, Schema, "regen")


def test_fmr_test_url_overridable(monkeypatch):
    """The FMR endpoint is read from TIDYSDMX_TEST_FMR_URL when set."""
    import importlib

    import fixtures.cassette_utils as cu

    monkeypatch.setenv("TIDYSDMX_TEST_FMR_URL", "https://example.org/fmr")
    reloaded = importlib.reload(cu)
    try:
        assert reloaded.FMR_TEST_URL == "https://example.org/fmr"
    finally:
        monkeypatch.delenv("TIDYSDMX_TEST_FMR_URL", raising=False)
        importlib.reload(cu)


def test_fmr_test_url_default():
    """Without an override, the QA registry is the default endpoint."""
    assert FMR_TEST_URL.endswith("/FMR/sdmx/v2")
