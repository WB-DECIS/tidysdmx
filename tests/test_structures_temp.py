from pysdmx.model.map import FixedValueMap, ImplicitComponentMap
from typeguard import TypeCheckError
import pytest
# Import tidysdmx functions
from tidysdmx.structures_temp import build_fixed_map, build_implicit_component_map

# region build_fixed_map
def test_build_fixed_map_normal():
    """Valid mapping with default located_in."""
    mapping = build_fixed_map("CONF_STATUS", "F")
    assert isinstance(mapping, FixedValueMap)
    assert mapping.target == "CONF_STATUS"
    assert mapping.value == "F"
    assert mapping.located_in == "target"

def test_build_fixed_map_valid_source_located_in():
    """Valid mapping with located_in set to 'source'."""
    mapping = build_fixed_map("REPORTING_STATUS", "FINAL", located_in="source")
    assert mapping.located_in == "source"
    assert mapping.target == "REPORTING_STATUS"
    assert mapping.value == "FINAL"

def test_build_fixed_map_invalid_target():
    """Empty target should raise ValueError."""
    with pytest.raises(ValueError):
        build_fixed_map("", "F")

def test_build_fixed_map_invalid_value():
    """Empty value should raise ValueError."""
    with pytest.raises(ValueError):
        build_fixed_map("CONF_STATUS", "")

def test_build_fixed_map_invalid_located_in_raises():
    """Invalid located_in should raise ValueError."""
    with pytest.raises(ValueError) as exc_info:
        build_fixed_map("CONF_STATUS", "F", located_in="invalid")

def test_build_fixed_map_type_safety():
    """Ensure type safety enforced by typeguard."""
    with pytest.raises(TypeCheckError):
        build_fixed_map(123, "F")  # target must be str
    with pytest.raises(TypeCheckError):
        build_fixed_map("CONF_STATUS", 456)  # value must be str
    with pytest.raises(TypeCheckError):
        build_fixed_map("CONF_STATUS", 456, located_in=None)
# endregion

# region build_implicit_component_map

def test_build_implicit_component_map_valid():
    """Valid mapping should return an ImplicitComponentMap instance."""
    mapping = build_implicit_component_map("FREQ", "FREQUENCY")
    assert isinstance(mapping, ImplicitComponentMap)
    assert mapping.source == "FREQ"
    assert mapping.target == "FREQUENCY"

def test_build_implicit_component_map_empty_source_raises():
    """Empty source should raise ValueError."""
    with pytest.raises(ValueError) as exc_info:
        build_implicit_component_map("", "FREQUENCY")
    assert "non-empty" in str(exc_info.value)

def test_build_implicit_component_map_empty_target_raises():
    """Empty target should raise ValueError."""
    with pytest.raises(ValueError) as exc_info:
        build_implicit_component_map("FREQ", "")
    assert "non-empty" in str(exc_info.value)

def test_build_implicit_component_map_type_safety_source():
    """Non-string source should raise TypeError due to typeguard."""
    with pytest.raises(TypeCheckError):
        build_implicit_component_map(123, "FREQUENCY")

def test_build_implicit_component_map_type_safety_target():
    """Non-string target should raise TypeError due to typeguard."""
    with pytest.raises(TypeCheckError):
        build_implicit_component_map("FREQ", 456)

@pytest.mark.skip(reason="Not implemented.")
def test_build_implicit_component_map_whitespace_source():
    """Whitespace-only source should raise ValueError."""
    with pytest.raises(ValueError):
        build_implicit_component_map("   ", "FREQUENCY")

@pytest.mark.skip(reason="Not implemented.")
def test_build_implicit_component_map_whitespace_target():
    """Whitespace-only target should raise ValueError."""
    with pytest.raises(ValueError):
        build_implicit_component_map("FREQ", "   ")

def test_build_implicit_component_map_case_sensitivity():
    """Ensure mapping preserves case of source and target."""
    mapping = build_implicit_component_map("freq", "Frequency")
    assert mapping.source == "freq"

# endregion