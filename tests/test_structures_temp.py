from pysdmx.model import FixedValueMap
from typeguard import TypeCheckError
import pytest
# Import tidysdmx functions
from tidysdmx.structures_temp import build_fixed_mapping

def test_build_fixed_mapping_normal():
    """Valid mapping with default located_in."""
    mapping = build_fixed_mapping("CONF_STATUS", "F")
    assert isinstance(mapping, FixedValueMap)
    assert mapping.target == "CONF_STATUS"
    assert mapping.value == "F"
    assert mapping.located_in == "target"

def test_build_fixed_mapping_valid_source_located_in():
    """Valid mapping with located_in set to 'source'."""
    mapping = build_fixed_mapping("REPORTING_STATUS", "FINAL", located_in="source")
    assert mapping.located_in == "source"
    assert mapping.target == "REPORTING_STATUS"
    assert mapping.value == "FINAL"

def test_build_fixed_mapping_invalid_target():
    """Empty target should raise ValueError."""
    with pytest.raises(ValueError):
        build_fixed_mapping("", "F")

def test_build_fixed_mapping_invalid_value():
    """Empty value should raise ValueError."""
    with pytest.raises(ValueError):
        build_fixed_mapping("CONF_STATUS", "")

def test_build_fixed_mapping_invalid_located_in_raises():
    """Invalid located_in should raise ValueError."""
    with pytest.raises(ValueError) as exc_info:
        build_fixed_mapping("CONF_STATUS", "F", located_in="invalid")

def test_build_fixed_mapping_type_safety():
    """Ensure type safety enforced by typeguard."""
    with pytest.raises(TypeCheckError):
        build_fixed_mapping(123, "F")  # target must be str
    with pytest.raises(TypeCheckError):
        build_fixed_mapping("CONF_STATUS", 456)  # value must be str
    with pytest.raises(TypeCheckError):
        build_fixed_mapping("CONF_STATUS", 456, located_in=None)
