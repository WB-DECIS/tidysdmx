# python
import importlib
import pytest
from typeguard import TypeCheckError
import tidysdmx.validation as v # import the module under test
importlib.reload(v) # reload if already imported

# extract_validation_info()
@pytest.mark.parametrize("invalid_input", [
    None,
    {},
    [],
    "not_a_schema",
    123,
])

def test_extract_validation_info(invalid_input):
    """Check TypeError raised when input is not of the expected type."""
    with pytest.raises(TypeCheckError):
        v.extract_validation_info(invalid_input)

def test_extract_validation_info_expected_output(dsd_schema):
    """Check function output structure is as expected"""
    result = v.extract_validation_info(dsd_schema)

    assert isinstance(result, dict)
    assert set(result.keys()) == {
        "valid_comp",
        "mandatory_comp",
        "coded_comp",
        "codelist_ids",
        "dim_comp"
    }

    assert all(isinstance(item, list) for key, item in result.items() if key != "codelist_ids")
    assert isinstance(result["codelist_ids"], dict)
