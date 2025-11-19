# python
import importlib
import pytest
from typeguard import TypeCheckError
import tidysdmx.utils as v # import the module under test
importlib.reload(v) # reload if already imported

# Global variables for this test file
# incorrect_ind_code = "INCORRECT_IND"

# region Testing extract_validation_info()
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

def test_extract_validation_has_expected_structure(ifpri_asti_schema):
    """Ensure the returned object has the expected type and structure."""
    result = v.extract_validation_info(ifpri_asti_schema)

    assert isinstance(result, dict)
    expected_keys = {"valid_comp", "mandatory_comp", "coded_comp", "codelist_ids", "dim_comp"}
    assert set(result.keys()) == expected_keys
    assert all(isinstance(item, list) for key, item in result.items() if key != "codelist_ids")
    assert isinstance(result["codelist_ids"], dict)

def test_extract_validation_has_expected_structure2(sdmx_schema):
    """Ensure the returned object has the expected type and structure."""
    result = v.extract_validation_info(sdmx_schema)

    assert isinstance(result, dict)
    expected_keys = {"valid_comp", "mandatory_comp", "coded_comp", "codelist_ids", "dim_comp"}
    assert set(result.keys()) == expected_keys
    assert all(isinstance(item, list) for key, item in result.items() if key != "codelist_ids")
    assert isinstance(result["codelist_ids"], dict)
# endregion

# region Testing get_codelist_ids()

def test_get_codelist_ids_has_expected_structure(ifpri_asti_schema):
    """Ensure the returned object has the expected type and structure."""
    comp = ifpri_asti_schema.components
    coded_comp = [c.id for c in comp if comp[c.id].local_codes is not None]

    result = v.get_codelist_ids(comp, coded_comp)
    assert isinstance(result, dict)
    for key, value in result.items():
        assert key in coded_comp
        assert isinstance(value, list)
        assert all(isinstance(code_id, str) for code_id in value)

# Test get_codelist_ids()
@pytest.mark.skip(reason="Test needs to be modified to use correct inputs")
def test_get_codelist_ids():
    comp = {"dim1": "Dimension 1", "dim2": "Dimension 2"}
    coded_comp = {"dim1": ["A", "B"], "dim2": ["C", "D"]}
    expected_output = {"dim1": ["A", "B"], "dim2": ["C", "D"]}
    assert v.get_codelist_ids(comp, coded_comp) == expected_output
# endregion