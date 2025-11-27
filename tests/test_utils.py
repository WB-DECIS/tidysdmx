import pytest
from typeguard import TypeCheckError
from pysdmx.model import Component, Components, Schema
# Import tidysdmx functions
from tidysdmx.utils import get_codelist_ids, extract_validation_info, extract_component_ids

# Global variables for this test file
# incorrect_ind_code = "INCORRECT_IND"

# region Testing extract_validation_info()
class TestExtractValidationInfo:
    @pytest.mark.parametrize("invalid_input", [
        None,
        {},
        [],
        "not_a_schema",
        123,
    ])

    def test_extract_validation_info(self, invalid_input):
        """Check TypeError raised when input is not of the expected type."""
        with pytest.raises(TypeCheckError):
            extract_validation_info(invalid_input)

    def test_extract_validation_has_expected_structure(self, ifpri_asti_schema):
        """Ensure the returned object has the expected type and structure."""
        result = extract_validation_info(ifpri_asti_schema)

        assert isinstance(result, dict)
        expected_keys = {"valid_comp", "mandatory_comp", "coded_comp", 
                         "codelist_ids", "dim_comp"}
        assert set(result.keys()) == expected_keys
        assert all(isinstance(item, list) for key, item in result.items() 
                   if key != "codelist_ids")
        assert isinstance(result["codelist_ids"], dict)

    def test_extract_validation_has_expected_structure2(self, sdmx_schema):
        """Ensure the returned object has the expected type and structure."""
        result = extract_validation_info(sdmx_schema)

        assert isinstance(result, dict)
        expected_keys = {"valid_comp", "mandatory_comp", "coded_comp", 
                         "codelist_ids", "dim_comp"}
        assert set(result.keys()) == expected_keys
        assert all(isinstance(item, list) for key, item in result.items() 
                   if key != "codelist_ids")
        assert isinstance(result["codelist_ids"], dict)
# endregion

# region Testing get_codelist_ids()
class TestGetCodelistIds:
    def test_get_codelist_ids_has_expected_structure(self, ifpri_asti_schema):
        """Ensure the returned object has the expected type and structure."""
        comp = ifpri_asti_schema.components
        coded_comp = [c.id for c in comp if comp[c.id].local_codes is not None]

        result = get_codelist_ids(comp, coded_comp)
        assert isinstance(result, dict)
        for key, value in result.items():
            assert key in coded_comp
            assert isinstance(value, list)
            assert all(isinstance(code_id, str) for code_id in value)

    # Test get_codelist_ids()
    @pytest.mark.skip(reason="Test needs to be modified to use correct inputs")
    def test_get_codelist_ids(self):
        comp = {"dim1": "Dimension 1", "dim2": "Dimension 2"}
        coded_comp = {"dim1": ["A", "B"], "dim2": ["C", "D"]}
        expected_output = {"dim1": ["A", "B"], "dim2": ["C", "D"]}
        assert get_codelist_ids(comp, coded_comp) == expected_output
# endregion

class TestExtractCodelistIds:  # noqa: D101
    def test_extract_component_ids_normal():
        """Retrieve IDs from a valid schema with multiple components."""
        comp1 = Component(id="FREQ")
        comp2 = Component(id="TIME_PERIOD")
        schema = Schema(context="datastructure", agency="ECB", id_="EXR",
                        components=Components([comp1, comp2]),
                        version="1.0.0", urns=[])
        result = extract_component_ids(schema)
        assert result == ["FREQ", "TIME_PERIOD"]
        assert all(isinstance(cid, str) for cid in result)

    def test_extract_component_ids_single_component(self):
        """Schema with a single component returns a list with one ID."""
        comp = Component(id="OBS_VALUE")
        schema = Schema(context="datastructure", agency="ECB", id_="EXR",
                        components=Components([comp]),
                        version="1.0.0", urns=[])
        result = extract_component_ids(schema)
        assert result == ["OBS_VALUE"]
        assert len(result) == 1

    def test_extract_component_ids_empty(self):
        """Schema with no components raises ValueError."""
        schema = Schema(context="datastructure", agency="ECB", id_="EXR",
                        components=Components([]),
                        version="1.0.0", urns=[])
        with pytest.raises(ValueError):
            extract_component_ids(schema)

    def test_extract_component_ids_invalid_type(self):
        """Non-Schema input raises TypeError."""
        with pytest.raises(TypeError):
            extract_component_ids("not_a_schema")

    def test_extract_component_ids_component_without_id(self):
        """Component without an ID should raise Error."""
        comp = Component(id=None)  # Simulate missing ID
        schema = Schema(context="datastructure", agency="ECB", id_="EXR",
                        components=Components([comp]),
                        version="1.0.0", urns=[])
        with pytest.raises(TypeError):
            extract_component_ids(schema)
