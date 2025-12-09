import pandas as pd
from pysdmx.model import Code, Codelist, Component, Role, Concept
import pytest
# Import tidysdmx functions
from tidysdmx.validation import (
    validate_no_missing_values, 
    validate_duplicates, 
    validate_codelist_ids, 
    validate_mandatory_columns, 
    validate_columns,
    get_codelist_ids
)

# Test validate_no_missing_values()
class TestValidateNoMissingValues:
    def test_validate_no_missing_values_no_missing(self):
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        mandatory_columns = ["col1", "col2"]
        try:
            validate_no_missing_values(df, mandatory_columns)
        except ValueError:
            pytest.fail("Unexpected ValueError raised")


    def test_validate_no_missing_values_missing_in_one_column(self):
        df = pd.DataFrame({"col1": [1, 2, None], "col2": [4, 5, 6]})
        mandatory_columns = ["col1", "col2"]
        with pytest.raises(ValueError, match="Missing values found in mandatory columns"):
            validate_no_missing_values(df, mandatory_columns)


    def test_validate_no_missing_values_missing_in_multiple_columns(self):
        df = pd.DataFrame({"col1": [1, None, 3], "col2": [None, 5, 6]})
        mandatory_columns = ["col1", "col2"]
        with pytest.raises(ValueError, match="Missing values found in mandatory columns"):
            validate_no_missing_values(df, mandatory_columns)


    def test_validate_no_missing_values_no_missing_but_extra_columns(self):
        df = pd.DataFrame(
            {"col1": [1, 2, 3], "col2": [4, 5, 6], "col3": [None, None, None]}
        )
        mandatory_columns = ["col1", "col2"]
        try:
            validate_no_missing_values(df, mandatory_columns)
        except ValueError:
            pytest.fail("Unexpected ValueError raised")

# Test validate_duplicates()
class TestValidateDuplicates:
    def test_validate_duplicates_no_duplicates(self):
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        dim_columns = ["col1", "col2"]
        try:
            validate_duplicates(df, dim_columns)
        except ValueError:
            pytest.fail("Unexpected ValueError raised")


    def test_validate_duplicates_with_duplicates(self):
        df = pd.DataFrame({"col1": [1, 2, 2], "col2": [4, 5, 5]})
        dim_columns = ["col1", "col2"]
        with pytest.raises(ValueError, match="Duplicate rows found"):
            validate_duplicates(df, dim_columns)

# Test validate_codelist_ids()
class TestValidateCodelistIds:
    @pytest.mark.skip(reason="Test needs to be modified to use correct inputs")
    def test_validate_codelist_ids_valid():
        df = pd.DataFrame({"col1": ["A", "B", "C"]})
        codelist_ids = {"col1": ["A", "B", "C"]}
        try:
            validate_codelist_ids(df, codelist_ids)
        except ValueError:
            pytest.fail("Unexpected ValueError raised")


    @pytest.mark.skip(reason="Test needs to be modified to use correct inputs")
    def test_validate_codelist_ids_invalid():
        df = pd.DataFrame({"col1": ["A", "B", "D"]})
        codelist_ids = {"col1": ["A", "B", "C"]}
        with pytest.raises(ValueError, match="Invalid codelist IDs found"):
            validate_codelist_ids(df, codelist_ids)

# Test validate_mandatory_columns()
class TestValidateMandatoryColumns:
    def test_validate_mandatory_columns_all_present(self):
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        mandatory_columns = ["col1", "col2"]
        try:
            validate_mandatory_columns(df, mandatory_columns, sdmx_cols=[])
        except ValueError:
            pytest.fail("Unexpected ValueError raised")


    def test_validate_mandatory_columns_missing(self):
        df = pd.DataFrame({"col1": [1, 2, 3]})
        mandatory_columns = ["col1", "col2"]
        with pytest.raises(ValueError, match="Missing mandatory columns"):
            validate_mandatory_columns(df, mandatory_columns, sdmx_cols=[])

# Test validate_columns()
class ValidateColumns:
    def test_validate_columns_all_valid(self):
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        valid_columns = ["col1", "col2", "col3"]
        try:
            validate_columns(df, valid_columns, sdmx_cols=[])
        except ValueError:
            pytest.fail("Unexpected ValueError raised")


    def test_validate_columns_invalid(self):
        df = pd.DataFrame({"col1": [1, 2, 3], "col4": [4, 5, 6]})
        valid_columns = ["col1", "col2", "col3"]
        with pytest.raises(ValueError, match="Found unexpected column: col4"):
            validate_columns(df, valid_columns, sdmx_cols=[])


class TestValidateColumns:
    """Tests for the validate_columns function which ensures all DataFrame columns are valid."""

    @pytest.mark.parametrize(
        "df_columns, valid_columns, sdmx_cols",
        [
            (["STRUCTURE", "STRUCTURE_ID", "ACTION"], ["COMP1", "COMP2"], ["STRUCTURE", "STRUCTURE_ID", "ACTION"]),
            (["COMP1", "COMP2"], ["COMP1", "COMP2"], ["STRUCTURE", "STRUCTURE_ID", "ACTION"]),
            (["COMP1", "STRUCTURE"], ["COMP1"], ["STRUCTURE", "STRUCTURE_ID", "ACTION"]),
        ]
    )
    def test_valid_columns_pass(self, df_columns, valid_columns, sdmx_cols):
        """Tests that validate_columns passes when all columns are valid."""
        df = pd.DataFrame(columns=df_columns)
        # Should not raise any exception
        validate_columns(df, valid_columns=valid_columns, sdmx_cols=sdmx_cols)

    @pytest.mark.parametrize(
        "df_columns, valid_columns, sdmx_cols, invalid_col",
        [
            (["COMP1", "INVALID"], ["COMP1"], ["STRUCTURE", "STRUCTURE_ID", "ACTION"], "INVALID"),
            (["STRUCTURE", "BAD_COL"], ["COMP1", "COMP2"], ["STRUCTURE", "STRUCTURE_ID", "ACTION"], "BAD_COL"),
        ]
    )
    def test_invalid_column_raises_value_error(self, df_columns, valid_columns, sdmx_cols, invalid_col):
        """Tests that validate_columns raises ValueError when an unexpected column is found."""
        df = pd.DataFrame(columns=df_columns)
        with pytest.raises(ValueError) as exc_info:
            validate_columns(df, valid_columns=valid_columns, sdmx_cols=sdmx_cols)
        assert f"Found unexpected column: {invalid_col}" in str(exc_info.value)

    def test_empty_dataframe_passes(self):
        """Tests that an empty DataFrame passes validation (no columns to check)."""
        df = pd.DataFrame()
        validate_columns(df, valid_columns=["COMP1"], sdmx_cols=["STRUCTURE", "STRUCTURE_ID", "ACTION"])

    def test_only_sdmx_columns_pass(self):
        """Tests that DataFrame with only SDMX columns passes validation."""
        df = pd.DataFrame(columns=["STRUCTURE", "STRUCTURE_ID"])
        validate_columns(df, valid_columns=[], sdmx_cols=["STRUCTURE", "STRUCTURE_ID", "ACTION"])


class TestGetCodelistIds:
    """Tests for get_codelist_ids using real pysdmx Component and Codelist objects."""

    @pytest.fixture
    def make_component(self):
        """Fixture that returns a factory to create a Component with a Codelist."""
        def _factory(id_prefix, ids):
            # Create Code objects
            codes = [Code(id=f"{id_prefix}{cid}", name=f"Name-{cid}") for cid in ids]
            # Create Codelist with those codes
            codelist = Codelist(
                id=f"CL_{id_prefix}",
                name=f"Codelist-{id_prefix}",
                agency="SDMX",
                items=codes,
                version="1.0"
            )
            # Create Component referencing the Codelist
            return Component(
                concept=Concept("TEST", name="A test concept"),
                id=f"comp_{id_prefix}", 
                name=f"Component-{id_prefix}", 
                local_codes=codelist, 
                required=True,
                role=Role.DIMENSION)
        return _factory

    def test_multiple_components_with_codes(self, make_component):
        """Tests that multiple components return correct codelist IDs."""
        comp = {
            "comp1": make_component("A", ["1", "2"]),
            "comp2": make_component("B", ["X", "Y"])
        }
        coded_comp = ["comp1", "comp2"]
        expected = {
            "comp1": ["A1", "A2"],
            "comp2": ["BX", "BY"]
        }
        assert get_codelist_ids(comp, coded_comp) == expected

    def test_empty_coded_comp_returns_empty_dict(self, make_component):
        """Tests that an empty coded_comp list returns an empty dictionary."""
        comp = {"comp1": make_component("A", ["1"])}
        coded_comp = []
        assert get_codelist_ids(comp, coded_comp) == {}

    def test_component_with_no_codes_returns_empty_list(self, make_component):
        """Tests that a component with no codes returns an empty list."""
        comp = {"comp1": make_component("A", [])}
        coded_comp = ["comp1"]
        expected = {"comp1": []}
        assert get_codelist_ids(comp, coded_comp) == expected

    def test_invalid_component_name_raises_keyerror(self, make_component):
        """Tests that an invalid component name raises KeyError."""
        comp = {"comp1": make_component("A", ["1"])}
        coded_comp = ["invalid_comp"]
        with pytest.raises(KeyError):
            get_codelist_ids(comp, coded_comp)

    @pytest.mark.parametrize(
        "coded_comp,expected",
        [
            (["comp1", "comp2"], {"comp1": ["A1"], "comp2": []}),
            (["comp2"], {"comp2": []}),
        ],
    )
    def test_mixed_components_some_empty(self, make_component, coded_comp, expected):
        """Tests that mixed components (some empty) return correct results."""
        comp = {
            "comp1": make_component("A", ["1"]),
            "comp2": make_component("B", []),
        }
        assert get_codelist_ids(comp, coded_comp) == expected





