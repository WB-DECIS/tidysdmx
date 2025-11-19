from pandas.testing import assert_frame_equal
import importlib
import pandas as pd
import numpy as np
import pysdmx as px
import pytest
import tidysdmx.validation as v # import the module under test
importlib.reload(v)

# Test validate_no_missing_values()
class TestValidateNoMissingValues:
    def test_validate_no_missing_values_no_missing(self):
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        mandatory_columns = ["col1", "col2"]
        try:
            v.validate_no_missing_values(df, mandatory_columns)
        except ValueError:
            pytest.fail("Unexpected ValueError raised")


    def test_validate_no_missing_values_missing_in_one_column(self):
        df = pd.DataFrame({"col1": [1, 2, None], "col2": [4, 5, 6]})
        mandatory_columns = ["col1", "col2"]
        with pytest.raises(ValueError, match="Missing values found in mandatory columns"):
            v.validate_no_missing_values(df, mandatory_columns)


    def test_validate_no_missing_values_missing_in_multiple_columns(self):
        df = pd.DataFrame({"col1": [1, None, 3], "col2": [None, 5, 6]})
        mandatory_columns = ["col1", "col2"]
        with pytest.raises(ValueError, match="Missing values found in mandatory columns"):
            v.validate_no_missing_values(df, mandatory_columns)


    def test_validate_no_missing_values_no_missing_but_extra_columns(self):
        df = pd.DataFrame(
            {"col1": [1, 2, 3], "col2": [4, 5, 6], "col3": [None, None, None]}
        )
        mandatory_columns = ["col1", "col2"]
        try:
            v.validate_no_missing_values(df, mandatory_columns)
        except ValueError:
            pytest.fail("Unexpected ValueError raised")

# Test validate_duplicates()
class TestValidateDuplicates:
    def test_validate_duplicates_no_duplicates(self):
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        dim_columns = ["col1", "col2"]
        try:
            v.validate_duplicates(df, dim_columns)
        except ValueError:
            pytest.fail("Unexpected ValueError raised")


    def test_validate_duplicates_with_duplicates(self):
        df = pd.DataFrame({"col1": [1, 2, 2], "col2": [4, 5, 5]})
        dim_columns = ["col1", "col2"]
        with pytest.raises(ValueError, match="Duplicate rows found"):
            v.validate_duplicates(df, dim_columns)

# Test validate_codelist_ids()
class TestValidateCodelistIds:
    @pytest.mark.skip(reason="Test needs to be modified to use correct inputs")
    def test_validate_codelist_ids_valid():
        df = pd.DataFrame({"col1": ["A", "B", "C"]})
        codelist_ids = {"col1": ["A", "B", "C"]}
        try:
            v.validate_codelist_ids(df, codelist_ids)
        except ValueError:
            pytest.fail("Unexpected ValueError raised")


    @pytest.mark.skip(reason="Test needs to be modified to use correct inputs")
    def test_validate_codelist_ids_invalid():
        df = pd.DataFrame({"col1": ["A", "B", "D"]})
        codelist_ids = {"col1": ["A", "B", "C"]}
        with pytest.raises(ValueError, match="Invalid codelist IDs found"):
            v.validate_codelist_ids(df, codelist_ids)

# Test validate_mandatory_columns()
class TestValidateMandatoryColumns:
    def test_validate_mandatory_columns_all_present(self):
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        mandatory_columns = ["col1", "col2"]
        try:
            v.validate_mandatory_columns(df, mandatory_columns, sdmx_cols=[])
        except ValueError:
            pytest.fail("Unexpected ValueError raised")


    def test_validate_mandatory_columns_missing(self):
        df = pd.DataFrame({"col1": [1, 2, 3]})
        mandatory_columns = ["col1", "col2"]
        with pytest.raises(ValueError, match="Missing mandatory columns"):
            v.validate_mandatory_columns(df, mandatory_columns, sdmx_cols=[])

# Test validate_columns()
class ValidateColumns:
    def test_validate_columns_all_valid(self):
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        valid_columns = ["col1", "col2", "col3"]
        try:
            v.validate_columns(df, valid_columns, sdmx_cols=[])
        except ValueError:
            pytest.fail("Unexpected ValueError raised")


    def test_validate_columns_invalid(self):
        df = pd.DataFrame({"col1": [1, 2, 3], "col4": [4, 5, 6]})
        valid_columns = ["col1", "col2", "col3"]
        with pytest.raises(ValueError, match="Found unexpected column: col4"):
            v.validate_columns(df, valid_columns, sdmx_cols=[])