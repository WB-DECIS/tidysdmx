import pandas as pd
import numpy as np
import pysdmx as px
import pytest

from tidysdmx import tidysdmx as tx


# Test cases for the parse_dsd_id function
class TestParseDsdId:
    def test_parse_dsd_id_valid_input(self):
        # Test with a valid DSD ID
        dsd_id = "WB:WDI(1.0)"
        expected_result = ("WB", "WDI", "1.0")
        assert tx.parse_dsd_id(dsd_id) == expected_result

    def test_parse_dsd_id_missing_colon(self):
        # Test with a DSD ID missing the colon
        dsd_id = "WBWDI(1.0)"
        try:
            tx.parse_dsd_id(dsd_id)
        except ValueError as e:
            assert str(e) == "Invalid dsd_id format. Expected format: 'agency:id(version)'"

    def test_parse_dsd_id_missing_parentheses(self):
        # Test with a DSD ID missing the parentheses
        dsd_id = "WB:WDI1.0"
        try:
            tx.parse_dsd_id(dsd_id)
        except ValueError as e:
            assert str(e) == "Invalid dsd_id format. Expected format: 'agency:id(version)'"

    def test_parse_dsd_id_empty_string(self):
        # Test with an empty string
        dsd_id = ""
        try:
            tx.parse_dsd_id(dsd_id)
        except ValueError as e:
            assert str(e) == "Invalid dsd_id format. Expected format: 'agency:id(version)'"

    def test_parse_dsd_id_extra_colon(self):
        # Test with an extra colon in the DSD ID
        dsd_id = "WB:WDI:Extra(1.0)"
        try:
            tx.parse_dsd_id(dsd_id)
        except ValueError as e:
            assert str(e) == "Invalid dsd_id format. Expected format: 'agency:id(version)'"


# Test cases for the parse_artefact_id function
class TestParseArtefactId:
    def test_parse_artefact_id_valid_input(self):
        # Test with a valid artefact ID
        artefact_id = "WB:WDI(1.0)"
        expected_result = ("WB", "WDI", "1.0")
        assert tx.parse_artefact_id(artefact_id) == expected_result

    def test_parse_artefact_id_missing_colon(self):
        # Test with a artefact ID missing the colon
        artefact_id = "WBWDI(1.0)"
        try:
            tx.parse_artefact_id(artefact_id)
        except ValueError as e:
            assert str(e) == "Invalid artefact_id format. Expected format: 'agency:id(version)'"

    def test_parse_artefact_id_missing_parentheses(self):
        # Test with a artefact ID missing the parentheses
        artefact_id = "WB:WDI1.0"
        try:
            tx.parse_artefact_id(artefact_id)
        except ValueError as e:
            assert str(e) == "Invalid artefact_id format. Expected format: 'agency:id(version)'"

    def test_parse_artefact_id_empty_string(self):
        # Test with an empty string
        artefact_id = ""
        try:
            tx.parse_artefact_id(artefact_id)
        except ValueError as e:
            assert str(e) == "Invalid artefact_id format. Expected format: 'agency:id(version)'"

    def test_parse_artefact_id_extra_colon(self):
        # Test with an extra colon in the artefact ID
        artefact_id = "WB:WDI:Extra(1.0)"
        try:
            tx.parse_artefact_id(artefact_id)
        except ValueError as e:
            assert str(e) == "Invalid artefact_id format. Expected format: 'agency:id(version)'"


# Test standardize_indicator_id()
class TestStandardizeIndicatorId:
    def test_standardize_indicator_id_basic(self):
        df = pd.DataFrame(
            {
                "DATABASE_ID": ["WB.DATA360"] * 2,
                "INDICATOR": ["indicator.one", "indicator.two"],
            }
        )
        expected_df = pd.DataFrame(
            {
                "DATABASE_ID": ["WB.DATA360"] * 2,
                "INDICATOR": ["WB_DATA360_INDICATOR_ONE", "WB_DATA360_INDICATOR_TWO"],
            }
        )
        pd.testing.assert_frame_equal(tx.standardize_indicator_id(df), expected_df)


    def test_standardize_indicator_id_mixed_case(self):
        df = pd.DataFrame(
            {
                "DATABASE_ID": ["WB.DATA360"] * 2,
                "INDICATOR": ["Indicator.One", "indicator.Two"],
            }
        )
        expected_df = pd.DataFrame(
            {
                "DATABASE_ID": ["WB.DATA360"] * 2,
                "INDICATOR": ["WB_DATA360_INDICATOR_ONE", "WB_DATA360_INDICATOR_TWO"],
            }
        )
        pd.testing.assert_frame_equal(tx.standardize_indicator_id(df), expected_df)


    def test_standardize_indicator_id_multiple_datasets(self):
        df = pd.DataFrame(
            {
                "DATABASE_ID": ["WB.DATA360", "WB.DATA360", "OTHER.DATASET"],
                "INDICATOR": ["indicator.one", "indicator.two", "indicator.three"],
            }
        )
        with pytest.raises(ValueError):
            tx.standardize_indicator_id(df)


    def test_standardize_indicator_id_no_prefix(self):
        df = pd.DataFrame(
            {
                "DATABASE_ID": ["WB.DATA360"] * 2,
                "INDICATOR": ["indicator.one", "indicator.two"],
            }
        )
        expected_df = pd.DataFrame(
            {
                "DATABASE_ID": ["WB.DATA360"] * 2,
                "INDICATOR": ["WB_DATA360_INDICATOR_ONE", "WB_DATA360_INDICATOR_TWO"],
            }
        )
        pd.testing.assert_frame_equal(tx.standardize_indicator_id(df), expected_df)


# Test vectorized_lookup_ordered_v1()
class TestVectorizedLookupOrderedV1:
    def test_vectorized_lookup_ordered_v1_basic(self):
        series = pd.Series(["A", "B", "C"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A$", "^B$", "^C$"], "TARGET": ["Alpha", "Bravo", "Charlie"]}
        )
        expected_output = pd.Series(["Alpha", "Bravo", "Charlie"])
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v1(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v1_no_match(self):
        series = pd.Series(["D", "E", "F"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A$", "^B$", "^C$"], "TARGET": ["Alpha", "Bravo", "Charlie"]}
        )
        expected_output = pd.Series(["D", "E", "F"])
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v1(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v1_partial_match(self):
        series = pd.Series(["AB", "BC", "CD"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A$", "^B$", "^C$"], "TARGET": ["Alpha", "Bravo", "Charlie"]}
        )
        expected_output = pd.Series(["AB", "BC", "CD"])
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v1(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v1_regex_match(self):
        series = pd.Series(["A1", "B2", "C3"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A.*$", "^B.*$", "^C.*$"], "TARGET": ["Alpha", "Bravo", "Charlie"]}
        )
        expected_output = pd.Series(["Alpha", "Bravo", "Charlie"])
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v1(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v1_empty_mapping(self):
        series = pd.Series(["A", "B", "C"])
        mapping_df = pd.DataFrame(columns=["SOURCE", "TARGET"])
        expected_output = series
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v1(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v1_single_rule(self):
        series = pd.Series(["A", "B", "C"])
        mapping_df = pd.DataFrame({"SOURCE": ["^A$"], "TARGET": ["Alpha"]})
        expected_output = pd.Series(["Alpha", "B", "C"])
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v1(series, mapping_df), expected_output
        )


    @pytest.mark.skip(reason="Not sure how to compare series containing nan")
    def test_vectorized_lookup_ordered_v1_nan_values(self):
        series = pd.Series(["A", np.nan, "C"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A$", "^B$", "^C$"], "TARGET": ["Alpha", "Bravo", "Charlie"]}
        )
        expected_output = pd.Series(["Alpha", np.nan, "Charlie"])
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v1(series, mapping_df),
            expected_output,
            check_exact=False,
            check_dtype=False,
        )


    def test_vectorized_lookup_ordered_v1_special_characters(self):
        series = pd.Series(["$", "^", "*"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^\\$", "^\\^", "^\\*"], "TARGET": ["Dollar", "Caret", "Asterisk"]}
        )
        expected_output = pd.Series(["Dollar", "Caret", "Asterisk"])
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v1(series, mapping_df), expected_output
        )


    @pytest.mark.skip(
        reason="Not handled currently. Relies on ordering of rules in the mapping file"
    )
    def test_vectorized_lookup_ordered_v1_multiple_matches(self):
        series = pd.Series(["A12", "A1", "A"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A", "^A1$", "^A12$"], "TARGET": ["Alpha", "Alpha1", "Alpha12"]}
        )
        expected_output = pd.Series(["Alpha12", "Alpha1", "Alpha"])
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v1(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v1_case_insensitive(self):
        series = pd.Series(["a", "b", "c"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^a$", "^b$", "^c$"], "TARGET": ["Alpha", "Bravo", "Charlie"]}
        )
        expected_output = pd.Series(["Alpha", "Bravo", "Charlie"])
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v1(series, mapping_df), expected_output
        )


# Test vectorized_lookup_ordered_v2()
class TestVectorizedLookupOrderedV2:
    def test_vectorized_lookup_ordered_v2_basic(self):
        series = pd.Series(["A", "B", "C"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A$", "^B$", "^C$"], "TARGET": ["Alpha", "Bravo", "Charlie"], "IS_REGEX": [True, True, True]}
        )
        expected_output = pd.Series(["Alpha", "Bravo", "Charlie"])
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v2(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v2_no_match(self):
        series = pd.Series(["D", "E", "F"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A$", "^B$", "^C$"], "TARGET": ["Alpha", "Bravo", "Charlie"], "IS_REGEX": [True, True, True]}
        )
        expected_output = pd.Series(["D", "E", "F"])
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v2(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v2_partial_match(self):
        series = pd.Series(["AB", "BC", "CD"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A$", "^B$", "^C$"], "TARGET": ["Alpha", "Bravo", "Charlie"], "IS_REGEX": [True, True, True]}
        )
        expected_output = pd.Series(["AB", "BC", "CD"])
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v2(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v2_regex_match(self):
        series = pd.Series(["A1", "B2", "C3"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A.*$", "^B.*$", "^C.*$"], "TARGET": ["Alpha", "Bravo", "Charlie"], "IS_REGEX": [True, True, True]}
        )
        expected_output = pd.Series(["Alpha", "Bravo", "Charlie"])
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v2(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v2_empty_mapping(self):
        series = pd.Series(["A", "B", "C"])
        mapping_df = pd.DataFrame(columns=["SOURCE", "TARGET"])
        expected_output = series
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v2(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v2_single_rule(self):
        series = pd.Series(["A", "B", "C"])
        mapping_df = pd.DataFrame({"SOURCE": ["^A$"], "TARGET": ["Alpha"], "IS_REGEX": [True]})
        expected_output = pd.Series(["Alpha", "B", "C"])
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v2(series, mapping_df), expected_output
        )


    @pytest.mark.skip(reason="Not sure how to compare series containing nan")
    def test_vectorized_lookup_ordered_v2_nan_values(self):
        series = pd.Series(["A", np.nan, "C"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A$", "^B$", "^C$"], "TARGET": ["Alpha", "Bravo", "Charlie"], "IS_REGEX": [True, True, True]}
        )
        expected_output = pd.Series(["Alpha", np.nan, "Charlie"])
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v2(series, mapping_df),
            expected_output,
            check_exact=False,
            check_dtype=False,
        )


    def test_vectorized_lookup_ordered_v2_special_characters(self):
        series = pd.Series(["$", "^", "*"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^\\$", "^\\^", "^\\*"], "TARGET": ["Dollar", "Caret", "Asterisk"], "IS_REGEX": [True, True, True]}
        )
        expected_output = pd.Series(["Dollar", "Caret", "Asterisk"])
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v2(series, mapping_df), expected_output
        )


    @pytest.mark.skip(
        reason="Not handled currently. Relies on ordering of rules in the mapping file"
    )
    def test_vectorized_lookup_ordered_v2_multiple_matches(self):
        series = pd.Series(["A12", "A1", "A"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A", "^A1$", "^A12$"], "TARGET": ["Alpha", "Alpha1", "Alpha12"], "IS_REGEX": [True, True, True]}
        )
        expected_output = pd.Series(["Alpha12", "Alpha1", "Alpha"])
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v2(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v2_case_insensitive(self):
        series = pd.Series(["a", "b", "c"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^a$", "^b$", "^c$"], "TARGET": ["Alpha", "Bravo", "Charlie"], "IS_REGEX": [True, True, True]}
        )
        expected_output = pd.Series(["Alpha", "Bravo", "Charlie"])
        pd.testing.assert_series_equal(
            tx.vectorized_lookup_ordered_v2(series, mapping_df), expected_output
        )


# Test validate_no_missing_values()
class TestValidateNoMissingValues:
    def test_validate_no_missing_values_no_missing(self):
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        mandatory_columns = ["col1", "col2"]
        try:
            tx.validate_no_missing_values(df, mandatory_columns)
        except ValueError:
            pytest.fail("Unexpected ValueError raised")


    def test_validate_no_missing_values_missing_in_one_column(self):
        df = pd.DataFrame({"col1": [1, 2, None], "col2": [4, 5, 6]})
        mandatory_columns = ["col1", "col2"]
        with pytest.raises(ValueError, match="Missing values found in mandatory columns"):
            tx.validate_no_missing_values(df, mandatory_columns)


    def test_validate_no_missing_values_missing_in_multiple_columns(self):
        df = pd.DataFrame({"col1": [1, None, 3], "col2": [None, 5, 6]})
        mandatory_columns = ["col1", "col2"]
        with pytest.raises(ValueError, match="Missing values found in mandatory columns"):
            tx.validate_no_missing_values(df, mandatory_columns)


    def test_validate_no_missing_values_no_missing_but_extra_columns(self):
        df = pd.DataFrame(
            {"col1": [1, 2, 3], "col2": [4, 5, 6], "col3": [None, None, None]}
        )
        mandatory_columns = ["col1", "col2"]
        try:
            tx.validate_no_missing_values(df, mandatory_columns)
        except ValueError:
            pytest.fail("Unexpected ValueError raised")


# Test validate_duplicates()
class TestValidateDuplicates:
    def test_validate_duplicates_no_duplicates(self):
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        dim_columns = ["col1", "col2"]
        try:
            tx.validate_duplicates(df, dim_columns)
        except ValueError:
            pytest.fail("Unexpected ValueError raised")


    def test_validate_duplicates_with_duplicates(self):
        df = pd.DataFrame({"col1": [1, 2, 2], "col2": [4, 5, 5]})
        dim_columns = ["col1", "col2"]
        with pytest.raises(ValueError, match="Duplicate rows found"):
            tx.validate_duplicates(df, dim_columns)


# Test validate_codelist_ids()
class TestValidateCodelistIds:
    @pytest.mark.skip(reason="Test needs to be modified to use correct inputs")
    def test_validate_codelist_ids_valid():
        df = pd.DataFrame({"col1": ["A", "B", "C"]})
        codelist_ids = {"col1": ["A", "B", "C"]}
        try:
            tx.validate_codelist_ids(df, codelist_ids)
        except ValueError:
            pytest.fail("Unexpected ValueError raised")


    @pytest.mark.skip(reason="Test needs to be modified to use correct inputs")
    def test_validate_codelist_ids_invalid():
        df = pd.DataFrame({"col1": ["A", "B", "D"]})
        codelist_ids = {"col1": ["A", "B", "C"]}
        with pytest.raises(ValueError, match="Invalid codelist IDs found"):
            tx.validate_codelist_ids(df, codelist_ids)


# Test get_codelist_ids()
@pytest.mark.skip(reason="Test needs to be modified to use correct inputs")
def test_get_codelist_ids():
    comp = {"dim1": "Dimension 1", "dim2": "Dimension 2"}
    coded_comp = {"dim1": ["A", "B"], "dim2": ["C", "D"]}
    expected_output = {"dim1": ["A", "B"], "dim2": ["C", "D"]}
    assert tx.get_codelist_ids(comp, coded_comp) == expected_output


# Test validate_mandatory_columns()
class TestValidateMandatoryColumns:
    def test_validate_mandatory_columns_all_present(self):
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        mandatory_columns = ["col1", "col2"]
        try:
            tx.validate_mandatory_columns(df, mandatory_columns, sdmx_cols=[])
        except ValueError:
            pytest.fail("Unexpected ValueError raised")


    def test_validate_mandatory_columns_missing(self):
        df = pd.DataFrame({"col1": [1, 2, 3]})
        mandatory_columns = ["col1", "col2"]
        with pytest.raises(ValueError, match="Missing mandatory columns"):
            tx.validate_mandatory_columns(df, mandatory_columns, sdmx_cols=[])


# Test validate_columns()
class ValidateColumns:
    def test_validate_columns_all_valid(self):
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        valid_columns = ["col1", "col2", "col3"]
        try:
            tx.validate_columns(df, valid_columns, sdmx_cols=[])
        except ValueError:
            pytest.fail("Unexpected ValueError raised")


    def test_validate_columns_invalid(self):
        df = pd.DataFrame({"col1": [1, 2, 3], "col4": [4, 5, 6]})
        valid_columns = ["col1", "col2", "col3"]
        with pytest.raises(ValueError, match="Found unexpected column: col4"):
            tx.validate_columns(df, valid_columns, sdmx_cols=[])


# Test create_keys_dict()
class TestCreateKeysDict:
    def test_basic_functionality(self):
        input_dict = {"file1.csv": "data1", "file2.json": "data2", "file3.txt": "data3"}
        expected_output = {
            "file1": "file1.csv",
            "file2": "file2.json",
            "file3": "file3.txt",
        }
        assert tx.create_keys_dict(input_dict) == expected_output


    def test_no_extension(self):
        input_dict = {"file1": "data1", "file2": "data2"}
        expected_output = {"file1": "file1", "file2": "file2"}
        assert tx.create_keys_dict(input_dict) == expected_output


    def test_mixed_keys(self):
        input_dict = {"file1.csv": "data1", "file2": "data2", "file3.txt": "data3"}
        expected_output = {"file1": "file1.csv", "file2": "file2", "file3": "file3.txt"}
        assert tx.create_keys_dict(input_dict) == expected_output


    def test_empty_dict(self):
        input_dict = {}
        expected_output = {}
        assert tx.create_keys_dict(input_dict) == expected_output


    def test_multiple_periods(self):
        input_dict = {
            "file.with.periods.csv": "data1",
            "another.file.with.periods.json": "data2",
        }
        expected_output = {
            "file.with.periods": "file.with.periods.csv",
            "another.file.with.periods": "another.file.with.periods.json",
        }
        assert tx.create_keys_dict(input_dict) == expected_output