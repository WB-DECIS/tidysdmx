import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np
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


# Test for fetch_schema()
class TestFetchSchema:
    def test_fetch_schema_valid(self):
        # This is a placeholder test. In a real scenario, you would mock the network call.
        base_url = "https://fmrqa.worldbank.org/"
        artefact_id = "WB.DATA360:DS_DATA360(1.3)"
        context = "datastructure"
        try:
            schema = tx.fetch_schema(base_url, artefact_id, context)
            assert schema is not None 
        except Exception as e:
            pytest.fail(f"Unexpected exception raised: {e}")


# Test for transform_source_to_target()
class TestTransformSourceToTarget:
    """Unit tests for the transform_source_to_target function."""

    # Fixtures
    @pytest.fixture
    def sample_raw_df(self):
        """Sample raw DataFrame for testing."""
        return pd.DataFrame({
            "col_a": [1, 2, 3],
            "col_b": ["x", "y", "z"],
            "col_extra": [10, 20, 30]
        })

    @pytest.fixture
    def sample_mapping_dict(self):
        """Sample mapping dict that includes components."""
        return {
            "components": [
                {"SOURCE": "col_a", "TARGET": "target_a"},
                {"SOURCE": "col_b", "TARGET": "target_b"},
                {"SOURCE": "NA", "TARGET": "target_c"}
            ]
        }
    
    # Tests
    def test_basic_transformation(self, sample_raw_df, sample_mapping_dict):
        """Test that source columns are properly renamed and mapped."""
        expected_df = pd.DataFrame({
            "target_a": [1, 2, 3],
            "target_b": ["x", "y", "z"],
            "target_c": [np.nan, np.nan, np.nan]
        })
        # Change dtype to be consistent with what is created by the function
        expected_df["target_c"] = expected_df["target_c"].astype(object) 

        result_df = tx.transform_source_to_target(sample_raw_df, 
                                                  sample_mapping_dict)
        assert_frame_equal(result_df, expected_df)

    def test_missing_source_column(self, sample_raw_df):
        """If a source column doesn't exist, the target should exist but be empty."""
        mapping = {
            "components": [
                {"SOURCE": "nonexistent", "TARGET": "target_x"},
                {"SOURCE": "col_a", "TARGET": "target_a"},
            ]
        }

        result_df = tx.transform_source_to_target(sample_raw_df, mapping)
        
        expected_df = pd.DataFrame({
            "target_x": [pd.NA, pd.NA, pd.NA],
            "target_a": [1, 2, 3]
        })

        assert set(result_df.columns) == set(expected_df.columns)
        assert result_df["target_a"].equals(expected_df["target_a"])
        assert result_df["target_x"].isna().all()

    def test_mapping_as_dataframe(self, sample_raw_df):
        """Ensure it works if mapping['components'] is a DataFrame instead of a list."""
        components_df = pd.DataFrame([
            {"SOURCE": "col_a", "TARGET": "target_a"},
            {"SOURCE": "col_b", "TARGET": "target_b"}
        ])
        mapping = {"components": components_df}

        result_df = tx.transform_source_to_target(sample_raw_df, mapping)
        expected_df = pd.DataFrame({
            "target_a": [1, 2, 3],
            "target_b": ["x", "y", "z"]
        })
        assert_frame_equal(result_df, expected_df)

    def test_empty_mapping(self, sample_raw_df):
        """If mapping is empty, should raise an error"""
        mapping = {"components": []}
        with pytest.raises(KeyError, match="The mapping file should contain 'components' key or its value should not be empty. Please make sure the mapping file has this key and its value is not empty."):
            tx.transform_source_to_target(sample_raw_df, mapping)

    def test_missing_components_key(self, sample_raw_df):
        """Should raise KeyError if mapping has no 'components' key."""
        mapping = {}
        with pytest.raises(KeyError, match="The mapping file should contain 'components' key or its value should not be empty. Please make sure the mapping file has this key and its value is not empty."):
            tx.transform_source_to_target(sample_raw_df, mapping)

    def test_extra_columns_in_raw_are_ignored(self, 
                                              sample_raw_df, 
                                              sample_mapping_dict):
        """Ensure extra columns in raw not defined in mapping are ignored."""
        result_df = tx.transform_source_to_target(sample_raw_df, sample_mapping_dict)
        assert set(result_df.columns) == {"target_a", "target_b", "target_c"}
        assert "col_extra" not in result_df.columns

    def test_invalid_mapping_type(self, sample_raw_df):
        """Optional: ensure invalid mapping type raises an error (if enforced later)."""
        with pytest.raises(Exception):
            tx.transform_source_to_target(sample_raw_df, ["invalid_structure"])