import pandas as pd
from pandas.testing import assert_frame_equal
from pysdmx.model import Schema, Components
from typeguard import TypeCheckError
from datetime import datetime, timezone
import numpy as np
import pytest

from tidysdmx.tidysdmx import (
    parse_dsd_id,
    parse_artefact_id,
    standardize_indicator_id,
    vectorized_lookup_ordered_v1,
    vectorized_lookup_ordered_v2,
    create_keys_dict,
    fetch_schema,
    transform_source_to_target,
    _extract_artefact_type,
    _add_sdmx_reference_cols,
    standardize_output

)

class TestParseDsdId:
    def test_parse_dsd_id_valid_input(self):
        # Test with a valid DSD ID
        dsd_id = "WB:WDI(1.0)"
        expected_result = ("WB", "WDI", "1.0")
        assert parse_dsd_id(dsd_id) == expected_result

    def test_parse_dsd_id_missing_colon(self):
        # Test with a DSD ID missing the colon
        dsd_id = "WBWDI(1.0)"
        try:
            parse_dsd_id(dsd_id)
        except ValueError as e:
            assert str(e) == "Invalid dsd_id format. Expected format: 'agency:id(version)'"

    def test_parse_dsd_id_missing_parentheses(self):
        # Test with a DSD ID missing the parentheses
        dsd_id = "WB:WDI1.0"
        try:
            parse_dsd_id(dsd_id)
        except ValueError as e:
            assert str(e) == "Invalid dsd_id format. Expected format: 'agency:id(version)'"

    def test_parse_dsd_id_empty_string(self):
        # Test with an empty string
        dsd_id = ""
        try:
            parse_dsd_id(dsd_id)
        except ValueError as e:
            assert str(e) == "Invalid dsd_id format. Expected format: 'agency:id(version)'"

    def test_parse_dsd_id_extra_colon(self):
        # Test with an extra colon in the DSD ID
        dsd_id = "WB:WDI:Extra(1.0)"
        try:
            parse_dsd_id(dsd_id)
        except ValueError as e:
            assert str(e) == "Invalid dsd_id format. Expected format: 'agency:id(version)'"

class TestParseArtefactId:
    def test_parse_artefact_id_valid_input(self):
        # Test with a valid artefact ID
        artefact_id = "WB:WDI(1.0)"
        expected_result = ("WB", "WDI", "1.0")
        assert parse_artefact_id(artefact_id) == expected_result

    def test_parse_artefact_id_missing_colon(self):
        # Test with a artefact ID missing the colon
        artefact_id = "WBWDI(1.0)"
        try:
            parse_artefact_id(artefact_id)
        except ValueError as e:
            assert str(e) == "Invalid artefact_id format. Expected format: 'agency:id(version)'"

    def test_parse_artefact_id_missing_parentheses(self):
        # Test with a artefact ID missing the parentheses
        artefact_id = "WB:WDI1.0"
        try:
            parse_artefact_id(artefact_id)
        except ValueError as e:
            assert str(e) == "Invalid artefact_id format. Expected format: 'agency:id(version)'"

    def test_parse_artefact_id_empty_string(self):
        # Test with an empty string
        artefact_id = ""
        try:
            parse_artefact_id(artefact_id)
        except ValueError as e:
            assert str(e) == "Invalid artefact_id format. Expected format: 'agency:id(version)'"

    def test_parse_artefact_id_extra_colon(self):
        # Test with an extra colon in the artefact ID
        artefact_id = "WB:WDI:Extra(1.0)"
        try:
            parse_artefact_id(artefact_id)
        except ValueError as e:
            assert str(e) == "Invalid artefact_id format. Expected format: 'agency:id(version)'"

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
        pd.testing.assert_frame_equal(standardize_indicator_id(df), expected_df)


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
        pd.testing.assert_frame_equal(standardize_indicator_id(df), expected_df)


    def test_standardize_indicator_id_multiple_datasets(self):
        df = pd.DataFrame(
            {
                "DATABASE_ID": ["WB.DATA360", "WB.DATA360", "OTHER.DATASET"],
                "INDICATOR": ["indicator.one", "indicator.two", "indicator.three"],
            }
        )
        with pytest.raises(ValueError):
            standardize_indicator_id(df)


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
        pd.testing.assert_frame_equal(standardize_indicator_id(df), expected_df)

class TestVectorizedLookupOrderedV1:
    def test_vectorized_lookup_ordered_v1_basic(self):
        series = pd.Series(["A", "B", "C"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A$", "^B$", "^C$"], "TARGET": ["Alpha", "Bravo", "Charlie"]}
        )
        expected_output = pd.Series(["Alpha", "Bravo", "Charlie"])
        pd.testing.assert_series_equal(
            vectorized_lookup_ordered_v1(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v1_no_match(self):
        series = pd.Series(["D", "E", "F"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A$", "^B$", "^C$"], "TARGET": ["Alpha", "Bravo", "Charlie"]}
        )
        expected_output = pd.Series(["D", "E", "F"])
        pd.testing.assert_series_equal(
            vectorized_lookup_ordered_v1(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v1_partial_match(self):
        series = pd.Series(["AB", "BC", "CD"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A$", "^B$", "^C$"], "TARGET": ["Alpha", "Bravo", "Charlie"]}
        )
        expected_output = pd.Series(["AB", "BC", "CD"])
        pd.testing.assert_series_equal(
            vectorized_lookup_ordered_v1(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v1_regex_match(self):
        series = pd.Series(["A1", "B2", "C3"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A.*$", "^B.*$", "^C.*$"], "TARGET": ["Alpha", "Bravo", "Charlie"]}
        )
        expected_output = pd.Series(["Alpha", "Bravo", "Charlie"])
        pd.testing.assert_series_equal(
            vectorized_lookup_ordered_v1(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v1_empty_mapping(self):
        series = pd.Series(["A", "B", "C"])
        mapping_df = pd.DataFrame(columns=["SOURCE", "TARGET"])
        expected_output = series
        pd.testing.assert_series_equal(
            vectorized_lookup_ordered_v1(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v1_single_rule(self):
        series = pd.Series(["A", "B", "C"])
        mapping_df = pd.DataFrame({"SOURCE": ["^A$"], "TARGET": ["Alpha"]})
        expected_output = pd.Series(["Alpha", "B", "C"])
        pd.testing.assert_series_equal(
            vectorized_lookup_ordered_v1(series, mapping_df), expected_output
        )


    @pytest.mark.skip(reason="Not sure how to compare series containing nan")
    def test_vectorized_lookup_ordered_v1_nan_values(self):
        series = pd.Series(["A", np.nan, "C"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A$", "^B$", "^C$"], "TARGET": ["Alpha", "Bravo", "Charlie"]}
        )
        expected_output = pd.Series(["Alpha", np.nan, "Charlie"])
        pd.testing.assert_series_equal(
            vectorized_lookup_ordered_v1(series, mapping_df),
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
            vectorized_lookup_ordered_v1(series, mapping_df), expected_output
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
            vectorized_lookup_ordered_v1(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v1_case_insensitive(self):
        series = pd.Series(["a", "b", "c"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^a$", "^b$", "^c$"], "TARGET": ["Alpha", "Bravo", "Charlie"]}
        )
        expected_output = pd.Series(["Alpha", "Bravo", "Charlie"])
        pd.testing.assert_series_equal(
            vectorized_lookup_ordered_v1(series, mapping_df), expected_output
        )

class TestVectorizedLookupOrderedV2:
    def test_vectorized_lookup_ordered_v2_basic(self):
        series = pd.Series(["A", "B", "C"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A$", "^B$", "^C$"], "TARGET": ["Alpha", "Bravo", "Charlie"], "IS_REGEX": [True, True, True]}
        )
        expected_output = pd.Series(["Alpha", "Bravo", "Charlie"])
        pd.testing.assert_series_equal(
            vectorized_lookup_ordered_v2(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v2_no_match(self):
        series = pd.Series(["D", "E", "F"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A$", "^B$", "^C$"], "TARGET": ["Alpha", "Bravo", "Charlie"], "IS_REGEX": [True, True, True]}
        )
        expected_output = pd.Series(["D", "E", "F"])
        pd.testing.assert_series_equal(
            vectorized_lookup_ordered_v2(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v2_partial_match(self):
        series = pd.Series(["AB", "BC", "CD"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A$", "^B$", "^C$"], "TARGET": ["Alpha", "Bravo", "Charlie"], "IS_REGEX": [True, True, True]}
        )
        expected_output = pd.Series(["AB", "BC", "CD"])
        pd.testing.assert_series_equal(
            vectorized_lookup_ordered_v2(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v2_regex_match(self):
        series = pd.Series(["A1", "B2", "C3"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A.*$", "^B.*$", "^C.*$"], "TARGET": ["Alpha", "Bravo", "Charlie"], "IS_REGEX": [True, True, True]}
        )
        expected_output = pd.Series(["Alpha", "Bravo", "Charlie"])
        pd.testing.assert_series_equal(
            vectorized_lookup_ordered_v2(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v2_empty_mapping(self):
        series = pd.Series(["A", "B", "C"])
        mapping_df = pd.DataFrame(columns=["SOURCE", "TARGET"])
        expected_output = series
        pd.testing.assert_series_equal(
            vectorized_lookup_ordered_v2(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v2_single_rule(self):
        series = pd.Series(["A", "B", "C"])
        mapping_df = pd.DataFrame({"SOURCE": ["^A$"], "TARGET": ["Alpha"], "IS_REGEX": [True]})
        expected_output = pd.Series(["Alpha", "B", "C"])
        pd.testing.assert_series_equal(
            vectorized_lookup_ordered_v2(series, mapping_df), expected_output
        )


    @pytest.mark.skip(reason="Not sure how to compare series containing nan")
    def test_vectorized_lookup_ordered_v2_nan_values(self):
        series = pd.Series(["A", np.nan, "C"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^A$", "^B$", "^C$"], "TARGET": ["Alpha", "Bravo", "Charlie"], "IS_REGEX": [True, True, True]}
        )
        expected_output = pd.Series(["Alpha", np.nan, "Charlie"])
        pd.testing.assert_series_equal(
            vectorized_lookup_ordered_v2(series, mapping_df),
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
            vectorized_lookup_ordered_v2(series, mapping_df), expected_output
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
            vectorized_lookup_ordered_v2(series, mapping_df), expected_output
        )


    def test_vectorized_lookup_ordered_v2_case_insensitive(self):
        series = pd.Series(["a", "b", "c"])
        mapping_df = pd.DataFrame(
            {"SOURCE": ["^a$", "^b$", "^c$"], "TARGET": ["Alpha", "Bravo", "Charlie"], "IS_REGEX": [True, True, True]}
        )
        expected_output = pd.Series(["Alpha", "Bravo", "Charlie"])
        pd.testing.assert_series_equal(
            vectorized_lookup_ordered_v2(series, mapping_df), expected_output
        )

class TestCreateKeysDict:
    def test_basic_functionality(self):
        input_dict = {"file1.csv": "data1", "file2.json": "data2", "file3.txt": "data3"}
        expected_output = {
            "file1": "file1.csv",
            "file2": "file2.json",
            "file3": "file3.txt",
        }
        assert create_keys_dict(input_dict) == expected_output


    def test_no_extension(self):
        input_dict = {"file1": "data1", "file2": "data2"}
        expected_output = {"file1": "file1", "file2": "file2"}
        assert create_keys_dict(input_dict) == expected_output


    def test_mixed_keys(self):
        input_dict = {"file1.csv": "data1", "file2": "data2", "file3.txt": "data3"}
        expected_output = {"file1": "file1.csv", "file2": "file2", "file3": "file3.txt"}
        assert create_keys_dict(input_dict) == expected_output


    def test_empty_dict(self):
        input_dict = {}
        expected_output = {}
        assert create_keys_dict(input_dict) == expected_output


    def test_multiple_periods(self):
        input_dict = {
            "file.with.periods.csv": "data1",
            "another.file.with.periods.json": "data2",
        }
        expected_output = {
            "file.with.periods": "file.with.periods.csv",
            "another.file.with.periods": "another.file.with.periods.json",
        }
        assert create_keys_dict(input_dict) == expected_output

class TestFetchSchema:
    def test_fetch_schema_valid(self):
        # This is a placeholder test. In a real scenario, you would mock the network call.
        base_url = "https://fmrqa.worldbank.org/"
        artefact_id = "WB.DATA360:DS_DATA360(1.3)"
        context = "datastructure"
        try:
            schema = fetch_schema(base_url, artefact_id, context)
            assert schema is not None 
        except Exception as e:
            pytest.fail(f"Unexpected exception raised: {e}")

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

        result_df = transform_source_to_target(sample_raw_df, 
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

        result_df = transform_source_to_target(sample_raw_df, mapping)
        
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

        result_df = transform_source_to_target(sample_raw_df, mapping)
        expected_df = pd.DataFrame({
            "target_a": [1, 2, 3],
            "target_b": ["x", "y", "z"]
        })
        assert_frame_equal(result_df, expected_df)

    def test_empty_mapping(self, sample_raw_df):
        """If mapping is empty, should raise an error"""
        mapping = {"components": []}
        with pytest.raises(KeyError, match="The mapping file should contain 'components' key or its value should not be empty. Please make sure the mapping file has this key and its value is not empty."):
            transform_source_to_target(sample_raw_df, mapping)

    def test_missing_components_key(self, sample_raw_df):
        """Should raise KeyError if mapping has no 'components' key."""
        mapping = {}
        with pytest.raises(KeyError, match="The mapping file should contain 'components' key or its value should not be empty. Please make sure the mapping file has this key and its value is not empty."):
            transform_source_to_target(sample_raw_df, mapping)

    def test_extra_columns_in_raw_are_ignored(self, 
                                              sample_raw_df, 
                                              sample_mapping_dict):
        """Ensure extra columns in raw not defined in mapping are ignored."""
        result_df = transform_source_to_target(sample_raw_df, sample_mapping_dict)
        assert set(result_df.columns) == {"target_a", "target_b", "target_c"}
        assert "col_extra" not in result_df.columns

    def test_invalid_mapping_type(self, sample_raw_df):
        """Optional: ensure invalid mapping type raises an error (if enforced later)."""
        with pytest.raises(Exception):
            transform_source_to_target(sample_raw_df, ["invalid_structure"])

class TestExtractArtefactType:
    """Tests for the _extract_artefact function which extracts SDMX artefact type from a Schema instance."""

    @pytest.mark.parametrize(
        "context",
        ["dataflow", "datastructure", "provisionagreement"]
    )
    def test_valid_contexts(self, context):
        """Tests that _extract_artefact returns the correct context for valid schema contexts."""
        comps = Components([])
        schema = Schema(context, "ECB", "EXR", comps, "1.0", [], generated=datetime.now(timezone.utc))
        result = _extract_artefact_type(schema)
        assert result == context

    def test_invalid_context_raises_value_error(self):
        """Tests that _extract_artefact raises ValueError when schema context is invalid."""
        comps = Components([])
        schema = Schema("invalid_context", "ECB", "EXR", comps, "1.0", [], generated=datetime.now(timezone.utc))
        with pytest.raises(ValueError) as exc_info:
            _extract_artefact_type(schema)
        assert "Invalid schema context" in str(exc_info.value)

    def test_error_message_contains_expected_contexts(self):
        """Tests that the ValueError message lists all valid contexts."""
        comps = Components([])
        schema = Schema("wrong", "ECB", "EXR", comps, "1.0", [], generated=datetime.now(timezone.utc))
        with pytest.raises(ValueError) as exc_info:
            _extract_artefact_type(schema)
        message = str(exc_info.value)
        assert "dataflow" in message
        assert "datastructure" in message
    
    def test_not_schema_raise_error(self):
        """Tests that _extract_artefact raises TypeCheckError when argument is not a valid schema."""
        schema = "Not a valid schema"
        with pytest.raises(TypeCheckError):
            _extract_artefact_type(schema)

class TestAddSdmxReferenceCols:
    """Tests for the `_add_sdmx_reference_cols` function which adds SDMX reference columns to a DataFrame."""

    @pytest.mark.parametrize(
        "artefact_type,expected_cols",
        [
            ("dataflow", ["OBS_VALUE", "DATAFLOW", "DATAFLOW_ID", "ACTION"]),
            ("datastructure", ["OBS_VALUE", "STRUCTURE", "STRUCTURE_ID", "ACTION"]),
            ("provisionagreement", ["OBS_VALUE", "PROVISIONAGREEMENT", "PROVISION_AGREEMENT_ID", "ACTION"]),
        ],
    )
    def test_add_columns_for_valid_types(self, artefact_type, expected_cols):
        """Tests that correct columns are added for each valid artefact_type."""
        df = pd.DataFrame({"OBS_VALUE": [100, 200]})
        result = _add_sdmx_reference_cols(df, "TEST_ID", artefact_type, "I")
        assert list(result.columns) == expected_cols
        assert all(result[expected_cols[1]] == artefact_type)
        assert all(result[expected_cols[2]] == "TEST_ID")
        assert all(result["ACTION"] == "I")

    @pytest.mark.parametrize("action", ["I", "U", "D"])
    def test_valid_actions(self, action):
        """Tests that valid actions are correctly applied."""
        df = pd.DataFrame({"OBS_VALUE": [1]})
        result = _add_sdmx_reference_cols(df, "ID123", "dataflow", action)
        assert result["ACTION"].iloc[0] == action

    def test_invalid_df_type_raises_typeerror(self):
        """Tests that passing a non-DataFrame raises TypeError."""
        with pytest.raises(TypeCheckError):
            _add_sdmx_reference_cols(["not", "a", "df"], "ID123", "dataflow")

    def test_invalid_artefact_type_raises_error(self):
        """Tests that invalid artefact_type raises TypeCheckError."""
        df = pd.DataFrame({"OBS_VALUE": [1]})
        with pytest.raises(TypeCheckError):
            _add_sdmx_reference_cols(df, "ID123", "invalid_type")

    def test_invalid_action_raises_error(self):
        """Tests that invalid action raises TypeCheckError."""
        df = pd.DataFrame({"OBS_VALUE": [1]})
        with pytest.raises(TypeCheckError):
            _add_sdmx_reference_cols(df, "ID123", "dataflow", "X")

class TestStandardizeOutput:
    """Tests for the `standardize_output` function using real schema fixture."""

    @pytest.fixture
    def sample_df(self):
        """Fixture providing a sample DataFrame for testing."""
        return pd.DataFrame({
            "FREQ": ["A", "Q"],
            "AREA": ["USA", "FRA"],
            "INDICATOR": ["GDP", "POP"],
            "SEX": ["M", "F"],
            "AGE": ["Y18T65", "Y25T44"],
            "URBANISATION": ["URB", "RUR"],
            "UNIT_MEASURE": ["USD", "USD"],
            "COMP_BREAKDOWN_1": ["_Z", "_Z"],
            "OBS_VALUE": [100, 200],
            "TIME_PERIOD": ["2020", "2021"]
        })

    def test_valid_case(self, sample_df, ifpri_asti_schema):
        """Tests that SDMX reference columns are added and reordered correctly."""
        artefact_id = "DF_IFPRI_ASTI"
        result = standardize_output(sample_df, artefact_id=artefact_id, schema=ifpri_asti_schema)

        # Check first three columns
        expected_first_cols = ["STRUCTURE", "STRUCTURE_ID", "ACTION"]
        assert list(result.columns[:3]) == expected_first_cols

        # Check values in added columns
        assert all(result["STRUCTURE_ID"] == artefact_id)
        assert all(result["ACTION"] == "I")

    def test_empty_dataframe_raises(self, ifpri_asti_schema):
        """Tests that ValueError is raised when DataFrame is empty."""
        empty_df = pd.DataFrame()
        with pytest.raises(ValueError, match="Input DataFrame `df` cannot be empty."):
            standardize_output(empty_df, artefact_id="DF_IFPRI_ASTI", schema=ifpri_asti_schema)

    @pytest.mark.parametrize("invalid_df", [None, "string", 123])
    def test_invalid_df_type_raises(self, invalid_df, ifpri_asti_schema):
        """Tests that TypeCheckError is raised for non-DataFrame input."""
        with pytest.raises(TypeCheckError):
            standardize_output(invalid_df, artefact_id="DF_IFPRI_ASTI", schema=ifpri_asti_schema)

    def test_empty_parameters_raise(self, ifpri_asti_schema, sample_df):
        """Tests that ValueError is raised when artefact_id is empty."""
        with pytest.raises(ValueError):
            standardize_output(sample_df, artefact_id="", schema=ifpri_asti_schema)

    def test_action_default_is_insert(self, sample_df, ifpri_asti_schema):
        """Tests that default action 'I' is applied when not provided."""
        result = standardize_output(sample_df, artefact_id="DF_IFPRI_ASTI", schema=ifpri_asti_schema)
        assert all(result["ACTION"] == "I")

    def test_action_custom_value(self, sample_df, ifpri_asti_schema):
        """Tests that custom action value is applied correctly."""
        result = standardize_output(sample_df, artefact_id="DF_IFPRI_ASTI", schema=ifpri_asti_schema, action="U")
        assert all(result["ACTION"] == "U")

    def test_columns_filtered_by_schema(self, sample_df, ifpri_asti_schema):
        """Tests that only schema components remain after filtering."""
        result = standardize_output(sample_df, artefact_id="DF_IFPRI_ASTI", schema=ifpri_asti_schema)
        # Ensure non-schema columns like OBS_VALUE and TIME_PERIOD are removed
        assert "SEX" not in result.columns
        assert "URBANISATION" not in result.columns



