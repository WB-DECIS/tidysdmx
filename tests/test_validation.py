# python
import importlib
import pytest
from typeguard import TypeCheckError
import pandas as pd
import tidysdmx.validation as v # import the module under test
importlib.reload(v) # reload if already imported

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

def test_extract_validation_has_expected_structure(dsd_schema):
    """Ensure the returned object has the expected type and structure."""
    result = v.extract_validation_info(dsd_schema)

    assert isinstance(result, dict)
    expected_keys = {"valid_comp", "mandatory_comp", "coded_comp", "codelist_ids", "dim_comp"}
    assert set(result.keys()) == expected_keys
    assert all(isinstance(item, list) for key, item in result.items() if key != "codelist_ids")
    assert isinstance(result["codelist_ids"], dict)
# endregion

# region Testing get_codelist_ids()

def test_get_codelist_ids_has_expected_structure(dsd_schema):
    """Ensure the returned object has the expected type and structure."""
    comp = dsd_schema.components
    coded_comp = [c.id for c in comp if comp[c.id].local_codes is not None]

    result = v.get_codelist_ids(comp, coded_comp)
    assert isinstance(result, dict)
    for key, value in result.items():
        assert key in coded_comp
        assert isinstance(value, list)
        assert all(isinstance(code_id, str) for code_id in value)
# endregion

# region Testing filter_rows()

@pytest.mark.parametrize("invalid_df", [
    None,
    "not_a_dataframe",
    123,
    [{"A": 1}],
])
def test_filter_rows_raises_on_invalid_df(invalid_df):
    """Ensure TypeCheckError is raised for invalid DataFrame input."""
    with pytest.raises((TypeCheckError, AttributeError)):
        v.filter_rows(invalid_df, {"A": [1]})


def test_filter_rows_returns_dataframe_with_same_columns():
    """Ensure returned DataFrame has same columns as input."""
    df = pd.DataFrame({"A": [1, 2], "B": ["x", "y"]})
    codelist_ids = {"A": [1]}
    result = v.filter_rows(df, codelist_ids)

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == list(df.columns)


# def test_filter_rows_index_type_preserved():
#     """Ensure index type is preserved in returned DataFrame."""
#     df = pd.DataFrame({"A": [1, 2]}, index=pd.Index([10, 20], name="custom_index"))
#     codelist_ids = {"A": [1]}
#     result = filter_rows(df, codelist_ids)

#     assert isinstance(result.index, pd.Index)
#     assert result.index.name == "custom_index"


# def test_filter_rows_empty_result_has_correct_structure():
#     """Ensure empty result still has same columns and correct dtypes."""
#     df = pd.DataFrame({"A": [1, 2], "B": ["x", "y"]})
#     codelist_ids = {"A": [99]}  # No matches
#     result = filter_rows(df, codelist_ids)

#     assert result.empty
#     assert list(result.columns) == list(df.columns)
#     # Dtypes should match original
#     for col in df.columns:
#         assert result[col].dtype == df[col].dtype


def test_filter_rows_returns_copy(sample_df):
    """Verify filter_rows with an empty filter returns a new DataFrame:
    - Content identical to input (same rows, order, values)
    - Different object identity (copy, not original)"""
    result = v.filter_rows(sample_df, {})
    assert result.equals(sample_df)  # Same number of rows and identical content
    assert result is not sample_df  # Distinct object (copy)


@pytest.mark.parametrize(
    "codelist_ids,expected_rows",
    # filter_rows() does not remove None currently. 
    # This is reflected in the expected results of the unit tests
    # This might not be the behavior that we want
    # May need to be changed
    [
        # ({"code": ["1", "2"]}, [0, 1]),  # Filter by numeric column
        ({"status": ["A", "C"]}, [0, 2, 4]),  # Filter by string column.
        ({"code": ["1", "2"], "status": ["A", "C"]}, [0]),  # Combined filter
        ({"code": ["do_not_exist"]}, [3]),  # No matches.
    ]
)
def test_filter_rows_basic_filtering(sample_df, codelist_ids, expected_rows):
    result = v.filter_rows(sample_df, codelist_ids)
    assert list(result.index) == expected_rows


def test_filter_rows_missing_column_handling(sample_df):
    # Column not in DataFrame should be ignored
    codelist_ids = {"missing": ["1"], "code": ["1"]}
    result = v.filter_rows(sample_df, codelist_ids)
    assert list(result.index) == [0, 3]


@pytest.mark.skip(reason="Integer currently not well supported. To review")
def test_filter_rows_preserves_dtypes(sample_df):
    result = v.filter_rows(sample_df, {"code": [1, 2]})
    assert str(result["code"].dtype) == "int64"


def test_filter_rows_does_not_mutate_input(sample_df):
    original_copy = sample_df.copy()
    _ = v.filter_rows(sample_df, {"code": ["1"]})
    # Ensure original DataFrame is unchanged
    assert sample_df.equals(original_copy)


# This may not be the behavior we want. TO BE REVIEWED
def test_filter_rows_nan_values_are_not_dropped(sample_df):
    # NaN/None should not be considered invalid if column is in filter
    result = v.filter_rows(sample_df, {"code": ["1"]})
    # Row with None in 'code' should remain
    assert 3 in result.index


def test_filter_rows_empty_dataframe():
    """Ensure filter_rows returns an empty DataFrame when called on an input with
    no rows; should not raise and result remains empty."""
    df = pd.DataFrame(columns=["code", "status"])
    result = v.filter_rows(df, {"code": ["1"]})
    assert result.empty

@pytest.mark.skip(reason="Numeric values currently not well supported. To review")
def test_filter_rows_allowed_values_as_strings(sample_df):
    # Allowed values provided as strings should match numeric column
    result = v.filter_rows(sample_df, {"code": ["1", "2"]})
    assert list(result.index) == [0, 1]

# endregion

# region Testing filter_rows()

# endregion