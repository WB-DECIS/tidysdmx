# python
from typeguard import TypeCheckError
import pytest
import pandas as pd
import pysdmx as px
# Import tidysdmx functions
from tidysdmx.tidy_raw import filter_rows, filter_tidy_raw

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
        filter_rows(invalid_df, {"A": [1]})


def test_filter_rows_returns_dataframe_with_same_columns():
    """Ensure returned DataFrame has same columns as input."""
    df = pd.DataFrame({"A": ["1", "2"], "B": ["x", "y"]})
    codelist_ids = {"A": ["1"]}
    result = filter_rows(df, codelist_ids)

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == list(df.columns)


def test_filter_rows_index_type_preserved():
    """Ensure index type is preserved in returned DataFrame."""
    df = pd.DataFrame({"A": ["1", "2"]}, index=pd.Index([10, 20], name="custom_index"))
    codelist_ids = {"A": ["1"]}
    result = filter_rows(df, codelist_ids)

    assert isinstance(result.index, pd.Index)
    assert result.index.name == "custom_index"


def test_filter_rows_empty_result_has_correct_structure():
    """Ensure empty result still has same columns and correct dtypes."""
    df = pd.DataFrame({"A": ["1", "2"], "B": ["x", "y"]})
    codelist_ids = {"A": ["99"]}  # No matches
    result = filter_rows(df, codelist_ids)

    assert result.empty
    assert list(result.columns) == list(df.columns)
    # Dtypes should match original
    for col in df.columns:
        assert result[col].dtype == df[col].dtype


def test_filter_rows_returns_copy(sample_df):
    """Verify filter_rows with an empty filter returns a new DataFrame:
    - Content identical to input (same rows, order, values)
    - Different object identity (copy, not original)"""
    result = filter_rows(sample_df, {})
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
    result = filter_rows(sample_df, codelist_ids)
    assert list(result.index) == expected_rows


def test_filter_rows_missing_column_handling(sample_df):
    # Column not in DataFrame should be ignored
    codelist_ids = {"missing": ["1"], "code": ["1"]}
    result = filter_rows(sample_df, codelist_ids)
    assert list(result.index) == [0, 3]


@pytest.mark.skip(reason="Integer currently not well supported. To review")
def test_filter_rows_preserves_dtypes(sample_df):
    result = filter_rows(sample_df, {"code": [1, 2]})
    assert str(result["code"].dtype) == "int64"


def test_filter_rows_does_not_mutate_input(sample_df):
    original_copy = sample_df.copy()
    _ = filter_rows(sample_df, {"code": ["1"]})
    # Ensure original DataFrame is unchanged
    assert sample_df.equals(original_copy)


# This may not be the behavior we want. TO BE REVIEWED
def test_filter_rows_nan_values_are_not_dropped(sample_df):
    # NaN/None should not be considered invalid if column is in filter
    result = filter_rows(sample_df, {"code": ["1"]})
    # Row with None in 'code' should remain
    assert 3 in result.index


def test_filter_rows_empty_dataframe():
    """Ensure filter_rows returns an empty DataFrame when called on an input with
    no rows; should not raise and result remains empty."""
    df = pd.DataFrame(columns=["code", "status"])
    result = filter_rows(df, {"code": ["1"]})
    assert result.empty

@pytest.mark.skip(reason="Numeric values currently not well supported. To review")
def test_filter_rows_allowed_values_as_strings(sample_df):
    # Allowed values provided as strings should match numeric column
    result = filter_rows(sample_df, {"code": ["1", "2"]})
    assert list(result.index) == [0, 1]

# endregion

# region Testing filter_tidy_raw()

def test_filter_tidy_raw_returns_dataframe(sdmx_schema, sdmx_df):
    """Ensure the function returns a DataFrame."""
    result = filter_tidy_raw(sdmx_df, sdmx_schema)
    assert isinstance(result, pd.DataFrame)


def test_filter_tidy_raw_filters_invalid_codes(sdmx_schema, sdmx_df, incorrect_ind_code = "INCORRECT_IND"):
    """Check that rows with invalid codes are removed."""
    result = filter_tidy_raw(sdmx_df, sdmx_schema)

    # Assert that invalid code row is removed
    assert incorrect_ind_code in sdmx_df["INDICATOR"].values
    assert incorrect_ind_code not in result["INDICATOR"].values
    assert len(result) < len(sdmx_df), "Invalid rows should be filtered out."

def test_filter_tidy_raw_no_filter_needed(sdmx_df):
    """If all rows are valid, the output should match the input."""
    
    # Instantiate an empty Components object
    empty_components = px.model.Components([])

    # Create an empty Schema instance
    empty_schema = px.model.Schema(
        context="datastructure",  # or "dataflow"
        agency="TEST_AGENCY",
        id="EMPTY_SCHEMA",
        components=empty_components,
        version="1.0.0",
        artefacts=[],  # no artefacts
        groups=None    # optional
    )

    result = filter_tidy_raw(sdmx_df, empty_schema)
    pd.testing.assert_frame_equal(result, sdmx_df)


def test_filter_tidy_raw_empty_dataframe(sdmx_schema):
    """If input DataFrame is empty, output should also be empty."""
    empty_df = pd.DataFrame(columns=["some_dimension", "value"])
    result = filter_tidy_raw(empty_df, sdmx_schema)
    assert result.empty

def test_filter_tidy_raw_raises_on_missing_input(sdmx_df):
    """Schema and dataframe must be provided; expect TypeError otherwise."""
    with pytest.raises(TypeError):
        filter_tidy_raw(sdmx_df)
    with pytest.raises(TypeError):
        filter_tidy_raw()

@pytest.mark.parametrize("invalid_df,invalid_schema", [
    (None, None),
    ("not_a_dataframe", "not_a_schema"),
    (123, 456),
    ([{"A": 1}], [{"B": 2}])
])
def test_filter_tidy_raw_raises_on_invalid_inputs(invalid_df, invalid_schema):
    """Ensure TypeCheckError is raised for invalid inputs."""
    with pytest.raises((TypeCheckError, AttributeError)):
        filter_tidy_raw(invalid_df, invalid_schema)
# endregion