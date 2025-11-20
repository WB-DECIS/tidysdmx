from typeguard import TypeCheckError
from pysdmx.model import FixedValueMap, ImplicitComponentMap
import pytest
import pandas as pd
# Load tidysdmx functions
from tidysdmx.mapping import apply_fixed_value_maps, apply_implicit_component_maps

# region create fixtures
@pytest.fixture
def sample_df():
    return pd.DataFrame({"OBS_VALUE": [100, 200], "FREQ": ["M", "Q"]})

@pytest.fixture
def fixed_maps():
    return [
        FixedValueMap(target="CONF_STATUS", value="C"),
        FixedValueMap(target="OBS_CONF", value="R"),
    ]

@pytest.fixture
def implicit_maps():
    # Example source and target components
    source1 = "OBS_VALUE"
    target1 = "NEW_VALUE"
    source2 = "FREQ"
    target2 = "NEW_FREQ"
    return [
        ImplicitComponentMap(source1, target1),
        ImplicitComponentMap(source2, target2)
    ]

# endregion

# region apply_fixed_value_maps
def test_apply_fixed_value_maps_adds_columns(sample_df, fixed_maps):
    result = apply_fixed_value_maps(sample_df, fixed_maps)
    # Original columns remain
    assert all(col in result.columns for col in ["OBS_VALUE", "FREQ"])
    # New columns added
    assert all(col in result.columns for col in ["CONF_STATUS", "OBS_CONF"])
    # Values are correctly set
    assert all(result["CONF_STATUS"] == "C")
    assert all(result["OBS_CONF"] == "R")

def test_apply_fixed_value_maps_empty_maps(sample_df):
    """ Empty map should not modify input dataframe"""
    result = apply_fixed_value_maps(sample_df, [])
    # Should return unchanged DataFrame
    pd.testing.assert_frame_equal(result, sample_df)

def test_apply_fixed_value_maps_invalid_df_type(fixed_maps):
    with pytest.raises(TypeCheckError):
        apply_fixed_value_maps("not_a_df", fixed_maps)

def test_apply_fixed_value_maps_invalid_maps_type(sample_df):
    with pytest.raises(TypeCheckError):
        apply_fixed_value_maps(sample_df, "not_a_list")

def test_apply_fixed_value_maps_invalid_map_instance(sample_df):
    with pytest.raises(TypeError, match="FixedValueMap instances"):
        apply_fixed_value_maps(sample_df, [FixedValueMap("A", "B"), "invalid"])

def test_apply_fixed_value_maps_does_not_mutate_original(sample_df, fixed_maps):
    original_copy = sample_df.copy()
    _ = apply_fixed_value_maps(sample_df, fixed_maps)
    pd.testing.assert_frame_equal(sample_df, original_copy)

# endregion


# region apply_implicit_component_maps

def test_apply_maps_add_new_columns(sample_df, implicit_maps):
    """Test that new columns are added correctly from source columns."""
    result = apply_implicit_component_maps(sample_df, implicit_maps)
    assert all(col in result.columns for col in ["NEW_VALUE", "NEW_FREQ"])
    assert all(result["NEW_VALUE"] == sample_df["OBS_VALUE"])
    assert all(result["NEW_FREQ"] == sample_df["FREQ"])


def test_apply_maps_overwrite_existing_column(sample_df):
    """Test that existing columns are overwritten when target already exists."""
    maps = [ImplicitComponentMap("OBS_VALUE", "FREQ")]  # overwrite column 'FREQ'
    result = apply_implicit_component_maps(sample_df, maps)
    assert all(result["FREQ"] == sample_df["OBS_VALUE"])  # 'FREQ' should now equal 'OBS_VALUE'

# Should at least trigger a warning. But probably needs to fails entirely with helpful message.
def test_skip_missing_source_column(sample_df):
    """Test that missing source columns are skipped without error."""
    maps = [ImplicitComponentMap("MISSING", "NEW_COL")]  # 'MISSING' does not exist
    with pytest.raises(TypeError):
        apply_implicit_component_maps(sample_df, maps)

# To review: Need consistent loggin approach
def test_verbose_output(capsys, sample_df):
    """Test verbose logging for added and skipped columns."""
    maps = [
        ImplicitComponentMap("OBS_VALUE", "NEW_VALUE"),
        ImplicitComponentMap("MISSING", "NEW_COL")  # missing source
    ]
    apply_implicit_component_maps(sample_df, maps, verbose=True)
    captured = capsys.readouterr()
    assert "✅ Added column 'NEW_VALUE'" in captured.out
    assert "⚠️ Source column 'MISSING' not found" in captured.out


@pytest.mark.parametrize("invalid_df", [None, "not_a_df", 123])
def test_invalid_df_type(invalid_df, implicit_maps):
    """Test that Error is raised for invalid df type."""
    with pytest.raises(TypeCheckError):
        apply_implicit_component_maps(invalid_df, implicit_maps)


@pytest.mark.parametrize("invalid_maps", [None, "not_a_list", 123])
def test_invalid_maps_type(sample_df, invalid_maps):
    """Test that TypeCheckError is raised for invalid implicit_maps type."""
    with pytest.raises(TypeCheckError):
        apply_implicit_component_maps(sample_df, invalid_maps)


def test_invalid_map_elements(sample_df):
    """Test that TypeError is raised when list contains invalid elements."""
    invalid_maps = [ImplicitComponentMap("OBS_VALUE", "NEW_VALUE"), "bad_element"]
    with pytest.raises(TypeError, match="All elements in implicit_maps must be"):
        apply_implicit_component_maps(sample_df, invalid_maps)

# endregion