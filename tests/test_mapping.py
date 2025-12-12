from typeguard import TypeCheckError
from pysdmx.model import FixedValueMap, ImplicitComponentMap, MultiComponentMap, MultiRepresentationMap, MultiValueMap, ComponentMap
import pytest
import pandas as pd
import numpy as np
# Load tidysdmx functions
from tidysdmx.mapping import (
    apply_fixed_value_maps, 
    apply_implicit_component_maps, 
    apply_component_map, 
    apply_multi_component_map, 
    map_structures
    )

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

class TestApplyFixedValueMaps: #noqa: D101
    def test_apply_fixed_value_maps_adds_columns(self, sample_df, fixed_maps):
        result = apply_fixed_value_maps(sample_df, fixed_maps)
        # Original columns remain
        assert all(col in result.columns for col in ["OBS_VALUE", "FREQ"])
        # New columns added
        assert all(col in result.columns for col in ["CONF_STATUS", "OBS_CONF"])
        # Values are correctly set
        assert all(result["CONF_STATUS"] == "C")
        assert all(result["OBS_CONF"] == "R")

    def test_apply_fixed_value_maps_empty_maps(self, sample_df):
        """Empty map should not modify input dataframe"""
        result = apply_fixed_value_maps(sample_df, [])
        # Should return unchanged DataFrame
        pd.testing.assert_frame_equal(result, sample_df)

    def test_apply_fixed_value_maps_invalid_df_type(self, fixed_maps):
        with pytest.raises(TypeCheckError):
            apply_fixed_value_maps("not_a_df", fixed_maps)

    def test_apply_fixed_value_maps_invalid_maps_type(self, sample_df):
        with pytest.raises(TypeCheckError):
            apply_fixed_value_maps(sample_df, "not_a_list")

    def test_apply_fixed_value_maps_invalid_map_instance(self, sample_df):
        with pytest.raises(TypeError, match="FixedValueMap instances"):
            apply_fixed_value_maps(sample_df, [FixedValueMap("A", "B"), "invalid"])

    def test_apply_fixed_value_maps_does_not_mutate_original(self, sample_df, 
                                                             fixed_maps):
        original_copy = sample_df.copy()
        _ = apply_fixed_value_maps(sample_df, fixed_maps)
        pd.testing.assert_frame_equal(sample_df, original_copy)

class TestApplyImplicitComponentMaps: #noqa: D101
    def test_apply_maps_add_new_columns(self, sample_df, implicit_maps):
        """Test that new columns are added correctly from source columns."""
        result = apply_implicit_component_maps(sample_df, implicit_maps)
        assert all(col in result.columns for col in ["NEW_VALUE", "NEW_FREQ"])
        assert all(result["NEW_VALUE"] == sample_df["OBS_VALUE"])
        assert all(result["NEW_FREQ"] == sample_df["FREQ"])


    def test_apply_maps_overwrite_existing_column(self, sample_df):
        """Test that existing columns are overwritten when target already exists."""
        maps = [ImplicitComponentMap("OBS_VALUE", "FREQ")]  # overwrite column 'FREQ'
        result = apply_implicit_component_maps(sample_df, maps)
        assert all(result["FREQ"] == sample_df["OBS_VALUE"])  # 'FREQ' should now equal 'OBS_VALUE'

    @pytest.mark.skip(reason="REVIEW FUNCTION LOGIC: Should at least trigger a warning. But probably needs to fails entirely with helpful message.")
    def test_skip_missing_source_column(self, sample_df):
        """Test that missing source columns are skipped without error."""
        maps = [ImplicitComponentMap("MISSING", "NEW_COL")]  # 'MISSING' does not exist
        with pytest.raises(TypeError):
            apply_implicit_component_maps(sample_df, maps)

    # To review: Need consistent loggin approach
    def test_verbose_output(self, capsys, sample_df):
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
    def test_invalid_df_type(self, invalid_df, implicit_maps):
        """Test that Error is raised for invalid df type."""
        with pytest.raises(TypeCheckError):
            apply_implicit_component_maps(invalid_df, implicit_maps)


    @pytest.mark.parametrize("invalid_maps", [None, "not_a_list", 123])
    def test_invalid_maps_type(self, sample_df, invalid_maps):
        """Test that TypeCheckError is raised for invalid implicit_maps type."""
        with pytest.raises(TypeCheckError):
            apply_implicit_component_maps(sample_df, invalid_maps)

    def test_invalid_map_elements(self, sample_df):
        """Test that TypeError is raised when list contains invalid elements."""
        invalid_maps = [ImplicitComponentMap("OBS_VALUE", "NEW_VALUE"), "bad_element"]
        with pytest.raises(TypeError, match="All elements in implicit_maps must be"):
            apply_implicit_component_maps(sample_df, invalid_maps)

class TestApplyComponentMap: #noqa: D101
    """Tests for the apply_component_map function using the existing ifpri_asti_sm fixture."""
    @pytest.fixture
    def component_map(self, ifpri_asti_sm):
        maps=ifpri_asti_sm.maps
        cm=maps[2]

        return(cm)

    def test_successful_mapping(self, component_map):
        """Tests that mapping is applied correctly using ComponentMap from component_map."""
        # Prepare DataFrame with source values
        df = pd.DataFrame({
            "INDICATOR": ["RES_FEMALE_TOT_FTE", "RES_MALE_TOT_FTE", "RES_TOT_FTE", "UNKNOWN"]
        })

        result = apply_component_map(df, component_map)
        assert "SEX" in result.columns
        assert list(result["SEX"]) == ["F", "M", "_T", np.nan]

    def test_original_dataframe_not_mutated(self, component_map):
        """Tests that the original DataFrame remains unchanged after mapping."""
        df = pd.DataFrame({"INDICATOR": ["RES_FEMALE_TOT_FTE"]})
        original_copy = df.copy()

        _ = apply_component_map(df, component_map)
        pd.testing.assert_frame_equal(df, original_copy)

    def test_verbose_output(self, component_map, capsys):
        """Tests that verbose=True prints mapping details."""
        df = pd.DataFrame({"INDICATOR": ["RES_FEMALE_TOT_FTE", "UNKNOWN"]})

        _ = apply_component_map(df, component_map, verbose=True)
        captured = capsys.readouterr()
        assert "Mapped 'INDICATOR' → 'SEX'" in captured.out
        assert "values could not be mapped" in captured.out

    def test_missing_source_column_raises_keyerror(self, component_map):
        """Tests that KeyError is raised when source column is missing."""
        df_missing = pd.DataFrame({"OTHER": ["RES_FEMALE_TOT_FTE"]})

        with pytest.raises(KeyError, match="Source column 'INDICATOR' not found"):
            apply_component_map(df_missing, component_map)

    def test_invalid_dataframe_type_raises_typeerror(self, component_map):
        """Tests that TypeError is raised when df is not a pandas DataFrame."""
        with pytest.raises(TypeCheckError):
            apply_component_map(["not", "a", "df"], component_map)

    def test_invalid_component_map_type_raises_typeerror(self):
        """Tests that TypeError is raised when component_map is not a ComponentMap."""
        df = pd.DataFrame({"INDICATOR": ["RES_FEMALE_TOT_FTE"]})

        with pytest.raises(TypeCheckError):
            apply_component_map(df, "not_a_component_map")

    def test_unmapped_values_are_nan(self, component_map):
        """Tests that unmapped values in source column become NaN in target column."""
        df = pd.DataFrame({"INDICATOR": ["UNKNOWN"]})

        result = apply_component_map(df, component_map)
        assert pd.isna(result["SEX"]).all()

class TestApplyMultiComponentMap:
    """Tests for apply_multi_component_map function."""
    
    @pytest.fixture
    def multi_component_map(self, ifpri_asti_sm):
        """Fixture providing a MultiComponentMap for Urbanisation mapping."""
        maps = ifpri_asti_sm.maps
        cm = maps[4]
        return cm

    def test_basic_mapping(self, multi_component_map):
        """Tests that rows are correctly mapped based on exact matches."""
        df = pd.DataFrame({
            "AREA": ["COL", "SWZ", "COL", "SWZ"],
            "NOTE": ["one", "one", "two", "two"]
        })

        result = apply_multi_component_map(df, multi_component_map)
        expected = ["RUR", "URB", "URB", "RUR"]
        assert list(result["URBANISATION"]) == expected

    def test_regex_mapping(self, multi_component_map):
        """Tests that regex rules apply when no exact match exists."""
        df = pd.DataFrame({
            "AREA": ["COL", "SWZ", "XYZ"],
            "NOTE": ["three", "three", "anything"]
        })

        result = apply_multi_component_map(df, multi_component_map)
        expected = ["_T", "_T", "_Z"]  # Matches regex rules
        assert list(result["URBANISATION"]) == expected

    def test_missing_source_columns_raises(self, multi_component_map):
        """Tests that KeyError is raised when source columns are missing."""
        df = pd.DataFrame({"AREA": ["COL"], "OTHER": ["one"]})
        with pytest.raises(KeyError) as excinfo:
            apply_multi_component_map(df, multi_component_map)
        assert "Missing source columns" in str(excinfo.value)
    
    @pytest.mark.skip(reason="Unmapped values are assigned _Z in this multi-component-map fixture")
    def test_unmapped_values_are_none(self, multi_component_map):
        """Tests that unmapped rows result in None values."""
        df = pd.DataFrame({
            "AREA": ["AAA", "BBB"],
            "NOTE": ["ccc", "ddd"]
        })

        result = apply_multi_component_map(df, multi_component_map)
        assert result["URBANISATION"].isna().sum() == 2

    @pytest.mark.parametrize("verbose", [True, False])
    def test_verbose_flag(self, multi_component_map, capsys, verbose):
        """Tests that verbose flag prints logs when True."""
        df = pd.DataFrame({
            "AREA": ["COL", "SWZ"],
            "NOTE": ["one", "two"]
        })

        apply_multi_component_map(df, multi_component_map, verbose=verbose)
        captured = capsys.readouterr()
        if verbose:
            assert "Mapped" in captured.out
        else:
            assert captured.out == ""


class TestMapStructures:
    """Tests for map_structures function."""

    def test_full_mapping_pipeline(self, ifpri_asti_sm):
        """Tests that all mapping components are applied correctly."""
        df = pd.DataFrame({
            "TIME_PERIOD": ["2020", "2021"],
            "OBS_VALUE": [100, 200],
            "INDICATOR": ["RES_FEMALE_TOT_FTE", "RES_MALE_TOT_FTE"],
            "AREA": ["COL", "SWZ"],
            "NOTE": ["one", "two"]
        })

        result = map_structures(df, ifpri_asti_sm)

        # Check implicit mappings
        assert "TIME_PERIOD" in result.columns
        assert "OBS_VALUE" in result.columns

        # Check ComponentMap mapping for SEX
        assert list(result["SEX"]) == ["F", "M"]

        # Check MultiComponentMap mapping for URBANISATION
        assert list(result["URBANISATION"]) == ["RUR", "RUR"]

        # Check FixedValueMap columns
        for col in ["COMP_BREAKDOWN_1", "COMP_BREAKDOWN_2", "COMP_BREAKDOWN_3"]:
            assert all(result[col] == "_Z")

    def test_unmapped_indicator_results_in_nan(self, ifpri_asti_sm):
        """Tests that unmapped indicator values result in NaN in SEX column."""
        df = pd.DataFrame({
            "TIME_PERIOD": ["2020"],
            "OBS_VALUE": [150],
            "INDICATOR": ["UNKNOWN_INDICATOR"],
            "AREA": ["COL"],
            "NOTE": ["one"]
        })

        result = map_structures(df, ifpri_asti_sm)
        assert pd.isna(result["SEX"].iloc[0])

    def test_multi_component_regex_rule(self, ifpri_asti_sm):
        """Tests regex-based mapping in MultiComponentMap."""
        df = pd.DataFrame({
            "TIME_PERIOD": ["2020"],
            "OBS_VALUE": [300],
            "INDICATOR": ["RES_TOT_FTE"],
            "AREA": ["COL"],
            "NOTE": ["anything"]
        })

        result = map_structures(df, ifpri_asti_sm)
        assert result["URBANISATION"].iloc[0] == "_T"

    def test_missing_source_column_raises(self, ifpri_asti_sm):
        """Tests that KeyError is raised when a required source column is missing."""
        df = pd.DataFrame({
            "TIME_PERIOD": ["2020"],
            "OBS_VALUE": [100],
            # Missing INDICATOR column
            "AREA": ["COL"],
            "NOTE": ["one"]
        })

        with pytest.raises(KeyError):
            map_structures(df, ifpri_asti_sm)

    @pytest.mark.parametrize("verbose", [True, False])
    def test_verbose_flag(self, ifpri_asti_sm, capsys, verbose):
        """Tests that verbose flag prints logs when True."""
        df = pd.DataFrame({
            "TIME_PERIOD": ["2020"],
            "OBS_VALUE": [100],
            "INDICATOR": ["RES_FEMALE_TOT_FTE"],
            "AREA": ["COL"],
            "NOTE": ["one"]
        })

        map_structures(df, ifpri_asti_sm, verbose=verbose)
        captured = capsys.readouterr()
        if verbose:
            assert "Applied" in captured.out
        else:
            assert captured.out == ""