import logging

import numpy as np
import pandas as pd
import pytest
from pysdmx.model import (
    ComponentMap,
    FixedValueMap,
    ImplicitComponentMap,
    MultiComponentMap,
    MultiRepresentationMap,
    MultiValueMap,
    RepresentationMap,
    ValueMap,
)
from typeguard import TypeCheckError

# Load tidysdmx functions
from tidysdmx.mapping import (
    apply_component_map,
    apply_fixed_value_maps,
    apply_implicit_component_maps,
    apply_multi_component_map,
    map_structures,
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
        ImplicitComponentMap(source2, target2),
    ]


# endregion


class TestApplyFixedValueMaps:
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
        """Empty map should not modify input dataframe."""
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

    def test_apply_fixed_value_maps_does_not_mutate_original(
        self, sample_df, fixed_maps
    ):
        original_copy = sample_df.copy()
        _ = apply_fixed_value_maps(sample_df, fixed_maps)
        pd.testing.assert_frame_equal(sample_df, original_copy)


class TestApplyImplicitComponentMaps:
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
        assert all(
            result["FREQ"] == sample_df["OBS_VALUE"]
        )  # 'FREQ' should now equal 'OBS_VALUE'

    @pytest.mark.skip(
        reason="REVIEW FUNCTION LOGIC: Should at least trigger a warning. But "
        "probably needs to fails entirely with helpful message."
    )
    def test_skip_missing_source_column(self, sample_df):
        """Test that missing source columns are skipped without error."""
        maps = [ImplicitComponentMap("MISSING", "NEW_COL")]  # 'MISSING' does not exist
        with pytest.raises(TypeError):
            apply_implicit_component_maps(sample_df, maps)

    def test_verbose_output(self, caplog, sample_df):
        """Test verbose logging for added and skipped columns."""
        maps = [
            ImplicitComponentMap("OBS_VALUE", "NEW_VALUE"),
            ImplicitComponentMap("MISSING", "NEW_COL"),  # missing source
        ]
        with caplog.at_level(logging.INFO, logger="tidysdmx.mapping"):
            apply_implicit_component_maps(sample_df, maps, verbose=True)
        assert "Added column 'NEW_VALUE'" in caplog.text
        assert "Source column 'MISSING' not found" in caplog.text

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


@pytest.mark.integration
class TestApplyComponentMap:
    """Tests for apply_component_map using the ifpri_asti_sm fixture."""

    @pytest.fixture
    def component_map(self, ifpri_asti_sm):
        maps = ifpri_asti_sm.maps
        cm = maps[2]

        return cm

    def test_successful_mapping(self, component_map):
        """Tests that mapping is applied correctly via the ComponentMap."""
        # Prepare DataFrame with source values
        df = pd.DataFrame(
            {
                "INDICATOR": [
                    "RES_FEMALE_TOT_FTE",
                    "RES_MALE_TOT_FTE",
                    "RES_TOT_FTE",
                    "UNKNOWN",
                ]
            }
        )

        result = apply_component_map(df, component_map)
        assert "SEX" in result.columns
        assert list(result["SEX"]) == ["F", "M", "_T", np.nan]

    def test_original_dataframe_not_mutated(self, component_map):
        """Tests that the original DataFrame remains unchanged after mapping."""
        df = pd.DataFrame({"INDICATOR": ["RES_FEMALE_TOT_FTE"]})
        original_copy = df.copy()

        _ = apply_component_map(df, component_map)
        pd.testing.assert_frame_equal(df, original_copy)

    def test_verbose_output(self, component_map, caplog):
        """Tests that verbose=True logs mapping details."""
        df = pd.DataFrame({"INDICATOR": ["RES_FEMALE_TOT_FTE", "UNKNOWN"]})

        with caplog.at_level(logging.INFO, logger="tidysdmx.mapping"):
            _ = apply_component_map(df, component_map, verbose=True)
        assert "Mapped 'INDICATOR' → 'SEX'" in caplog.text
        assert "values could not be mapped" in caplog.text

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


class TestApplyComponentMapInMemory:
    """Unit tests for apply_component_map with in-memory maps (no FMR)."""

    @staticmethod
    def _component_map_with_catch_all(value_maps):
        """Build a ComponentMap (AREA -> REGION) from the given value maps."""
        return ComponentMap(
            source="AREA",
            target="REGION",
            values=RepresentationMap(
                id="RM",
                agency="WB",
                name="RM",
                source="String",
                target="String",
                maps=value_maps,
            ),
        )

    def test_default_value_catch_all_assigns_default(self):
        """A regex catch-all ValueMap assigns the default to unlisted values."""
        cm = self._component_map_with_catch_all(
            [
                ValueMap(source="FR", target="EU"),
                ValueMap(source="DE", target="EU"),
                ValueMap(source="regex:.*", target="_Z"),
            ]
        )
        df = pd.DataFrame({"AREA": ["FR", "DE", "BR"]})

        result = apply_component_map(df, cm)
        assert list(result["REGION"]) == ["EU", "EU", "_Z"]

    def test_default_value_catch_all_order_independent(self):
        """Explicit values win even when the catch-all is stored first."""
        cm = self._component_map_with_catch_all(
            [
                ValueMap(source="regex:.*", target="_Z"),  # catch-all stored first
                ValueMap(source="FR", target="EU"),
            ]
        )
        df = pd.DataFrame({"AREA": ["FR", "BR"]})

        result = apply_component_map(df, cm)
        assert list(result["REGION"]) == ["EU", "_Z"]

    def test_na_source_not_caught_by_catch_all(self):
        """Missing source values stay NaN instead of receiving the default."""
        cm = self._component_map_with_catch_all(
            [
                ValueMap(source="FR", target="EU"),
                ValueMap(source="regex:.*", target="_Z"),
            ]
        )
        df = pd.DataFrame({"AREA": ["FR", np.nan, None]})

        result = apply_component_map(df, cm)
        assert result["REGION"].iloc[0] == "EU"
        assert result["REGION"].iloc[1:].isna().all()

    def test_invalid_regex_raises_valueerror_even_when_unused(self):
        """A malformed regex pattern fails fast even if no value needs it."""
        cm = self._component_map_with_catch_all(
            [
                ValueMap(source="FR", target="EU"),
                ValueMap(source="regex:[unclosed", target="_Z"),
            ]
        )
        df = pd.DataFrame({"AREA": ["FR"]})  # fully literal-mapped

        with pytest.raises(ValueError, match="Invalid regex pattern"):
            apply_component_map(df, cm)


@pytest.mark.integration
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
        df = pd.DataFrame(
            {"AREA": ["COL", "SWZ", "COL", "SWZ"], "NOTE": ["one", "one", "two", "two"]}
        )

        result = apply_multi_component_map(df, multi_component_map)
        expected = ["RUR", "URB", "URB", "RUR"]
        assert list(result["URBANISATION"]) == expected

    def test_regex_mapping(self, multi_component_map):
        """Tests that regex rules apply when no exact match exists."""
        df = pd.DataFrame(
            {"AREA": ["COL", "SWZ", "XYZ"], "NOTE": ["three", "three", "anything"]}
        )

        result = apply_multi_component_map(df, multi_component_map)
        expected = ["_T", "_T", "_Z"]  # Matches regex rules
        assert list(result["URBANISATION"]) == expected

    def test_missing_source_columns_raises(self, multi_component_map):
        """Tests that KeyError is raised when source columns are missing."""
        df = pd.DataFrame({"AREA": ["COL"], "OTHER": ["one"]})
        with pytest.raises(KeyError) as excinfo:
            apply_multi_component_map(df, multi_component_map)
        assert "Missing source columns" in str(excinfo.value)

    @pytest.mark.skip(
        reason="Unmapped values are assigned _Z in this multi-component-map fixture"
    )
    def test_unmapped_values_are_none(self, multi_component_map):
        """Tests that unmapped rows result in None values."""
        df = pd.DataFrame({"AREA": ["AAA", "BBB"], "NOTE": ["ccc", "ddd"]})

        result = apply_multi_component_map(df, multi_component_map)
        assert result["URBANISATION"].isna().sum() == 2

    @pytest.mark.parametrize("verbose", [True, False])
    def test_verbose_flag(self, multi_component_map, caplog, verbose):
        """Tests that progress is logged at INFO only when verbose=True."""
        df = pd.DataFrame({"AREA": ["COL", "SWZ"], "NOTE": ["one", "two"]})

        with caplog.at_level(logging.INFO, logger="tidysdmx.mapping"):
            apply_multi_component_map(df, multi_component_map, verbose=verbose)
        info_messages = [r.message for r in caplog.records if r.levelname == "INFO"]
        if verbose:
            assert any("Mapped" in msg for msg in info_messages)
        else:
            assert not info_messages


class TestApplyMultiComponentMapInMemory:
    """Unit tests for apply_multi_component_map with in-memory maps (no FMR)."""

    @staticmethod
    def _multi_map_with_catch_all(multi_value_maps):
        """Build a MultiComponentMap (AREA, NOTE -> URBANISATION) from value maps."""
        return MultiComponentMap(
            source=["AREA", "NOTE"],
            target=["URBANISATION"],
            values=MultiRepresentationMap(
                id="MR",
                agency="WB",
                name="MR",
                source=["String", "String"],
                target=["String"],
                maps=multi_value_maps,
            ),
        )

    def test_default_value_catch_all_assigns_default(self):
        """A regex catch-all MultiValueMap assigns the default to unlisted tuples."""
        mcm = self._multi_map_with_catch_all(
            [
                MultiValueMap(source=["COL", "one"], target=["RUR"]),
                MultiValueMap(source=["regex:.*", "regex:.*"], target=["_Z"]),
            ]
        )
        df = pd.DataFrame({"AREA": ["COL", "XYZ"], "NOTE": ["one", "anything"]})

        result = apply_multi_component_map(df, mcm)
        assert list(result["URBANISATION"]) == ["RUR", "_Z"]

    def test_default_value_catch_all_order_independent(self):
        """Explicit tuples win even when the catch-all is stored first."""
        mcm = self._multi_map_with_catch_all(
            [
                MultiValueMap(source=["regex:.*", "regex:.*"], target=["_Z"]),
                MultiValueMap(source=["COL", "one"], target=["RUR"]),
            ]
        )
        df = pd.DataFrame({"AREA": ["COL", "XYZ"], "NOTE": ["one", "x"]})

        result = apply_multi_component_map(df, mcm)
        assert list(result["URBANISATION"]) == ["RUR", "_Z"]

    def test_na_source_not_caught_by_catch_all(self):
        """Rows with any missing source value stay NaN, bypassing the catch-all."""
        mcm = self._multi_map_with_catch_all(
            [
                MultiValueMap(source=["COL", "one"], target=["RUR"]),
                MultiValueMap(source=["regex:.*", "regex:.*"], target=["_Z"]),
            ]
        )
        df = pd.DataFrame(
            {"AREA": ["COL", np.nan, "XYZ"], "NOTE": ["one", "one", None]}
        )

        result = apply_multi_component_map(df, mcm)
        assert result["URBANISATION"].iloc[0] == "RUR"
        assert result["URBANISATION"].iloc[1:].isna().all()

    def test_all_unmapped_values_are_none(self):
        """Rows matching no rule yield None when there is no catch-all."""
        mcm = self._multi_map_with_catch_all(
            [MultiValueMap(source=["COL", "one"], target=["RUR"])]
        )
        df = pd.DataFrame({"AREA": ["AAA", "BBB"], "NOTE": ["ccc", "ddd"]})

        result = apply_multi_component_map(df, mcm)
        assert result["URBANISATION"].isna().all()

    def test_no_rules_yields_all_none(self):
        """A MultiComponentMap with no rules sets the target column to None."""
        mcm = self._multi_map_with_catch_all([])
        df = pd.DataFrame({"AREA": ["COL", "SWZ"], "NOTE": ["one", "two"]})

        result = apply_multi_component_map(df, mcm)
        assert result["URBANISATION"].isna().all()

    def test_exact_match_on_nullable_string_dtype(self):
        """Exact matching works on a nullable 'string' column with pd.NA."""
        mcm = MultiComponentMap(
            source=["GEO"],
            target=["COUNTRY"],
            values=MultiRepresentationMap(
                id="MR",
                agency="WB",
                name="MR",
                source=["String"],
                target=["String"],
                maps=[MultiValueMap(source=["BE"], target=["BEL"])],
            ),
        )
        df = pd.DataFrame({"GEO": pd.array(["BE", pd.NA, "FR"], dtype="string")})

        result = apply_multi_component_map(df, mcm)
        assert result["COUNTRY"].iloc[0] == "BEL"
        assert pd.isna(result["COUNTRY"].iloc[1])
        assert pd.isna(result["COUNTRY"].iloc[2])

    def test_regex_on_datetime_column_uses_str_semantics(self):
        """Regexes match the per-cell str() of datetime values.

        An all-midnight datetime column must stringify per cell (keeping the
        " 00:00:00" time part), not via the array-wide date-only formatter.
        """
        mcm = MultiComponentMap(
            source=["DATE"],
            target=["FLAG"],
            values=MultiRepresentationMap(
                id="MR",
                agency="WB",
                name="MR",
                source=["String"],
                target=["String"],
                maps=[
                    MultiValueMap(
                        source=[r"regex:2020-01-01 00:00:00"], target=["MATCHED"]
                    )
                ],
            ),
        )
        df = pd.DataFrame(
            {"DATE": [pd.Timestamp("2020-01-01"), pd.Timestamp("2021-03-05")]}
        )

        result = apply_multi_component_map(df, mcm)
        assert result["FLAG"].iloc[0] == "MATCHED"
        assert pd.isna(result["FLAG"].iloc[1])

    def test_multiple_regex_rules_on_same_column(self):
        """Several regex rules on one column resolve with first-match-wins."""
        mcm = self._multi_map_with_catch_all(
            [
                MultiValueMap(source=["regex:C.*", "regex:one|two"], target=["C_"]),
                MultiValueMap(source=["regex:.*L", "regex:one|two"], target=["_L"]),
            ]
        )
        df = pd.DataFrame(
            {"AREA": ["COL", "BEL", "COL"], "NOTE": ["one", "two", "three"]}
        )

        result = apply_multi_component_map(df, mcm)
        # "COL"/"one" matches both rules; the first stored rule wins.
        assert list(result["URBANISATION"]) == ["C_", "_L", None]


@pytest.mark.integration
class TestMapStructures:
    """Tests for map_structures function."""

    def test_full_mapping_pipeline(self, ifpri_asti_sm):
        """Tests that all mapping components are applied correctly."""
        df = pd.DataFrame(
            {
                "TIME_PERIOD": ["2020", "2021"],
                "OBS_VALUE": [100, 200],
                "INDICATOR": ["RES_FEMALE_TOT_FTE", "RES_MALE_TOT_FTE"],
                "AREA": ["COL", "SWZ"],
                "NOTE": ["one", "two"],
            }
        )

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
        df = pd.DataFrame(
            {
                "TIME_PERIOD": ["2020"],
                "OBS_VALUE": [150],
                "INDICATOR": ["UNKNOWN_INDICATOR"],
                "AREA": ["COL"],
                "NOTE": ["one"],
            }
        )

        result = map_structures(df, ifpri_asti_sm)
        assert pd.isna(result["SEX"].iloc[0])

    def test_multi_component_regex_rule(self, ifpri_asti_sm):
        """Tests regex-based mapping in MultiComponentMap."""
        df = pd.DataFrame(
            {
                "TIME_PERIOD": ["2020"],
                "OBS_VALUE": [300],
                "INDICATOR": ["RES_TOT_FTE"],
                "AREA": ["COL"],
                "NOTE": ["anything"],
            }
        )

        result = map_structures(df, ifpri_asti_sm)
        assert result["URBANISATION"].iloc[0] == "_T"

    def test_missing_source_column_raises(self, ifpri_asti_sm):
        """Tests that KeyError is raised when a required source column is missing."""
        df = pd.DataFrame(
            {
                "TIME_PERIOD": ["2020"],
                "OBS_VALUE": [100],
                # Missing INDICATOR column
                "AREA": ["COL"],
                "NOTE": ["one"],
            }
        )

        with pytest.raises(KeyError):
            map_structures(df, ifpri_asti_sm)

    @pytest.mark.parametrize("verbose", [True, False])
    def test_verbose_flag(self, ifpri_asti_sm, caplog, verbose):
        """Tests that progress is logged at INFO only when verbose=True."""
        df = pd.DataFrame(
            {
                "TIME_PERIOD": ["2020"],
                "OBS_VALUE": [100],
                "INDICATOR": ["RES_FEMALE_TOT_FTE"],
                "AREA": ["COL"],
                "NOTE": ["one"],
            }
        )

        with caplog.at_level(logging.INFO, logger="tidysdmx.mapping"):
            map_structures(df, ifpri_asti_sm, verbose=verbose)
        info_messages = [r.message for r in caplog.records if r.levelname == "INFO"]
        if verbose:
            assert any("Applied" in msg for msg in info_messages)
        else:
            assert not info_messages
