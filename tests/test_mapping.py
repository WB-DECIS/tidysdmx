import datetime
import functools
import logging
import re
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest
from pysdmx.model import (
    ComponentMap,
    DatePatternMap,
    FixedValueMap,
    ImplicitComponentMap,
    MultiComponentMap,
    MultiRepresentationMap,
    MultiValueMap,
    RepresentationMap,
    StructureMap,
    ValueMap,
)
from typeguard import TypeCheckError

# Load tidysdmx functions
from tidysdmx import mapping as mapping_module
from tidysdmx.mapping import (
    apply_component_map,
    apply_fixed_value_maps,
    apply_implicit_component_maps,
    apply_multi_component_map,
    map_structures,
)
from tidysdmx.structures import build_structure_map_from_template_wb


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

    def test_apply_implicit_component_maps_reads_input_columns(self, sample_df):
        """Each map copies from the input, so two maps can swap columns."""
        maps = [
            ImplicitComponentMap("OBS_VALUE", "FREQ"),
            ImplicitComponentMap("FREQ", "OBS_VALUE"),
        ]
        result = apply_implicit_component_maps(sample_df, maps)
        expected = pd.DataFrame({"OBS_VALUE": ["M", "Q"], "FREQ": [100, 200]})
        pd.testing.assert_frame_equal(result[["OBS_VALUE", "FREQ"]], expected)

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


# Only the tests that use the ifpri_asti_sm cassette are integration-marked
# (TEST-02); those that build their maps inline run in the unit lane.
class TestApplyComponentMap:
    """Tests for apply_component_map."""

    @pytest.fixture
    def component_map(self, ifpri_asti_sm):
        maps = ifpri_asti_sm.maps
        cm = maps[2]

        return cm

    @pytest.mark.integration
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

    @pytest.mark.integration
    def test_original_dataframe_not_mutated(self, component_map):
        """Tests that the original DataFrame remains unchanged after mapping."""
        df = pd.DataFrame({"INDICATOR": ["RES_FEMALE_TOT_FTE"]})
        original_copy = df.copy()

        _ = apply_component_map(df, component_map)
        pd.testing.assert_frame_equal(df, original_copy)

    @pytest.mark.integration
    def test_verbose_output(self, component_map, caplog):
        """Tests that verbose=True logs mapping details."""
        df = pd.DataFrame({"INDICATOR": ["RES_FEMALE_TOT_FTE", "UNKNOWN"]})

        with caplog.at_level(logging.INFO, logger="tidysdmx.mapping"):
            _ = apply_component_map(df, component_map, verbose=True)
        assert "Mapped 'INDICATOR' → 'SEX'" in caplog.text
        assert "values could not be mapped" in caplog.text

    @pytest.mark.integration
    def test_missing_source_column_raises_keyerror(self, component_map):
        """Tests that KeyError is raised when source column is missing."""
        df_missing = pd.DataFrame({"OTHER": ["RES_FEMALE_TOT_FTE"]})

        with pytest.raises(KeyError, match="Source column 'INDICATOR' not found"):
            apply_component_map(df_missing, component_map)

    @pytest.mark.integration
    def test_invalid_dataframe_type_raises_typeerror(self, component_map):
        """Tests that TypeError is raised when df is not a pandas DataFrame."""
        with pytest.raises(TypeCheckError):
            apply_component_map(["not", "a", "df"], component_map)

    def test_invalid_component_map_type_raises_typeerror(self):
        """Tests that TypeError is raised when component_map is not a ComponentMap."""
        df = pd.DataFrame({"INDICATOR": ["RES_FEMALE_TOT_FTE"]})

        with pytest.raises(TypeCheckError):
            apply_component_map(df, "not_a_component_map")

    @pytest.mark.integration
    def test_unmapped_values_are_nan(self, component_map):
        """Tests that unmapped values in source column become NaN in target column."""
        df = pd.DataFrame({"INDICATOR": ["UNKNOWN"]})

        result = apply_component_map(df, component_map)
        assert pd.isna(result["SEX"]).all()

    @staticmethod
    def _component_map_with_catch_all(value_maps):
        """Build a ComponentMap (AREA -> REGION) from the given value maps."""
        return ComponentMap(
            source="AREA",
            target="REGION",
            values=RepresentationMap(
                id="RM",
                agency="WB",
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

    def test_missing_source_gets_default(self):
        """Missing source values receive the catch-all (DEFAULT_VALUE) target."""
        cm = self._component_map_with_catch_all(
            [
                ValueMap(source="FR", target="EU"),
                ValueMap(source="regex:.*", target="_Z"),
            ]
        )
        df = pd.DataFrame({"AREA": ["FR", np.nan, None]})

        result = apply_component_map(df, cm)
        assert list(result["REGION"]) == ["EU", "_Z", "_Z"]

    def test_missing_source_skips_non_catch_all_regex(self):
        """A missing value is not matched as "nan" by a narrower regex."""
        cm = self._component_map_with_catch_all(
            [
                ValueMap(source="regex:n.*", target="N_"),
                ValueMap(source="regex:.*", target="_Z"),
            ]
        )
        df = pd.DataFrame({"AREA": ["nl", np.nan]})

        result = apply_component_map(df, cm)
        assert list(result["REGION"]) == ["N_", "_Z"]

    def test_missing_source_default_logs_warning(self, caplog):
        """A warning counts the missing source values given the default."""
        cm = self._component_map_with_catch_all(
            [
                ValueMap(source="FR", target="EU"),
                ValueMap(source="regex:.*", target="_Z"),
            ]
        )
        df = pd.DataFrame({"AREA": ["FR", np.nan, None, "BR"]})

        with caplog.at_level(logging.WARNING, logger="tidysdmx.mapping"):
            apply_component_map(df, cm)
        assert "2 missing source value(s) in 'AREA' assigned the default '_Z'." in (
            caplog.messages
        )


# Only the tests that use the ifpri_asti_sm cassette are integration-marked
# (TEST-02); those that build their maps inline run in the unit lane.
class TestApplyMultiComponentMap:
    """Tests for apply_multi_component_map function."""

    @pytest.fixture
    def multi_component_map(self, ifpri_asti_sm):
        """Fixture providing a MultiComponentMap for Urbanisation mapping."""
        maps = ifpri_asti_sm.maps
        cm = maps[4]
        return cm

    @pytest.mark.integration
    def test_basic_mapping(self, multi_component_map):
        """Tests that rows are correctly mapped based on exact matches."""
        df = pd.DataFrame(
            {"AREA": ["COL", "SWZ", "COL", "SWZ"], "NOTE": ["one", "one", "two", "two"]}
        )

        result = apply_multi_component_map(df, multi_component_map)
        expected = ["RUR", "URB", "URB", "RUR"]
        assert list(result["URBANISATION"]) == expected

    @pytest.mark.integration
    def test_regex_mapping(self, multi_component_map):
        """Tests that regex rules apply when no exact match exists."""
        df = pd.DataFrame(
            {"AREA": ["COL", "SWZ", "XYZ"], "NOTE": ["three", "three", "anything"]}
        )

        result = apply_multi_component_map(df, multi_component_map)
        expected = ["_T", "_T", "_Z"]  # Matches regex rules
        assert list(result["URBANISATION"]) == expected

    @pytest.mark.integration
    def test_missing_source_columns_raises(self, multi_component_map):
        """Tests that KeyError is raised when source columns are missing."""
        df = pd.DataFrame({"AREA": ["COL"], "OTHER": ["one"]})
        with pytest.raises(KeyError) as excinfo:
            apply_multi_component_map(df, multi_component_map)
        assert "Missing source columns" in str(excinfo.value)

    @pytest.mark.integration
    @pytest.mark.skip(
        reason="Unmapped values are assigned _Z in this multi-component-map fixture"
    )
    def test_unmapped_values_are_none(self, multi_component_map):
        """Tests that unmapped rows result in None values."""
        df = pd.DataFrame({"AREA": ["AAA", "BBB"], "NOTE": ["ccc", "ddd"]})

        result = apply_multi_component_map(df, multi_component_map)
        assert result["URBANISATION"].isna().sum() == 2

    @pytest.mark.integration
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

    @staticmethod
    def _multi_map_with_catch_all(multi_value_maps):
        """Build a MultiComponentMap (AREA, NOTE -> URBANISATION) from value maps."""
        return MultiComponentMap(
            source=["AREA", "NOTE"],
            target=["URBANISATION"],
            values=MultiRepresentationMap(
                id="MR",
                agency="WB",
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

    def test_missing_source_gets_default(self):
        """Rows with any missing source value receive the catch-all target."""
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
        assert list(result["URBANISATION"]) == ["RUR", "_Z", "_Z"]

    def test_missing_source_skips_partial_regex(self):
        """A rule that is only partly "regex:.*" does not match a missing value."""
        mcm = self._multi_map_with_catch_all(
            [
                MultiValueMap(source=["regex:.*", "one"], target=["ANY1"]),
                MultiValueMap(source=["regex:.*", "regex:.*"], target=["_Z"]),
            ]
        )
        df = pd.DataFrame({"AREA": ["COL", np.nan], "NOTE": ["one", "one"]})

        result = apply_multi_component_map(df, mcm)
        assert list(result["URBANISATION"]) == ["ANY1", "_Z"]

    def test_missing_source_without_catch_all_stays_none(self):
        """Without a catch-all, rows with a missing source value stay unmapped."""
        mcm = self._multi_map_with_catch_all(
            [MultiValueMap(source=["regex:.*", "one"], target=["ANY1"])]
        )
        df = pd.DataFrame({"AREA": ["COL", np.nan], "NOTE": ["one", "one"]})

        result = apply_multi_component_map(df, mcm)
        assert _as_list(result["URBANISATION"]) == ["ANY1", None]

    def test_missing_source_default_logs_warning(self, caplog):
        """A warning counts the rows with a missing source given the default."""
        mcm = self._multi_map_with_catch_all(
            [
                MultiValueMap(source=["COL", "one"], target=["RUR"]),
                MultiValueMap(source=["regex:.*", "regex:.*"], target=["_Z"]),
            ]
        )
        df = pd.DataFrame(
            {"AREA": ["COL", np.nan, "XYZ", None], "NOTE": ["one", "one", None, None]}
        )

        with caplog.at_level(logging.WARNING, logger="tidysdmx.mapping"):
            apply_multi_component_map(df, mcm)
        assert (
            "3 row(s) with a missing source value in ['AREA', 'NOTE'] assigned "
            "the default '_Z'." in caplog.messages
        )

    def test_catch_all_still_fullmatches_present_values(self):
        """Only missing values bypass the regex; present ones are still matched.

        "regex:.*" does not fullmatch a value holding a newline, in
        apply_component_map and here alike.
        """
        mcm = self._multi_map_with_catch_all(
            [MultiValueMap(source=["regex:.*", "regex:.*"], target=["_Z"])]
        )
        df = pd.DataFrame({"AREA": ["a\nb", None], "NOTE": ["one", "one"]})

        result = apply_multi_component_map(df, mcm)
        assert _as_list(result["URBANISATION"]) == [None, "_Z"]

    def test_short_catch_all_rule_still_rejected(self):
        """An all-"regex:.*" rule with too few patterns is malformed, not a default.

        pysdmx refuses to build such a MultiRepresentationMap, so the rule is
        handed to the resolver directly.
        """
        combinations = [pd.Series(["COL"]), pd.Series(["one"])]
        rules = [{"patterns": ["regex:.*"], "target": "_Z"}]

        with pytest.raises(ValueError, match="zip"):
            mapping_module._resolve_multi_values(combinations, rules)

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
        # "COL"/"three" matches neither and stays unmapped.
        assert result["URBANISATION"].iloc[:2].tolist() == ["C_", "_L"]
        assert pd.isna(result["URBANISATION"].iloc[2])


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


class TestMapStructuresRejectsUnusableMaps:
    """Maps that cannot be applied fail with a TypeError naming the problem."""

    REP_MAP_URN = (
        "urn:sdmx:org.sdmx.infomodel.structuremapping.RepresentationMap=WB:RM_SEX(1.0)"
    )

    def test_component_map_with_urn_values_raises(self):
        """A RepresentationMap referenced by URN has no value maps to apply."""
        df = pd.DataFrame({"SEX_RAW": ["F"]})
        cmap = ComponentMap(source="SEX_RAW", target="SEX", values=self.REP_MAP_URN)

        with pytest.raises(TypeError, match=r"FmrClient\.fetch_structure_map"):
            apply_component_map(df, cmap)

    def test_multi_component_map_with_urn_values_raises(self):
        """A MultiRepresentationMap referenced by URN has no value maps either."""
        df = pd.DataFrame({"A": ["1"], "B": ["2"]})
        mcm = MultiComponentMap(
            source=["A", "B"], target=["C"], values=self.REP_MAP_URN
        )

        with pytest.raises(TypeError, match="references its representation map"):
            apply_multi_component_map(df, mcm)

    def test_map_structures_with_urn_values_raises(self):
        """map_structures surfaces the same error instead of an AttributeError."""
        df = pd.DataFrame({"SEX_RAW": ["F"]})
        sm = StructureMap(
            id="SM",
            agency="WB",
            name="SM",
            maps=[
                ComponentMap(source="SEX_RAW", target="SEX", values=self.REP_MAP_URN)
            ],
        )

        with pytest.raises(TypeError, match="RepresentationMap=WB:RM_SEX"):
            map_structures(df, sm)

    def test_map_structures_rejects_date_pattern_map(self):
        """DatePatternMaps are not applied, so they are rejected up front."""
        df = pd.DataFrame({"DATE": ["2020-01"]})
        sm = StructureMap(
            id="SM",
            agency="WB",
            name="SM",
            maps=[
                DatePatternMap(
                    source="DATE",
                    target="TIME_PERIOD",
                    pattern="yyyy-MM",
                    frequency="M",
                )
            ],
        )

        with pytest.raises(TypeError, match=r"Unknown map type.*DatePatternMap"):
            map_structures(df, sm)


# Value pairs shared by the source-isolation tests: one source column, METRIC,
# feeds several targets, one of which is METRIC itself (issue #265).
METRIC_TO_METRIC: dict[str, str] = {"GDP_USD_MN": "GDP", "POP_TH": "POP"}
METRIC_TO_UNIT_MEASURE: dict[str, str] = {"GDP_USD_MN": "USD", "POP_TH": "PS"}
METRIC_TO_UNIT_MULT: dict[str, str] = {"GDP_USD_MN": "6", "POP_TH": "3"}


class TestMapStructuresSourceIsolation:
    """map_structures reads every map's source from the input frame (#265)."""

    @pytest.fixture
    def metric_df(self):
        return pd.DataFrame(
            {"METRIC": ["GDP_USD_MN", "POP_TH"], "REF_AREA": ["KEN", "KEN"]}
        )

    @staticmethod
    def _component_map(source, target, pairs):
        """Build a ComponentMap from a {source value: target value} dict."""
        return ComponentMap(
            source=source,
            target=target,
            values=RepresentationMap(
                id=f"RM_{source}_{target}",
                agency="WB",
                source="String",
                target="String",
                maps=[ValueMap(source=s, target=t) for s, t in pairs.items()],
            ),
        )

    @staticmethod
    def _structure_map(maps):
        return StructureMap(
            id="SM", name="SM", agency="WB", source="", target="", maps=maps
        )

    @pytest.mark.parametrize(
        "same_name_first", [True, False], ids=["same_name_first", "same_name_last"]
    )
    def test_map_structures_shared_source_feeds_every_target(
        self, metric_df, same_name_first
    ):
        """A source mapped onto its own name still feeds its other targets."""
        same_name = self._component_map("METRIC", "METRIC", METRIC_TO_METRIC)
        others = [
            self._component_map("METRIC", "UNIT_MEASURE", METRIC_TO_UNIT_MEASURE),
            self._component_map("METRIC", "UNIT_MULT", METRIC_TO_UNIT_MULT),
        ]
        maps = [same_name, *others] if same_name_first else [*others, same_name]

        result = map_structures(metric_df, self._structure_map(maps))

        expected = pd.DataFrame(
            {
                "METRIC": ["GDP", "POP"],
                "UNIT_MEASURE": ["USD", "PS"],
                "UNIT_MULT": ["6", "3"],
            }
        )
        pd.testing.assert_frame_equal(
            result[["METRIC", "UNIT_MEASURE", "UNIT_MULT"]], expected
        )

    def test_map_structures_template_shared_source(self, metric_df):
        """The #265 template: METRIC feeds METRIC and the unit targets."""
        targets = ["METRIC", "UNIT_MEASURE", "UNIT_TYPE", "UNIT_MULTIPLIER"]
        mappings = {
            "INFO": pd.DataFrame({"Key": ["dataflow"], "Value": ["WB:DF_GEM(1.0)"]}),
            "COMP_MAPPING": pd.DataFrame(
                {
                    "SOURCE": ["METRIC"] * len(targets),
                    "TARGET": targets,
                    "MAPPING_RULES": ["representation"] * len(targets),
                }
            ),
            "REP_MAPPING": pd.DataFrame(
                {
                    "S:METRIC": ["GDP_USD_MN", "POP_TH"],
                    "T:METRIC": ["GDP", "POP"],
                    "T:UNIT_MEASURE": ["USD", "PS"],
                    "T:UNIT_TYPE": ["CUR", "NUM"],
                    "T:UNIT_MULTIPLIER": ["6", "3"],
                }
            ),
        }
        structure_map = build_structure_map_from_template_wb(mappings)

        result = map_structures(metric_df, structure_map)

        expected = pd.DataFrame(
            {
                "METRIC": ["GDP", "POP"],
                "UNIT_MEASURE": ["USD", "PS"],
                "UNIT_TYPE": ["CUR", "NUM"],
                "UNIT_MULTIPLIER": ["6", "3"],
            }
        )
        pd.testing.assert_frame_equal(result[targets], expected)

    def test_map_structures_fixed_target_does_not_feed_component(self, metric_df):
        """A ComponentMap reads its source, not a fixed value written over it."""
        maps = [
            FixedValueMap(target="METRIC", value="_Z"),
            self._component_map("METRIC", "UNIT_MEASURE", METRIC_TO_UNIT_MEASURE),
        ]
        result = map_structures(metric_df, self._structure_map(maps))
        assert list(result["UNIT_MEASURE"]) == ["USD", "PS"]

    def test_map_structures_implicit_target_does_not_feed_component(self, metric_df):
        """A ComponentMap reads its source, not an implicit copy written over it."""
        maps = [
            ImplicitComponentMap("REF_AREA", "METRIC"),
            self._component_map("METRIC", "UNIT_MEASURE", METRIC_TO_UNIT_MEASURE),
        ]
        result = map_structures(metric_df, self._structure_map(maps))
        assert list(result["UNIT_MEASURE"]) == ["USD", "PS"]

    def test_map_structures_component_target_does_not_feed_multi(self, metric_df):
        """A MultiComponentMap matches the source values, not mapped ones."""
        multi = MultiComponentMap(
            source=["METRIC", "REF_AREA"],
            target=["INDICATOR"],
            values=MultiRepresentationMap(
                id="MR",
                agency="WB",
                source=["String", "String"],
                target=["String"],
                maps=[
                    MultiValueMap(source=["GDP_USD_MN", "KEN"], target=["KEN_GDP"]),
                    MultiValueMap(source=["POP_TH", "KEN"], target=["KEN_POP"]),
                ],
            ),
        )
        maps = [self._component_map("METRIC", "METRIC", METRIC_TO_METRIC), multi]
        result = map_structures(metric_df, self._structure_map(maps))
        assert list(result["INDICATOR"]) == ["KEN_GDP", "KEN_POP"]

    def test_map_structures_last_write_wins(self, metric_df):
        """When two maps write one target, the later map type wins."""
        maps = [
            FixedValueMap(target="UNIT_MEASURE", value="_Z"),
            self._component_map("METRIC", "UNIT_MEASURE", METRIC_TO_UNIT_MEASURE),
        ]
        result = map_structures(metric_df, self._structure_map(maps))
        assert list(result["UNIT_MEASURE"]) == ["USD", "PS"]

    def test_map_structures_source_written_by_another_map_raises(self, metric_df):
        """A source that only another map produces is not in the input."""
        maps = [
            ImplicitComponentMap("METRIC", "NEW"),
            self._component_map("NEW", "UNIT_MEASURE", METRIC_TO_UNIT_MEASURE),
        ]
        with pytest.raises(KeyError, match="Source column 'NEW' not found"):
            map_structures(metric_df, self._structure_map(maps))

    def test_map_structures_does_not_mutate_input(self, metric_df):
        """The input frame is left untouched."""
        original = metric_df.copy()
        maps = [self._component_map("METRIC", "METRIC", METRIC_TO_METRIC)]
        map_structures(metric_df, self._structure_map(maps))
        pd.testing.assert_frame_equal(metric_df, original)


class TestMapStructuresTemplateDefaultValue:
    """COMP_MAPPING's DEFAULT_VALUE reaches missing source values end to end."""

    @pytest.fixture
    def structure_map(self):
        mappings = {
            "INFO": pd.DataFrame({"Key": ["dataflow"], "Value": ["WB:DF_GEM(1.0)"]}),
            "COMP_MAPPING": pd.DataFrame(
                {
                    "SOURCE": ["AREA", "FREQ|AREA"],
                    "TARGET": ["REF_AREA", "INDICATOR"],
                    "MAPPING_RULES": ["representation", "multi_representation"],
                    "DEFAULT_VALUE": ["_Z", "_X"],
                }
            ),
            "REP_MAPPING": pd.DataFrame(
                {
                    "S:AREA": ["FR", "DE"],
                    "T:REF_AREA": ["FRA", "DEU"],
                    "S:FREQ": ["A", "Q"],
                    "T:INDICATOR": ["GDP_A", "GDP_Q"],
                }
            ),
        }
        return build_structure_map_from_template_wb(mappings)

    def test_map_structures_missing_source_gets_default_value(self, structure_map):
        df = pd.DataFrame(
            {"AREA": ["FR", None, "BR", "DE"], "FREQ": ["A", "A", "A", None]}
        )

        result = map_structures(df, structure_map)

        assert result["REF_AREA"].tolist() == ["FRA", "_Z", "_Z", "DEU"]
        assert result["INDICATOR"].tolist() == ["GDP_A", "_X", "_X", "_X"]


# region row-wise oracle
# The pre-branch implementations matched every row on its own. These helpers
# restate their rules row by row, with no grouping at all, so the
# distinct-value implementation can be checked against them on inputs where
# grouping is subtle: values that compare equal but print differently.


def _as_list(series):
    """Return the series' values, with every missing value as None."""
    return [None if pd.isna(v) else v for v in series.tolist()]


def _rank(patterns):
    """Literal patterns first, then regexes, then the pure catch-all."""
    if all(p == "regex:.*" for p in patterns):
        return 2
    if any(p.startswith("regex:") for p in patterns):
        return 1
    return 0


def _row_wise_matches(column, pattern):
    """Match each value of ``column`` against one pattern on its own."""
    if pattern.startswith("regex:"):
        regex = pattern.removeprefix("regex:")
        # Series.map(str), as the row-wise implementation stringified: on a
        # nullable integer column holding NA, pandas passes 1 to str() as 1.0.
        return [
            not pd.isna(s) and re.fullmatch(regex, s) is not None
            for s in column.map(str, na_action="ignore").tolist()
        ]
    return (column == pattern).fillna(False).tolist()


def _row_wise_component(column, value_maps):
    """Apply apply_component_map's rules to each value of ``column`` alone."""
    literals = {
        vm.source: vm.target for vm in value_maps if not vm.source.startswith("regex:")
    }
    regexes = sorted(
        (vm for vm in value_maps if vm.source.startswith("regex:")),
        key=lambda vm: _rank([vm.source]),
    )
    # Only the catch-all (DEFAULT_VALUE) maps a missing value.
    default = next((vm.target for vm in regexes if _rank([vm.source]) == 2), None)
    result = []
    for value, literal in zip(
        column.tolist(), _as_list(column.map(literals)), strict=True
    ):
        if pd.isna(value):
            result.append(default)
            continue
        if literal is not None:
            result.append(literal)
            continue
        result.append(
            next(
                (
                    vm.target
                    for vm in regexes
                    if re.fullmatch(vm.source.removeprefix("regex:"), str(value))
                ),
                None,
            )
        )
    return result


def _row_wise_multi(columns, value_maps):
    """Apply apply_multi_component_map's rules to each row of ``columns`` alone."""
    rules = sorted(value_maps, key=lambda mv: _rank(mv.source))
    # The catch-all (DEFAULT_VALUE) also matches a missing value.
    hits = [
        [
            [
                hit or (_rank(mv.source) == 2 and missing)
                for hit, missing in zip(
                    _row_wise_matches(c, p), c.isna().tolist(), strict=True
                )
            ]
            for c, p in zip(columns, mv.source, strict=True)
        ]
        for mv in rules
    ]
    return [
        next(
            (
                mv.target[0]
                for mv, rule_hits in zip(rules, hits, strict=True)
                if all(column_hits[row] for column_hits in rule_hits)
            ),
            None,
        )
        for row in range(len(columns[0]))
    ]


# Every string storage pandas 3 offers. "str" is the first or the second
# depending on whether pyarrow is installed (Databricks has it), so each is
# named explicitly. Whatever the source's storage, Series.map() returns an
# Arrow-backed "str" whenever pyarrow is installed.
_STRING_DTYPES = {
    "str-python": pd.StringDtype("python", na_value=np.nan),
    "str-pyarrow": pd.StringDtype("pyarrow", na_value=np.nan),
    "string-python": pd.StringDtype("python"),
    "string-pyarrow": pd.StringDtype("pyarrow"),
    "arrow-large-string": "large_string[pyarrow]",
}

# Built fresh per test: Series are mutable.
_TRICKY_COLUMNS = {
    "object-numbers": lambda: pd.Series(
        [1, 1.0, True, "1", 2, "COL", None], dtype=object
    ),
    "object-decimals": lambda: pd.Series(
        [Decimal("1"), Decimal("1.0"), 1, None], dtype=object
    ),
    "object-zeros": lambda: pd.Series([0.0, -0.0, 0, False, np.nan], dtype=object),
    "object-dates": lambda: pd.Series(
        [
            pd.Timestamp("2020-01-01"),
            np.datetime64("2020-01-01"),
            datetime.date(2020, 1, 1),
            None,
        ],
        dtype=object,
    ),
    "float64-zeros": lambda: pd.Series([0.0, -0.0, 1.0, np.nan, -0.0]),
    "Float64-zeros": lambda: pd.Series([0.0, -0.0, 1.0, None, -0.0], dtype="Float64"),
    "float32-zeros": lambda: pd.Series([0.0, -0.0, 0.1, np.nan], dtype="float32"),
    "str": lambda: pd.Series(["COL", "BEL", None, "COL", "1"], dtype="str"),
    **{
        f"{name}-storage": functools.partial(
            pd.Series, ["COL", "BEL", None, "COL", "1"], dtype=dtype
        )
        for name, dtype in _STRING_DTYPES.items()
    },
    "category": lambda: pd.Series(
        pd.Categorical(["COL", "BEL", None, "COL"], categories=["BEL", "COL", "ZZZ"])
    ),
    "Int64": lambda: pd.Series([1, None, 1, 2], dtype="Int64"),
    "datetime": lambda: pd.Series(pd.to_datetime(["2020-01-01", None, "2020-01-01"])),
}


def _tricky_column(name, order):
    """Build a tricky column in forward or reversed order, or duplicate its index."""
    column = _TRICKY_COLUMNS[name]()
    if order == "reversed":
        return column.iloc[::-1].reset_index(drop=True)
    if order == "duplicated-index":
        return column.set_axis([i // 2 for i in range(len(column))])
    return column


_TRICKY_VALUE_MAPS = [
    ValueMap(source="COL", target="CO"),
    ValueMap(source="1", target="LIT1"),
    ValueMap(source="regex:-.*", target="NEG"),
    ValueMap(source="regex:1", target="ONE"),
    ValueMap(source="regex:2020-01-01.*", target="DATE"),
]
_CATCH_ALL = ValueMap(source="regex:.*", target="_Z")

# The tricky column comes first; "x" matches the constant second column.
_TRICKY_MULTI_VALUE_MAPS = [
    MultiValueMap(source=["COL", "x"], target=["CO"]),
    MultiValueMap(source=["1", "x"], target=["LIT1"]),
    MultiValueMap(source=["regex:-.*", "x"], target=["NEG"]),
    MultiValueMap(source=["regex:1", "regex:.*"], target=["ONE"]),
    MultiValueMap(source=["regex:2020-01-01.*", "x"], target=["DATE"]),
    MultiValueMap(source=["regex:.*", "regex:.*"], target=["_T"]),
]

# endregion


class TestMappingOnDistinctValues:
    """Maps resolve each distinct source value once, then broadcast to rows."""

    @staticmethod
    def _component_map(source, target, value_maps):
        return ComponentMap(
            source=source,
            target=target,
            values=RepresentationMap(
                id="RM",
                name="RM",
                agency="WB",
                source="urn:x",
                target="urn:y",
                maps=value_maps,
            ),
        )

    @staticmethod
    def _multi_map(value_maps):
        return MultiComponentMap(
            source=["AREA", "NOTE"],
            target=["URBANISATION"],
            values=MultiRepresentationMap(
                id="MRM",
                name="MRM",
                agency="WB",
                source=["urn:x", "urn:x2"],
                target=["urn:y"],
                maps=value_maps,
            ),
        )

    @pytest.fixture
    def area_map(self):
        return self._component_map(
            "AREA",
            "REF_AREA",
            [
                ValueMap(source="COL", target="CO"),
                ValueMap(source="regex:B.*", target="B_"),
                ValueMap(source="regex:.*", target="_Z"),
            ],
        )

    @pytest.fixture
    def urbanisation_map(self):
        return self._multi_map(
            [
                MultiValueMap(source=["COL", "one"], target=["CO1"]),
                MultiValueMap(source=["regex:B.*", "regex:.*"], target=["B_"]),
                MultiValueMap(source=["regex:.*", "regex:.*"], target=["_T"]),
            ]
        )

    @pytest.mark.parametrize("dtype", ["str", "object", "category"])
    def test_apply_component_map_repeated_values_across_dtypes(self, area_map, dtype):
        df = pd.DataFrame({"AREA": ["COL", "BEL", "COL", "KEN", "BEL"]}, dtype=dtype)
        result = apply_component_map(df, area_map)
        assert result["REF_AREA"].tolist() == ["CO", "B_", "CO", "_Z", "B_"]

    @pytest.mark.parametrize("dtype", ["str", "object", "category"])
    def test_apply_component_map_missing_source_gets_catch_all(self, area_map, dtype):
        df = pd.DataFrame({"AREA": ["COL", None, "BEL"]}, dtype=dtype)
        result = apply_component_map(df, area_map)
        assert result["REF_AREA"].tolist() == ["CO", "_Z", "B_"]

    @pytest.fixture
    def literal_area_map(self):
        return self._component_map(
            "AREA",
            "REF_AREA",
            [ValueMap(source="COL", target="CO"), ValueMap(source="BEL", target="BE")],
        )

    @pytest.mark.parametrize(
        "dtype", list(_STRING_DTYPES.values()), ids=list(_STRING_DTYPES)
    )
    def test_apply_component_map_literal_only_across_string_storages(
        self, literal_area_map, dtype
    ):
        """A literal-only result is Arrow-backed when pyarrow is installed.

        Broadcasting it through pd.api.extensions.take() raised TypeError:
        ArrowExtensionArray.take() takes no axis.
        """
        df = pd.DataFrame({"AREA": pd.Series(["COL", "BEL", "COL"], dtype=dtype)})
        result = apply_component_map(df, literal_area_map)
        assert result["REF_AREA"].tolist() == ["CO", "BE", "CO"]

    @pytest.mark.parametrize(
        "dtype", list(_STRING_DTYPES.values()), ids=list(_STRING_DTYPES)
    )
    def test_apply_component_map_literal_only_missing_stays_unmapped(
        self, literal_area_map, dtype
    ):
        df = pd.DataFrame({"AREA": pd.Series(["COL", None, "KEN"], dtype=dtype)})
        result = apply_component_map(df, literal_area_map)
        assert result["REF_AREA"].isna().tolist() == [False, True, True]

    def test_apply_component_map_regex_result_keeps_object_dtype(self, area_map):
        df = pd.DataFrame({"AREA": ["COL", "BEL", "KEN"]})
        result = apply_component_map(df, area_map)
        assert result["REF_AREA"].dtype == object

    def test_apply_component_map_keeps_row_index(self, area_map):
        df = pd.DataFrame({"AREA": ["BEL", "COL"]}, index=[10, 3])
        result = apply_component_map(df, area_map)
        assert result["REF_AREA"].to_dict() == {10: "B_", 3: "CO"}

    def test_apply_component_map_empty_frame(self, area_map):
        df = pd.DataFrame({"AREA": pd.Series([], dtype="str")})
        result = apply_component_map(df, area_map)
        assert result["REF_AREA"].tolist() == []

    @pytest.mark.parametrize(
        ("values", "expected"),
        [([1, 1.0], ["ONE", None]), ([1.0, 1], [None, "ONE"])],
        ids=["int-first", "float-first"],
    )
    def test_apply_component_map_regex_sees_each_printed_form(self, values, expected):
        """1 and 1.0 compare equal, but a regex sees "1" and "1.0", in any order."""
        cmap = self._component_map(
            "CODE", "TARGET", [ValueMap(source="regex:1", target="ONE")]
        )
        df = pd.DataFrame({"CODE": pd.Series(values, dtype=object)})
        result = apply_component_map(df, cmap)
        assert _as_list(result["TARGET"]) == expected

    @pytest.mark.parametrize(
        ("values", "expected"),
        [([0.0, -0.0], ["ZERO", "NEG"]), ([-0.0, 0.0], ["NEG", "ZERO"])],
        ids=["positive-first", "negative-first"],
    )
    def test_apply_component_map_regex_tells_signed_zeros_apart(self, values, expected):
        """0.0 and -0.0 compare equal, but a regex sees "0.0" and "-0.0"."""
        cmap = self._component_map(
            "CODE",
            "TARGET",
            [
                ValueMap(source="regex:-.*", target="NEG"),
                ValueMap(source="regex:.*", target="ZERO"),
            ],
        )
        df = pd.DataFrame({"CODE": pd.Series(values, dtype="float64")})
        result = apply_component_map(df, cmap)
        assert _as_list(result["TARGET"]) == expected

    @pytest.mark.parametrize(
        ("values", "expected"),
        [([1, 1.0], ["ONE", None]), ([1.0, 1], [None, "ONE"])],
        ids=["int-first", "float-first"],
    )
    def test_apply_multi_component_map_regex_sees_each_printed_form(
        self, values, expected
    ):
        mcm = self._multi_map([MultiValueMap(source=["regex:1", "x"], target=["ONE"])])
        df = pd.DataFrame({"AREA": pd.Series(values, dtype=object), "NOTE": "x"})
        result = apply_multi_component_map(df, mcm)
        assert _as_list(result["URBANISATION"]) == expected

    @pytest.mark.parametrize(
        ("values", "expected"),
        [([0.0, -0.0], ["ZERO", "NEG"]), ([-0.0, 0.0], ["NEG", "ZERO"])],
        ids=["positive-first", "negative-first"],
    )
    def test_apply_multi_component_map_regex_tells_signed_zeros_apart(
        self, values, expected
    ):
        mcm = self._multi_map(
            [
                MultiValueMap(source=["regex:-.*", "x"], target=["NEG"]),
                MultiValueMap(source=["regex:.*", "regex:.*"], target=["ZERO"]),
            ]
        )
        df = pd.DataFrame({"AREA": pd.Series(values, dtype="float64"), "NOTE": "x"})
        result = apply_multi_component_map(df, mcm)
        assert _as_list(result["URBANISATION"]) == expected

    @pytest.mark.parametrize(
        "catch_all", [False, True], ids=["no-catch-all", "catch-all"]
    )
    @pytest.mark.parametrize("order", ["forward", "reversed", "duplicated-index"])
    @pytest.mark.parametrize("name", list(_TRICKY_COLUMNS))
    def test_apply_component_map_matches_row_wise_rules(self, name, order, catch_all):
        column = _tricky_column(name, order)
        value_maps = _TRICKY_VALUE_MAPS + ([_CATCH_ALL] if catch_all else [])
        cmap = self._component_map("CODE", "TARGET", value_maps)
        result = apply_component_map(pd.DataFrame({"CODE": column}), cmap)
        assert _as_list(result["TARGET"]) == _row_wise_component(column, value_maps)

    @pytest.mark.parametrize("tricky_first", [True, False], ids=["first", "second"])
    @pytest.mark.parametrize("order", ["forward", "reversed", "duplicated-index"])
    @pytest.mark.parametrize("name", list(_TRICKY_COLUMNS))
    def test_apply_multi_component_map_matches_row_wise_rules(
        self, name, order, tricky_first
    ):
        tricky = _tricky_column(name, order)
        constant = pd.Series("x", index=tricky.index, dtype="str")
        columns = [tricky, constant] if tricky_first else [constant, tricky]
        value_maps = [
            mv
            if tricky_first
            else MultiValueMap(source=mv.source[::-1], target=mv.target)
            for mv in _TRICKY_MULTI_VALUE_MAPS
        ]
        df = pd.DataFrame({"AREA": columns[0], "NOTE": columns[1]})
        result = apply_multi_component_map(df, self._multi_map(value_maps))
        assert _as_list(result["URBANISATION"]) == _row_wise_multi(columns, value_maps)

    def test_apply_multi_component_map_interleaved_combinations(self, urbanisation_map):
        df = pd.DataFrame(
            {
                "AREA": ["KEN", "COL", "BEL", "COL", "KEN", "BEL"],
                "NOTE": ["two", "one", "two", "one", "two", "one"],
            }
        )
        result = apply_multi_component_map(df, urbanisation_map)
        assert result["URBANISATION"].tolist() == [
            "_T",
            "CO1",
            "B_",
            "CO1",
            "_T",
            "B_",
        ]

    @pytest.mark.parametrize("dtype", ["str", "object", "category"])
    def test_apply_multi_component_map_missing_value_gets_catch_all(
        self, urbanisation_map, dtype
    ):
        # ("BEL", None) takes the catch-all's "_T", not "B_": the partly-regex
        # rule ["regex:B.*", "regex:.*"] never matches a missing value.
        df = pd.DataFrame(
            {"AREA": ["COL", None, "BEL", "COL"], "NOTE": ["one", "one", None, "one"]},
            dtype=dtype,
        )
        result = apply_multi_component_map(df, urbanisation_map)
        assert result["URBANISATION"].tolist() == ["CO1", "_T", "_T", "CO1"]

    def test_apply_multi_component_map_keeps_row_index(self, urbanisation_map):
        df = pd.DataFrame(
            {"AREA": ["BEL", "COL"], "NOTE": ["two", "one"]}, index=[10, 3]
        )
        result = apply_multi_component_map(df, urbanisation_map)
        assert result["URBANISATION"].to_dict() == {10: "B_", 3: "CO1"}

    def test_apply_multi_component_map_empty_frame(self, urbanisation_map):
        df = pd.DataFrame(
            {"AREA": pd.Series([], dtype="str"), "NOTE": pd.Series([], dtype="str")}
        )
        result = apply_multi_component_map(df, urbanisation_map)
        assert result["URBANISATION"].tolist() == []

    def test_map_structures_does_not_deep_copy_passthrough_columns(self, area_map):
        df = pd.DataFrame({"AREA": ["COL", "BEL"], "OBS_VALUE": [1.0, 2.0]})
        sm = StructureMap(
            id="SM", name="SM", agency="WB", source="", target="", maps=[area_map]
        )
        result = map_structures(df, sm)
        assert np.shares_memory(
            result["OBS_VALUE"].to_numpy(), df["OBS_VALUE"].to_numpy()
        )

    def test_map_structures_result_edits_leave_input_untouched(self, area_map):
        df = pd.DataFrame({"AREA": ["COL", "BEL"], "OBS_VALUE": [1.0, 2.0]})
        sm = StructureMap(
            id="SM", name="SM", agency="WB", source="", target="", maps=[area_map]
        )
        result = map_structures(df, sm)
        result.loc[0, "OBS_VALUE"] = 99.0
        assert df["OBS_VALUE"].tolist() == [1.0, 2.0]

    @pytest.mark.parametrize(
        "apply",
        [
            pytest.param(
                lambda df, cmap, mcm: map_structures(
                    df,
                    StructureMap(
                        id="SM",
                        name="SM",
                        agency="WB",
                        source="",
                        target="",
                        maps=[FixedValueMap(target="FREQ", value="A")],
                    ),
                ),
                id="map_structures-fixed",
            ),
            pytest.param(
                lambda df, cmap, mcm: map_structures(
                    df,
                    StructureMap(
                        id="SM",
                        name="SM",
                        agency="WB",
                        source="",
                        target="",
                        maps=[cmap],
                    ),
                ),
                id="map_structures-component",
            ),
            pytest.param(
                lambda df, cmap, mcm: apply_fixed_value_maps(df, []),
                id="apply_fixed_value_maps-empty",
            ),
            pytest.param(
                lambda df, cmap, mcm: apply_implicit_component_maps(df, []),
                id="apply_implicit_component_maps-empty",
            ),
            pytest.param(
                lambda df, cmap, mcm: apply_component_map(df, cmap),
                id="apply_component_map",
            ),
            pytest.param(
                lambda df, cmap, mcm: apply_multi_component_map(df, mcm),
                id="apply_multi_component_map",
            ),
        ],
    )
    def test_renaming_result_axes_leaves_input_untouched(
        self, apply, area_map, urbanisation_map
    ):
        """Shallow copies get their own axis objects, so renames stay local."""
        df = pd.DataFrame(
            {"AREA": ["COL", "BEL"], "NOTE": ["one", "two"]},
            index=pd.Index([1, 2], name="orig"),
        )
        df.columns.name = "cols"
        result = apply(df, area_map, urbanisation_map)
        result.index.name = "renamed"
        result.columns.name = "renamed"
        assert (df.index.name, df.columns.name) == ("orig", "cols")

    def test_apply_multi_component_map_index_named_like_source(self, urbanisation_map):
        """An index level named like a source column does not confuse grouping."""
        df = pd.DataFrame({"AREA": ["COL", "BEL"], "NOTE": ["one", "two"]}).set_index(
            "AREA", drop=False
        )
        result = apply_multi_component_map(df, urbanisation_map)
        assert result["URBANISATION"].tolist() == ["CO1", "B_"]

    def test_map_structures_index_named_like_source(self, urbanisation_map):
        df = pd.DataFrame({"AREA": ["COL", "BEL"], "NOTE": ["one", "two"]}).set_index(
            "AREA", drop=False
        )
        sm = StructureMap(
            id="SM",
            name="SM",
            agency="WB",
            source="",
            target="",
            maps=[urbanisation_map],
        )
        result = map_structures(df, sm)
        assert result["URBANISATION"].tolist() == ["CO1", "B_"]

    def test_apply_multi_component_map_repeated_source_column(self):
        """A source listed twice is matched once per pattern position."""
        mcm = MultiComponentMap(
            source=["AREA", "AREA"],
            target=["URBANISATION"],
            values=MultiRepresentationMap(
                id="MRM",
                name="MRM",
                agency="WB",
                source=["urn:x", "urn:x2"],
                target=["urn:y"],
                maps=[
                    MultiValueMap(source=["COL", "regex:C.*"], target=["C1"]),
                    MultiValueMap(source=["regex:.*", "regex:.*"], target=["_T"]),
                ],
            ),
        )
        df = pd.DataFrame({"AREA": ["COL", "BEL"]})
        result = apply_multi_component_map(df, mcm)
        assert result["URBANISATION"].tolist() == ["C1", "_T"]

    def test_apply_multi_component_map_duplicate_index_labels(self, urbanisation_map):
        df = pd.DataFrame(
            {"AREA": ["COL", "BEL", "COL"], "NOTE": ["one", "two", "one"]},
            index=[0, 0, 1],
        )
        result = apply_multi_component_map(df, urbanisation_map)
        assert result["URBANISATION"].tolist() == ["CO1", "B_", "CO1"]

    def test_apply_multi_component_map_resolves_only_distinct_source_combinations(
        self, urbanisation_map, monkeypatch
    ):
        """Only the source columns of each distinct combination are matched."""
        resolve = mapping_module._resolve_multi_values
        seen = []

        def spy(combinations, rules):
            seen.append([len(column) for column in combinations])
            return resolve(combinations, rules)

        monkeypatch.setattr(mapping_module, "_resolve_multi_values", spy)
        df = pd.DataFrame(
            {
                "AREA": ["COL", "BEL", "COL", "KEN", "BEL", "KEN"],
                "NOTE": ["one", "two", "one", "two", "two", "two"],
                "OBS_VALUE": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            }
        )
        apply_multi_component_map(df, urbanisation_map)
        assert seen == [[3, 3]]

    def test_apply_component_map_resolves_only_distinct_values(
        self, area_map, monkeypatch
    ):
        resolve = mapping_module._resolve_component_values
        seen = []

        def spy(values, rep_map):
            seen.append(len(values))
            return resolve(values, rep_map)

        monkeypatch.setattr(mapping_module, "_resolve_component_values", spy)
        df = pd.DataFrame({"AREA": ["COL", "BEL", "COL", "KEN", "BEL", "KEN"]})
        apply_component_map(df, area_map)
        assert seen == [3]


class TestDistinctRows:
    """_distinct_rows numbers each row's distinct combination of source values."""

    @staticmethod
    def _first_rows(sources):
        """Return, for each row, the first row of its combination."""
        codes, positions = mapping_module._distinct_rows(sources)
        return positions[codes].tolist()

    def test_distinct_rows_points_each_row_at_its_first_occurrence(self):
        area = pd.Series(["x", None, "x", "y", np.nan])
        note = pd.Series([1, 1, 1, 2, 1])
        assert self._first_rows([area, note]) == [0, 1, 0, 3, 1]

    def test_distinct_rows_numbers_combinations_densely(self):
        """Code i is the combination whose first row is positions[i]."""
        area = pd.Series(["x", None, "x", "y", np.nan])
        note = pd.Series([1, 1, 1, 2, 1])
        codes, positions = mapping_module._distinct_rows([area, note])
        assert codes[positions].tolist() == list(range(len(positions)))

    def test_distinct_rows_keeps_missing_values_together(self):
        values = pd.Series([None, np.nan, 1, None], dtype=object)
        assert self._first_rows([values]) == [0, 0, 2, 0]

    def test_distinct_rows_splits_equal_values_that_print_differently(self):
        values = pd.Series([1, 1.0, True, 1, "1", 1.0], dtype=object)
        assert self._first_rows([values]) == [0, 1, 2, 0, 4, 1]

    def test_distinct_rows_splits_signed_zeros(self):
        values = pd.Series([0.0, -0.0, 0.0, -0.0])
        assert self._first_rows([values]) == [0, 1, 0, 1]

    def test_distinct_rows_groups_equal_floats(self):
        values = pd.Series([1.5, 0.0, 1.5, np.nan, 0.0, np.nan])
        assert self._first_rows([values]) == [0, 1, 0, 3, 1, 3]

    def test_distinct_rows_rejects_no_sources(self):
        with pytest.raises(ValueError, match="at least one source column"):
            mapping_module._distinct_rows([])

    def test_distinct_rows_merges_printed_forms_across_chunks(self, monkeypatch):
        monkeypatch.setattr(mapping_module, "_PRINT_FORM_CHUNK_ROWS", 2)
        values = pd.Series([1, 1.0, True, 1.0, 1, True], dtype=object)
        assert self._first_rows([values]) == [0, 1, 2, 1, 0, 2]

    def test_distinct_rows_tolerates_str_that_sets_float_flags(self):
        """A __str__ leaving the FPU's invalid flag set must not warn.

        CPython 3.14.9's str() of 1, 1.0 or True left it set, which a NumPy
        ufunc reports as "invalid value encountered" once its loop ends.
        """

        class SetsInvalidFlag:
            def __str__(self) -> str:
                float("inf") - float("inf")
                return "flagged"

        flagged = SetsInvalidFlag()
        values = pd.Series([flagged, 1, flagged], dtype=object)
        assert self._first_rows([values]) == [0, 1, 0]

    def test_distinct_rows_keeps_equal_values_together_without_regex(self):
        """Literal rules match equal values alike, so print forms are not split."""
        values = pd.Series([1, 1.0, True, 1.0], dtype=object)
        codes, positions = mapping_module._distinct_rows([values], [False])
        assert positions[codes].tolist() == [0, 0, 0, 0]

    def test_print_form_key_ignores_values_that_occur_once(self):
        """A value seen once cannot share its group with another printed form."""
        values = pd.Series([1, 2.5, 3, 7], dtype=object)
        codes, uniques = pd.factorize(values)
        assert mapping_module._print_form_key(values, codes, uniques) is None

    def test_print_form_key_splits_only_repeated_groups(self):
        values = pd.Series([1, 1.0, 2, 3.5], dtype=object)
        codes, uniques = pd.factorize(values)
        key, _ = mapping_module._print_form_key(values, codes, uniques)
        assert (key[0] != key[1], key[2] == key[3] == 0) == (True, True)


class TestPrintFormsOnlyForRegex:
    """Print forms are computed only for sources that a regex pattern reads."""

    @staticmethod
    def _spy(monkeypatch):
        calls = []
        original = mapping_module._print_form_key

        def spy(source, codes, uniques):
            calls.append(source.name)
            return original(source, codes, uniques)

        monkeypatch.setattr(mapping_module, "_print_form_key", spy)
        return calls

    @staticmethod
    def _rep_map(sources):
        return RepresentationMap(
            id="RM",
            name="RM",
            agency="WB",
            source="urn:x",
            target="urn:y",
            maps=[ValueMap(source=s, target="T") for s in sources],
        )

    @staticmethod
    def _multi_map(patterns):
        return MultiComponentMap(
            source=["CODE", "NOTE"],
            target=["OUT"],
            values=MultiRepresentationMap(
                id="MRM",
                name="MRM",
                agency="WB",
                source=["urn:x", "urn:x2"],
                target=["urn:y"],
                maps=[MultiValueMap(source=list(p), target=["T"]) for p in patterns],
            ),
        )

    @pytest.fixture
    def df(self):
        return pd.DataFrame(
            {
                "CODE": pd.Series([1, 1.0, True, 2], dtype=object),
                "NOTE": pd.Series([1, 1.0, 1, 1], dtype=object),
            }
        )

    def test_apply_component_map_literal_only_skips_print_forms(self, df, monkeypatch):
        calls = self._spy(monkeypatch)
        cmap = ComponentMap(source="CODE", target="OUT", values=self._rep_map(["1"]))
        apply_component_map(df, cmap)
        assert calls == []

    def test_apply_component_map_regex_computes_print_forms(self, df, monkeypatch):
        calls = self._spy(monkeypatch)
        cmap = ComponentMap(
            source="CODE", target="OUT", values=self._rep_map(["1", "regex:1.*"])
        )
        apply_component_map(df, cmap)
        assert calls == ["CODE"]

    def test_apply_multi_component_map_literal_only_skips_print_forms(
        self, df, monkeypatch
    ):
        calls = self._spy(monkeypatch)
        apply_multi_component_map(df, self._multi_map([("1", "1")]))
        assert calls == []

    def test_apply_multi_component_map_splits_only_regex_positions(
        self, df, monkeypatch
    ):
        calls = self._spy(monkeypatch)
        apply_multi_component_map(df, self._multi_map([("1", "regex:1.*")]))
        assert calls == ["NOTE"]

    def test_apply_multi_component_map_regex_position_still_splits(self, df):
        """1 and 1.0 still match a regex separately at a regex position."""
        result = apply_multi_component_map(
            df, self._multi_map([("regex:.*", "regex:1")])
        )
        assert result["OUT"].isna().tolist() == [False, True, False, False]


class TestRequirePandas3:
    """tidysdmx.mapping refuses pandas older than 3, whose copies are not CoW."""

    @pytest.mark.parametrize("version", ["2.3.3", "1.5.3"])
    def test_require_pandas_3_rejects_older_pandas(self, version):
        with pytest.raises(
            ImportError, match=rf"pandas {re.escape(version)} is installed.*uv sync"
        ):
            mapping_module._require_pandas_3(version)

    @pytest.mark.parametrize("version", ["3.0.6", "3.1.0.dev0+1.gabc", "10.0.0"])
    def test_require_pandas_3_accepts_pandas_3_and_later(self, version):
        mapping_module._require_pandas_3(version)

    def test_require_pandas_3_accepts_an_unparsed_version(self):
        """A source build without version metadata reports "0+unknown"."""
        mapping_module._require_pandas_3("0+unknown")
