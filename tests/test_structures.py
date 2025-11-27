from typeguard import TypeCheckError
from datetime import datetime, timezone
from pysdmx.model.map import (
    FixedValueMap, 
    ImplicitComponentMap, 
    DatePatternMap, 
    ValueMap, 
    MultiValueMap, 
    RepresentationMap,
    ComponentMap
    )
import pandas as pd
import pytest
import re
# Import tidysdmx functions
from tidysdmx.structures import (
    infer_role_dimension, 
    build_fixed_map, 
    build_implicit_component_map, 
    build_date_pattern_map,
    build_value_map,
    build_value_map_list,
    build_multi_value_map_list,
    build_representation_map,
    build_single_component_map
    )

# region fixtures

@pytest.fixture
def id():
    return "RM_ID"

@pytest.fixture
def name():
    return "Country Map"

@pytest.fixture
def agency():
    return "ECB"

@pytest.fixture
def source_cl():
    return "urn:source:codelist"

@pytest.fixture
def target_cl():
    return "urn:target:codelist"

@pytest.fixture
def description():
    return "Mapping ISO2 to ISO3 codes"
# endregion

# # region infer_role_dimension
# @pytest.mark.parametrize(
#     "data,expected",
#     [
#         # Case 1: Single unique column
#         (
#             pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]}),
#             {"id"}
#         ),
#         # Case 2: Two columns together form unique key
#         (
#             pd.DataFrame({
#                 "first": ["a", "a", "b"],
#                 "second": [1, 2, 1],
#                 "value": [10, 20, 30]
#             }),
#             {"first", "second"}
#         ),
#         # Case 3: All columns together form unique key
#         (
#             pd.DataFrame({
#                 "x": [1, 1],
#                 "value": [2, 3]
#             }),
#             {}
#         ),
#     ]
# )
# def test_infer_role_dimension_basic(data, expected):
#     result = infer_role_dimension(data, "value")
#     assert result == expected


# def test_infer_role_dimension_empty_dataframe():
#     df = pd.DataFrame()
#     result = infer_role_dimension(df, value_col="value")
#     assert result == set(), "Empty DataFrame should return empty set"


# def test_infer_role_dimension_with_duplicates():
#     df = pd.DataFrame({
#         "id": [1, 1, 2],
#         "name": ["a", "a", "b"]
#     })
#     result = infer_role_dimension(df, value_col="value")
#     assert result == set(), "No unique key should exist when duplicates are present"


# def test_infer_role_dimension_with_nan_values():
#     df = pd.DataFrame({
#         "value": [1, 2, None],
#         "code": ["x", "y", "z"]
#     })
#     result = infer_role_dimension(df, value_col="value")
#     # 'code' is unique even with NaN in 'id'
#     assert result == {"code"}

# def test_infer_role_dimension_large_dataframe():
#     # Performance sanity check
#     df = pd.DataFrame({
#         "id": range(1000),
#         "value": range(1000)
#     })
#     result = infer_role_dimension(df, value_col="value")
#     assert result == {"id"}


# def test_infer_role_dimension_no_unique_keys():
#     df = pd.DataFrame({
#         "x": [1, 1, 1],
#         "y": [2, 2, 2]
#     })
#     result = infer_role_dimension(df, value_col="value")
#     assert result == set(), "No unique keys should be found"

# region Test build_fixed_map
class TestBuildFixedMap:  # noqa: D101
    def test_build_fixed_map_normal(self):
        """Valid mapping with default located_in."""
        mapping = build_fixed_map(target="CONF_STATUS", value="F")
        assert isinstance(mapping, FixedValueMap)
        assert mapping.target == "CONF_STATUS"
        assert mapping.value == "F"
        assert mapping.located_in == "target"

    def test_build_fixed_map_valid_source_located_in(self):
        """Valid mapping with located_in set to 'source'."""
        mapping = build_fixed_map("REPORTING_STATUS", "FINAL", located_in="source")
        assert mapping.located_in == "source"
        assert mapping.target == "REPORTING_STATUS"
        assert mapping.value == "FINAL"

    def test_build_fixed_map_invalid_target(self):
        """Empty target should raise ValueError."""
        with pytest.raises(ValueError):
            build_fixed_map("", "F")

    def test_build_fixed_map_invalid_value(self):
        """Empty value should raise ValueError."""
        with pytest.raises(ValueError):
            build_fixed_map("CONF_STATUS", "")

    def test_build_fixed_map_invalid_located_in_raises(self):
        """Invalid located_in should raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            build_fixed_map("CONF_STATUS", "F", located_in="invalid")

    def test_build_fixed_map_type_safety(self):
        """Ensure type safety enforced by typeguard."""
        with pytest.raises(TypeCheckError):
            build_fixed_map(123, "F")  # target must be str
        with pytest.raises(TypeCheckError):
            build_fixed_map("CONF_STATUS", 456)  # value must be str
        with pytest.raises(TypeCheckError):
            build_fixed_map("CONF_STATUS", 456, located_in=None)
# endregion

# region Test build_fixed_map
class TestBuildImplicitComponentMap:  # noqa: D101
    def test_build_implicit_component_map_valid(self):
        """Valid mapping should return an ImplicitComponentMap instance."""
        mapping = build_implicit_component_map("FREQ", "FREQUENCY")
        assert isinstance(mapping, ImplicitComponentMap)
        assert mapping.source == "FREQ"
        assert mapping.target == "FREQUENCY"

    def test_build_implicit_component_map_empty_source_raises(self):
        """Empty source should raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            build_implicit_component_map("", "FREQUENCY")
        assert "non-empty" in str(exc_info.value)

    def test_build_implicit_component_map_empty_target_raises(self):
        """Empty target should raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            build_implicit_component_map("FREQ", "")
        assert "non-empty" in str(exc_info.value)

    def test_build_implicit_component_map_type_safety_source(self):
        """Non-string source should raise TypeError due to typeguard."""
        with pytest.raises(TypeCheckError):
            build_implicit_component_map(123, "FREQUENCY")

    def test_build_implicit_component_map_type_safety_target(self):
        """Non-string target should raise TypeError due to typeguard."""
        with pytest.raises(TypeCheckError):
            build_implicit_component_map("FREQ", 456)

    @pytest.mark.skip(reason="Not implemented.")
    def test_build_implicit_component_map_whitespace_source(self):
        """Whitespace-only source should raise ValueError."""
        with pytest.raises(ValueError):
            build_implicit_component_map("   ", "FREQUENCY")

    @pytest.mark.skip(reason="Not implemented.")
    def test_build_implicit_component_map_whitespace_target(self):
        """Whitespace-only target should raise ValueError."""
        with pytest.raises(ValueError):
            build_implicit_component_map("FREQ", "   ")

    def test_build_implicit_component_map_case_sensitivity(self):
        """Ensure mapping preserves case of source and target."""
        mapping = build_implicit_component_map("freq", "Frequency")
        assert mapping.source == "freq"
# endregion

# region Test build_date_pattern_map()
class TestBuildDatePatternMap:  # noqa: D101
    
    def test_build_date_pattern_map_valid_fixed(self):
        """Valid case with fixed pattern type."""
        dpm = build_date_pattern_map("DATE", "TIME_PERIOD", "MMM yy", "M")
        assert isinstance(dpm, DatePatternMap)
        assert dpm.source == "DATE"
        assert dpm.target == "TIME_PERIOD"
        assert dpm.pattern == "MMM yy"
        assert dpm.frequency == "M"
        assert dpm.pattern_type == "fixed"
        assert dpm.locale == "en"

    def test_build_date_pattern_map_valid_variable(self):
        """Valid case with variable pattern type."""
        dpm = build_date_pattern_map("DATE", "TIME_PERIOD", "MMM yy", "FREQ", pattern_type="variable")
        assert dpm.pattern_type == "variable"
        assert dpm.frequency == "FREQ"

    def test_build_date_pattern_map_with_id_and_locale(self):
        """Valid case with custom ID and locale."""
        dpm = build_date_pattern_map("DATE", "TIME_PERIOD", "MMM yy", "M", id="map1", locale="fr")
        assert dpm.id == "map1"
        assert dpm.locale == "fr"

    def test_build_date_pattern_map_with_resolve_period(self):
        """Valid case with resolve_period specified."""
        dpm = build_date_pattern_map("DATE", "TIME_PERIOD", "MMM yy", "M", resolve_period="endOfPeriod")
        assert dpm.resolve_period == "endOfPeriod"

    def test_build_date_pattern_map_empty_source(self):
        """Empty source should raise ValueError."""
        with pytest.raises(ValueError):
            build_date_pattern_map("", "TIME_PERIOD", "MMM yy", "M")

    def test_build_date_pattern_map_empty_target(self):
        """Empty target should raise ValueError."""
        with pytest.raises(ValueError):
            build_date_pattern_map("DATE", "", "MMM yy", "M")

    def test_build_date_pattern_map_empty_pattern(self):
        """Empty pattern should raise ValueError."""
        with pytest.raises(ValueError):
            build_date_pattern_map("DATE", "TIME_PERIOD", "", "M")

    def test_build_date_pattern_map_empty_frequency(self):
        """Empty frequency should raise ValueError."""
        with pytest.raises(ValueError):
            build_date_pattern_map("DATE", "TIME_PERIOD", "MMM yy", "")

    def test_build_date_pattern_map_invalid_pattern_type(self):
        """Invalid pattern_type should raise TypeError."""
        with pytest.raises(TypeCheckError):
            build_date_pattern_map("DATE", "TIME_PERIOD", "MMM yy", "M", pattern_type="wrong")  # type: ignore

    def test_build_date_pattern_map_invalid_resolve_period(self):
        """Invalid resolve_period should raise TypeError."""
        with pytest.raises(TypeCheckError):
            build_date_pattern_map("DATE", "TIME_PERIOD", "MMM yy", "M", resolve_period="invalid")  # type: ignore

    def test_build_date_pattern_map_whitespace_source(self):
        """Whitespace-only source should raise ValueError."""
        with pytest.raises(ValueError):
            build_date_pattern_map("   ", "TIME_PERIOD", "MMM yy", "M")

    def test_build_date_pattern_map_whitespace_target(self):
        """Whitespace-only target should raise ValueError."""
        with pytest.raises(ValueError):
            build_date_pattern_map("DATE", "   ", "MMM yy", "M")

    def test_build_date_pattern_map_whitespace_pattern(self):
        """Whitespace-only pattern should raise ValueError."""
        with pytest.raises(ValueError):
            build_date_pattern_map("DATE", "TIME_PERIOD", "   ", "M")
# endregion

# region Test build_value_map()
class TestBuildValueMap:  # noqa: D101
    def test_build_value_map_normal(self):
        """Test normal case with source and target values."""
        vm = build_value_map("BE", "BEL")
        assert isinstance(vm, ValueMap)
        assert vm.source == "BE"
        assert vm.target == "BEL"
        assert vm.valid_from is None
        assert vm.valid_to is None

    def test_build_value_map_with_validity_dates(self):
        """Test case with both validity dates provided."""
        start = datetime(2021, 1, 1)
        end = datetime(2022, 1, 1)
        vm = build_value_map("FR", "FRA", valid_from=start, valid_to=end)
        assert vm.valid_from == start
        assert vm.valid_to == end

    def test_build_value_map_with_only_valid_from(self):
        """Test case with only valid_from date provided."""
        start = datetime(2020, 6, 15)
        vm = build_value_map("DE", "GER", valid_from=start)
        assert vm.valid_from == start
        assert vm.valid_to is None

    def test_build_value_map_with_only_valid_to(self):
        """Test case with only valid_to date provided."""
        end = datetime(2030, 12, 31)
        vm = build_value_map("IT", "ITA", valid_to=end)
        assert vm.valid_to == end
        assert vm.valid_from is None

    def test_build_value_map_empty_source(self):
        """Empty source raises ValueError."""
        with pytest.raises(ValueError):
            build_value_map("", "BEL")

    def test_build_value_map_empty_target(self):
        """Empty target raises ValueError."""
        with pytest.raises(ValueError):
            build_value_map("BE", "")

    def test_build_value_map_invalid_source_type(self):
        """Non-string source raises TypeError."""
        with pytest.raises(TypeError):
            build_value_map(123, "BEL")

    def test_build_value_map_invalid_target_type(self):
        """Non-string target raises TypeError."""
        with pytest.raises(TypeError):
            build_value_map("BE", 456)

    def test_build_value_map_whitespace_source(self):
        """Whitespace-only source raises ValueError."""
        with pytest.raises(ValueError):
            build_value_map("   ", "BEL")

    def test_build_value_map_whitespace_target(self):
        """Whitespace-only target raises ValueError."""
        with pytest.raises(ValueError):
            build_value_map("BE", "   ")

    def test_build_value_map_type_safety(self):
        """Ensure returned object is of type ValueMap."""
        vm = build_value_map("NL", "NLD")
        assert isinstance(vm, ValueMap)

    def test_build_value_map_validity_type(self):
        """Ensure validity dates are datetime or None."""
        vm = build_value_map("ES", "ESP", valid_from=datetime(2025, 1, 1))
        assert isinstance(vm.valid_from, datetime)
# endregion

class TestBuildValueMapList:  # noqa: D101
    def test_build_value_map_list_valid(self, value_map_df_mandatory_cols):
        """Valid DataFrame should return a list of ValueMap objects."""
        result = build_value_map_list(value_map_df_mandatory_cols, "source", "target")
        assert isinstance(result, list)
        assert all(isinstance(vm, ValueMap) for vm in result)
        assert len(result) == value_map_df_mandatory_cols.shape[0]
        assert result[1].source == "UY"
        assert result[1].target == "URY"
        assert result[0].typed_source == re.compile(r"^A") 
    
    
    def test_build_value_map_list_validity_columns(self):
        """DataFrame with custom validity column names should populate ValueMap."""
        df = pd.DataFrame({
            "source": ["BE", "FR"],
            "target": ["BEL", "FRA"],
            "start_date": ["2020-01-01", None],
            "end_date": ["2025-12-31", None]
        })
        result = build_value_map_list(df, "source", "target", valid_from_col="start_date", valid_to_col="end_date")
        assert result[0].valid_from == "2020-01-01"
        assert result[0].valid_to == "2025-12-31"
        assert result[1].valid_from is None
        assert result[1].valid_to is None
 
    def test_build_value_map_list_validity_columns_empty(self):
        """Validity columns present but empty should be ignored."""
        df = pd.DataFrame({
            "source": ["BR"],
            "target": ["BRA"],
            "valid_from": [None],
            "valid_to": [None]
        })
        result = build_value_map_list(df, "source", "target")
        assert result[0].valid_from is None
        assert result[0].valid_to is None

    def test_build_value_map_list_empty_df(self):
        """Empty DataFrame should raise ValueError."""
        empty_df = pd.DataFrame(columns=["source", "target"])
        with pytest.raises(ValueError):
            build_value_map_list(empty_df, "source", "target")

    def test_build_value_map_list_missing_column(self, value_map_df_mandatory_cols):
        """Missing column should raise ValueError."""
        df_missing = value_map_df_mandatory_cols.drop(columns=["target"])
        with pytest.raises(ValueError):
            build_value_map_list(df_missing, "source", "target")

    def test_build_value_map_list_single_row(self):
        """Single-row DataFrame should return a list with one ValueMap."""
        df_single = pd.DataFrame({"source": ["BR"], "target": ["BRA"]})
        result = build_value_map_list(df_single, "source", "target")
        assert len(result) == 1
        assert result[0].source == "BR"
        assert result[0].target == "BRA"

    def test_build_value_map_list_column_order_irrelevant(self, value_map_df_mandatory_cols):
        """Column order should not affect the result."""
        df_reordered = value_map_df_mandatory_cols[["target", "source"]]
        # result = build_value_map_list(df_reordered.rename(columns={"target": "target", "source": "source"}), "source", "target")
        result = build_value_map_list(df_reordered, "source", "target")
        assert len(result) == value_map_df_mandatory_cols.shape[0]
        assert result[1].source == "UY"
        assert result[1].target == "URY"

class TestBuildMultiValueMapList:  # noqa: D101
    def test_build_multi_value_map_list_normal(self, multi_value_map_df):
        """Normal case: first row should produce a valid MultiValueMap."""
        df = multi_value_map_df.iloc[[0]]  # Row with DE/EUR
        result = build_multi_value_map_list(df, ['country', 'currency'], 'iso_code')
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], MultiValueMap)
        assert result[0].source == ['DE', 'LC']
        assert result[0].target == ['EUR']


    def test_build_multi_value_map_list_regex_source(self, multi_value_map_df):
        """Row with regex pattern in source should still work."""
        df = multi_value_map_df.iloc[[1]]  # regex:^A
        result = build_multi_value_map_list(df, ['country', 'currency'], 'iso_code')
        assert result[0].source[0].startswith("regex:^A")
        assert result[0].typed_source[0] == re.compile(r"^A") 
        assert result[0].target == ['ARG']


    def test_build_multi_value_map_list_empty_source(self, multi_value_map_df):
        """Row with empty string in source should be accepted as valid string."""
        df = multi_value_map_df.iloc[[2]]  # Empty country
        result = build_multi_value_map_list(df, ['country', 'currency'], 'iso_code')
        assert result[0].source[0] == ""
        assert result[0].target == ['CHF']


    def test_build_multi_value_map_list_with_validity(self, multi_value_map_df):
        """Row with valid_from and valid_to should include datetime fields."""
        df = multi_value_map_df.iloc[[3]]  # FR/FRA with validity
        result = build_multi_value_map_list(df, ['country', 'currency'], 'iso_code')
        mv_map = result[0]
        assert mv_map.source == ['FR', 'LC']
        assert mv_map.target == ['FRA']
        assert isinstance(mv_map.valid_from, datetime)
        assert isinstance(mv_map.valid_to, datetime)
        assert mv_map.valid_from.isoformat() == "2020-01-01T00:00:00"
        assert mv_map.valid_to.isoformat() == "2025-12-31T00:00:00"


    def test_build_multi_value_map_list_invalid_type(self, multi_value_map_df):
        """Row with non-string source should raise TypeError."""
        df = multi_value_map_df.iloc[[4]]  # Invalid type in country
        with pytest.raises(TypeError):
            build_multi_value_map_list(df, ['country', 'currency'], 'iso_code')


    def test_build_multi_value_map_list_missing_target_column(self, multi_value_map_df):
        """Missing target column should raise ValueError."""
        df = multi_value_map_df.drop(columns=['iso_code'])
        with pytest.raises(ValueError):
            build_multi_value_map_list(df, ['country', 'currency'], 'iso_code')


    def test_build_multi_value_map_list_missing_source_column(self, multi_value_map_df):
        """Missing one source column should raise ValueError."""
        df = multi_value_map_df.drop(columns=['currency'])
        with pytest.raises(ValueError):
            build_multi_value_map_list(df, ['country', 'currency'], 'iso_code')


    def test_build_multi_value_map_list_empty_dataframe(self):
        """Empty DataFrame should raise ValueError."""
        df = pd.DataFrame()
        with pytest.raises(ValueError):
            build_multi_value_map_list(df, ['country', 'currency'], 'iso_code')


    def test_build_multi_value_map_list_multiple_rows(self, multi_value_map_df):
        """Multiple rows should return a list of MultiValueMap objects."""
        df = multi_value_map_df.iloc[:3]  # First three rows
        result = build_multi_value_map_list(df, ['country', 'currency'], 'iso_code')
        assert isinstance(result, list)
        assert len(result) == df.shape[0]
        for mv_map in result:
            assert isinstance(mv_map, MultiValueMap)
            assert isinstance(mv_map.source, list)
            assert isinstance(mv_map.target, list)

class TestBuildRepresentationMap:  # noqa: D101

    def test_build_representation_map_success(self, value_map_df_mandatory_cols, id, name, agency, source_cl, target_cl, description):
        """Test successful creation of RepresentationMap from valid DataFrame."""
        rm = build_representation_map(
            df=value_map_df_mandatory_cols,
            id=id,
            name=name,
            agency=agency,
            source_cl=source_cl,
            target_cl=target_cl,
            description=description
        )
        assert isinstance(rm, RepresentationMap)
        assert rm.id == id
        assert rm.name == name
        assert rm.agency == agency
        assert rm.source == source_cl
        assert rm.target == target_cl
        assert rm.description == description
        assert rm.version == "1.0"
        assert len(rm.maps) == value_map_df_mandatory_cols.shape[0]
        for vm in rm.maps:
            assert isinstance(vm, ValueMap)

    def test_build_representation_map_empty_df_raises(self):
        """Empty DataFrame should raise ValueError."""
        empty_df = pd.DataFrame(columns=["source", "target"])
        with pytest.raises(ValueError):
            build_representation_map(empty_df)

    def test_build_representation_map_missing_columns_raises(self):
        """Missing mandatory columns should raise ValueError."""
        bad_df = pd.DataFrame({"src": ["A"], "tgt": ["B"]})
        with pytest.raises(ValueError):
            build_representation_map(bad_df)

    def test_build_representation_map_non_string_values_raises(self):
        """Non-string values in source or target columns should raise TypeError."""
        bad_df = pd.DataFrame({"source": [123], "target": ["ABC"]})
        with pytest.raises(TypeError):
            build_representation_map(bad_df)

    def test_build_representation_map_custom_column_names(self):
        """Test with custom column names for source and target."""
        df = pd.DataFrame({
            "src": ["A", "B"],
            "tgt": ["X", "Y"],
            "valid_from": [None, None],
            "valid_to": [None, None]
        })
        rm = build_representation_map(
            df=df,
            source_col="src",
            target_col="tgt",
            id="RM_CUSTOM",
            name="Custom Map"
        )
        assert isinstance(rm, RepresentationMap)
        assert rm.id == "RM_CUSTOM"
        assert rm.name == "Custom Map"
        assert len(rm.maps) == 2

    def test_build_representation_map_validity_dates(self, value_map_df_mandatory_cols):
        """Ensure validity dates are parsed correctly when present."""
        rm = build_representation_map(df=value_map_df_mandatory_cols)
        for vm in rm.maps:
            if vm.valid_from:
                assert isinstance(vm.valid_from, datetime)
                assert vm.valid_from.tzinfo == timezone.utc or vm.valid_from.tzinfo is None
            if vm.valid_to:
                assert isinstance(vm.valid_to, datetime)
                assert vm.valid_to.tzinfo == timezone.utc or vm.valid_to.tzinfo is None


class TestBuildSingleComponentMap:  # noqa: D101
    def test_build_single_component_map_valid(self, value_map_df_mandatory_cols):
        """Test normal case with valid DataFrame and fixture."""
        df = value_map_df_mandatory_cols
        cm = build_single_component_map(
            df,
            source_component="COUNTRY",
            target_component="COUNTRY",
            agency="ECB",
            id="CM1",
            name="Country Component Map",
            source_cl="urn:source:codelist",
            target_cl="urn:target:codelist"
        )
        assert isinstance(cm, ComponentMap)
        assert cm.source == "COUNTRY"
        assert cm.target == "COUNTRY"
        assert isinstance(cm.values, RepresentationMap)


    def test_build_single_component_map_empty_df(self):
        """Empty DataFrame should raise ValueError."""
        df = pd.DataFrame(columns=["source", "target"])
        with pytest.raises(ValueError):
            build_single_component_map(df, "COUNTRY", "COUNTRY")


    def test_build_single_component_map_missing_column(self, value_map_df_mandatory_cols):
        """Missing required column should raise ValueError."""
        df = value_map_df_mandatory_cols.drop(columns=["source"])
        with pytest.raises(ValueError):
            build_single_component_map(df, "COUNTRY", "COUNTRY")


    def test_build_single_component_map_invalid_type(self):
        """Non-string values in source column should raise TypeError."""
        df = pd.DataFrame({"source": [123], "target": ["BEL"]})
        with pytest.raises(TypeError):
            build_single_component_map(df, "COUNTRY", "COUNTRY")


    def test_build_single_component_map_custom_columns(self):
        """Test with custom column names for source and target."""
        df = pd.DataFrame({
            "src": ["BE", "FR"],
            "tgt": ["BEL", "FRA"],
            "valid_from": ["2020-01-01", None],
            "valid_to": ["2025-12-31", None]
        })
        cm = build_single_component_map(
            df,
            source_component="COUNTRY",
            target_component="COUNTRY",
            source_col="src",
            target_col="tgt"
        )
        assert isinstance(cm, ComponentMap)
        assert isinstance(cm.values, RepresentationMap)


    def test_build_single_component_map_with_validity_dates(self, value_map_df_mandatory_cols):
        """Ensure validity columns are handled correctly."""
        df = value_map_df_mandatory_cols.copy()
        df["valid_from"] = ["2020-01-01", None, None]
        df["valid_to"] = ["2025-12-31", None, None]
        cm = build_single_component_map(
            df,
            source_component="COUNTRY",
            target_component="COUNTRY"
        )
        assert isinstance(cm.values, RepresentationMap)
        assert len(cm.values.maps) == len(df)


    def test_build_single_component_map_id_and_name(self, value_map_df_mandatory_cols):
        """Check that id and name propagate correctly to RepresentationMap."""
        cm = build_single_component_map(
            value_map_df_mandatory_cols,
            source_component="COUNTRY",
            target_component="COUNTRY",
            id="TEST_ID",
            name="Test Name"
        )
        assert cm.values.id == "TEST_ID"
        assert cm.values.name == "Test Name"


    def test_build_single_component_map_version_and_description(self, value_map_df_mandatory_cols):
        """Check version and description fields."""
        cm = build_single_component_map(
            value_map_df_mandatory_cols,
            source_component="COUNTRY",
            target_component="COUNTRY",
            version="2.0",
            description="Test Description"
        )
        assert cm.values.version == "2.0"
        assert cm.values.description == "Test Description"
