from typeguard import TypeCheckError
from pysdmx.model.map import FixedValueMap, ImplicitComponentMap
import pandas as pd
import pytest
# Import tidysdmx functions
from tidysdmx.structures import infer_role_dimension, build_fixed_map, build_implicit_component_map

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