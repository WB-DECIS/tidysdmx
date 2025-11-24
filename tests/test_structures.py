import pandas as pd
import pytest
# Import tidysdmx functions
from tidysdmx.structures import infer_role_dimension, build_fixed_map

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
class TestBuildFixedMap:
    def test_build_fixed_map_normal():
        """Valid mapping with default located_in."""
        mapping = build_fixed_map("CONF_STATUS", "F")
        assert isinstance(mapping, FixedValueMap)
        assert mapping.target == "CONF_STATUS"
        assert mapping.value == "F"
        assert mapping.located_in == "target"

    def test_build_fixed_map_valid_source_located_in():
        """Valid mapping with located_in set to 'source'."""
        mapping = build_fixed_map("REPORTING_STATUS", "FINAL", located_in="source")
        assert mapping.located_in == "source"
        assert mapping.target == "REPORTING_STATUS"
        assert mapping.value == "FINAL"

    def test_build_fixed_map_invalid_target():
        """Empty target should raise ValueError."""
        with pytest.raises(ValueError):
            build_fixed_map("", "F")

    def test_build_fixed_map_invalid_value():
        """Empty value should raise ValueError."""
        with pytest.raises(ValueError):
            build_fixed_map("CONF_STATUS", "")

    def test_build_fixed_map_invalid_located_in_raises():
        """Invalid located_in should raise ValueError."""
        with pytest.raises(ValueError) as exc_info:
            build_fixed_map("CONF_STATUS", "F", located_in="invalid")

    def test_build_fixed_map_type_safety():
        """Ensure type safety enforced by typeguard."""
        with pytest.raises(TypeCheckError):
            build_fixed_map(123, "F")  # target must be str
        with pytest.raises(TypeCheckError):
            build_fixed_map("CONF_STATUS", 456)  # value must be str
        with pytest.raises(TypeCheckError):
            build_fixed_map("CONF_STATUS", 456, located_in=None)
# endregion