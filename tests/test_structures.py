import pandas as pd
import pytest
# Import tidysdmx functions
from tidysdmx.structures import infer_role_dimension

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
