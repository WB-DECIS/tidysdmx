"""Filter SDMX DataFrames against schema codelist constraints."""

import pandas as pd
import pysdmx as px
from typeguard import typechecked

from .utils import extract_validation_info


@typechecked
def filter_rows(
    df: pd.DataFrame,
    codelist_ids: dict[str, list[str]],
) -> pd.DataFrame:
    """Filter out rows where values are not in the allowed codelist.

    Compares as strings but does not change DataFrame dtypes.
    Does not mutate the input DataFrame.

    Args:
        df: The input DataFrame.
        codelist_ids: A mapping of column names to allowed codelist IDs.

    Returns:
        A filtered copy of the DataFrame containing only selected rows.
    """
    if not codelist_ids:
        return df.copy()

    rows_to_drop = pd.Series(False, index=df.index)

    for col, allowed in codelist_ids.items():
        if col not in df.columns:
            continue
        allowed_str = set(map(str, allowed))
        col_as_str = df[col].astype(str)
        unselected_mask = ~col_as_str.isin(allowed_str) & df[col].notna()
        rows_to_drop |= unselected_mask

    return df.loc[~rows_to_drop].copy()


@typechecked
def filter_tidy_raw(
    df: pd.DataFrame,
    schema: px.model.dataflow.Schema,
) -> pd.DataFrame:
    """Filter an SDMX DataFrame by removing rows that violate codelist constraints.

    Args:
        df: The input DataFrame.
        schema: The SDMX schema to validate against.

    Returns:
        A filtered DataFrame with invalid code rows removed.
    """
    valid = extract_validation_info(schema)

    return filter_rows(
        df=df,
        codelist_ids=valid.get("codelist_ids", {}),
    )
