from typing import Dict, List, Any
from typeguard import typechecked
from .utils import *
import pandas as pd
import pysdmx as px


@typechecked
def filter_rows(
        df: pd.DataFrame, 
        codelist_ids: Dict[str, list[str]]
    ) -> pd.DataFrame:
    """Filters out rows where values are not in the allowed codelist for coded columns.
    Compares as strings but does not change df dtypes.
    Does not mutate input df.

    Args:
        df (pd.DataFrame): The input DataFrame.
        codelist_ids (Dict[str, list[str]]): A dictionary mapping column names to lists of allowed codelist IDs.

    Returns:
        - Filtered DataFrame (only selected rows)
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
    """Validate and filter SDMX-like input, returning a cleaned DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.
        schema (px.model.dataflow.Schema): The SDMX schema to validate against.
    
    Returns:
        pd.DataFrame: The filtered DataFrame.
    """
    # if schema is None:
    #     raise ValueError("Schema must be provided.")

    valid = extract_validation_info(schema)

    # Filter rows based on codelist constraints
    df_filtered = filter_rows(
        df=df,
        codelist_ids=valid.get("codelist_ids", {}),
    )

    return df_filtered