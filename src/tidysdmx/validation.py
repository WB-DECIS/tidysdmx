from typing import Dict, List, Any
import pandas as pd
import pysdmx as px
from typeguard import typechecked

@typechecked
def extract_validation_info(schema: px.model.dataflow.Schema) -> Dict[str, object]:
    """
    Extract validation information from a given schema.

    Args:
    ---------_
    schema: A pysdmx.model.dataflow.Schema object.
    The schema object contains all necessary validation information.

    Returns:
        dict: A dictionary containing validation information with the following keys:
            - valid_comp: List of valid component names.
            - mandatory_comp: List of mandatory component names.
            - coded_comp: List of coded component names.
            - codelist_ids: Dictionary with coded components as keys and list of codelist IDs as values.
            - dim_comp: List of dimension component names.
    """
    comp = schema.components
    # Precompute reusable objects
    valid_comp = [c.id for c in comp]
    mandatory_comp = [c.id for c in comp if comp[c.id].required]
    coded_comp = [c.id for c in comp if comp[c.id].local_codes is not None]
    dim_comp = [c.id for c in comp if comp[c.id].role == px.model.Role.DIMENSION]

    out = {
        "valid_comp": valid_comp,
        "mandatory_comp": mandatory_comp,
        "coded_comp": coded_comp,
        "codelist_ids": get_codelist_ids(comp, coded_comp),
        "dim_comp": dim_comp,
    }

    return out

@typechecked
def get_codelist_ids(comp: px.model.dataflow.Components, coded_comp: List) -> Dict[str, list[str]]:
    """
    Retrieve all codelist IDs for given coded components.

    Args:
        comp (list): List of components.
        coded_comp (list): List of coded components.

    Returns:
        dict: Dictionary with coded components as keys and list of codelist IDs as values.
    """
    codelist_dict = {}
    for component in coded_comp:
        codes = comp[component].local_codes.items
        codelist_dict[component] = [code.id for code in codes]
    
    return codelist_dict

def filter_rows(df: pd.DataFrame, codelist_ids: Dict[str, list[str]]) -> pd.DataFrame:
    """
    Filters out rows where values are not in the allowed codelist for coded columns.
    Compares as strings but does not change df dtypes.
    Does not mutate input df.

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

def filter_raw(
    df: pd.DataFrame,
    schema: Dict[str, Any],
) -> pd.DataFrame:
    """
    Validate and filter SDMX-like input, returning a cleaned DataFrame.
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