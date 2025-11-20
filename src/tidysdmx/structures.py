from itertools import combinations
from pysdmx.model.dataflow import Schema, Components, Component
from pysdmx.model import Concept, Role, DataType, Codelist, Code
import pandas as pd

import pandas as pd
from itertools import combinations
from typing import List, Tuple, Union

def infer_role_dimension(
    df: pd.DataFrame, 
    value_col: str
) -> List[Union[str, Tuple[str, ...]]]:
    """
    Infers the minimal set of unique keys (dimension columns) for an observation column.

    This function identifies the smallest combination of columns that uniquely 
    identifies each record's observation value. It starts by checking single 
    columns and progressively checks larger combinations until a key is found.

    Args:
        df (pd.DataFrame): The input DataFrame containing the data.
        value_col (str): The name of the column containing the 
                                  observation values to be identified.

    Returns:
        List[Union[str, Tuple[str, ...]]]: 
            A list of the minimal unique keys found.
            - For single-column keys, returns a list of strings (e.g., ['col1', 'col2']).
            - For composite keys, returns a list of tuples (e.g., [('col1', 'col2')]).
            Returns an empty list if no key is found.
    """
    if value_col not in df.columns:
        raise ValueError(f"Observation column '{value_col}' not found in DataFrame.")

    dimension_columns = [col for col in df.columns if col != value_col]
    df_cleaned = df.dropna(subset=dimension_columns, how='all')
    
    if len(df_cleaned) == 0:
        return []

    for combination_size in range(1, len(dimension_columns) + 1):
        unique_keys_found = []
        
        for column_combination in combinations(dimension_columns, combination_size):
            key_candidate = list(column_combination)
            
            if not df_cleaned.duplicated(subset=key_candidate).any():
                # --- FIX IS HERE ---
                # If the key has only one column, store it as a string.
                # Otherwise, store it as a sorted tuple.
                if len(key_candidate) == 1:
                    unique_keys_found.append(key_candidate[0])
                else:
                    unique_keys_found.append(tuple(sorted(key_candidate)))

        if unique_keys_found:
            # Sort the final list for consistent output
            # This handles cases where multiple keys of the same size are found
            return sorted(unique_keys_found, key=lambda k: str(k))
            
    return []

def infer_role(col_name: str, series, dimension_cols: set) -> Role:
    """
    Infer SDMX role for a column based on dimension detection and data characteristics.
    """
    if col_name in dimension_cols:
        return Role.DIMENSION
    if series.dtype.kind in ["i", "f"]:  # numeric
        return Role.MEASURE
    # Heuristic for remaining columns
    unique_ratio = series.nunique() / len(series)
    return Role.DIMENSION if unique_ratio < 0.5 else Role.ATTRIBUTE

def infer_dtype(series) -> DataType:
    """Map pandas dtype to SDMX DataType."""
    if series.dtype.kind in ["i", "f"]:
        return DataType.FLOAT
    if "datetime" in str(series.dtype):
        return DataType.PERIOD
    return DataType.STRING

def infer_schema(df, agency="WB", id="INFERRED_SCHEMA", value_col="OBS_VALUE"):
    """Infer a pysdmx Schema from a pandas DataFrame."""
    # Infer dimension columns
    dim_cols = infer_role_dimension(df = df, value_col = value_col)
    
    components = []

    for col in df.columns:
        role = infer_role(col_name = col, series = df[col], dimension_cols = dim_cols)
        dtype = infer_dtype(df[col])
        concept = Concept(id=col, name=col.title(), dtype=dtype)

        # Build local codes for categorical dimensions
        local_codes = None
        if role == Role.DIMENSION and dtype == DataType.STRING:
            unique_vals = sorted(df[col].dropna().unique())
            if len(unique_vals) <= 500:  # avoid huge codelists
                code_items = [Code(id=str(v), name=str(v)) for v in unique_vals]
                local_codes = Codelist(
                    id=f"CL_{col}",
                    name=f"{col} Codes",
                    agency=agency,  # ✅ Required for maintainable artefacts
                    items=code_items
                )

        component = Component(
            id=col,
            required=(role != Role.ATTRIBUTE),
            role=role,
            concept=concept,
            name=col.title(),
            local_dtype=dtype,
            local_codes=local_codes
        )
        components.append(component)

    return Schema(
        context="datastructure",
        agency=agency,
        id=id,
        components=Components(components),
        version="1.0"
    )
