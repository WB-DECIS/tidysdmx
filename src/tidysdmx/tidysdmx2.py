from typing import Optional, Dict, List, Tuple, Any
import pandas as pd
import pysdmx as px
import re

def extract_validation_info(schema, role=px.model.Role.DIMENSION):
    """
    Extract validation information from a given schema.

    Args:
        schema: The schema object containing validation information.

    Returns:
        dict: A dictionary containing validation information with the following keys:
            - valid_comp: List of valid component names.
            - mandatory_comp: List of mandatory component names.
            - coded_comp: List of coded component names.
            - codelist_ids: Dictionary with coded components as keys and list of codelist IDs as values.
            - dim_comp: List of dimension component names.
    """
    comp = schema.components
    validation_info = {
        "valid_comp": [c.id for c in comp],
        "mandatory_comp": [c.id for c in comp if comp[c.id].required],
        "coded_comp": [c.id for c in comp if comp[c.id].local_codes is not None],
        "codelist_ids": get_codelist_ids(
            comp, [c.id for c in comp if comp[c.id].local_codes is not None]
        ),
        "dim_comp": [c.id for c in comp if comp[c.id].role == role],
    }
    return validation_info

def get_codelist_ids(comp, coded_comp):
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

def filter_rows(
    df: pd.DataFrame,
    codelist_ids: Dict[str, List[Any]],
) -> pd.DataFrame:
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
    if schema is None:
        raise ValueError("Schema must be provided.")

    valid = extract_validation_info(schema)

    # Filter rows based on codelist constraints
    df_filtered = filter_rows(
        df=df,
        codelist_ids=valid.get("codelist_ids", {}),
    )

    return df_filtered

# region Funtions to handle mapping files

def map_structures(df: pd.DataFrame, structure_map: px.model.map.StructureMap, verbose: bool = False) -> pd.DataFrame:
    """
    Apply all mapping components from a StructureMap to a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The source dataset.
    structure_map : StructureMap
        A StructureMap object containing various mapping components.
    verbose : bool, optional
        If True, print logs about applied mappings.

    Returns
    -------
    pd.DataFrame
        Modified DataFrame with all mappings applied.
    """
    result_df = df.copy()

    # Separate maps by type
    fixed_value_maps = []
    implicit_maps = []
    component_maps = []
    multi_component_maps = []

    for m in structure_map.maps:
        if isinstance(m, px.model.map.FixedValueMap):
            fixed_value_maps.append(m)
        elif isinstance(m, px.model.map.ImplicitComponentMap):
            implicit_maps.append(m)
        elif isinstance(m, px.model.map.ComponentMap):
            component_maps.append(m)
        elif isinstance(m, px.model.map.MultiComponentMap):
            multi_component_maps.append(m)
        else:
            raise TypeError(f"Unknown map type: {type(m)}")

    # Apply each type of map
    if fixed_value_maps:
        result_df = apply_fixed_value_maps(result_df, fixed_value_maps)
        if verbose:
            print(f"✅ Applied {len(fixed_value_maps)} FixedValueMap(s).")

    if implicit_maps:
        result_df = apply_implicit_component_maps(result_df, implicit_maps, verbose=verbose)

    for cmap in component_maps:
        result_df = apply_component_map(result_df, cmap, verbose=verbose)

    for mcm in multi_component_maps:
        result_df = apply_multi_component_map(result_df, mcm, verbose=verbose)

    return result_df

def apply_fixed_value_maps(
    df: pd.DataFrame, fixed_value_maps: List[px.model.map.FixedValueMap]
) -> pd.DataFrame:
    """
    Apply FixedValueMap rules to a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The source dataset.
    fixed_value_maps : List[FixedValueMap]
        A list of FixedValueMap objects containing target and value.

    Returns
    -------
    pd.DataFrame
        Modified DataFrame with fixed value columns added.
    """

    # Validate input types
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")
    if not isinstance(fixed_value_maps, list):
        raise TypeError("fixed_value_maps must be a list of FixedValueMap objects.")
    if not all(isinstance(m, px.model.map.FixedValueMap) for m in fixed_value_maps):
        raise TypeError(
            "All elements in fixed_value_maps must be FixedValueMap instances."
        )

    # Work on a copy to avoid mutating the original DataFrame
    result_df = df.copy()

    for fmap in fixed_value_maps:
        # Each FixedValueMap has attributes: target (column name), value (fixed value)
        result_df[fmap.target] = fmap.value

    return result_df

def apply_implicit_component_maps(
    df: pd.DataFrame,
    implicit_maps: List[px.model.map.ImplicitComponentMap],
    verbose: bool = False,
) -> pd.DataFrame:
    """
    Apply ImplicitComponentMap rules to a DataFrame, supporting different source/target names.

    Parameters
    ----------
    df : pd.DataFrame
        The source dataset.
    implicit_maps : List[ImplicitComponentMap]
        A list of ImplicitComponentMap objects containing source and target.
    verbose : bool, optional
        If True, print logs about applied mappings and conflicts.

    Returns
    -------
    pd.DataFrame
        Modified DataFrame with implicit component mappings applied.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")
    if not isinstance(implicit_maps, list):
        raise TypeError("implicit_maps must be a list of ImplicitComponentMap objects.")
    if not all(isinstance(m, px.model.map.ImplicitComponentMap) for m in implicit_maps):
        raise TypeError(
            "All elements in implicit_maps must be ImplicitComponentMap instances."
        )

    result_df = df.copy()

    for imap in implicit_maps:
        source_col = imap.source
        target_col = imap.target

        # Check if source column exists
        if source_col not in result_df.columns:
            if verbose:
                print(f"⚠️ Source column '{source_col}' not found. Skipping.")
            continue

        # Copy values from source to target
        result_df[target_col] = result_df[source_col]
        if verbose:
            action = "Overwritten" if target_col in df.columns else "Added"
            print(f"✅ {action} column '{target_col}' from source '{source_col}'.")

    return result_df

def apply_component_map(
    df: pd.DataFrame, component_map: px.model.map.ComponentMap, verbose: bool = False
) -> pd.DataFrame:
    """
    Apply a single ComponentMap with a RepresentationMap to a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Source data.
    component_map : ComponentMap
        ComponentMap object with source, target, and values (RepresentationMap).
    verbose : bool, optional
        If True, print progress.

    Returns
    -------
    pd.DataFrame
        DataFrame with the target column added or overwritten.
    """

    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")
    if not isinstance(component_map, px.model.map.ComponentMap):
        raise TypeError("component_maps must be ComponentMap object.")

    # Copy to avoid mutating original
    result_df = df.copy()

    source_col = component_map.source
    target_col = component_map.target
    rep_map = component_map.values

    # Check source column exists
    if source_col not in result_df.columns:
        raise KeyError(f"Source column '{source_col}' not found in DataFrame.")

    # Build mapping dictionary from ValueMap list
    mapping = {vm.source: vm.target for vm in rep_map.maps}

    # Apply mapping
    result_df[target_col] = result_df[source_col].map(mapping)

    if verbose:
        print(f"✅ Mapped '{source_col}' → '{target_col}' using {len(mapping)} pairs.")
        unmapped = result_df[target_col].isna().sum()
        if unmapped > 0:
            print(f"⚠️ {unmapped} values could not be mapped (set to NaN).")

    return result_df

def apply_multi_component_map(
    df: pd.DataFrame,
    multi_component_map: px.model.map.MultiComponentMap,
    verbose: bool = False,
) -> pd.DataFrame:
    """
    Apply a single MultiComponentMap with regex support, preserving rule order.

    Rules are applied in the order they appear in multi_component_map.values.maps.
    The first matching rule wins.

    Parameters
    ----------
    df : pd.DataFrame
        Source data.
    multi_component_map : MultiComponentMap
        MultiComponentMap object with source columns, target column, and values (MultiRepresentationMap).
    verbose : bool, optional
        If True, print progress.

    Returns
    -------
    pd.DataFrame
        DataFrame with the target column added or overwritten.
    """
    result_df = df.copy()

    source_cols = multi_component_map.source
    target_col = multi_component_map.target[0]  # Assuming one target column
    rep_map = multi_component_map.values

    # Check all source columns exist
    missing_cols = [col for col in source_cols if col not in result_df.columns]
    if missing_cols:
        raise KeyError(f"Missing source columns: {missing_cols}")

    # Prepare ordered rules (preserve original order)
    rules = []
    for mv in rep_map.maps:
        rules.append(
            {
                "patterns": mv.source,  # list of patterns or exact values
                "target": mv.target[0],
            }
        )

    # Apply mapping row-wise with regex support
    def match_row(row):
        for rule in rules:  # Apply in order
            match = True
            for col_val, pattern in zip(row, rule["patterns"]):
                if pattern.startswith("regex:"):
                    regex = pattern.replace("regex:", "")
                    if not re.fullmatch(regex, str(col_val)):
                        match = False
                        break
                else:
                    if col_val != pattern:
                        match = False
                        break
            if match:
                return rule["target"]  # First match wins
        return None  # No match found

    result_df[target_col] = result_df[source_cols].apply(match_row, axis=1)

    if verbose:
        print(
            f"✅ Mapped {source_cols} → '{target_col}' using {len(rules)} ordered rules."
        )
        unmapped = result_df[target_col].isna().sum()
        if unmapped > 0:
            print(f"⚠️ {unmapped} rows could not be mapped (set to NaN).")

    return result_df

# endregion