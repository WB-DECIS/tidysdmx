"""Apply pysdmx StructureMap objects to pandas DataFrames."""

import numpy as np
import pandas as pd
import pysdmx as px
from typeguard import typechecked


@typechecked
def map_structures(
    df: pd.DataFrame,
    structure_map: px.model.map.StructureMap,
    verbose: bool = False,
) -> pd.DataFrame:
    """Apply all mapping components from a StructureMap to a DataFrame.

    Separates the maps by type and applies them in order:
    FixedValueMap, ImplicitComponentMap, ComponentMap, MultiComponentMap.

    Args:
        df: The source dataset.
        structure_map: A StructureMap containing various mapping components.
        verbose: If True, print logs about applied mappings.

    Returns:
        Modified DataFrame with all mappings applied.
    """
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

    result_df = df

    if fixed_value_maps:
        result_df = apply_fixed_value_maps(result_df, fixed_value_maps)
        if verbose:
            print(f"[OK] Applied {len(fixed_value_maps)} FixedValueMap(s).")

    if implicit_maps:
        result_df = apply_implicit_component_maps(
            result_df, implicit_maps, verbose=verbose
        )

    for cmap in component_maps:
        result_df = apply_component_map(result_df, cmap, verbose=verbose)

    for mcm in multi_component_maps:
        result_df = apply_multi_component_map(result_df, mcm, verbose=verbose)

    return result_df


@typechecked
def apply_fixed_value_maps(
    df: pd.DataFrame,
    fixed_value_maps: list[px.model.map.FixedValueMap],
) -> pd.DataFrame:
    """Apply FixedValueMap rules to a DataFrame.

    Args:
        df: The source dataset.
        fixed_value_maps: A list of FixedValueMap objects containing target and value.

    Returns:
        DataFrame with fixed value columns added.
    """
    if not all(isinstance(m, px.model.map.FixedValueMap) for m in fixed_value_maps):
        raise TypeError(
            "All elements in fixed_value_maps must be FixedValueMap instances."
        )

    result_df = df.copy()

    for fmap in fixed_value_maps:
        result_df[fmap.target] = fmap.value

    return result_df


@typechecked
def apply_implicit_component_maps(
    df: pd.DataFrame,
    implicit_maps: list[px.model.map.ImplicitComponentMap],
    verbose: bool = False,
) -> pd.DataFrame:
    """Apply ImplicitComponentMap rules to a DataFrame.

    Copies values from source to target columns, supporting different
    source/target names.

    Args:
        df: The source dataset.
        implicit_maps: A list of ImplicitComponentMap objects.
        verbose: If True, print logs about applied mappings and conflicts.

    Returns:
        DataFrame with implicit component mappings applied.
    """
    if not all(isinstance(m, px.model.map.ImplicitComponentMap) for m in implicit_maps):
        raise TypeError(
            "All elements in implicit_maps must be ImplicitComponentMap instances."
        )

    result_df = df.copy()

    for imap in implicit_maps:
        source_col = imap.source
        target_col = imap.target

        if source_col not in result_df.columns:
            if verbose:
                print(f"[WARN] Source column '{source_col}' not found. Skipping.")
            continue

        result_df[target_col] = result_df[source_col]
        if verbose:
            action = "Overwritten" if target_col in df.columns else "Added"
            print(f"[OK] {action} column '{target_col}' from source '{source_col}'.")

    return result_df


@typechecked
def apply_component_map(
    df: pd.DataFrame,
    component_map: px.model.map.ComponentMap,
    verbose: bool = False,
) -> pd.DataFrame:
    """Apply a single ComponentMap with a RepresentationMap to a DataFrame.

    Args:
        df: Source data.
        component_map: ComponentMap with source, target, and values.
        verbose: If True, print progress.

    Returns:
        DataFrame with the target column added or overwritten.
    """
    result_df = df.copy()

    source_col = component_map.source
    target_col = component_map.target
    rep_map = component_map.values

    if source_col not in result_df.columns:
        raise KeyError(f"Source column '{source_col}' not found in DataFrame.")

    mapping = {vm.source: vm.target for vm in rep_map.maps}
    result_df[target_col] = result_df[source_col].map(mapping)

    if verbose:
        print(
            f"[OK] Mapped '{source_col}' → '{target_col}' using {len(mapping)} pairs."
        )
        unmapped = result_df[target_col].isna().sum()
        if unmapped > 0:
            print(f"[WARN] {unmapped} values could not be mapped (set to NaN).")

    return result_df


@typechecked
def apply_multi_component_map(
    df: pd.DataFrame,
    multi_component_map: px.model.map.MultiComponentMap,
    verbose: bool = False,
) -> pd.DataFrame:
    """Apply a single MultiComponentMap with regex support, preserving rule order.

    Rules are applied in the order they appear in the MultiRepresentationMap.
    The first matching rule wins. Patterns prefixed with ``"regex:"`` are
    matched using ``re.fullmatch``.

    Only the first target column is used; multi-target MultiComponentMaps
    are not supported.

    Args:
        df: Source data.
        multi_component_map: MultiComponentMap with source columns, target
            column, and values.
        verbose: If True, print progress.

    Returns:
        DataFrame with the target column added or overwritten.
    """
    result_df = df.copy()

    source_cols = multi_component_map.source
    target_col = multi_component_map.target[0]
    rep_map = multi_component_map.values

    missing_cols = [col for col in source_cols if col not in result_df.columns]
    if missing_cols:
        raise KeyError(f"Missing source columns: {missing_cols}")

    # Build one boolean mask per rule, vectorised across the source columns,
    # then resolve them with np.select (first matching rule wins, preserving
    # rule order). This replaces a row-wise .apply(axis=1) and scales with the
    # number of rules/columns rather than the number of data rows.
    n_rows = len(result_df)
    conditions = []
    choices = []
    for mv in rep_map.maps:
        mask = np.ones(n_rows, dtype=bool)
        # strict=True preserves the contract that each rule must supply one
        # pattern per source column.
        for col, pattern in zip(source_cols, mv.source, strict=True):
            col_series = result_df[col]
            if pattern.startswith("regex:"):
                regex = pattern.removeprefix("regex:")
                col_mask = (
                    col_series.astype(str)
                    .str.fullmatch(regex)
                    .fillna(False)
                    .to_numpy(dtype=bool)
                )
            else:
                col_mask = (col_series == pattern).to_numpy(dtype=bool)
            mask &= col_mask
        conditions.append(mask)
        choices.append(mv.target[0])

    if conditions:
        result_df[target_col] = pd.Series(
            np.select(conditions, choices, default=None), index=result_df.index
        )
    else:
        result_df[target_col] = None

    if verbose:
        print(
            f"[OK] Mapped {source_cols} → '{target_col}' "
            f"using {len(conditions)} ordered rules."
        )
        unmapped = result_df[target_col].isna().sum()
        if unmapped > 0:
            print(f"[WARN] {unmapped} rows could not be mapped (set to NaN).")

    return result_df
