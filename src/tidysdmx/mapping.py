"""Apply pysdmx StructureMap objects to pandas DataFrames."""

import re
from collections.abc import Sequence

import pandas as pd
import pysdmx as px
from typeguard import typechecked


def _value_map_rank(patterns: Sequence[str]) -> int:
    """Return the apply-order priority of a value map's source pattern(s).

    Lower ranks are evaluated first, so explicit literal maps win over regex
    maps and a pure catch-all is always tried last, regardless of the order in
    which the maps are stored (e.g. after an FMR round-trip):

    * 0 — every pattern is a literal value (exact match)
    * 1 — at least one pattern is a regex, but not a pure catch-all
    * 2 — every pattern is the catch-all ``"regex:.*"``

    Args:
        patterns: The source pattern(s) of a ``ValueMap`` or ``MultiValueMap``.

    Returns:
        The priority rank: 0 (literal), 1 (regex), or 2 (catch-all).
    """
    if all(p == "regex:.*" for p in patterns):
        return 2
    if any(p.startswith("regex:") for p in patterns):
        return 1
    return 0


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

    # Apply literal (exact) value maps first via a fast vectorised lookup;
    # these always win. Source values left unmapped then fall through to the
    # regex value maps, with any catch-all ("regex:.*") evaluated last.
    literal_mapping = {
        vm.source: vm.target
        for vm in rep_map.maps
        if not vm.source.startswith("regex:")
    }
    regex_maps = sorted(
        (vm for vm in rep_map.maps if vm.source.startswith("regex:")),
        key=lambda vm: _value_map_rank([vm.source]),
    )

    mapped = result_df[source_col].map(literal_mapping)

    if regex_maps:

        def _regex_target(value: object) -> object:
            for vm in regex_maps:
                pattern = vm.source.removeprefix("regex:")
                if re.fullmatch(pattern, str(value)):
                    return vm.target
            return None

        unmatched = mapped.isna()
        if unmatched.any():
            mapped = mapped.astype(object)
            mapped.loc[unmatched] = result_df.loc[unmatched, source_col].map(
                _regex_target
            )

    result_df[target_col] = mapped

    if verbose:
        n_pairs = len(literal_mapping) + len(regex_maps)
        print(f"[OK] Mapped '{source_col}' → '{target_col}' using {n_pairs} pairs.")
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
    """Apply a single MultiComponentMap with regex and catch-all support.

    Rules are evaluated by priority so that results do not depend on the stored
    order of the value maps: explicit (literal) tuples first, then regex tuples,
    then any pure catch-all (``"regex:.*"`` for every component) last. The first
    matching rule wins. Patterns prefixed with ``"regex:"`` are matched using
    ``re.fullmatch``.

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

    rules = [{"patterns": mv.source, "target": mv.target[0]} for mv in rep_map.maps]

    # Order rules so explicit (literal) tuples are tried before regex ones and
    # the catch-all ("regex:.*" for every component) is tried last. The sort is
    # stable, so the stored order within each priority tier is preserved; this
    # keeps results independent of the maps' stored order (e.g. after an FMR
    # round-trip).
    rules.sort(key=lambda rule: _value_map_rank(rule["patterns"]))

    def match_row(row):
        for rule in rules:
            match = True
            for col_val, pattern in zip(row, rule["patterns"], strict=True):
                if pattern.startswith("regex:"):
                    regex = pattern.removeprefix("regex:")
                    if not re.fullmatch(regex, str(col_val)):
                        match = False
                        break
                elif col_val != pattern:
                    match = False
                    break
            if match:
                return rule["target"]
        return None

    result_df[target_col] = result_df[source_cols].apply(match_row, axis=1)

    if verbose:
        print(
            f"[OK] Mapped {source_cols} → '{target_col}' "
            f"using {len(rules)} ordered rules."
        )
        unmapped = result_df[target_col].isna().sum()
        if unmapped > 0:
            print(f"[WARN] {unmapped} rows could not be mapped (set to NaN).")

    return result_df
