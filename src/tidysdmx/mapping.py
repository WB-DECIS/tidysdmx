"""Apply pysdmx StructureMap objects to pandas DataFrames."""

import logging
import re
from collections.abc import Sequence

import numpy as np
import pandas as pd
from pysdmx.model.map import (
    ComponentMap,
    FixedValueMap,
    ImplicitComponentMap,
    MultiComponentMap,
    StructureMap,
)
from typeguard import typechecked

logger = logging.getLogger(__name__)


def _progress_level(verbose: bool) -> int:
    """Return the log level for progress messages (INFO when verbose)."""
    return logging.INFO if verbose else logging.DEBUG


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
    structure_map: StructureMap,
    verbose: bool = False,
) -> pd.DataFrame:
    """Apply all mapping components from a StructureMap to a DataFrame.

    Separates the maps by type and applies them in order:
    FixedValueMap, ImplicitComponentMap, ComponentMap, MultiComponentMap.

    Args:
        df: The source dataset.
        structure_map: A StructureMap containing various mapping components.
        verbose: If True, log applied mappings at INFO level (DEBUG otherwise).
            Data-loss warnings are always logged at WARNING level.

    Returns:
        Modified DataFrame with all mappings applied.
    """
    fixed_value_maps = []
    implicit_maps = []
    component_maps = []
    multi_component_maps = []

    for m in structure_map.maps:
        if isinstance(m, FixedValueMap):
            fixed_value_maps.append(m)
        elif isinstance(m, ImplicitComponentMap):
            implicit_maps.append(m)
        elif isinstance(m, ComponentMap):
            component_maps.append(m)
        elif isinstance(m, MultiComponentMap):
            multi_component_maps.append(m)
        else:
            raise TypeError(f"Unknown map type: {type(m)}")

    result_df = df

    if fixed_value_maps:
        result_df = apply_fixed_value_maps(result_df, fixed_value_maps)
        logger.log(
            _progress_level(verbose),
            "Applied %d FixedValueMap(s).",
            len(fixed_value_maps),
        )

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
    fixed_value_maps: list[FixedValueMap],
) -> pd.DataFrame:
    """Apply FixedValueMap rules to a DataFrame.

    Args:
        df: The source dataset.
        fixed_value_maps: A list of FixedValueMap objects containing target and value.

    Returns:
        DataFrame with fixed value columns added.
    """
    if not all(isinstance(m, FixedValueMap) for m in fixed_value_maps):
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
    implicit_maps: list[ImplicitComponentMap],
    verbose: bool = False,
) -> pd.DataFrame:
    """Apply ImplicitComponentMap rules to a DataFrame.

    Copies values from source to target columns, supporting different
    source/target names.

    Args:
        df: The source dataset.
        implicit_maps: A list of ImplicitComponentMap objects.
        verbose: If True, log applied mappings at INFO level (DEBUG otherwise).
            Missing-source warnings are always logged at WARNING level.

    Returns:
        DataFrame with implicit component mappings applied.
    """
    if not all(isinstance(m, ImplicitComponentMap) for m in implicit_maps):
        raise TypeError(
            "All elements in implicit_maps must be ImplicitComponentMap instances."
        )

    result_df = df.copy()

    for imap in implicit_maps:
        source_col = imap.source
        target_col = imap.target

        if source_col not in result_df.columns:
            logger.warning("Source column '%s' not found. Skipping.", source_col)
            continue

        result_df[target_col] = result_df[source_col]
        action = "Overwritten" if target_col in df.columns else "Added"
        logger.log(
            _progress_level(verbose),
            "%s column '%s' from source '%s'.",
            action,
            target_col,
            source_col,
        )

    return result_df


@typechecked
def apply_component_map(
    df: pd.DataFrame,
    component_map: ComponentMap,
    verbose: bool = False,
) -> pd.DataFrame:
    """Apply a single ComponentMap with a RepresentationMap to a DataFrame.

    Missing source values (NaN/None) remain unmapped: they are never matched
    by regex value maps or the catch-all (``"regex:.*"``) and yield NaN in the
    target column.

    Args:
        df: Source data.
        component_map: ComponentMap with source, target, and values.
        verbose: If True, log progress at INFO level (DEBUG otherwise).
            Unmapped-value warnings are always logged at WARNING level.

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

        # Missing source values stay unmapped: str(NaN) is "nan", which a
        # regex (especially the catch-all "regex:.*") would otherwise match.
        unmatched = mapped.isna() & result_df[source_col].notna()
        if unmatched.any():
            mapped = mapped.astype(object)
            mapped.loc[unmatched] = result_df.loc[unmatched, source_col].map(
                _regex_target
            )

    result_df[target_col] = mapped

    n_pairs = len(literal_mapping) + len(regex_maps)
    logger.log(
        _progress_level(verbose),
        "Mapped '%s' → '%s' using %d pairs.",
        source_col,
        target_col,
        n_pairs,
    )
    unmapped = result_df[target_col].isna().sum()
    if unmapped > 0:
        logger.warning("%d values could not be mapped (set to NaN).", unmapped)

    return result_df


@typechecked
def apply_multi_component_map(
    df: pd.DataFrame,
    multi_component_map: MultiComponentMap,
    verbose: bool = False,
) -> pd.DataFrame:
    """Apply a single MultiComponentMap with regex and catch-all support.

    Rules are evaluated by priority so that results do not depend on the stored
    order of the value maps: explicit (literal) tuples first, then regex tuples,
    then any pure catch-all (``"regex:.*"`` for every component) last. The first
    matching rule wins. Patterns prefixed with ``"regex:"`` must match the full
    stringified value (``Series.str.fullmatch``).

    Rows with a missing value (NaN/None) in any source column remain
    unmapped: they are never matched by any rule (including the catch-all)
    and yield NaN in the target column.

    Only the first target column is used; multi-target MultiComponentMaps
    are not supported.

    Args:
        df: Source data.
        multi_component_map: MultiComponentMap with source columns, target
            column, and values.
        verbose: If True, log progress at INFO level (DEBUG otherwise).
            Unmapped-value warnings are always logged at WARNING level.

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

    str_cols: dict[str, pd.Series] = {}

    def _stringified(col: str) -> pd.Series:
        # Per-cell str() (not .astype("string")) keeps the exact regex
        # semantics of the previous row-wise implementation and of
        # apply_component_map: .astype("string") formats some dtypes
        # differently, and array-dependently (e.g. it trims " 00:00:00"
        # from datetime columns, but only when every value is midnight).
        # na_action="ignore" keeps missing values missing so that na=False
        # below treats them as non-matches and they stay unmapped.
        if col not in str_cols:
            str_cols[col] = result_df[col].map(str, na_action="ignore")
        return str_cols[col]

    # Build one boolean mask per rule, vectorised across the source columns,
    # then resolve them with np.select: rules are already rank-sorted, and
    # np.select picks the first true condition per row, so the first matching
    # rule wins. This replaces a row-wise .apply(axis=1) so matching cost
    # scales with the number of rules/columns instead of the number of rows.
    n_rows = len(result_df)
    conditions = []
    choices = []
    for rule in rules:
        mask = np.ones(n_rows, dtype=bool)
        # strict=True preserves the contract that each rule must supply one
        # pattern per source column.
        for col, pattern in zip(source_cols, rule["patterns"], strict=True):
            col_series = result_df[col]
            if pattern.startswith("regex:"):
                regex = pattern.removeprefix("regex:")
                col_mask = (
                    _stringified(col)
                    .str.fullmatch(regex, na=False)
                    .to_numpy(dtype=bool)
                )
            else:
                # fillna(False) guards nullable-boolean comparison results
                # (e.g. the "string" dtype) so missing values never match and
                # the conversion to a plain bool array cannot fail on pd.NA.
                col_mask = (col_series == pattern).fillna(False).to_numpy(dtype=bool)
            mask &= col_mask
        conditions.append(mask)
        choices.append(rule["target"])

    if conditions:
        result_df[target_col] = pd.Series(
            np.select(conditions, choices, default=None), index=result_df.index
        )
    else:
        result_df[target_col] = None

    logger.log(
        _progress_level(verbose),
        "Mapped %s → '%s' using %d ordered rules.",
        source_cols,
        target_col,
        len(rules),
    )
    unmapped = result_df[target_col].isna().sum()
    if unmapped > 0:
        logger.warning("%d rows could not be mapped (set to NaN).", unmapped)

    return result_df
