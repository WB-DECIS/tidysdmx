"""Apply pysdmx StructureMap objects to pandas DataFrames."""

import logging
import re
from collections.abc import Sequence
from typing import Any

import numpy as np
import pandas as pd
from pysdmx.model.map import (
    ComponentMap,
    FixedValueMap,
    ImplicitComponentMap,
    MultiComponentMap,
    RepresentationMap,
    StructureMap,
    ValueMap,
)
from typeguard import typechecked


def _require_pandas_3(version: str) -> None:
    """Refuse to run on a pandas older than 3.

    The maps return shallow copies, which only pandas 3's Copy-on-Write keeps
    isolated from their input: on pandas 2, editing a returned frame would
    silently edit the caller's DataFrame. pip enforces ``pandas>=3``, but a
    ``--no-deps`` install or a stale environment does not.

    Args:
        version: A pandas version string, such as ``pd.__version__``.

    Raises:
        ImportError: If ``version`` is older than 3.
    """
    major = version.partition(".")[0]
    if major.isdigit() and int(major) < 3:
        raise ImportError(
            f"tidysdmx requires pandas>=3 (Copy-on-Write), but pandas {version} "
            "is installed. Upgrade pandas, or run `uv sync` in a checkout."
        )


_require_pandas_3(pd.__version__)

logger = logging.getLogger(__name__)

_SUPPORTED_MAP_TYPES: tuple[type, ...] = (
    FixedValueMap,
    ImplicitComponentMap,
    ComponentMap,
    MultiComponentMap,
)

# Rows stringified at a time when equal source values must be told apart by
# their printed form (see _print_form_key), so that the strings held at once
# stay bounded however long the column is.
_PRINT_FORM_CHUNK_ROWS: int = 2**20


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

    Every map reads its source column(s) from ``df`` as passed in, never from
    another map's output: a StructureMap's sources belong to the source
    structure. One source column can therefore feed several targets,
    including a target of the same name (``METRIC`` → ``METRIC`` alongside
    ``METRIC`` → ``UNIT_MEASURE``). Targets are written by map type —
    FixedValueMap, ImplicitComponentMap, ComponentMap, MultiComponentMap —
    and in stored order within a type, so when several maps write the same
    target, the last one wins.

    Args:
        df: The source dataset.
        structure_map: A StructureMap containing various mapping components.
        verbose: If True, log applied mappings at INFO level (DEBUG otherwise).
            Data-loss warnings are always logged at WARNING level.

    Returns:
        A new DataFrame holding ``df``'s columns plus every target column,
        added or overwritten. Passthrough columns are not copied up front:
        pandas' Copy-on-Write copies one only if it is later modified, so
        editing the result never changes ``df``.

    Raises:
        KeyError: If a ComponentMap or MultiComponentMap source column is not
            in ``df``.
        TypeError: If the StructureMap contains an unsupported map type
            (e.g. ``DatePatternMap``), or a ComponentMap/MultiComponentMap
            references its representation map by URN instead of embedding it.
    """
    fixed_value_maps, implicit_maps, component_maps, multi_component_maps = (
        _split_maps_by_type(structure_map)
    )

    # Maps read from ``df`` and write only their target into ``result_df``, so
    # no map sees another's output. Chaining them instead let METRIC → METRIC
    # overwrite METRIC before METRIC → UNIT_MEASURE ran (issue #265). The copy
    # is shallow: under pandas' Copy-on-Write, ``result_df`` is still isolated
    # from ``df``, but its passthrough columns cost no memory until written to.
    result_df = df.copy(deep=False)

    if fixed_value_maps:
        result_df = apply_fixed_value_maps(df, fixed_value_maps)
        logger.log(
            _progress_level(verbose),
            "Applied %d FixedValueMap(s).",
            len(fixed_value_maps),
        )

    if implicit_maps:
        implicit_df = apply_implicit_component_maps(df, implicit_maps, verbose=verbose)
        for imap in implicit_maps:
            # A map with a missing source was skipped (and logged) above.
            if imap.source in df.columns:
                result_df[imap.target] = implicit_df[imap.target]

    for cmap in component_maps:
        mapped = apply_component_map(df, cmap, verbose=verbose)
        result_df[cmap.target] = mapped[cmap.target]

    for mcm in multi_component_maps:
        target = mcm.target[0]
        mapped = apply_multi_component_map(df, mcm, verbose=verbose)
        result_df[target] = mapped[target]

    return result_df


def _split_maps_by_type(
    structure_map: StructureMap,
) -> tuple[
    list[FixedValueMap],
    list[ImplicitComponentMap],
    list[ComponentMap],
    list[MultiComponentMap],
]:
    """Group a StructureMap's maps by type, keeping their stored order.

    Delegates to pysdmx's typed views (``StructureMap.fixed_value_maps`` and
    friends), which filter ``maps`` in stored order.

    Raises:
        TypeError: If a map is not one of the four supported types.
    """
    for m in structure_map.maps:
        if not isinstance(m, _SUPPORTED_MAP_TYPES):
            raise TypeError(f"Unknown map type: {type(m)}")

    return (
        list(structure_map.fixed_value_maps),
        list(structure_map.implicit_component_maps),
        list(structure_map.component_maps),
        list(structure_map.multi_component_maps),
    )


def _unresolved_values_error(kind: str, source: object, urn: str) -> TypeError:
    """Build the error for a component map whose values are only a URN."""
    return TypeError(
        f"The {kind} for source {source!r} references its representation map "
        f"by URN ({urn!r}) instead of embedding it, so there are no value maps "
        "to apply. Pass a StructureMap whose maps embed their representation "
        "maps, e.g. one fetched with FmrClient.fetch_structure_map(), which "
        "resolves them; FmrClient.fetch_representation_map(urn) fetches the one "
        "this map references."
    )


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

    result_df = df.copy(deep=False)

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
    source/target names. Each map copies from ``df`` as passed in, so maps
    never see each other's output (two maps can swap columns).

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

    result_df = df.copy(deep=False)

    for imap in implicit_maps:
        source_col = imap.source
        target_col = imap.target

        if source_col not in df.columns:
            logger.warning("Source column '%s' not found. Skipping.", source_col)
            continue

        result_df[target_col] = df[source_col]
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

    Missing source values (NaN/None) are matched only by the catch-all
    (``"regex:.*"``, the COMP_MAPPING ``DEFAULT_VALUE``), never by a literal
    or any other regex value map. With a catch-all they take its target, and
    a warning reports how many did; without one they yield NaN in the target
    column.

    Each distinct source value is matched once and the result broadcast to
    every row, so value-map matching grows with the number of distinct values
    rather than rows. Values count as distinct by printed form as well as by
    equality, because a regex matches ``str(value)``: 1 and 1.0 compare equal
    but are matched apart. In object columns holding non-string values, that
    takes one ``str()`` call per such row.

    Args:
        df: Source data.
        component_map: ComponentMap with source, target, and values.
        verbose: If True, log progress at INFO level (DEBUG otherwise).
            Unmapped-value warnings are always logged at WARNING level.

    Returns:
        DataFrame with the target column added or overwritten.

    Raises:
        KeyError: If the source column is not in ``df``.
        TypeError: If ``component_map.values`` is a URN string rather than an
            embedded RepresentationMap.
    """
    source_col = component_map.source
    target_col = component_map.target
    rep_map = component_map.values
    if isinstance(rep_map, str):
        raise _unresolved_values_error("ComponentMap", source_col, rep_map)

    if source_col not in df.columns:
        raise KeyError(f"Source column '{source_col}' not found in DataFrame.")

    # Resolve each distinct source value once, then broadcast the result back
    # to every row by its code: matching then costs a few hashing passes over
    # the rows plus work proportional to the number of distinct values,
    # instead of a Python-level regex call per row. Missing values form a
    # group of their own, which only the catch-all maps.
    source = df[source_col]
    reads_regex = any(vm.source.startswith("regex:") for vm in rep_map.maps)
    codes, positions = _distinct_rows([source], [reads_regex])
    literal_mapping, regex_maps, resolved = _resolve_component_values(
        source.iloc[positions].reset_index(drop=True), rep_map
    )

    # dtype= keeps the resolved dtype: an object result (the regex path) would
    # otherwise be re-inferred as "str" when rebuilt from a NumPy array. The
    # array's own take(), not pd.api.extensions.take(): without allow_fill that
    # passes axis=, which Arrow-backed arrays (pandas 3's "str" whenever pyarrow
    # is installed) reject. codes hold no -1, so nothing needs filling.
    result_df = df.copy(deep=False)
    result_df[target_col] = pd.Series(
        resolved.array.take(codes),
        index=df.index,
        dtype=resolved.dtype,
    )

    n_pairs = len(literal_mapping) + len(regex_maps)
    logger.log(
        _progress_level(verbose),
        "Mapped '%s' → '%s' using %d pairs.",
        source_col,
        target_col,
        n_pairs,
    )
    default = _catch_all_target(regex_maps)
    n_missing = source.isna().sum()
    if default is not None and n_missing > 0:
        logger.warning(
            "%d missing source value(s) in '%s' assigned the default '%s'.",
            n_missing,
            source_col,
            default,
        )
    unmapped = result_df[target_col].isna().sum()
    if unmapped > 0:
        logger.warning("%d values could not be mapped (set to NaN).", unmapped)

    return result_df


def _resolve_component_values(
    values: pd.Series, rep_map: RepresentationMap
) -> tuple[dict[str, str], list[ValueMap], pd.Series]:
    """Map source values through a RepresentationMap's value maps.

    Literal (exact) value maps are applied first via a vectorised lookup and
    always win. Values left unmapped then fall through to the regex value maps,
    with any catch-all (``"regex:.*"``) evaluated last. Missing values are
    matched by the catch-all alone, never by a literal or another regex.

    Args:
        values: The source values to map, typically the distinct values of a
            source column.
        rep_map: The RepresentationMap holding the value maps.

    Returns:
        The literal lookup, the rank-sorted regex value maps, and the mapped
        values, aligned with ``values``.
    """
    literal_mapping = {
        vm.source: vm.target
        for vm in rep_map.maps
        if not vm.source.startswith("regex:")
    }
    regex_maps = sorted(
        (vm for vm in rep_map.maps if vm.source.startswith("regex:")),
        key=lambda vm: _value_map_rank([vm.source]),
    )

    mapped = values.map(literal_mapping)

    if regex_maps:

        def _regex_target(value: object) -> object:
            for vm in regex_maps:
                pattern = vm.source.removeprefix("regex:")
                if re.fullmatch(pattern, str(value)):
                    return vm.target
            return None

        # Missing source values skip the regex pass: str(NaN) is "nan", which
        # a regex such as "regex:n.*" would otherwise match.
        unmatched = mapped.isna() & values.notna()
        if unmatched.any():
            mapped = mapped.astype(object)
            mapped.loc[unmatched] = values.loc[unmatched].map(_regex_target)

        # The catch-all (DEFAULT_VALUE) means "anything not listed", so it
        # covers missing source values too.
        default = _catch_all_target(regex_maps)
        missing = values.isna()
        if default is not None and missing.any():
            mapped = mapped.astype(object)
            mapped.loc[missing] = default

    return literal_mapping, regex_maps, mapped


def _catch_all_target(regex_maps: Sequence[ValueMap]) -> str | None:
    """Return the target of the first catch-all (``"regex:.*"``) value map."""
    return next(
        (vm.target for vm in regex_maps if _value_map_rank([vm.source]) == 2),
        None,
    )


def _distinct_rows(
    sources: Sequence[pd.Series], split_print_forms: Sequence[bool] | None = None
) -> tuple[np.ndarray, np.ndarray]:
    """Number the distinct combinations of source values, row by row.

    Values are grouped by equality and, for a source a regex value map reads,
    by printed form too, since a regex matches ``str(value)`` and equal values
    can print differently (see ``_print_form_key``). Literal value maps match
    equal values alike, so a source only literal maps read skips that pass.
    All missing values of a source form one group.

    Args:
        sources: The source columns, aligned row by row.
        split_print_forms: For each source, whether a regex value map reads
            it. None treats every source as read by a regex.

    Returns:
        ``codes``, numbering each row's combination densely from 0, and
        ``positions``, the first row of each combination, so that
        ``codes[positions[i]] == i``.

    Raises:
        ValueError: If ``sources`` is empty.
    """
    if split_print_forms is None:
        split_print_forms = [True] * len(sources)
    codes: np.ndarray | None = None
    for source, split in zip(sources, split_print_forms, strict=True):
        value_codes, uniques = pd.factorize(source)
        value_codes[value_codes < 0] = len(uniques)
        keys = [(value_codes, len(uniques) + 1)]
        print_form = _print_form_key(source, value_codes, uniques) if split else None
        if print_form is not None:
            keys.append(print_form)
        for key, n_keys in keys:
            # codes and key are both at most len(source), so the pair number
            # stays below (len(source) + 1) ** 2, far inside int64.
            codes = key if codes is None else pd.factorize(codes * n_keys + key)[0]
    if codes is None:
        raise ValueError("A map needs at least one source column to group by.")

    first = pd.Series(codes).drop_duplicates()
    positions = np.empty(len(first), dtype=np.intp)
    positions[first.to_numpy()] = first.index.to_numpy()
    return codes, positions


def _print_form_key(
    source: pd.Series, codes: np.ndarray, uniques: pd.Index
) -> tuple[np.ndarray, int] | None:
    """Tell apart values of ``source`` that compare equal but print differently.

    Grouping by equality alone would match 1, 1.0 and True (or 0.0 and -0.0)
    once, through whichever came first, but a regex value map matches
    ``str(value)``, which differs between them. Only float columns holding
    -0.0, and object or complex columns holding non-string values, can hold
    such values, and only in a group that occurs more than once, so only
    those rows are stringified.

    Args:
        source: A source column.
        codes: ``source``'s factorize codes, with missing values at
            ``len(uniques)``.
        uniques: ``source``'s distinct values, from ``pd.factorize``.

    Returns:
        A key for each row and the key's number of distinct values, or None
        when no two equal values of ``source`` print differently.
    """
    if source.dtype.kind == "f":
        values = source.to_numpy(dtype=np.float64, na_value=np.nan)
        negative_zero = (values == 0) & np.signbit(values)
        if not negative_zero.any():
            return None
        return negative_zero.astype(np.intp), 2

    if not (pd.api.types.is_object_dtype(source.dtype) or source.dtype.kind == "c"):
        return None
    if pd.api.types.infer_dtype(uniques, skipna=True) == "string":
        return None
    # A string equals only strings, which print as themselves, and a group of
    # one row holds one printed form, so only rows whose group holds a
    # non-string and repeats need their printed form. Stringifying every row
    # of a high-cardinality numeric column cost gigabytes for nothing. The
    # missing values' code, len(uniques), is never split.
    repeated = np.bincount(codes, minlength=len(uniques) + 1)[: len(uniques)] > 1
    split = repeated & np.fromiter(
        (not isinstance(value, str) for value in uniques),
        dtype=bool,
        count=len(uniques),
    )
    if not split.any():
        return None
    rows = np.flatnonzero(np.append(split, False)[codes])

    # Stringify in chunks, merging each chunk's distinct forms into one
    # numbering. An object array keeps them Python strings, where
    # Series.map(str) would convert them to the "str" dtype and make that
    # merge slow. Not np.frompyfunc(str): a ufunc checks the FPU status flags
    # after its loop, so a __str__ that leaves one set (CPython 3.14.9's for
    # 1, 1.0 or True) raises a spurious "invalid value" RuntimeWarning.
    values = source.to_numpy()
    key = np.zeros(len(source), dtype=np.intp)
    forms: dict[str, int] = {}
    for start in range(0, len(rows), _PRINT_FORM_CHUNK_ROWS):
        chunk = rows[start : start + _PRINT_FORM_CHUNK_ROWS]
        printed = np.fromiter(map(str, values[chunk]), dtype=object, count=len(chunk))
        chunk_codes, chunk_forms = pd.factorize(printed)
        ids = np.fromiter(
            (forms.setdefault(form, len(forms) + 1) for form in chunk_forms),
            dtype=np.intp,
            count=len(chunk_forms),
        )
        key[chunk] = ids[chunk_codes]
    return key, len(forms) + 1


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

    Rows with a missing value (NaN/None) in any source column are matched
    only by the pure catch-all (the COMP_MAPPING ``DEFAULT_VALUE``), never by
    a literal or partially-regex rule. With a catch-all they take its target,
    and a warning reports how many did; without one they yield NaN in the
    target column.

    Only the first target column is used; multi-target MultiComponentMaps
    are not supported.

    Each distinct combination of source values is matched once and the result
    broadcast to every row, so rule matching grows with the number of
    distinct combinations rather than the number of rows. As in
    ``apply_component_map``, values that compare equal but print differently
    (1 and 1.0) count as distinct.

    Args:
        df: Source data.
        multi_component_map: MultiComponentMap with source columns, target
            column, and values.
        verbose: If True, log progress at INFO level (DEBUG otherwise).
            Unmapped-value warnings are always logged at WARNING level.

    Returns:
        DataFrame with the target column added or overwritten.

    Raises:
        KeyError: If a source column is not in ``df``.
        TypeError: If ``multi_component_map.values`` is a URN string rather
            than an embedded MultiRepresentationMap.
        ValueError: If ``multi_component_map`` lists no source columns.
    """
    source_cols = list(multi_component_map.source)
    target_col = multi_component_map.target[0]
    rep_map = multi_component_map.values
    if isinstance(rep_map, str):
        raise _unresolved_values_error("MultiComponentMap", source_cols, rep_map)

    missing_cols = [col for col in source_cols if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Missing source columns: {missing_cols}")

    rules = [{"patterns": mv.source, "target": mv.target[0]} for mv in rep_map.maps]

    # Order rules so explicit (literal) tuples are tried before regex ones and
    # the catch-all ("regex:.*" for every component) is tried last. The sort is
    # stable, so the stored order within each priority tier is preserved; this
    # keeps results independent of the maps' stored order (e.g. after an FMR
    # round-trip).
    rules.sort(key=lambda rule: _value_map_rank(rule["patterns"]))

    # Resolve each distinct combination of source values once, then broadcast
    # back to every row by its code. Evaluating the rules over all rows
    # instead held one row-length mask per rule, which ran out of memory on
    # large frames. Rows with a missing value land in combinations of their
    # own, which only the catch-all maps. Each source is taken as a
    # Series, so a label that also names an index level, or appears twice in
    # source_cols, is no problem.
    sources = [df[col] for col in source_cols]
    # Only a source some rule reads with a regex needs its printed forms. A
    # rule with too few patterns is left for _resolve_multi_values to reject.
    reads_regex = [
        any(
            pattern.startswith("regex:")
            for rule in rules
            for pattern in rule["patterns"][i : i + 1]
        )
        for i in range(len(sources))
    ]
    codes, positions = _distinct_rows(sources, reads_regex)
    # Gather only the source columns, one row per combination: element i of
    # each Series belongs to the combination with code i.
    combinations = [s.iloc[positions].reset_index(drop=True) for s in sources]
    resolved = _resolve_multi_values(combinations, rules)

    result_df = df.copy(deep=False)
    result_df[target_col] = pd.Series(resolved[codes], index=df.index)

    logger.log(
        _progress_level(verbose),
        "Mapped %s → '%s' using %d ordered rules.",
        source_cols,
        target_col,
        len(rules),
    )
    default = next(
        (rule["target"] for rule in rules if _value_map_rank(rule["patterns"]) == 2),
        None,
    )
    if default is not None:
        n_missing = np.logical_or.reduce([s.isna().to_numpy() for s in sources]).sum()
        if n_missing > 0:
            logger.warning(
                "%d row(s) with a missing source value in %s assigned the "
                "default '%s'.",
                n_missing,
                source_cols,
                default,
            )
    unmapped = result_df[target_col].isna().sum()
    if unmapped > 0:
        logger.warning("%d rows could not be mapped (set to NaN).", unmapped)

    return result_df


def _resolve_multi_values(
    combinations: list[pd.Series],
    rules: list[dict[str, Any]],
) -> np.ndarray:
    """Return the first matching rule's target for each source combination.

    Args:
        combinations: One Series per source column, in the order of each
            rule's patterns; element i of every Series forms combination i.
        rules: Rank-sorted rules, each with ``"patterns"`` (one per source
            column) and ``"target"``.

    Returns:
        An object array with one entry per combination, holding the matched
        target, or None where no rule matches.
    """
    str_cols: dict[int, pd.Series] = {}

    def _stringified(position: int) -> pd.Series:
        # Per-cell str() (not .astype("string")) keeps the exact regex
        # semantics of the previous row-wise implementation and of
        # apply_component_map: .astype("string") formats some dtypes
        # differently, and array-dependently (e.g. it trims " 00:00:00"
        # from datetime columns, but only when every value is midnight).
        # na_action="ignore" keeps missing values missing so that na=False
        # below treats them as non-matches; only the catch-all maps them.
        if position not in str_cols:
            str_cols[position] = combinations[position].map(str, na_action="ignore")
        return str_cols[position]

    # Build one boolean mask per rule, vectorised across the source columns,
    # then resolve them with np.select: rules are already rank-sorted, and
    # np.select picks the first true condition per row, so the first matching
    # rule wins.
    n_rows = len(combinations[0])
    conditions = []
    choices = []
    for rule in rules:
        mask = np.ones(n_rows, dtype=bool)
        # The catch-all (DEFAULT_VALUE) means "anything not listed", so it also
        # matches a missing value, which every other rule skips.
        catch_all = _value_map_rank(rule["patterns"]) == 2
        # strict=True preserves the contract that each rule must supply one
        # pattern per source column.
        for position, (col_series, pattern) in enumerate(
            zip(combinations, rule["patterns"], strict=True)
        ):
            if pattern.startswith("regex:"):
                regex = pattern.removeprefix("regex:")
                col_mask = (
                    _stringified(position)
                    .str.fullmatch(regex, na=False)
                    .to_numpy(dtype=bool)
                )
                if catch_all:
                    # Not |=: under Copy-on-Write to_numpy() can be read-only.
                    col_mask = col_mask | col_series.isna().to_numpy(dtype=bool)
            else:
                # fillna(False) guards nullable-boolean comparison results
                # (e.g. the "string" dtype) so missing values never match and
                # the conversion to a plain bool array cannot fail on pd.NA.
                col_mask = (col_series == pattern).fillna(False).to_numpy(dtype=bool)
            mask &= col_mask
        conditions.append(mask)
        choices.append(rule["target"])

    if not conditions:
        return np.full(n_rows, None, dtype=object)
    return np.select(conditions, choices, default=None)
