"""Apply pysdmx StructureMap objects to pandas DataFrames."""

import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from pandas.api.extensions import ExtensionArray
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

# A value map whose source starts with this prefix holds a regular expression,
# which must match the whole printed value; any other source is a literal.
_REGEX_PREFIX: str = "regex:"
# The regex value map that matches every value the others leave: a default.
_CATCH_ALL: str = "regex:.*"

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
    if all(p == _CATCH_ALL for p in patterns):
        return 2
    if any(p.startswith(_REGEX_PREFIX) for p in patterns):
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

    Literal (exact) value maps always win, and are looked up for every row in
    one vectorised pass. Rows they leave unmatched fall through to the regex
    value maps, with any catch-all (``"regex:.*"``) evaluated last. A regex
    is matched once per distinct leftover value and the result broadcast back
    to the rows, so regex matching grows with the number of distinct values
    rather than rows. Values count as distinct by printed form as well as by
    equality, because a regex matches ``str(value)``: 1 and 1.0 compare equal
    but are matched apart. In object columns holding non-string values, that
    takes one ``str()`` call per such row.

    Missing source values (NaN/None) remain unmapped: they are never matched
    by regex value maps or the catch-all, and yield NaN in the target column.

    The target column has the dtype of the value maps' targets (``str``),
    whatever the source column's dtype, or ``object`` once regex value maps
    were applied.

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

    literal_mapping, regex_maps = _split_value_maps(rep_map)
    mapped, n_unmapped = _map_values(df[source_col], literal_mapping, regex_maps)

    # dtype= keeps the mapped dtype: an object result (the regex path) would
    # otherwise be re-inferred as "str". copy=False, because ``mapped`` is a
    # temporary nothing else holds.
    result_df = df.copy(deep=False)
    result_df[target_col] = pd.Series(
        mapped, index=df.index, dtype=mapped.dtype, copy=False
    )

    logger.log(
        _progress_level(verbose),
        "Mapped '%s' → '%s' using %d pairs.",
        source_col,
        target_col,
        len(literal_mapping) + len(regex_maps),
    )
    if n_unmapped > 0:
        logger.warning("%d values could not be mapped (set to NaN).", n_unmapped)

    return result_df


def _split_value_maps(
    rep_map: RepresentationMap,
) -> tuple[dict[str, str], list[ValueMap]]:
    """Separate a RepresentationMap's literal value maps from its regex ones.

    Args:
        rep_map: The RepresentationMap holding the value maps.

    Returns:
        The literal lookup, source value → target (of two maps with the same
        source, the later one wins), and the regex value maps in apply order:
        by ``_value_map_rank``, so a pure catch-all comes last, and in stored
        order within a rank.
    """
    literal_mapping = {
        vm.source: vm.target
        for vm in rep_map.maps
        if not vm.source.startswith(_REGEX_PREFIX)
    }
    regex_maps = sorted(
        (vm for vm in rep_map.maps if vm.source.startswith(_REGEX_PREFIX)),
        key=lambda vm: _value_map_rank([vm.source]),
    )
    return literal_mapping, regex_maps


def _map_values(
    source: pd.Series,
    literal_mapping: dict[str, str],
    regex_maps: Sequence[ValueMap],
) -> tuple[ExtensionArray | np.ndarray, int]:
    """Map a source column's values through literal, then regex, value maps.

    Args:
        source: The source column.
        literal_mapping: Literal source value → target.
        regex_maps: The regex value maps, in apply order.

    Returns:
        The mapped values, one per row of ``source``: an ExtensionArray of the
        literal targets' dtype, or an object array once regex value maps were
        applied. And how many of them are missing.
    """
    # Series.map(literal_mapping) would build this mapper and look the rows up
    # in its index. Doing so by hand keeps the match codes, which say which
    # rows are left for the regexes and how many stay unmapped, so neither
    # needs a pass over the result. An empty mapper is float64, as Series.map's.
    mapper = (
        pd.Series(literal_mapping) if literal_mapping else pd.Series(dtype="float64")
    )
    codes = (
        _literal_codes(source, mapper.index)
        if literal_mapping
        else np.full(len(source), -1, dtype=np.intp)
    )
    leftover = codes < 0

    if regex_maps and leftover.any():
        # Each distinct leftover value is matched once and the result broadcast
        # by code. Missing values are never matched: str(NaN) is "nan", which a
        # regex (especially the catch-all) would otherwise match.
        pending_codes, (values,) = _distinct_values([source[leftover]])
        present = values.notna()
        if present.any():
            # Regex targets land in an object column, as they always have.
            mapped = np.asarray(
                mapper.astype(object).array.take(codes, allow_fill=True),
                dtype=object,
            )
            targets = _regex_targets(values[present], regex_maps)
            targets = targets.reindex(values.index)
            mapped[leftover] = np.asarray(targets, dtype=object)[pending_codes]
            # Rows the literals matched are mapped; only leftovers can be missing.
            return mapped, _count_unmapped(targets, pending_codes)

    return mapper.array.take(codes, allow_fill=True), int(leftover.sum())


def _literal_codes(source: pd.Series, keys: pd.Index) -> np.ndarray:
    """Return each row's position in ``keys``, or -1 where it matches none.

    Matching is ``Index.get_indexer``'s, which is also what ``Series.map`` does
    with a dict: 1 never matches "1", and a missing value matches nothing.

    Python-backed string and object columns are looked up row by row, where
    pandas reuses each str's cached hash; that beats factorizing the column
    first, whose string hash table re-hashes every value. Every other column
    is factorized and only its distinct values looked up: Arrow-backed strings
    dictionary-encode faster than they convert to Python objects, and
    comparing str keys with numeric or datetime rows is slow although none
    ever matches.

    Args:
        source: The source column.
        keys: The literal source values; unique.

    Returns:
        An integer array aligned with ``source``.
    """
    array = source.array
    if isinstance(array, pd.arrays.StringArray) or (
        isinstance(array, pd.arrays.NumpyExtensionArray) and array.dtype == object
    ):
        # The keys in the source's own dtype: looking Python-backed rows up in
        # an Arrow-backed Index (the default wherever pyarrow is installed)
        # converts every row first, which is slower than factorizing.
        return keys.astype(source.dtype).get_indexer(source)
    value_codes, uniques = pd.factorize(source)
    # Missing values factorize to -1, which picks the -1 appended here.
    return np.append(keys.get_indexer(uniques), -1)[value_codes]


def _regex_targets(values: pd.Series, regex_maps: Sequence[ValueMap]) -> pd.Series:
    """Match each value against the regex value maps; the first match wins.

    Args:
        values: Non-missing values, typically the distinct values a literal
            lookup left unmatched.
        regex_maps: The regex value maps, in apply order.

    Returns:
        The matching map's target per value, aligned with ``values`` and
        missing where none matches, with the dtype ``Series.map`` would infer:
        ``str`` once any value matched, ``object`` otherwise.
    """
    # A regex matches the printed form; a string prints as itself.
    printed = (
        values
        if isinstance(values.dtype, pd.StringDtype)
        else values.map(str, na_action="ignore")
    )
    patterns = [
        (re.compile(vm.source.removeprefix(_REGEX_PREFIX)), vm.target)
        for vm in regex_maps
        if vm.source != _CATCH_ALL
    ]
    if patterns:

        def _first_match(text: str) -> str | None:
            for pattern, target in patterns:
                if pattern.fullmatch(text):
                    return target
            return None

        targets = np.array(printed.map(_first_match), dtype=object)
    else:
        targets = np.full(len(values), None, dtype=object)

    catch_all = next((vm.target for vm in regex_maps if vm.source == _CATCH_ALL), None)
    if catch_all is not None:
        # The catch-all is a mask, not a regex call per value. Its "." does not
        # match a newline, so re.fullmatch(".*", text) fails on exactly the
        # values that contain one.
        has_newline = printed.str.contains("\n", regex=False).to_numpy(dtype=bool)
        targets[pd.isna(targets) & ~has_newline] = catch_all
    # Rebuilt through the constructor, so the dtype is inferred as Series.map
    # infers it and missing values are spelled the same way.
    return pd.Series(targets, index=values.index)


@dataclass(frozen=True)
class _FactorizedSource:
    """A source column factorized, its missing values coded ``len(uniques)``."""

    source: pd.Series
    codes: np.ndarray
    uniques: pd.Index
    has_missing: bool
    # See _print_form_key.
    print_form: tuple[np.ndarray, int] | None

    def representatives(self) -> pd.Series:
        """Return the distinct values in code order, plus a missing one if coded."""
        values = pd.Series(self.uniques, dtype=self.source.dtype)
        if self.has_missing:
            values = values.reindex(range(len(self.uniques) + 1))
        return values


def _factorize_source(source: pd.Series) -> _FactorizedSource:
    codes, uniques = pd.factorize(source)
    missing = codes < 0
    has_missing = bool(missing.any())
    codes[missing] = len(uniques)
    print_form = _print_form_key(source, codes, uniques)
    return _FactorizedSource(source, codes, uniques, has_missing, print_form)


def _distinct_values(
    sources: Sequence[pd.Series],
) -> tuple[np.ndarray, list[pd.Series]]:
    """Number the distinct combinations of source values, each represented once.

    Values are grouped by equality and, where equal values can print
    differently, by printed form too, since a regex value map matches
    ``str(value)`` (see ``_print_form_key``). All missing values of a source
    form one group.

    Args:
        sources: The source columns, aligned row by row.

    Returns:
        ``codes``, numbering each row's combination densely from 0 in order
        of first appearance, and one Series per source whose element i is
        that source's value in combination i.

    Raises:
        ValueError: If ``sources`` is empty.
    """
    if not sources:
        raise ValueError("A map needs at least one source column to group by.")
    parts = [_factorize_source(source) for source in sources]
    if any(part.print_form is not None for part in parts):
        # Equal values that print differently need a real row each, which
        # only a pass over the rows can find.
        return _distinct_rows(parts)

    # Otherwise a source's representatives are its distinct values, in order
    # of first appearance, plus one missing value when any row lacks one, so
    # a row's value code is its representative's position.
    representatives = [part.representatives() for part in parts]
    codes = parts[0].codes
    steps: list[tuple[np.ndarray, int]] = []
    for part, values in zip(parts[1:], representatives[1:], strict=True):
        # codes and part.codes are both at most len(source), so the pair
        # number stays below (len(source) + 1) ** 2, far inside int64.
        codes, pairs = pd.factorize(codes * len(values) + part.codes)
        steps.append((pairs, len(values)))
    if not steps:
        return codes, representatives

    # Decode each combination back into per-source positions, last source
    # first: a pair number is (combination so far, this source's code).
    combination = np.arange(len(steps[-1][0]))
    decoded: list[pd.Series] = []
    for (pairs, n_values), values in zip(
        reversed(steps), reversed(representatives[1:]), strict=True
    ):
        combination, position = np.divmod(pairs[combination], n_values)
        decoded.append(values.iloc[position].reset_index(drop=True))
    decoded.append(representatives[0].iloc[combination].reset_index(drop=True))
    return codes, decoded[::-1]


def _distinct_rows(
    parts: Sequence[_FactorizedSource],
) -> tuple[np.ndarray, list[pd.Series]]:
    """Number the distinct combinations by row, telling printed forms apart.

    Args:
        parts: The factorized source columns; at least one.

    Returns:
        As ``_distinct_values``, each combination represented by its first row.
    """
    keys: list[tuple[np.ndarray, int]] = []
    for part in parts:
        keys.append((part.codes, len(part.uniques) + 1))
        if part.print_form is not None:
            keys.append(part.print_form)
    codes = keys[0][0]
    for key, n_keys in keys[1:]:
        # codes and key are both at most len(source), so the pair number
        # stays below (len(source) + 1) ** 2, far inside int64.
        codes = pd.factorize(codes * n_keys + key)[0]

    first = pd.Series(codes).drop_duplicates()
    positions = np.empty(len(first), dtype=np.intp)
    positions[first.to_numpy()] = first.index.to_numpy()
    return codes, [part.source.iloc[positions].reset_index(drop=True) for part in parts]


def _count_unmapped(resolved: pd.Series, codes: np.ndarray) -> int:
    """Count the rows whose combination, ``codes[row]``, resolved to missing.

    Args:
        resolved: The resolved target per combination.
        codes: Each row's combination.

    Returns:
        The number of such rows.
    """
    missing = resolved.isna().to_numpy()
    if not missing.any():
        return 0
    return int(np.bincount(codes, minlength=len(missing))[missing].sum())


def _print_form_key(
    source: pd.Series, codes: np.ndarray, uniques: pd.Index
) -> tuple[np.ndarray, int] | None:
    """Tell apart values of ``source`` that compare equal but print differently.

    Grouping by equality alone would match 1, 1.0 and True (or 0.0 and -0.0)
    once, through whichever came first, but a regex value map matches
    ``str(value)``, which differs between them. Only float columns holding
    -0.0, and object or complex columns holding non-string values, can hold
    such values.

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
    # A string equals only strings, which print as themselves, so only rows
    # whose group holds a non-string need their printed form. The missing
    # values' code, len(uniques), is never split.
    split = np.fromiter(
        (not isinstance(value, str) for value in uniques),
        dtype=bool,
        count=len(uniques),
    )
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

    Rows with a missing value (NaN/None) in any source column remain
    unmapped: they are never matched by any rule (including the catch-all)
    and yield NaN in the target column.

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
    # own, which the rules then leave unmapped. Each source is taken as a
    # Series, so a label that also names an index level, or appears twice in
    # source_cols, is no problem.
    sources = [df[col] for col in source_cols]
    codes, combinations = _distinct_values(sources)
    # Inferring the dtype over the combinations gives what inferring it over
    # the rows did, as they hold the same values: "str", with NaN for the
    # unmapped, once any rule matched; object, with None, otherwise.
    resolved = pd.Series(_resolve_multi_values(combinations, rules))

    result_df = df.copy(deep=False)
    result_df[target_col] = pd.Series(
        resolved.array.take(codes), index=df.index, dtype=resolved.dtype, copy=False
    )

    logger.log(
        _progress_level(verbose),
        "Mapped %s → '%s' using %d ordered rules.",
        source_cols,
        target_col,
        len(rules),
    )
    n_unmapped = _count_unmapped(resolved, codes)
    if n_unmapped > 0:
        logger.warning("%d rows could not be mapped (set to NaN).", n_unmapped)

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
        # below treats them as non-matches and they stay unmapped.
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
