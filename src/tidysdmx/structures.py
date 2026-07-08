"""Build SDMX structure artefacts (StructureMap, ValueMap, Codelist, etc.)."""

import re
from collections import namedtuple
from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Literal

import pandas as pd
from pysdmx.model import (
    Code,
    Codelist,
    Component,
    Components,
    Concept,
    DataType,
    ItemReference,
    Role,
)
from pysdmx.model.map import (
    ComponentMap,
    DatePatternMap,
    FixedValueMap,
    ImplicitComponentMap,
    MultiComponentMap,
    MultiRepresentationMap,
    MultiValueMap,
    RepresentationMap,
    StructureMap,
    ValueMap,
)
from typeguard import typechecked

from ._deprecation import deprecated
from .artefact_builder import (
    build_codelist,
    build_concept_scheme,
    build_data_structure_definition,
)
from .artefact_builder import (
    build_multi_representation_map as _build_multi_representation_map,
)
from .artefact_builder import (
    build_representation_map as _build_representation_map,
)
from .tidysdmx import parse_artefact_id


@typechecked
def _resolve_representation_ref(
    codelist_urn: str | None = None,
    default_dtype: str = DataType.STRING,
) -> str:
    """Resolve a representation reference to either a codelist URN or DataType string.

    When a RepresentationMap maps between codelists, the source/target should
    contain the codelist URN. When mapping plain strings (no codelist), the
    source/target should contain a DataType value (e.g. "String").

    Args:
        codelist_urn: A codelist URN string, or None if no codelist applies.
        default_dtype: The DataType value to use when no codelist is provided.
            Defaults to DataType.STRING ("String").

    Returns:
        The codelist URN if provided, otherwise the default_dtype string value.

    Note:
        The pysdmx XML writer currently always outputs ``<str:SourceCodelist>``
        / ``<str:TargetCodelist>`` tags, even when the value is a DataType
        string. DataType-based RepresentationMaps will not produce fully valid
        SDMX-ML until pysdmx adds ``<str:SourceDataType>`` /
        ``<str:TargetDataType>`` support. The JSON writer handles both cases
        correctly.
    """
    if codelist_urn is not None and str(codelist_urn).strip():
        return str(codelist_urn).strip()
    return str(default_dtype)


def _parse_validity_date(value: object) -> datetime | None:
    """Coerce a validity-date cell to a ``datetime``, or None when missing.

    pysdmx declares ``ValueMap.valid_from``/``valid_to`` as ``Optional[datetime]``,
    so DataFrame cells (ISO strings, pandas Timestamps, datetimes) must be
    converted before constructing the map objects.

    Args:
        value: An ISO-8601 string, pandas Timestamp, datetime, or NaN/None.

    Returns:
        The value as a plain ``datetime``, or None when the cell is null.

    Raises:
        TypeError: If the value cannot be interpreted as a date.
        ValueError: If a string value is not valid ISO-8601.
    """
    if value is None or pd.isna(value):
        return None
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    if hasattr(value, "to_pydatetime"):  # pandas Timestamp
        return value.to_pydatetime()
    if isinstance(value, datetime):
        return value
    raise TypeError(f"Cannot interpret validity date value: {value!r}")


def _validate_string_columns(
    df: pd.DataFrame,
    cols: Sequence[str],
    *,
    allow_na: bool = False,
    message: str | None = None,
) -> None:
    """Check that the given DataFrame columns contain only string values.

    Args:
        df: The DataFrame to check.
        cols: Column names to validate.
        allow_na: Whether NaN/None cells are tolerated alongside strings.
        message: Optional error message overriding the per-column default.

    Raises:
        TypeError: If any checked column contains non-string values.
    """
    for col in cols:
        series = df[col].dropna() if allow_na else df[col]
        if not series.map(lambda x: isinstance(x, str)).all():
            raise TypeError(
                message or f"Column '{col}' must contain only string values."
            )


# --- Structure map builders ---
@typechecked
def build_fixed_map(
    target: str, value: str, located_in: str | None = "target"
) -> FixedValueMap:
    """Build a pysdmx FixedValueMap for setting a component to a fixed value.

    Args:
        target: The ID of the target component in the structure map.
        value: The fixed value to assign to the target component.
        located_in: Indicates whether the mapping is located in 'source'
            or 'target'. Defaults to 'target'.

    Returns:
        A pysdmx FixedValueMap object representing the fixed mapping.

    Raises:
        ValueError: If ``target`` or ``value`` is empty.
        ValueError: If ``located_in`` is not 'source' or 'target'.

    Examples:
        >>> mapping = build_fixed_map("CONF_STATUS", "F")
        >>> isinstance(mapping, FixedValueMap)
        True
    """
    if not target or not value:
        raise ValueError("Both 'target' and 'value' must be non-empty strings.")
    if located_in not in {"source", "target"}:
        raise ValueError("Parameter 'located_in' must be either 'source' or 'target'.")

    return FixedValueMap(target=target, value=value, located_in=located_in)


@typechecked
def build_implicit_component_map(source: str, target: str) -> ImplicitComponentMap:
    """Build a pysdmx ImplicitComponentMap for implicit mapping rules.

    Args:
        source: The ID of the source component in the structure map.
        target: The ID of the target component in the structure map.

    Returns:
        A pysdmx ImplicitComponentMap object.

    Raises:
        ValueError: If ``source`` or ``target`` is empty.

    Examples:
        >>> mapping = build_implicit_component_map("FREQ", "FREQUENCY")
        >>> isinstance(mapping, ImplicitComponentMap)
        True
    """
    if not source or not target:
        raise ValueError("Both 'source' and 'target' must be non-empty strings.")

    return ImplicitComponentMap(source=source, target=target)


@typechecked
def build_date_pattern_map(
    source: str,
    target: str,
    pattern: str,
    frequency: str,
    id: str | None = None,
    locale: str = "en",
    pattern_type: Literal["fixed", "variable"] = "fixed",
    resolve_period: Literal["startOfPeriod", "endOfPeriod", "midPeriod"] | None = None,
) -> DatePatternMap:
    """Build a DatePatternMap object for mapping date patterns between SDMX components.

    Args:
        source: The ID of the source component.
        target: The ID of the target component.
        pattern: The SDMX date pattern describing the source date (e.g., "MMM yy").
        frequency: The frequency code or reference (e.g., "M" for monthly).
        id: Optional map ID as defined in the registry.
        locale: Locale for parsing the input date pattern. Defaults to "en".
        pattern_type: Type of date pattern. Defaults to "fixed".
            - "fixed": frequency is a fixed value (e.g., "A" for annual).
            - "variable": frequency references a dimension or attribute (e.g., "FREQ").
        resolve_period: Point in time to resolve when mapping from low to
            high frequency periods.

    Returns:
        A fully constructed DatePatternMap instance.

    Raises:
        ValueError: If any required argument is empty or invalid.
        TypeError: If argument types do not match expected types.

    Examples:
        >>> dpm = build_date_pattern_map(
        ...     source="DATE",
        ...     target="TIME_PERIOD",
        ...     pattern="MMM yy",
        ...     frequency="M"
        ... )
        >>> print(dpm)
        source: DATE, target: TIME_PERIOD, pattern: MMM yy, frequency: M
    """
    if not source.strip():
        raise ValueError("Source component ID cannot be empty.")
    if not target.strip():
        raise ValueError("Target component ID cannot be empty.")
    if not pattern.strip():
        raise ValueError("Pattern cannot be empty.")
    if not frequency.strip():
        raise ValueError("Frequency cannot be empty.")

    return DatePatternMap(
        source=source,
        target=target,
        pattern=pattern,
        frequency=frequency,
        id=id,
        locale=locale,
        pattern_type=pattern_type,
        resolve_period=resolve_period,
    )


@typechecked
def build_value_map(
    source: str,
    target: str,
    valid_from: datetime | None = None,
    valid_to: datetime | None = None,
) -> ValueMap:
    """Create a pysdmx ValueMap object mapping a source value to a target value.

    Args:
        source: The source value to map.
        target: The target value to map to.
        valid_from: Start of business validity for the mapping.
        valid_to: End of business validity for the mapping.

    Returns:
        A pysdmx ValueMap object representing the mapping.

    Raises:
        ValueError: If source or target is empty.
        TypeError: If source or target is not a string.

    Examples:
        >>> from datetime import datetime
        >>> vm = build_value_map("BE", "BEL")
        >>> isinstance(vm, ValueMap)
        True
        >>> vm.source
        'BE'
        >>> vm.target
        'BEL'

        >>> vm2 = build_value_map("DE", "GER", valid_from=datetime(2020, 1, 1))
        >>> vm2.valid_from.year
        2020
    """
    if not source.strip() or not target.strip():
        raise ValueError("Source and target cannot be empty.")

    return ValueMap(
        source=source, target=target, valid_from=valid_from, valid_to=valid_to
    )


# --- Representation maps ---
@typechecked
def build_value_map_list(
    df: pd.DataFrame,
    source_col: str = "source",
    target_col: str = "target",
    valid_from_col: str = "valid_from",
    valid_to_col: str = "valid_to",
    default_value: str | None = None,
) -> list[ValueMap]:
    """Build a list of ValueMap objects from a DataFrame, with optional validity.

    Args:
        df: DataFrame where each row represents a mapping.
        source_col: Column name for source values.
        target_col: Column name for target values.
        valid_from_col: Optional column name for validity start date.
            Defaults to "valid_from".
        valid_to_col: Optional column name for validity end date.
            Defaults to "valid_to".
        default_value: Optional catch-all target value. When provided, a final
            ValueMap with source ``"regex:.*"`` is appended so that source
            values not listed in the DataFrame resolve to this value instead of
            remaining unmapped. Defaults to None (no catch-all).

    Returns:
        List of ValueMap objects created from the DataFrame.

    Raises:
        ValueError: If DataFrame is empty or required columns are missing.
        TypeError: If source or target columns contain non-string values.

    Notes:
        - If validity columns exist and contain non-null values, they will be used.
        - If validity columns are absent or contain only nulls, they are ignored.

    Examples:
        >>> import pandas as pd
        >>> data = {
        ...     'source': ['BE', 'FR'],
        ...     'target': ['BEL', 'FRA'],
        ...     'valid_from': ['2020-01-01', None],
        ...     'valid_to': ['2025-12-31', None]
        ... }
        >>> df = pd.DataFrame(data)
        >>> value_maps = build_value_map_list(df, 'source', 'target')
        >>> isinstance(value_maps[0], ValueMap)
        True
    """
    if df.empty:
        raise ValueError("Input DataFrame cannot be empty.")
    if source_col not in df.columns or target_col not in df.columns:
        raise ValueError(
            f"Columns '{source_col}' and '{target_col}' must exist in DataFrame."
        )
    _validate_string_columns(
        df,
        [source_col, target_col],
        message="Source and target columns must contain only string values.",
    )

    has_valid_from = valid_from_col in df.columns
    has_valid_to = valid_to_col in df.columns

    value_maps: list[ValueMap] = []
    for _, row in df.iterrows():
        kwargs = {"source": row[source_col], "target": row[target_col]}
        if has_valid_from:
            valid_from = _parse_validity_date(row.get(valid_from_col))
            if valid_from is not None:
                kwargs["valid_from"] = valid_from
        if has_valid_to:
            valid_to = _parse_validity_date(row.get(valid_to_col))
            if valid_to is not None:
                kwargs["valid_to"] = valid_to
        value_maps.append(ValueMap(**kwargs))

    if default_value is not None:
        # Catch-all: matches any source value not listed above. Apply-time
        # rule ordering guarantees explicit value maps win (see mapping.py).
        value_maps.append(ValueMap(source="regex:.*", target=default_value))

    return value_maps


@typechecked
def build_multi_value_map_list(
    df: pd.DataFrame,
    source_cols: Sequence[str],
    target_cols: Sequence[str],
    valid_from_col: str = "valid_from",
    valid_to_col: str = "valid_to",
    default_value: str | None = None,
) -> list[MultiValueMap]:
    """Build a list of MultiValueMap objects from a pandas DataFrame.

    Iterates through the DataFrame rows to create mapping objects that map
    values from multiple source columns to multiple target columns.

    Args:
        df: DataFrame where each row represents a mapping.
        source_cols: Column names for source values.
        target_cols: Column names for target values.
        valid_from_col: Optional column name for validity start date.
            Defaults to "valid_from".
        valid_to_col: Optional column name for validity end date.
            Defaults to "valid_to".
        default_value: Optional catch-all target value. When provided, a final
            MultiValueMap with source ``"regex:.*"`` for every source component
            is appended so that source-value tuples not listed in the DataFrame
            resolve to this value instead of remaining unmapped. Defaults to
            None (no catch-all).

    Returns:
        List of MultiValueMap objects created from the DataFrame.

    Raises:
        ValueError: If DataFrame is empty or required columns are missing.
        TypeError: If source or target columns contain non-string values.

    Examples:
        >>> import pandas as pd
        >>> data = {
        ...     'country': ['DE', 'CH'],
        ...     'currency_src': ['LC', 'LC'],
        ...     'currency_tgt': ['EUR', 'CHF'],
        ...     'region_tgt': ['EU', 'Non-EU']
        ... }
        >>> df = pd.DataFrame(data)
        >>> maps = build_multi_value_map_list(
        ...     df,
        ...     ['country', 'currency_src'],
        ...     ['currency_tgt', 'region_tgt']
        ... )
        >>> len(maps)
        2
        >>> maps[0].source
        ('DE', 'LC')
        >>> maps[0].target
        ('EUR', 'EU')
    """
    if df.empty:
        raise ValueError("Input DataFrame cannot be empty.")

    # 1. Validate Column Existence
    missing_source = [col for col in source_cols if col not in df.columns]
    if missing_source:
        raise ValueError(f"Source columns missing in DataFrame: {missing_source}")

    missing_target = [col for col in target_cols if col not in df.columns]
    if missing_target:
        raise ValueError(f"Target columns missing in DataFrame: {missing_target}")

    # 2. Validate Data Types (Must be strings for SDMX mappings)
    for col in source_cols:
        _validate_string_columns(
            df,
            [col],
            message=f"Source column '{col}' must contain only string values.",
        )
    for col in target_cols:
        _validate_string_columns(
            df,
            [col],
            message=f"Target column '{col}' must contain only string values.",
        )

    has_valid_from = valid_from_col in df.columns
    has_valid_to = valid_to_col in df.columns

    multi_value_maps: list[MultiValueMap] = []

    # 3. Iterate and Build
    for _, row in df.iterrows():
        # Correctly extract source AND target using their respective lists
        source_values = [row[col] for col in source_cols]
        target_values = [row[col] for col in target_cols]

        # MultiValueMap expects sequences for source/target, keyword-only args
        kwargs = {
            "source": source_values,
            "target": target_values,
        }

        # Handle Validity Dates
        if has_valid_from:
            valid_from = _parse_validity_date(row[valid_from_col])
            if valid_from is not None:
                kwargs["valid_from"] = valid_from

        if has_valid_to:
            valid_to = _parse_validity_date(row[valid_to_col])
            if valid_to is not None:
                kwargs["valid_to"] = valid_to

        multi_value_maps.append(MultiValueMap(**kwargs))

    if default_value is not None:
        # Catch-all: matches any source tuple not listed above. Apply-time
        # rule ordering guarantees explicit value maps win (see mapping.py).
        multi_value_maps.append(
            MultiValueMap(
                source=["regex:.*"] * len(source_cols),
                target=[default_value] * len(target_cols),
            )
        )

    return multi_value_maps


@typechecked
def build_representation_map_from_df(
    df: pd.DataFrame,
    agency: str = "FAKE_AGENCY",
    id: str | None = None,
    name: str | None = None,
    source_cl: str | None = None,
    target_cl: str | None = None,
    version: str = "1.0",
    description: str | None = None,
    source_col: str = "source",
    target_col: str = "target",
    valid_from_col: str = "valid_from",
    valid_to_col: str = "valid_to",
    generate_urn: bool = True,
    default_value: str | None = None,
) -> RepresentationMap:
    """Build a validated RepresentationMap from a DataFrame.

    Builds the individual :class:`~pysdmx.model.map.ValueMap` rows via
    :func:`build_value_map_list`, then delegates the actual construction to
    :func:`tidysdmx.artefact_builder.build_representation_map`, which
    validates the result (publish-readiness rules ``M001``-``M003`` and
    ``R001``-``R003``) before returning it.

    Args:
        df: DataFrame where each row represents a mapping.
        agency: Agency maintaining the representation map.
        id: Identifier for the representation map.
        name: Name of the representation map.
        source_cl: URN or identifier for the source codelist or data type.
        target_cl: URN or identifier for the target codelist or data type.
        version: Version of the representation map. Defaults to "1.0".
        description: Optional description of the representation map.
        source_col: Column name for source values. Defaults to "source".
        target_col: Column name for target values. Defaults to "target".
        valid_from_col: Column name for validity start date.
            Defaults to "valid_from".
        valid_to_col: Column name for validity end date.
            Defaults to "valid_to".
        generate_urn: If True, automatically generate URN. Defaults to True.
        default_value: Optional catch-all target value. When provided, source
            values not listed in the DataFrame resolve to this value instead of
            remaining unmapped. Defaults to None (no catch-all).

    Returns:
        A publish-ready RepresentationMap object containing the mappings.

    Raises:
        ValueError: If required columns are missing.
        TypeError: If source or target columns contain non-string values.
        ValidationError: If ``id``/``name`` resolve to an empty string, or
            the DataFrame yields no value mappings (rule R003) — see
            :mod:`tidysdmx.artefact_validation`.

    Examples:
        >>> import pandas as pd
        >>> data = {
        ...     'source': ['BE', 'FR'],
        ...     'target': ['BEL', 'FRA'],
        ...     'valid_from': ['2020-01-01', None],
        ...     'valid_to': ['2025-12-31', None]
        ... }
        >>> df = pd.DataFrame(data)
        >>> rm = build_representation_map_from_df(
        ...     df,
        ...     agency='ECB',
        ...     id='RM1',
        ...     name='Country Map',
        ...     source_cl='urn:source:codelist',
        ...     target_cl='urn:target:codelist',
        ... )
        >>> isinstance(rm, RepresentationMap)
        True
    """
    # An empty frame yields no value maps. Defer to the value builder's
    # publish-readiness check (rule R003) instead of raising here, so an
    # empty-frame call surfaces the same ValidationError as any other
    # invalid input, rather than a bespoke ValueError from
    # build_value_map_list (which still enforces non-empty input for its
    # own direct callers).
    value_maps = (
        []
        if df.empty
        else build_value_map_list(
            df,
            source_col=source_col,
            target_col=target_col,
            valid_from_col=valid_from_col,
            valid_to_col=valid_to_col,
            default_value=default_value,
        )
    )

    urn = (
        gen_urn("RepresentationMap", agency, id, version)
        if (generate_urn and id)
        else None
    )
    return _build_representation_map(
        id=id,
        agency=agency,
        name=name,
        source=_resolve_representation_ref(source_cl),
        target=_resolve_representation_ref(target_cl),
        maps=value_maps,
        version=version,
        description=description,
        urn=urn,
    )


@deprecated(replacement="build_representation_map_from_df")
def build_representation_map(*args: object, **kwargs: object) -> RepresentationMap:
    """Deprecated alias for :func:`build_representation_map_from_df`."""
    return build_representation_map_from_df(*args, **kwargs)


@typechecked
def build_multi_representation_map_from_df(
    df: pd.DataFrame,
    agency: str = "FAKE_AGENCY",
    id: str | None = None,
    name: str | None = None,
    source_cls: list[str] | None = None,
    target_cls: list[str] | None = None,
    version: str = "1.0",
    description: str | None = None,
    source_cols: list[str] | None = None,  # Changed to Optional
    target_cols: list[str] | None = None,  # Changed to Optional
    valid_from_col: str = "valid_from",
    valid_to_col: str = "valid_to",
    generate_urn: bool = True,
    default_value: str | None = None,
) -> MultiRepresentationMap:
    """Build a validated MultiRepresentationMap object from a pandas DataFrame.

    Builds the individual MultiValueMap rows, then delegates construction to
    :func:`tidysdmx.artefact_builder.build_multi_representation_map`, which
    validates the result (publish-readiness rules ``M001``-``M003`` and
    ``R001``-``R003``) before returning it.

    Unlike :func:`build_representation_map_from_df`, which raises a
    ``ValidationError`` for an empty DataFrame (rule R003 — no value
    mappings), this function raises a plain ``ValueError`` for an empty
    DataFrame, because its ``if df.empty`` guard runs before the value-map
    builder is ever invoked.

    Args:
        df: DataFrame where each row represents a multi-mapping.
        agency: Agency maintaining the map. Defaults to "FAKE_AGENCY".
        id: Identifier for the map.
        name: Name of the map.
        source_cls: URNs/IDs for source codelists/types, one per source
            column. When omitted, each source is represented as
            ``DataType.STRING``.
        target_cls: URNs/IDs for target codelists/types, one per target
            column. When omitted, each target is represented as
            ``DataType.STRING``.
        version: Version of the map. Defaults to "1.0".
        description: Description of the map.
        source_cols: Source columns. Defaults to ["source"].
        target_cols: Target columns. Defaults to ["target"].
        valid_from_col: Validity start column. Defaults to "valid_from".
        valid_to_col: Validity end column. Defaults to "valid_to".
        generate_urn: If True and ``id`` is provided, generate a URN for the
            MultiRepresentationMap. Defaults to True.
        default_value: Optional catch-all target value. When provided,
            source-value tuples not listed in the DataFrame resolve to this
            value instead of remaining unmapped. Defaults to None (no
            catch-all).

    Returns:
        A publish-ready MultiRepresentationMap object.

    Raises:
        ValueError: If the DataFrame is empty, required columns are missing,
            or the length of ``source_cls``/``target_cls`` does not match
            the number of source/target columns.
        TypeError: If non-string data is found in source/target columns.
        ValidationError: If ``id``/``name`` resolve to an empty string — see
            :mod:`tidysdmx.artefact_validation`.
    """
    if df.empty:
        raise ValueError("Input DataFrame cannot be empty.")

    # Handle mutable defaults
    _source_cols = source_cols if source_cols is not None else ["source"]
    _target_cols = target_cols if target_cols is not None else ["target"]

    # Validate required columns
    required_cols = set(_source_cols + _target_cols)
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Representation refs, when provided, must align with the mapped columns
    if source_cls is not None and len(source_cls) != len(_source_cols):
        raise ValueError(
            f"Length of source_cls ({len(source_cls)}) must match the number "
            f"of source columns ({len(_source_cols)})."
        )
    if target_cls is not None and len(target_cls) != len(_target_cols):
        raise ValueError(
            f"Length of target_cls ({len(target_cls)}) must match the number "
            f"of target columns ({len(_target_cols)})."
        )

    # Validate data types (String check)
    for col in _source_cols + _target_cols:
        _validate_string_columns(
            df,
            [col],
            allow_na=True,
            message=f"Column '{col}' contains non-string values.",
        )

    # Build list of maps (Using the new target_cols signature)
    multi_value_maps = build_multi_value_map_list(
        df,
        source_cols=_source_cols,
        target_cols=_target_cols,  # Correct: passes list[str]
        valid_from_col=valid_from_col,
        valid_to_col=valid_to_col,
        default_value=default_value,
    )

    urn = (
        gen_urn("MultiRepresentationMap", agency, id, version)
        if (generate_urn and id)
        else None
    )
    return _build_multi_representation_map(
        id=id,
        agency=agency,
        name=name,
        source=[_resolve_representation_ref(s) for s in source_cls]
        if source_cls
        else [str(DataType.STRING)] * len(_source_cols),
        target=[_resolve_representation_ref(t) for t in target_cls]
        if target_cls
        else [str(DataType.STRING)] * len(_target_cols),
        maps=multi_value_maps,
        version=version,
        description=description,
        urn=urn,
    )


@deprecated(replacement="build_multi_representation_map_from_df")
def build_multi_representation_map(
    *args: object, **kwargs: object
) -> MultiRepresentationMap:
    """Deprecated alias for :func:`build_multi_representation_map_from_df`."""
    return build_multi_representation_map_from_df(*args, **kwargs)


@typechecked
def build_single_component_map(
    df: pd.DataFrame,
    source_component: str,
    target_component: str,
    agency: str = "FAKE_AGENCY",
    id: str | None = None,
    name: str | None = None,
    source_cl: str | None = None,
    target_cl: str | None = None,
    version: str = "1.0",
    description: str | None = None,
    source_col: str = "source",
    target_col: str = "target",
    valid_from_col: str = "valid_from",
    valid_to_col: str = "valid_to",
    generate_urn: bool = True,
    default_value: str | None = None,
) -> ComponentMap:
    """Build a ComponentMap from a DataFrame, mapping one source to one target.

    Args:
        df: DataFrame where each row represents a mapping.
        source_component: ID of the source component.
        target_component: ID of the target component.
        agency: Agency maintaining the representation map.
            Defaults to "FAKE_AGENCY".
        id: Identifier for the representation map.
        name: Name of the representation map.
        source_cl: URN or identifier for the source codelist or data type.
        target_cl: URN or identifier for the target codelist or data type.
        version: Version of the representation map. Defaults to "1.0".
        description: Optional description of the representation map.
        source_col: Column name for source values. Defaults to "source".
        target_col: Column name for target values. Defaults to "target".
        valid_from_col: Column name for validity start date.
            Defaults to "valid_from".
        valid_to_col: Column name for validity end date.
            Defaults to "valid_to".
        generate_urn: If True, generate URN for the RepresentationMap.
            Defaults to True.
        default_value: Optional catch-all target value. When provided, source
            values not listed in the DataFrame resolve to this value instead of
            remaining unmapped. Defaults to None (no catch-all).

    Returns:
        A ComponentMap object mapping the source to the target component.

    Raises:
        ValueError: If the DataFrame is empty or required columns are missing.
        TypeError: If source or target columns contain non-string values.
        ValidationError: If ``id``/``name`` resolve to an empty string — see
            :mod:`tidysdmx.artefact_validation`.

    Examples:
        >>> import pandas as pd
        >>> data = {
        ...     'source': ['BE', 'FR'],
        ...     'target': ['BEL', 'FRA'],
        ...     'valid_from': ['2020-01-01', None],
        ...     'valid_to': ['2025-12-31', None]
        ... }
        >>> df = pd.DataFrame(data)
        >>> cm = build_single_component_map(
        ...     df,
        ...     source_component="COUNTRY",
        ...     target_component="COUNTRY",
        ...     agency="ECB",
        ...     id="CM1",
        ...     name="Country Component Map",
        ...     source_cl="urn:source:codelist",
        ...     target_cl="urn:target:codelist"
        ... )
        >>> isinstance(cm, ComponentMap)
        True
    """
    # Validate DataFrame
    if df.empty:
        raise ValueError("Input DataFrame cannot be empty.")
    for col in [source_col, target_col]:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
        _validate_string_columns(
            df,
            [col],
            allow_na=True,
            message=f"Column '{col}' must contain only string values or NaN.",
        )

    # Build RepresentationMap using the provided helper
    representation_map = build_representation_map_from_df(
        df=df,
        agency=agency,
        id=id,
        name=name,
        source_cl=source_cl,
        target_cl=target_cl,
        version=version,
        description=description,
        source_col=source_col,
        target_col=target_col,
        valid_from_col=valid_from_col,
        valid_to_col=valid_to_col,
        generate_urn=generate_urn,
        default_value=default_value,
    )

    # Return ComponentMap
    return ComponentMap(
        source=source_component, target=target_component, values=representation_map
    )


@typechecked
def build_multi_component_map(
    df: pd.DataFrame,
    source_components: Sequence[str],
    target_components: Sequence[str],
    agency: str = "FAKE_AGENCY",
    id: str | None = None,
    name: str | None = None,
    source_cls: list[str] | None = None,
    target_cls: list[str] | None = None,
    version: str = "1.0",
    description: str | None = None,
    valid_from_col: str = "valid_from",
    valid_to_col: str = "valid_to",
    generate_urn: bool = True,
    default_value: str | None = None,
) -> MultiComponentMap:
    """Build a MultiComponentMap mapping several source components to target(s).

    Mirrors :func:`build_single_component_map` for the N-source case: it builds
    a :class:`MultiRepresentationMap` from ``df`` (whose columns are named after
    the component IDs) and wraps it in a :class:`MultiComponentMap`.

    Args:
        df: DataFrame whose columns are named after the source and target
            component IDs; each row is one value-tuple mapping.
        source_components: Ordered IDs of the source components. Must also be
            present as columns in ``df``.
        target_components: Ordered IDs of the target components. Must also be
            present as columns in ``df``.
        agency: Agency maintaining the representation map.
            Defaults to "FAKE_AGENCY".
        id: Identifier for the representation map.
        name: Name of the representation map.
        source_cls: URNs/IDs for the source codelists or data types. When
            omitted, each source is represented as ``DataType.STRING``.
        target_cls: URNs/IDs for the target codelists or data types. When
            omitted, each target is represented as ``DataType.STRING``.
        version: Version of the representation map. Defaults to "1.0".
        description: Optional description of the representation map.
        valid_from_col: Column name for validity start date.
            Defaults to "valid_from".
        valid_to_col: Column name for validity end date.
            Defaults to "valid_to".
        generate_urn: If True and ``id`` is provided, generate a URN for the
            underlying MultiRepresentationMap. Defaults to True.
        default_value: Optional catch-all target value. When provided,
            source-value tuples not listed in ``df`` resolve to this value
            instead of remaining unmapped. Defaults to None (no catch-all).

    Returns:
        A MultiComponentMap mapping the source components to the target(s).

    Raises:
        ValueError: If the DataFrame is empty or required columns are missing.
        TypeError: If source or target columns contain non-string values.
        ValidationError: If ``id``/``name`` resolve to an empty string — see
            :mod:`tidysdmx.artefact_validation`.

    Examples:
        >>> import pandas as pd
        >>> df = pd.DataFrame({
        ...     "COUNTRY": ["DE", "CH"],
        ...     "CURRENCY": ["LC", "LC"],
        ...     "ISO_CURRENCY": ["EUR", "CHF"],
        ... })
        >>> cm = build_multi_component_map(
        ...     df,
        ...     source_components=["COUNTRY", "CURRENCY"],
        ...     target_components=["ISO_CURRENCY"],
        ...     id="MCM1",
        ...     name="Currency by country",
        ... )
        >>> isinstance(cm, MultiComponentMap)
        True
    """
    multi_representation_map = build_multi_representation_map_from_df(
        df=df,
        agency=agency,
        id=id,
        name=name,
        source_cls=source_cls,
        target_cls=target_cls,
        version=version,
        description=description,
        source_cols=list(source_components),
        target_cols=list(target_components),
        valid_from_col=valid_from_col,
        valid_to_col=valid_to_col,
        generate_urn=generate_urn,
        default_value=default_value,
    )

    return MultiComponentMap(
        source=list(source_components),
        target=list(target_components),
        values=multi_representation_map,
    )


# --- Schema generation from DataFrame ---
@typechecked
def _infer_sdmx_type(dtype: object) -> DataType:
    """Infer the SDMX DataType from a pandas/numpy dtype.

    Args:
        dtype: The pandas/numpy data type.

    Returns:
        The corresponding SDMX DataType.
    """
    # Unwrap categorical dtypes to the dtype of their categories so a
    # category of numbers/dates is classified by its underlying type.
    if isinstance(dtype, pd.CategoricalDtype):
        dtype = dtype.categories.dtype

    # Use pandas' dtype predicates (not substring checks on ``str(dtype)``)
    # so nullable extension dtypes (Int64, Float64, boolean) are handled.
    if pd.api.types.is_bool_dtype(dtype):
        return DataType.BOOLEAN
    if pd.api.types.is_integer_dtype(dtype):
        return DataType.INTEGER
    if pd.api.types.is_float_dtype(dtype):
        return DataType.DOUBLE
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return DataType.DATE_TIME
    return DataType.STRING


_ID_PATTERN = re.compile(r"[^A-Za-z0-9_]+")


@typechecked
def _to_identifier(raw: str) -> str:
    """Convert a raw string to a valid SDMX identifier."""
    cleaned = _ID_PATTERN.sub("_", raw).strip("_")
    if not cleaned:
        raise ValueError(
            f"Column name {raw!r} cannot be converted to a valid SDMX identifier."
        )
    if cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned.upper()


@typechecked
def _code_id(raw: str, uppercase: bool = True) -> str:
    """Sanitize a raw string into an SDMX code ID."""
    candidate = _to_identifier(str(raw))
    return (candidate if uppercase else candidate.lower()) or "UNSPECIFIED"


@typechecked
def sanitize_variable(value: str, uppercase: bool = True) -> str:
    """Sanitize a raw string value into a valid SDMX code ID.

    Applies the same sanitization used internally by ``create_schema_from_table``
    when building codelist code IDs from DataFrame column values. Use this
    function during your data cleaning phase to ensure that the values in your
    DataFrame will match the code IDs generated in the schema.

    The sanitization rules are:
    - Non-alphanumeric/underscore characters (including dots) are replaced with ``_``.
    - Leading/trailing underscores are stripped.
    - IDs starting with a digit are prefixed with ``_``.
    - Result is uppercased by default (controlled by ``uppercase``).

    Args:
        value: The raw string value to sanitize (e.g. ``"per_allsp.adq_ep_preT_tot"``).
        uppercase: If True (default), the result is uppercased, matching the default
            behaviour of ``create_schema_from_table``. Set to False if you called
            ``create_schema_from_table`` with ``uppercase_code_ids=False``.

    Returns:
        A sanitized SDMX-safe identifier string.

    Examples:
        >>> sanitize_variable("per_allsp.adq_ep_preT_tot")
        'PER_ALLSP_ADQ_EP_PRET_TOT'
        >>> sanitize_variable("per_allsp.adq_ep_preT_tot", uppercase=False)
        'per_allsp_adq_ep_pret_tot'
    """
    return _code_id(value, uppercase=uppercase)


# Create the namedtuple type
SchemaComponents = namedtuple(
    "SchemaComponents", ["dsd", "concept_scheme", "codelists"]
)


@typechecked
def _create_codelist_for_component(
    dataframe: pd.DataFrame,
    column: str,
    comp_id: str,
    agency_id: str,
    version: str,
    uppercase_code_ids: bool = True,
) -> Codelist:
    """Create a codelist from unique values in a DataFrame column."""
    values = dataframe[column].dropna().astype(str).unique()
    codes = [
        Code(id=_code_id(value, uppercase=uppercase_code_ids), name=value)
        for value in values
    ]
    cl_id = f"CL_{comp_id}"

    return build_codelist(
        id=cl_id,
        agency=agency_id,
        name=f"{column} codelist",
        codes=codes,
        version=version,
        urn=gen_urn("Codelist", agency_id, cl_id, version),
    )


@typechecked
def _create_dimension_component(
    dataframe: pd.DataFrame,
    column: str,
    agency_id: str,
    scheme_id: str,
    version: str,
    concept_items: list[Concept],
    codelists: list[Codelist],
    uppercase_code_ids: bool = True,
) -> Component:
    """Create a dimension component with its concept and codelist."""
    comp_id = _to_identifier(column)
    dtype = _infer_sdmx_type(dataframe[column].dtype)

    # Create concept reference
    ref = _mk_concept_helper(
        column, comp_id, dtype, agency_id, scheme_id, version, concept_items
    )

    # Create codelist
    codelist = _create_codelist_for_component(
        dataframe,
        column,
        comp_id,
        agency_id,
        version,
        uppercase_code_ids=uppercase_code_ids,
    )
    codelists.append(codelist)

    return Component(
        id=comp_id,
        name=column,
        required=True,
        role=Role.DIMENSION,
        concept=ref,
        local_codes=codelist,
    )


@typechecked
def _create_attribute_component(
    dataframe: pd.DataFrame,
    column: str,
    agency_id: str,
    scheme_id: str,
    version: str,
    concept_items: list[Concept],
    codelists: list[Codelist],
    uppercase_code_ids: bool = True,
) -> Component:
    """Create an attribute component with optional codelist."""
    comp_id = _to_identifier(column)
    dtype = _infer_sdmx_type(dataframe[column].dtype)

    # Create concept reference
    ref = _mk_concept_helper(
        column, comp_id, dtype, agency_id, scheme_id, version, concept_items
    )

    # Create codelist only for string types
    local_codes = None
    if dtype == DataType.STRING:
        local_codes = _create_codelist_for_component(
            dataframe,
            column,
            comp_id,
            agency_id,
            version,
            uppercase_code_ids=uppercase_code_ids,
        )
        codelists.append(local_codes)

    return Component(
        id=comp_id,
        name=column,
        required=False,
        role=Role.ATTRIBUTE,
        concept=ref,
        local_codes=local_codes,
        attachment_level="O",
    )


@typechecked
def _mk_concept_helper(
    column: str,
    concept_id: str,
    dtype: DataType | None,
    agency_id: str,
    scheme_id: str,
    version: str,
    concept_items: list[Concept],
) -> ItemReference:
    """Create a concept and return its item reference."""
    urn = (
        f"urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept="
        f"{agency_id}:{scheme_id}({version}).{concept_id}"
    )
    concept_items.append(Concept(id=concept_id, name=column, dtype=dtype, urn=urn))
    return ItemReference(
        sdmx_type="Concept",
        agency=agency_id,
        id=scheme_id,
        version=version,
        item_id=concept_id,
    )


@typechecked
def create_schema_from_table(
    dataframe: pd.DataFrame,
    dimensions: list[str],
    measure: str,
    time_dimension: str,
    attributes: list[str] | None = None,
    agency_id: str = "WB.DP",
    schema_id: str = "DP_SCHEMA",
    version: str = "1.0",
    uppercase_code_ids: bool = True,
) -> SchemaComponents:
    """Create a DSD, ConceptScheme, and Codelists from a DataFrame.

    Args:
        dataframe: The source DataFrame.
        dimensions: Column names to use as SDMX dimensions.
        measure: Column name to use as the SDMX measure.
        time_dimension: Column name to use as the SDMX time dimension.
        attributes: Optional column names to use as SDMX attributes.
        agency_id: Agency identifier for the generated artefacts.
        schema_id: Base identifier for the generated DSD and concept scheme.
        version: Version string for the generated artefacts.
        uppercase_code_ids: If True (default), codelist code IDs are uppercased.
            Set to False to preserve the original casing of code values.

    Returns:
        SchemaComponents: A named tuple with ``dsd``, ``concept_scheme``,
            and ``codelists`` fields.

    Raises:
        ValueError: If any of the specified column names are missing from
            the DataFrame.
        ValidationError: If the generated artefacts are not publish-ready
            (e.g. an empty codelist).
    """
    attributes = attributes or []
    required = [*dimensions, measure, time_dimension, *attributes]
    missing = [col for col in required if col not in dataframe.columns]
    if missing:
        raise ValueError(f"Columns not found in dataframe: {missing}")

    dsd_id = _to_identifier(schema_id)
    scheme_id = f"{dsd_id}_CS"
    concept_items: list[Concept] = []
    codelists: list[Codelist] = []
    components: list[Component] = []

    # Process dimensions
    for column in dimensions:
        component = _create_dimension_component(
            dataframe,
            column,
            agency_id,
            scheme_id,
            version,
            concept_items,
            codelists,
            uppercase_code_ids=uppercase_code_ids,
        )
        components.append(component)

    # Process time dimension
    time_ref = _mk_concept_helper(
        time_dimension,
        "TIME_PERIOD",
        DataType.PERIOD,
        agency_id,
        scheme_id,
        version,
        concept_items,
    )
    components.append(
        Component(
            id="TIME_PERIOD",
            name=time_dimension,
            required=True,
            role=Role.DIMENSION,
            concept=time_ref,
            local_dtype=DataType.PERIOD,
        )
    )

    # Process measure
    meas_id = _to_identifier(measure)
    meas_dtype = _infer_sdmx_type(dataframe[measure].dtype)
    meas_ref = _mk_concept_helper(
        measure, meas_id, meas_dtype, agency_id, scheme_id, version, concept_items
    )
    components.append(
        Component(
            id=meas_id,
            name=measure,
            required=True,
            role=Role.MEASURE,
            concept=meas_ref,
            local_dtype=meas_dtype,
        )
    )

    # Process attributes
    for column in attributes:
        component = _create_attribute_component(
            dataframe,
            column,
            agency_id,
            scheme_id,
            version,
            concept_items,
            codelists,
            uppercase_code_ids=uppercase_code_ids,
        )
        components.append(component)

    # Create concept scheme and DSD
    concept_scheme = build_concept_scheme(
        id=scheme_id,
        agency=agency_id,
        name=f"{schema_id} generated concept scheme",
        concepts=concept_items,
        version=version,
        urn=gen_urn("ConceptScheme", agency_id, scheme_id, version),
    )

    dsd = build_data_structure_definition(
        id=dsd_id,
        agency=agency_id,
        name=f"{schema_id} generated DSD",
        components=Components(components),
        version=version,
        urn=gen_urn("DataStructure", agency_id, dsd_id, version),
    )

    return SchemaComponents(dsd=dsd, concept_scheme=concept_scheme, codelists=codelists)


# --- Excel template parsing ---
@typechecked
def _parse_info_sheet(
    sheets: dict[str, pd.DataFrame], sheet_name: str = "INFO"
) -> pd.DataFrame:
    """Parse the INFO sheet into key-value metadata.

    Extracts a specific DataFrame from the provided dictionary. Handles arbitrary
    layouts by treating headers as potential data, unless the headers appear to be
    auto-generated (RangeIndex). Filters for rows containing exactly two non-empty
    values.

    Args:
        sheets: Dictionary containing DataFrames, typically from pd.read_excel.
        sheet_name: Name of the sheet to parse. Defaults to "INFO".

    Returns:
        A DataFrame with columns ['Key', 'Value'] containing the extracted metadata.

    Raises:
        ValueError: If the specified sheet_name is not found in the dictionary.
    """
    if sheet_name not in sheets:
        raise ValueError(f"Sheet '{sheet_name}' not found in the provided dictionary.")

    df = sheets[sheet_name]

    # Normalize data extraction:
    # 1. If columns are a RangeIndex (0, 1, 2...), they are likely
    #    auto-generated by pandas (e.g. pd.DataFrame() without columns) and
    #    should be ignored.
    # 2. Otherwise, we treat columns as the first row of data, which covers
    #    cases where pd.read_excel(header=0) consumes the first row of actual
    #    metadata as the header.
    if isinstance(df.columns, pd.RangeIndex):
        all_rows = df.values.tolist()
    else:
        all_rows = [df.columns.tolist(), *df.values.tolist()]

    cleaned_rows: list[list[str]] = []

    for row in all_rows:
        valid_cells = []
        for cell in row:
            # Basic validation: check for NaN/None
            if pd.isna(cell):
                continue

            s_cell = str(cell).strip()

            # Check for empty strings, 'nan' string literals, and pandas
            # 'Unnamed' artifacts
            if s_cell == "" or s_cell.lower() == "nan" or s_cell.startswith("Unnamed:"):
                continue

            valid_cells.append(s_cell)

        if not valid_cells:
            continue

        # Ignore the specific header row mentioned in requirements
        if any("DATA CURATION PROCESS" in cell for cell in valid_cells):
            continue

        # We strictly look for Key-Value pairs (but allow the second item to be empty)
        if len(valid_cells) <= 2:
            cleaned_rows.append(valid_cells)

    if not cleaned_rows:
        return pd.DataFrame(columns=["Key", "Value"])

    return pd.DataFrame(cleaned_rows, columns=["Key", "Value"])


@typechecked
def _parse_comp_mapping_sheet(
    sheets: dict[str, pd.DataFrame], sheet_name: str = "COMP_MAPPING"
) -> pd.DataFrame:
    """Parse the COMP_MAPPING sheet, validating strict structure conformance.

    Expects the sheet to contain specific headers: 'SOURCE', 'TARGET',
    and 'MAPPING_RULES'. Extracts these columns, removes completely empty rows,
    and returns the resulting DataFrame. Rows with partial data (e.g., missing
    'SOURCE') are preserved as they are valid mapping rules.

    Args:
        sheets: Dictionary containing DataFrames, typically from pd.read_excel.
        sheet_name: Name of the sheet to parse. Defaults to "COMP_MAPPING".

    Returns:
        A DataFrame containing 'SOURCE', 'TARGET', and 'MAPPING_RULES' columns.

    Raises:
        ValueError: If the sheet is missing or does not contain the required columns.
    """
    if sheet_name not in sheets:
        raise ValueError(f"Sheet '{sheet_name}' not found in the provided dictionary.")

    df = sheets[sheet_name]

    required_columns = ["SOURCE", "TARGET", "MAPPING_RULES"]
    optional_columns = ["SOURCE_CL", "TARGET_CL", "DEFAULT_VALUE"]

    # Validate that required columns exist
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Sheet '{sheet_name}' is missing required columns: {missing_columns}. "
            f"Found: {df.columns.tolist()}"
        )

    # Extract required columns + any optional columns that exist
    cols_to_extract = required_columns + [
        c for c in optional_columns if c in df.columns
    ]
    result_df = df[cols_to_extract].copy()

    # Ensure optional columns exist in output (fill with None if absent from sheet)
    for col in optional_columns:
        if col not in result_df.columns:
            result_df[col] = None

    # Remove rows where ALL columns are NaN/None (empty rows)
    # We do not use how='any' because some mapping rules might have an empty SOURCE
    result_df.dropna(how="all", inplace=True)

    return result_df


@typechecked
def _parse_rep_mapping_sheet(
    sheets: dict[str, pd.DataFrame], sheet_name: str = "REP_MAPPING"
) -> dict[str, pd.DataFrame]:
    """Parses the REP_MAPPING sheet to split columns into Source and Target DataFrames.

    The function expects column headers to be prefixed with "S:" for source columns and
    "T:" for target columns. Columns without these prefixes are ignored. The prefixes
    are removed in the output DataFrames.

    Args:
        sheets (dict[str, pd.DataFrame]): Dictionary containing DataFrames.
        sheet_name (str): Name of the sheet to parse. Defaults to "REP_MAPPING".

    Returns:
        dict[str, pd.DataFrame]: A dict with keys ``"source"`` and ``"target"``,
            each containing a DataFrame with the prefix stripped from column names.

    Raises:
        ValueError: If the sheet is missing, or if no Source/Target columns are found.
    """
    if sheet_name not in sheets:
        raise ValueError(f"Sheet '{sheet_name}' not found in the provided dictionary.")

    df = sheets[sheet_name]

    # Identify columns based on prefixes
    source_cols = [col for col in df.columns if str(col).startswith("S:")]
    target_cols = [col for col in df.columns if str(col).startswith("T:")]

    if not source_cols:
        raise ValueError(
            f"No source columns (prefixed with 'S:') found in '{sheet_name}'."
        )

    if not target_cols:
        raise ValueError(
            f"No target columns (prefixed with 'T:') found in '{sheet_name}'."
        )

    # Create distinct DataFrames
    source_df = df[source_cols]
    target_df = df[target_cols]

    # Rename columns by removing the first 2 characters ("S:" and "T:")
    source_df.columns = [col[2:] for col in source_cols]
    target_df.columns = [col[2:] for col in target_cols]

    return {"source": source_df, "target": target_df}


@typechecked
def _extract_artefact_id(
    info_df: pd.DataFrame,
    structure_type: Literal["dataflow", "dsd", "provision-agreement"],
) -> str:
    """Extract the SDMX ID for a specific structure type from the parsed INFO DataFrame.

    Searches the provided DataFrame (output of ``_parse_info_sheet``) for specific
    keys corresponding to the requested structure type. Handles standard SDMX
    reference formats like 'Agency:ID(Version)' by parsing out just the 'ID'.

    Args:
        info_df: DataFrame containing metadata with 'Key' and 'Value' columns.
        structure_type: The type of artefact to extract. Must be one of:
            'dataflow', 'dsd', 'provision-agreement'.

    Returns:
        The extracted SDMX ID.

    Raises:
        ValueError: If the `structure_type` is invalid, the key is not found,
                    or the value is empty/null.
    """
    # Map friendly structure types to the actual keys found in the Excel/CSV
    # We use a case-insensitive match strategy in logic, but these are the
    # expected targets.
    # Based on the file snippets:
    # 'dsd' -> 'datastructure'
    # 'dataflow' -> 'dataflow'
    # 'provision-agreement' -> 'provisionagreement'
    type_map = {
        "dataflow": "dataflow",
        "dsd": "datastructure",
        "provision-agreement": "provisionagreement",
    }

    if structure_type not in type_map:
        raise ValueError(
            f"Invalid structure_type '{structure_type}'. "
            f"Must be one of {list(type_map.keys())}."
        )

    target_key = type_map[structure_type]

    # Perform case-insensitive search for the key
    # Create a mask for matching keys
    mask = info_df["Key"].astype(str).str.strip().str.lower() == target_key.lower()

    if not mask.any():
        raise ValueError(
            f"Could not find metadata key '{target_key}' "
            f"for structure type '{structure_type}'."
        )

    # Get the value associated with the key
    raw_value = info_df.loc[mask, "Value"].iloc[0]

    # Validate value is present and not empty/nan
    if pd.isna(raw_value) or str(raw_value).strip() == "":
        raise ValueError(f"Metadata for '{target_key}' is present but empty.")

    artefact_id = str(raw_value).strip()

    return artefact_id


@typechecked
def _match_column_name(target_name: str, available_columns: list[str]) -> str:
    """Match a business name from COMP_MAPPING to cleaned column names in REP_MAPPING.

    Handles discrepancies like 'Series code' (business name) vs 'Series'
    (Excel header).

    Args:
        target_name: The name to look for (e.g., 'Series code').
        available_columns: The list of available column headers.

    Returns:
        The matching column name.

    Raises:
        ValueError: If no suitable match is found.
    """
    # 1. Exact match
    if target_name in available_columns:
        return target_name

    # 2. Normalized match (ignore case, spaces, underscores)
    norm_target = target_name.replace(" ", "").replace("_", "").lower()
    norm_cols = [
        (col, col.replace(" ", "").replace("_", "").lower())
        for col in available_columns
    ]

    # 2a. Normalized equality takes priority and must be unambiguous.
    equal = [col for col, norm in norm_cols if norm == norm_target]
    if len(equal) == 1:
        return equal[0]
    if len(equal) > 1:
        raise ValueError(
            f"Ambiguous match for '{target_name}': columns {equal} all normalize "
            f"identically. Rename the REP_MAPPING header to match the component id."
        )

    # 2b. Fall back to substring containment, but only when the candidate is
    # unique — otherwise a header like 'AGE' would silently bind to 'PERCENTAGE'.
    contained = [
        col for col, norm in norm_cols if norm in norm_target or norm_target in norm
    ]
    if len(contained) == 1:
        return contained[0]
    if len(contained) > 1:
        raise ValueError(
            f"Ambiguous match for '{target_name}': multiple candidate columns "
            f"{contained}. Rename the REP_MAPPING header to match the component "
            f"id exactly."
        )

    raise ValueError(
        f"Could not find a column in REP_MAPPING matching '{target_name}'. "
        f"Available: {available_columns}"
    )


@typechecked
def _collect_required_sheet_errors(
    mappings: dict[str, pd.DataFrame],
    required_keys: Iterable[str],
) -> list[str]:
    """Collect validation errors related to missing required sheets."""
    errors: list[str] = []

    # Check that mandatory sheets are present
    for sheet_name in required_keys:
        if sheet_name not in mappings:
            msg = f"Missing required sheet: '{sheet_name}'."
            errors.append(msg)

    return errors


@typechecked
def _collect_mapping_rules_errors(
    comp_mapping: pd.DataFrame,
    *,  # Ensures following args are keyword-only
    valid_rules: Iterable[str],
    valid_prefixes: Iterable[str],
) -> list[str]:
    """Collect validation errors for the MAPPING_RULES column in COMP_MAPPING.

    Rules:
        * Column 'MAPPING_RULES' must exist.
        * Each non-null entry must be:
            - one of valid_rules
            - or start with one of valid_prefixes followed by a non-empty value.
    """
    errors: list[str] = []

    if "MAPPING_RULES" not in comp_mapping.columns:
        msg = "COMP_MAPPING sheet is missing required 'MAPPING_RULES' column."

        errors.append(msg)
        return errors

    valid_set = set(valid_rules)

    # Normalize prefixes once
    prefixes = tuple(str(p) for p in valid_prefixes)
    if not prefixes or any(p == "" for p in prefixes):
        raise ValueError("Argument 'valid_prefixes' must contain non-empty strings.")

    rules_series = comp_mapping["MAPPING_RULES"]

    for row_idx, raw_value in rules_series.items():
        if pd.isna(raw_value):
            continue

        value = str(raw_value).strip()

        # 1) Literal rules
        if value in valid_set:
            continue

        # 2) Prefixed rules (enforce non-empty parsed_value)
        matched_prefix = next((p for p in prefixes if value.startswith(p)), None)
        if matched_prefix is not None:
            parsed_value = value[len(matched_prefix) :].strip()
            if not parsed_value:
                msg = (
                    f"Invalid MAPPING_RULES value at row {row_idx}: '{raw_value}'. "
                    f"Rule '{matched_prefix}' must be followed by a non-empty value, "
                    f"e.g. '{matched_prefix}A'."
                )

                errors.append(msg)
            # either way, we handled a recognized prefix (valid or invalid)
            continue

        # 3) Everything else invalid
        msg = (
            f"Invalid MAPPING_RULES value at row {row_idx}: '{raw_value}'. "
            "Expected one of "
            f"{sorted(valid_set)!r} "
            f"or a string starting with one of {list(prefixes)!r}."
        )

        errors.append(msg)

    return errors


@typechecked
def _validate_mapping_template_wb(
    mappings: dict[str, pd.DataFrame],
    *,  # Ensures following args are keyword-only
    required_keys: Iterable[str] = ("INFO", "COMP_MAPPING", "REP_MAPPING"),
    valid_rules: Iterable[str] = ("representation", "multi_representation", "implicit"),
    valid_prefixes: Iterable[str] = ("fixed:",),
) -> None:
    """Validate a mapping template workbook represented as a mapping of DataFrames.

    If any validation fails, raises ValueError listing all issues.
    """
    # Ensure functions arguments are of the expected type
    for key in mappings:
        # All keys should be strings
        if not isinstance(key, str):
            raise ValueError(
                f"All keys must be strings. Key: '{key}' is of type "
                f"{type(key).__name__}."
            )
        # Values should be dataframes
        if not isinstance(mappings[key], pd.DataFrame):
            raise ValueError(
                f"Sheet '{key}' must be a pandas DataFrame, "
                f"got {type(mappings[key]).__name__}."
            )

    errors: list[str] = []

    # 1) Check required sheet presence and type validity
    errors.extend(_collect_required_sheet_errors(mappings, required_keys))

    # 2) Validate mapping_rules
    comp_mapping = mappings.get("COMP_MAPPING")
    if comp_mapping is not None:
        errors.extend(
            _collect_mapping_rules_errors(
                comp_mapping,
                valid_rules=valid_rules,
                valid_prefixes=valid_prefixes,
            )
        )

    if errors:
        full_message = (
            "Mapping template workbook validation failed with the "
            "following issues:\n- " + "\n- ".join(errors)
        )
        raise ValueError(full_message)


# --- Main template builder ---

STRUCTURE_TYPE_TO_ARTEFACT: dict[str, str] = {
    "datastructure": "DataStructure",
    "dataflow": "Dataflow",
    "provisionagreement": "ProvisionAgreement",
}

SDMX_PACKAGE_MAP: dict[str, str] = {
    "StructureMap": "structuremapping",
    "RepresentationMap": "structuremapping",
    "MultiRepresentationMap": "structuremapping",
    "Codelist": "codelist",
    "ConceptScheme": "conceptscheme",
    "DataStructure": "datastructure",
    "DataStructureDefinition": "datastructure",
    "Dataflow": "datastructure",
    "AgencyScheme": "base",
    "ProvisionAgreement": "registry",
}


@typechecked
def gen_urn(
    artefact_type: str, agency: str, artefact_id: str, version: str = "1.0"
) -> str:
    """Generate a full SDMX URN for any maintainable artefact.

    Args:
        artefact_type: The type of artefact (e.g., "StructureMap", "RepresentationMap")
        agency: The agency ID
        artefact_id: The artefact ID
        version: The version (default "1.0")

    Returns:
        Full URN string

    Example:
        >>> gen_urn("StructureMap", "BIS", "SM_TEST", "1.0")
        'urn:sdmx:org.sdmx.infomodel.structuremapping.StructureMap=BIS:SM_TEST(1.0)'
    """
    package = SDMX_PACKAGE_MAP.get(artefact_type, "base")
    urn = (
        f"urn:sdmx:org.sdmx.infomodel.{package}.{artefact_type}"
        f"={agency}:{artefact_id}({version})"
    )
    return urn


@typechecked
def build_structure_map_from_template_wb(
    mappings: dict[str, pd.DataFrame],
    agency: str = "SDMX",
    structure_map_id: str = "WB_STRUCTURE_MAP",
    structure_type: Literal[
        "datastructure", "dataflow", "provisionagreement"
    ] = "datastructure",
    version: str = "1.0",
    required_keys: Iterable[str] = ("INFO", "COMP_MAPPING", "REP_MAPPING"),
    valid_rules: Iterable[str] = ("representation", "multi_representation", "implicit"),
    valid_prefixes: Iterable[str] = ("fixed:",),
    generate_urns: bool = True,
    source_structure_id: str | None = None,
    target_structure_id: str | None = None,
) -> StructureMap:
    """Build a complete StructureMap object by parsing a WB-format Excel template.

    Args:
        mappings: Dictionary of DataFrames containing all sheets.
        agency: Fallback agency ID if not found in INFO.
        structure_map_id: ID for the resulting StructureMap.
        structure_type: The type of artefact to extract from INFO.
        version: Fallback version if not found in INFO.
        required_keys: Required sheet names to validate.
        valid_rules: Valid literal mapping rules.
        valid_prefixes: Valid prefixes for parameterized mapping rules.
        generate_urns: If True, automatically generate URNs for StructureMap
            and nested RepresentationMaps. Defaults to True.
        source_structure_id: Optional source structure reference in
            ``"AGENCY:ID(VERSION)"`` format (e.g. ``"WB:DSD_ASPIRE(1.0)"``).
            When provided and ``generate_urns`` is True, a full SDMX URN is
            built and set as the StructureMap's ``source``.
        target_structure_id: Optional target structure reference in
            ``"AGENCY:ID(VERSION)"`` format (e.g. ``"WB:DSD_WDI(1.0)"``).
            When provided and ``generate_urns`` is True, a full SDMX URN is
            built and set as the StructureMap's ``target``.

    Returns:
        A valid pysdmx StructureMap object.

    Raises:
        ValueError: If mandatory sheets/columns are missing or mapping rules
            are invalid.

    Examples:
        >>> mappings = {
        ...     "INFO": pd.DataFrame({"Key": ["FMR_AGENCY"], "Value": ["TEST_AGENCY"]}),
        ...     "COMP_MAPPING": pd.DataFrame(
        ...         {
        ...             "SOURCE": ["src"],
        ...             "TARGET": ["tgt"],
        ...             "MAPPING_RULES": ["fixed:VAL"],
        ...         }
        ...     ),
        ...     "REP_MAPPING": pd.DataFrame({"source": ["a"], "target": ["b"]})
        ... }
        >>> smap = build_structure_map_from_template_wb(mappings)
        >>> isinstance(smap, StructureMap)
        True
    """
    # Validate mappings upfront
    _validate_mapping_template_wb(
        mappings,
        required_keys=required_keys,
        valid_rules=valid_rules,
        valid_prefixes=valid_prefixes,
    )

    # 1. Extract Metadata (Agency & Version)
    info_df = _parse_info_sheet(mappings)
    current_agency, current_version, artefact_ref = _extract_metadata_from_info_sheet(
        info_df=info_df, agency=agency, version=version, structure_type=structure_type
    )

    # 2. Parse Component Mappings Rules
    comp_df = _parse_comp_mapping_sheet(mappings)

    # 3. Prepare Representation Data
    # Defer invalid-REP_MAPPING failures: a template may not reference it at
    # all. Stash the parse error so it can be surfaced if a rule *does* need it.
    rep_data: dict[str, pd.DataFrame] = {}
    rep_data_error: str | None = None
    try:
        rep_data = _parse_rep_mapping_sheet(mappings)
    except ValueError as exc:
        rep_data_error = str(exc)

    generated_maps: list[
        FixedValueMap | ImplicitComponentMap | ComponentMap | MultiComponentMap
    ] = []

    # Track RepresentationMap IDs to avoid duplicates
    rep_map_counter = {}

    # 4. Generate structure map elements
    for _, row in comp_df.iterrows():
        try:
            parsed = _extract_mapping_rule(row)
            mapping_rule = parsed["mapping_rule"]
            source_id = parsed["source_id"] or ""  # normalize to str
            target_id = parsed["target_id"] or ""  # normalize to str

            if mapping_rule == "skip":
                continue

            if mapping_rule == "fixed":
                fixed_val = parsed["fixed_value"]  # guaranteed non-empty by parser
                generated_maps.append(build_fixed_map(target_id, fixed_val))  # type: ignore[arg-type]

            elif mapping_rule == "implicit":
                generated_maps.append(
                    build_implicit_component_map(source_id, target_id)
                )

            elif mapping_rule == "representation":
                rep_mapping_df = _extract_representation_map(
                    rep_data=rep_data,
                    source_id=source_id,
                    target_id=target_id,
                    rep_data_error=rep_data_error,
                )

                # Generate unique ID for RepresentationMap
                base_id = f"RM_{source_id}_{target_id}"
                if base_id in rep_map_counter:
                    rep_map_counter[base_id] += 1
                    rep_map_id = f"{base_id}_{rep_map_counter[base_id]}"
                else:
                    rep_map_counter[base_id] = 0
                    rep_map_id = base_id

                comp_map = build_single_component_map(
                    df=rep_mapping_df,
                    source_component=source_id,
                    target_component=target_id,
                    agency=current_agency,
                    id=rep_map_id,  # Use unique ID
                    name=f"Mapping {source_id} to {target_id}",
                    source_cl=parsed.get("source_cl"),
                    target_cl=parsed.get("target_cl"),
                    source_col="source",
                    target_col="target",
                    version=current_version,
                    generate_urn=generate_urns,  # Pass flag through
                    default_value=parsed.get("default_value"),
                )
                generated_maps.append(comp_map)

            elif mapping_rule == "multi_representation":
                # SOURCE is a '|'-delimited list of >= 2 components
                source_ids = [s.strip() for s in source_id.split("|") if s.strip()]

                multi_df = _extract_multi_representation_map(
                    rep_data=rep_data,
                    source_ids=source_ids,
                    target_id=target_id,
                    rep_data_error=rep_data_error,
                )

                # Generate unique ID for the MultiRepresentationMap
                base_id = f"MRM_{'_'.join(source_ids)}_{target_id}"
                if base_id in rep_map_counter:
                    rep_map_counter[base_id] += 1
                    rep_map_id = f"{base_id}_{rep_map_counter[base_id]}"
                else:
                    rep_map_counter[base_id] = 0
                    rep_map_id = base_id

                multi_comp_map = build_multi_component_map(
                    df=multi_df,
                    source_components=source_ids,
                    target_components=[target_id],
                    agency=current_agency,
                    id=rep_map_id,  # Use unique ID
                    name=f"Mapping {'|'.join(source_ids)} to {target_id}",
                    target_cls=[parsed["target_cl"]]
                    if parsed.get("target_cl")
                    else None,
                    version=current_version,
                    generate_urn=generate_urns,  # Pass flag through
                    default_value=parsed.get("default_value"),
                )
                generated_maps.append(multi_comp_map)

            else:
                # Defensive guard
                raise ValueError(f"Unhandled mapping rule: {mapping_rule}")

        except ValueError as e:
            target_for_msg = str(row.get("TARGET", "")).strip()
            raise ValueError(
                f"Error processing mapping for Target '{target_for_msg}': {e!s}"
            ) from e

    # 5. Generate URNs if requested
    structure_map_urn = None
    source_urn = ""
    target_urn = ""
    if generate_urns:
        structure_map_urn = gen_urn(
            "StructureMap", current_agency, structure_map_id, current_version
        )
        artefact_type = STRUCTURE_TYPE_TO_ARTEFACT[structure_type]
        if source_structure_id:
            s_agency, s_id, s_version = parse_artefact_id(source_structure_id)
            source_urn = gen_urn(artefact_type, s_agency, s_id, s_version)
        if target_structure_id:
            t_agency, t_id, t_version = parse_artefact_id(target_structure_id)
            target_urn = gen_urn(artefact_type, t_agency, t_id, t_version)

    # 6. Construct Final Object
    name_suffix = artefact_ref if artefact_ref else structure_map_id
    return StructureMap(
        id=structure_map_id,
        agency=current_agency,
        version=current_version,
        name=f"Structure Map for {name_suffix}",
        urn=structure_map_urn,
        source=source_urn,
        target=target_urn,
        maps=generated_maps,
    )


@typechecked
def _extract_all_artefact_ids(info_df: pd.DataFrame) -> dict[str, str]:
    """Extract artefact IDs from the INFO DataFrame as a structure-type-to-ID mapping.

    Scans the DataFrame for keys corresponding to SDMX artefacts such as
    'dataflow', 'datastructure', and 'provisionagreement', and returns a
    dictionary where each structure type is linked to its parsed ID.

    Args:
        info_df: DataFrame containing metadata with 'Key' and 'Value' columns.

    Returns:
        Dictionary mapping structure types to artefact IDs.

    Raises:
        ValueError: If the DataFrame is empty, lacks required columns, or no
            artefacts are found.
        TypeError: If info_df is not a pandas DataFrame.

    Examples:
        >>> df = pd.DataFrame({
        ...     'Key': ['dataflow', 'datastructure', 'provisionagreement'],
        ...     'Value': ['AGENCY:DF1(1.0)', 'AGENCY:DSD1(1.0)', 'AGENCY:PA1(1.0)']
        ... })
        >>> _extract_all_artefact_ids(df)
        {'dataflow': 'DF1', 'datastructure': 'DSD1', 'provisionagreement': 'PA1'}
    """
    if not isinstance(info_df, pd.DataFrame):
        raise TypeError("info_df must be a pandas DataFrame.")
    if info_df.empty:
        raise ValueError("info_df is empty.")
    if not {"Key", "Value"}.issubset(info_df.columns):
        raise ValueError("info_df must contain 'Key' and 'Value' columns.")

    # Define structure types to look for
    structure_types = {"dataflow", "datastructure", "provisionagreement"}

    # Normalize keys for case-insensitive matching (without mutating input)
    normalized_keys = info_df["Key"].astype(str).str.strip().str.lower()

    # Filter rows matching structure types
    filtered_df = info_df[normalized_keys.isin(structure_types)].copy()
    filtered_df["Key"] = normalized_keys[filtered_df.index]

    if filtered_df.empty:
        raise ValueError("No artefact keys found in info_df.")

    artefact_dict: dict[str, str] = {}
    for _, row in filtered_df.iterrows():
        raw_value = row["Value"]
        if pd.isna(raw_value) or str(raw_value).strip() == "":
            continue
        # Extract ID from 'Agency:ID(Version)' format
        value_str = str(raw_value).strip()
        artefact_dict[row["Key"]] = value_str

    if not artefact_dict:
        raise ValueError("Artefact keys found but all values are empty or invalid.")

    return artefact_dict


@typechecked
def _extract_metadata_from_info_sheet(
    info_df: pd.DataFrame,
    agency: str,
    version: str,
    structure_type: Literal[
        "datastructure", "dataflow", "provisionagreement"
    ] = "datastructure",
) -> tuple[str, str, str | None]:
    """Extract (agency, version, artefact_ref) from the INFO sheet.

    Uses ``structure_type`` preference, falling back to other artefacts
    and FMR_AGENCY when needed.

    Args:
        info_df: INFO sheet with 'Key'/'Value' columns.
        agency: Default agency used when extraction fails.
        version: Default version used when extraction fails.
        structure_type: Preferred structure type. Defaults to
            ``"datastructure"``.

    Returns:
        A tuple of (agency, version, artefact_ref) where artefact_ref
        is the raw reference string or None if not found.
    """
    current_agency = agency
    current_version = version
    artefact_ref: str | None = None

    try:
        artefact_dict: dict[str, str] = _extract_all_artefact_ids(info_df)
    except (ValueError, TypeError, KeyError):
        artefact_dict = {}

    # Preferred artefact by requested structure_type, otherwise fallback order
    if structure_type in artefact_dict:
        artefact_ref = artefact_dict[structure_type]
    else:
        for fallback_type in ("datastructure", "dataflow", "provisionagreement"):
            if fallback_type in artefact_dict:
                artefact_ref = artefact_dict[fallback_type]
                break

    # Parse agency/version from artefact_ref if available
    if artefact_ref:
        try:
            parsed_agency, _, parsed_version = parse_artefact_id(artefact_ref)
            if parsed_agency:
                current_agency = parsed_agency
            if parsed_version:
                current_version = parsed_version
        except (ValueError, TypeError, KeyError):
            # Keep defaults if parsing fails
            pass

    return current_agency, current_version, artefact_ref


# tokens that mean "missing" for MAPPING_RULES
_MISSING_RULE_TOKENS = {"nan", "<na>", ""}


@typechecked
def _is_missing_token(s: str) -> bool:
    """Return True if s is a case-insensitive missing token."""
    return s.strip().lower() in _MISSING_RULE_TOKENS


@typechecked
def _extract_mapping_rule(row: "pd.Series") -> dict[str, str | None]:
    """Parse a COMP_MAPPING row into a dict of mapping rules.

    This performs *syntax-level* validation only; it never touches external
    data.

    Returns a dict with the following keys:
      - mapping_rule: one of {"skip", "fixed", "implicit", "representation"}
      - source_id: normalized SOURCE (may be empty for fixed)
      - target_id: normalized TARGET (empty only if mapping_rule == "skip")
      - fixed_value: present only for mapping_rule == "fixed", else None
      - source_cl: codelist URN for the source component, or None
      - target_cl: codelist URN for the target component, or None
      - default_value: catch-all target for unlisted source values, or None
        (only on "representation" and "multi_representation" rules)

    Raises:
      - ValueError: if the rule is syntactically invalid (e.g., bad 'fixed:' format),
                    or for implicit/representation if SOURCE is missing,
                    or for unknown rule strings.
    """
    source_id = str(row.get("SOURCE", "")).strip()
    target_id = str(row.get("TARGET", "")).strip()
    raw_rule = str(row.get("MAPPING_RULES", "")).strip()

    # Extract optional codelist URNs (None when absent or empty)
    raw_source_cl = row.get("SOURCE_CL")
    source_cl = str(raw_source_cl).strip() if pd.notna(raw_source_cl) else ""
    source_cl = source_cl or None
    raw_target_cl = row.get("TARGET_CL")
    target_cl = str(raw_target_cl).strip() if pd.notna(raw_target_cl) else ""
    target_cl = target_cl or None

    # Optional default (catch-all) target value (None when absent or empty)
    raw_default = row.get("DEFAULT_VALUE")
    default_value = str(raw_default).strip() if pd.notna(raw_default) else ""
    default_value = default_value or None

    # Skip when TARGET is empty or rule is missing-ish
    if not target_id or _is_missing_token(raw_rule):
        return {
            "mapping_rule": "skip",
            "source_id": source_id,
            "target_id": target_id,
            "fixed_value": None,
            "source_cl": source_cl,
            "target_cl": target_cl,
        }

    rule_lower = raw_rule.lower()

    # fixed:<VALUE>
    if rule_lower.startswith("fixed:"):
        parts = raw_rule.split(":", 1)
        if len(parts) < 2 or not parts[1].strip():
            raise ValueError(f"Invalid fixed rule format: {raw_rule}")
        fixed_val = parts[1].strip()
        return {
            "mapping_rule": "fixed",
            "source_id": source_id,
            "target_id": target_id,
            "fixed_value": fixed_val,
            "source_cl": source_cl,
            "target_cl": target_cl,
        }

    # implicit
    if rule_lower == "implicit":
        if not source_id:
            raise ValueError(
                "Implicit map rule requires a non-empty 'SOURCE' component ID."
            )
        return {
            "mapping_rule": "implicit",
            "source_id": source_id,
            "target_id": target_id,
            "fixed_value": None,
            "source_cl": source_cl,
            "target_cl": target_cl,
        }

    # representation
    if rule_lower == "representation":
        if not source_id or not target_id:
            raise ValueError(
                "Representation map rule requires non-empty 'SOURCE' and "
                "'TARGET' component ID."
            )
        return {
            "mapping_rule": "representation",
            "source_id": source_id,
            "target_id": target_id,
            "fixed_value": None,
            "source_cl": source_cl,
            "target_cl": target_cl,
            "default_value": default_value,
        }

    # multi_representation: SOURCE is a '|'-delimited list of >= 2 components
    if rule_lower == "multi_representation":
        source_tokens = [s.strip() for s in source_id.split("|") if s.strip()]
        if len(source_tokens) < 2:
            raise ValueError(
                "Multi-representation map rule requires at least two source "
                "components joined by '|' (e.g. 'FREQ|REF_AREA'). Use "
                "'representation' for a single source component."
            )
        return {
            "mapping_rule": "multi_representation",
            "source_id": source_id,
            "target_id": target_id,
            "fixed_value": None,
            "source_cl": source_cl,
            "target_cl": target_cl,
            "default_value": default_value,
        }

    # unknown
    raise ValueError(f"Unknown mapping rule: '{raw_rule}'")


@typechecked
def _extract_representation_map(
    rep_data: dict[str, pd.DataFrame],
    source_id: str,
    target_id: str,
    rep_data_error: str | None = None,
) -> pd.DataFrame:
    """Build the (source, target) mapping pairs DataFrame for a representation rule.

    Resolves column names and performs sanitization.

    Args:
        rep_data: Dictionary containing 'source' and 'target' DataFrames
            derived from REP_MAPPING.
        source_id: Component identifier to be matched to a column in
            ``rep_data['source']``.
        target_id: Component identifier to be matched to a column in
            ``rep_data['target']``.
        rep_data_error: Optional message from an earlier REP_MAPPING parse
            failure, appended to the raised error when ``rep_data`` is empty.

    Returns:
        Two-column DataFrame with columns ['source', 'target'], NA rows
        dropped and duplicate row pairs removed.

    Raises:
        ValueError: If rep_data is missing, either DataFrame is empty,
            column resolution fails, or no valid mapping pairs remain.
    """
    # 1) Validate presence and non-empty REP_MAPPING inputs
    if (
        not rep_data
        or "source" not in rep_data
        or "target" not in rep_data
        or rep_data["source"] is None
        or rep_data["target"] is None
        or rep_data["source"].empty
        or rep_data["target"].empty
    ):
        message = (
            "Mapping rule requires 'REP_MAPPING' sheet with data, but it was "
            "invalid or empty."
        )
        if rep_data_error:
            message += f" Underlying REP_MAPPING error: {rep_data_error}"
        raise ValueError(message)

    source_df = rep_data["source"]
    target_df = rep_data["target"]

    # 2) Resolve actual column names (can raise if not found)
    actual_source_col = _match_column_name(source_id, source_df.columns.tolist())
    actual_target_col = _match_column_name(target_id, target_df.columns.tolist())

    # 3) Build, sanitize, and deduplicate pairs
    rep_mapping_df = (
        pd.DataFrame(
            {
                "source": source_df[actual_source_col],
                "target": target_df[actual_target_col],
            }
        )
        .dropna(subset=["source", "target"], how="any")
        .drop_duplicates()
    )

    # 4) Enforce non-empty result
    if rep_mapping_df.empty:
        raise ValueError(
            f"No valid mapping rows found between source column '{actual_source_col}' "
            f"and target column '{actual_target_col}'."
        )

    return rep_mapping_df


@typechecked
def _extract_multi_representation_map(
    rep_data: dict[str, pd.DataFrame],
    source_ids: list[str],
    target_id: str,
    rep_data_error: str | None = None,
) -> pd.DataFrame:
    """Build the value-tuple mapping DataFrame for a multi-representation rule.

    Resolves each source component ID and the target component ID to columns in
    the parsed REP_MAPPING data, then assembles a DataFrame whose columns are
    named after the component IDs (so they line up with the ``source_cols`` /
    ``target_cols`` consumed by :func:`build_multi_component_map`).

    Args:
        rep_data: Dictionary containing 'source' and 'target' DataFrames
            derived from REP_MAPPING.
        source_ids: Ordered source component IDs, each matched to a column in
            ``rep_data['source']``.
        target_id: Target component ID, matched to a column in
            ``rep_data['target']``.
        rep_data_error: Optional message from an earlier REP_MAPPING parse
            failure, appended to the raised error when ``rep_data`` is empty.

    Returns:
        A DataFrame with one column per ``source_ids`` entry followed by a
        ``target_id`` column, NA rows dropped and duplicate tuples removed.

    Raises:
        ValueError: If rep_data is missing/empty, a column cannot be resolved,
            or no valid mapping rows remain.
    """
    # 1) Validate presence and non-empty REP_MAPPING inputs
    if (
        not rep_data
        or "source" not in rep_data
        or "target" not in rep_data
        or rep_data["source"] is None
        or rep_data["target"] is None
        or rep_data["source"].empty
        or rep_data["target"].empty
    ):
        message = (
            "Mapping rule requires 'REP_MAPPING' sheet with data, "
            "but it was invalid or empty."
        )
        if rep_data_error:
            message += f" Underlying REP_MAPPING error: {rep_data_error}"
        raise ValueError(message)

    source_df = rep_data["source"]
    target_df = rep_data["target"]

    # 2) Resolve actual column names (can raise if not found), keyed by component ID
    columns: dict[str, pd.Series] = {}
    for source_id in source_ids:
        actual_col = _match_column_name(source_id, source_df.columns.tolist())
        columns[source_id] = source_df[actual_col]
    actual_target_col = _match_column_name(target_id, target_df.columns.tolist())
    columns[target_id] = target_df[actual_target_col]

    # 3) Build, sanitize, and deduplicate tuples
    rep_mapping_df = pd.DataFrame(columns).dropna(how="any").drop_duplicates()

    # 4) Enforce non-empty result
    if rep_mapping_df.empty:
        raise ValueError(
            f"No valid mapping rows found for sources {source_ids} "
            f"and target '{target_id}'."
        )

    return rep_mapping_df
