"""Builders for SDMX map artefacts (value, component, and representation maps)."""

from collections.abc import Sequence
from datetime import datetime
from typing import Literal

import pandas as pd
from pysdmx.model import (
    DataType,
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
    ValueMap,
)
from typeguard import typechecked

from .._deprecation import deprecated
from ..artefact_builder import (
    build_multi_representation_map as _build_multi_representation_map,
)
from ..artefact_builder import (
    build_representation_map as _build_representation_map,
)
from .urn import gen_urn


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
        ValidationError: If ``id``/``name`` are omitted or empty (rules
            M001/M003), or the DataFrame yields no value mappings (rule
            R003) — see :mod:`tidysdmx.artefact_validation`.

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
    # None -> "" so omitted id/name fail the value builder's publish-readiness
    # checks (M001/M003) instead of typeguard's stricter `str` annotation.
    return _build_representation_map(
        id=id or "",
        agency=agency,
        name=name or "",
        source=_resolve_representation_ref(source_cl),
        target=_resolve_representation_ref(target_cl),
        maps=value_maps,
        version=version,
        description=description,
        urn=urn,
    )


@deprecated(replacement="build_representation_map_from_df")
def build_representation_map(
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
    """Deprecated alias for :func:`build_representation_map_from_df`.

    Mirrors that function's signature so ``help()``/IDE introspection keep
    showing the real parameters during the deprecation window; see it for
    the full documentation.
    """
    return build_representation_map_from_df(
        df,
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
        ValidationError: If ``id``/``name`` are omitted or empty (rules
            M001/M003) — see :mod:`tidysdmx.artefact_validation`.
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
    # None -> "" so omitted id/name fail the value builder's publish-readiness
    # checks (M001/M003) instead of typeguard's stricter `str` annotation.
    return _build_multi_representation_map(
        id=id or "",
        agency=agency,
        name=name or "",
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
    df: pd.DataFrame,
    agency: str = "FAKE_AGENCY",
    id: str | None = None,
    name: str | None = None,
    source_cls: list[str] | None = None,
    target_cls: list[str] | None = None,
    version: str = "1.0",
    description: str | None = None,
    source_cols: list[str] | None = None,
    target_cols: list[str] | None = None,
    valid_from_col: str = "valid_from",
    valid_to_col: str = "valid_to",
    generate_urn: bool = True,
    default_value: str | None = None,
) -> MultiRepresentationMap:
    """Deprecated alias for :func:`build_multi_representation_map_from_df`.

    Mirrors that function's signature so ``help()``/IDE introspection keep
    showing the real parameters during the deprecation window; see it for
    the full documentation.
    """
    return build_multi_representation_map_from_df(
        df,
        agency=agency,
        id=id,
        name=name,
        source_cls=source_cls,
        target_cls=target_cls,
        version=version,
        description=description,
        source_cols=source_cols,
        target_cols=target_cols,
        valid_from_col=valid_from_col,
        valid_to_col=valid_to_col,
        generate_urn=generate_urn,
        default_value=default_value,
    )


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
