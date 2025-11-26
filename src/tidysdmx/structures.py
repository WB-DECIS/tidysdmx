from typeguard import typechecked
from typing import List, Tuple, Union, Optional, Literal, Sequence
from itertools import combinations
from datetime import datetime
from pysdmx.model.dataflow import Schema, Components, Component
from pysdmx.model import Concept, Role, DataType, Codelist, Code
from pysdmx.model.map import (
    RepresentationMap, 
    FixedValueMap, 
    ImplicitComponentMap, 
    DatePatternMap, 
    ValueMap, 
    MultiValueMap
    )
import pandas as pd

# region infer dataset structure
def infer_role_dimension(
    df: pd.DataFrame, 
    value_col: str
) -> List[Union[str, Tuple[str, ...]]]:
    """Infers the minimal set of unique keys (dimension columns) for an observation column.

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

def infer_role(
        col_name: str, 
        series, 
        dimension_cols: set
    ) -> Role:
    """Infer SDMX role for a column based on dimension detection and data characteristics.

    Args:
        col_name (str): The name of the column.
        series (pd.Series): The data series for the column.
        dimension_cols (set): A set of dimension column names.

    Returns:
        Role: The inferred role for the column.
    """
    if col_name in dimension_cols:
        return Role.DIMENSION
    if series.dtype.kind in ["i", "f"]:  # numeric
        return Role.MEASURE
    # Heuristic for remaining columns
    unique_ratio = series.nunique() / len(series)
    return Role.DIMENSION if unique_ratio < 0.5 else Role.ATTRIBUTE

def infer_dtype(series) -> DataType:
    """Map pandas dtype to SDMX DataType.
    
    Args:
        series (pd.Series): The data series for the column.
    
    Returns:
        DataType: The inferred SDMX data type.
    """
    if series.dtype.kind in ["i", "f"]:
        return DataType.FLOAT
    if "datetime" in str(series.dtype):
        return DataType.PERIOD
    return DataType.STRING

def infer_schema(
        df: pd.DataFrame, 
        agency: str = "WB", 
        id: str = "INFERRED_SCHEMA", 
        value_col: str = "OBS_VALUE"
    ) -> Schema:
    """Infer a pysdmx Schema from a pandas DataFrame.
    
    Args:
        df (pd.DataFrame): The input DataFrame.
        agency (str, optional): The agency ID for the schema. Defaults to "WB".
        id (str, optional): The schema ID. Defaults to "INFERRED_SCHEMA".
        value_col (str, optional): The name of the observation value column. Defaults to "OBS_VALUE".
    
    Returns:
        Schema: The inferred pysdmx Schema object.
    """
    # Infer dimension columns
    dim_cols = infer_role_dimension(df = df, value_col = value_col)
    
    components = []

    for col in df.columns:
        role = infer_role(col_name = col, series = df[col], 
                          dimension_cols = dim_cols)
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
# endregion

# region structure map
@typechecked
def build_fixed_map(target: str, value: str, located_in: Optional[str] = "target") -> FixedValueMap:
    """Build a pysdmx FixedValueMap for setting a component to a fixed value.

    Args:
    target (str): The ID of the target component in the structure map.
    value (str): The fixed value to assign to the target component.
    located_in (Optional[str]): Indicates whether the mapping is located in 'source' or 'target'.
        Defaults to 'target'.

    Returns:
    FixedValueMap: A pysdmx FixedValueMap object representing the fixed mapping.

    Raises:
    ValueError: If `target` or `value` is empty.
    ValueError: If `located_in` is not 'source' or 'target'.

    Examples:
    >>> mapping = build_fixed_map("CONF_STATUS", "F")
    >>> isinstance(mapping, FixedValueMap)
    True
    >>> str(mapping)
    'target: CONF_STATUS, value: F, located_in: target'
    """
    if not target or not value:
        raise ValueError("Both 'target' and 'value' must be non-empty strings.")
    if located_in not in {"source", "target"}:
        raise ValueError("Parameter 'located_in' must be either 'source' or 'target'.")

    return FixedValueMap(target=target, value=value, located_in=located_in)

@typechecked
def build_implicit_component_map(source: str, target: str) -> ImplicitComponentMap:
    """Build a pysdmx ImplicitComponentMap for mapping a source component to a target component using implicit mapping rules (e.g., same representation or concept).

    Args:
    source (str): The ID of the source component in the structure map.
    target (str): The ID of the target component in the structure map.

    Returns:
    ImplicitComponentMap: A pysdmx ImplicitComponentMap object representing the implicit mapping.

    Raises:
    ValueError: If `source` or `target` is empty.

    Examples:
    >>> mapping = build_implicit_component_map("FREQ", "FREQUENCY")
    >>> isinstance(mapping, ImplicitComponentMap)
    True
    >>> mapping.source
    'FREQ'
    >>> mapping.target
    'FREQUENCY'
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
    id: Optional[str] = None,
    locale: str = "en",
    pattern_type: Literal["fixed", "variable"] = "fixed",
    resolve_period: Optional[Literal["startOfPeriod", "endOfPeriod", "midPeriod"]] = None
) -> DatePatternMap:
    """Build a DatePatternMap object for mapping date patterns between SDMX components.

    Args:
        source (str): The ID of the source component.
        target (str): The ID of the target component.
        pattern (str): The SDMX date pattern describing the source date (e.g., "MMM yy").
        frequency (str): The frequency code or reference (e.g., "M" for monthly).
        id (Optional[str]): Optional map ID as defined in the registry.
        locale (str): Locale for parsing the input date pattern. Defaults to "en".
        pattern_type (Literal["fixed", "variable"]): Type of date pattern. Defaults to "fixed".
            - "fixed": frequency is a fixed value (e.g., "A" for annual).
            - "variable": frequency references a dimension or attribute (e.g., "FREQ").
        resolve_period (Optional[Literal["startOfPeriod", "endOfPeriod", "midPeriod"]]): Point in time to resolve when mapping from low to high frequency periods.

    Returns:
        DatePatternMap: A fully constructed DatePatternMap instance.

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
        resolve_period=resolve_period
    )


typechecked
def build_value_map(
    source: str,
    target: str,
    valid_from: Optional[datetime] = None,
    valid_to: Optional[datetime] = None
) -> ValueMap:
    """Create a pysdmx ValueMap object mapping a source value to a target value.

    Args:
        source (str): The source value to map.
        target (str): The target value to map to.
        valid_from (Optional[datetime]): Start of business validity for the mapping.
        valid_to (Optional[datetime]): End of business validity for the mapping.

    Returns:
        ValueMap: A pysdmx ValueMap object representing the mapping.

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
    if not isinstance(source, str) or not isinstance(target, str):
        raise TypeError("Source and target must be strings.")
    if not source.strip() or not target.strip():
        raise ValueError("Source and target cannot be empty.")

    return ValueMap(source=source, target=target, valid_from=valid_from, valid_to=valid_to)

# endregion

# region representation maps
@typechecked
def build_value_map_list(
    df: pd.DataFrame,
    source_col: str = "source",
    target_col: str = "target",
    valid_from_col: str = "valid_from",
    valid_to_col: str = "valid_to"
) -> list[ValueMap]:
    """Build a list of ValueMap objects from a pandas DataFrame, optionally including validity periods.

    Args:
        df (pd.DataFrame): DataFrame where each row represents a mapping.
        source_col (str): Column name for source values.
        target_col (str): Column name for target values.
        valid_from_col (str): Optional column name for validity start date. Defaults to "valid_from".
        valid_to_col (str): Optional column name for validity end date. Defaults to "valid_to".

    Returns:
        list[ValueMap]: List of ValueMap objects created from the DataFrame.

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
        raise ValueError(f"Columns '{source_col}' and '{target_col}' must exist in DataFrame.")
    if not df[source_col].map(lambda x: isinstance(x, str)).all() or \
       not df[target_col].map(lambda x: isinstance(x, str)).all():
        raise TypeError("Source and target columns must contain only string values.")

    has_valid_from = valid_from_col in df.columns
    has_valid_to = valid_to_col in df.columns

    value_maps: list[ValueMap] = []
    for _, row in df.iterrows():
        kwargs = {
            "source": row[source_col],
            "target": row[target_col]
        }
        if has_valid_from and pd.notna(row.get(valid_from_col)):
            kwargs["valid_from"] = str(row[valid_from_col])
        if has_valid_to and pd.notna(row.get(valid_to_col)):
            kwargs["valid_to"] = str(row[valid_to_col])
        value_maps.append(ValueMap(**kwargs))

    return value_maps


@typechecked
def build_multi_value_map_list(
    df: pd.DataFrame,
    source_cols: Sequence[str],
    target_col: str,
    valid_from_col: str = "valid_from",
    valid_to_col: str = "valid_to"
) -> list[MultiValueMap]:
    """Build a list of MultiValueMap objects from a pandas DataFrame, optionally including validity periods.

    Args:
        df (pd.DataFrame): DataFrame where each row represents a mapping.
        source_cols (Sequence[str]): Column names for source values (multiple allowed).
        target_col (str): Column name for target value (single column).
        valid_from_col (str): Optional column name for validity start date. Defaults to "valid_from".
        valid_to_col (str): Optional column name for validity end date. Defaults to "valid_to".

    Returns:
        list[MultiValueMap]: List of MultiValueMap objects created from the DataFrame.

    Raises:
        ValueError: If DataFrame is empty or required columns are missing.
        TypeError: If source or target columns contain non-string values.

    Examples:
        >>> import pandas as pd
        >>> data = {
        ...     'country': ['DE', 'CH'],
        ...     'currency': ['LC', 'LC'],
        ...     'iso_code': ['EUR', 'CHF']
        ... }
        >>> df = pd.DataFrame(data)
        >>> multi_maps = build_multi_value_map_list(df, ['country', 'currency'], 'iso_code')
        >>> isinstance(multi_maps[0], MultiValueMap)
        True
    """
    if df.empty:
        raise ValueError("Input DataFrame cannot be empty.")
    for col in source_cols:
        if col not in df.columns:
            raise ValueError(f"Source column '{col}' must exist in DataFrame.")
    if target_col not in df.columns:
        raise ValueError(f"Target column '{target_col}' must exist in DataFrame.")

    # Validate data types
    for col in source_cols:
        if not df[col].map(lambda x: isinstance(x, str)).all():
            raise TypeError(f"Source column '{col}' must contain only string values.")
    if not df[target_col].map(lambda x: isinstance(x, str)).all():
        raise TypeError(f"Target column '{target_col}' must contain only string values.")

    has_valid_from = valid_from_col in df.columns
    has_valid_to = valid_to_col in df.columns

    multi_value_maps: list[MultiValueMap] = []
    for _, row in df.iterrows():
        source_values = [row[col] for col in source_cols]
        target_value = row[target_col]

        kwargs = {
            "source": source_values,
            "target": [target_value]  # Wrap in list for consistency with MultiValueMap
        }
        if has_valid_from and pd.notna(row.get(valid_from_col)):
            kwargs["valid_from"] = datetime.fromisoformat(str(row[valid_from_col]))
        if has_valid_to and pd.notna(row.get(valid_to_col)):
            kwargs["valid_to"] = datetime.fromisoformat(str(row[valid_to_col]))

        multi_value_maps.append(MultiValueMap(**kwargs))

    return multi_value_maps


@typechecked
def build_representation_map(
    df: pd.DataFrame,
    agency: str = "FAKE_AGENCY",
    id: Optional[str] = None,
    name: Optional[str] = None,
    source_cl: Optional[str] = None,
    target_cl: Optional[str] = None,
    version: str = "1.0",
    description: Optional[str] = None,
    source_col: str = "source",
    target_col: str = "target",
    valid_from_col: str = "valid_from",
    valid_to_col: str = "valid_to"
) -> RepresentationMap:
    """Build a RepresentationMap object from a pandas DataFrame using build_value_map_list.

    Args:
        df (pd.DataFrame): DataFrame where each row represents a mapping.
        agency (str): Agency maintaining the representation map.
        id (str): Identifier for the representation map.
        name (str): Name of the representation map.
        source_cl (str): URN or identifier for the source codelist or data type.
        target_cl (str): URN or identifier for the target codelist or data type.
        version (str): Version of the representation map. Defaults to "1.0".
        description (Optional[str]): Optional description of the representation map.
        source_col (str): Column name for source values. Defaults to "source".
        target_col (str): Column name for target values. Defaults to "target".
        valid_from_col (str): Column name for validity start date. Defaults to "valid_from".
        valid_to_col (str): Column name for validity end date. Defaults to "valid_to".

    Returns:
        RepresentationMap: A RepresentationMap object containing the mappings.

    Raises:
        ValueError: If DataFrame is empty or required columns are missing.
        TypeError: If source or target columns contain non-string values.

    Examples:
        >>> import pandas as pd
        >>> data = {
        ...     'source': ['BE', 'FR'],
        ...     'target': ['BEL', 'FRA'],
        ...     'valid_from': ['2020-01-01', None],
        ...     'valid_to': ['2025-12-31', None]
        ... }
        >>> df = pd.DataFrame(data)
        >>> rm = build_representation_map(df, 'urn:source:codelist', 'urn:target:codelist', 'RM1', 'Country Map', 'ECB')
        >>> isinstance(rm, RepresentationMap)
        True
    """
    # Use the existing function to build value maps
    value_maps = build_value_map_list(
        df,
        source_col=source_col,
        target_col=target_col,
        valid_from_col=valid_from_col,
        valid_to_col=valid_to_col
    )

    return RepresentationMap(
        id=id,
        name=name,
        agency=agency,
        source=source_cl,
        target=target_cl,
        maps=value_maps,
        description=description,
        version=version
    )

# endregion