from typeguard import typechecked
from dataclasses import dataclass
from typing import List, Tuple, Union, Optional, Literal, Sequence, Any, Dict
from itertools import combinations
from datetime import datetime, timezone
from pysdmx.model.dataflow import Schema, Components, Component
from pysdmx.model import Concept, Role, DataType, Codelist, Code
from openpyxl import Workbook, load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from pathlib import Path
from pysdmx.model.map import (
    RepresentationMap, 
    FixedValueMap, 
    ImplicitComponentMap, 
    DatePatternMap, 
    ValueMap, 
    MultiValueMap,
    MultiRepresentationMap,
    ComponentMap,
    StructureMap
    )
import pandas as pd
import re
# Import tidysdmx functions
from .tidysdmx import parse_artefact_id

# region infer dataset structure
# def infer_role_dimension(
#     df: pd.DataFrame, 
#     value_col: str
# ) -> List[Union[str, Tuple[str, ...]]]:
#     """Infers the minimal set of unique keys (dimension columns) for an observation column.

#     This function identifies the smallest combination of columns that uniquely 
#     identifies each record's observation value. It starts by checking single 
#     columns and progressively checks larger combinations until a key is found.

#     Args:
#         df (pd.DataFrame): The input DataFrame containing the data.
#         value_col (str): The name of the column containing the 
#                                   observation values to be identified.

#     Returns:
#         List[Union[str, Tuple[str, ...]]]: 
#             A list of the minimal unique keys found.
#             - For single-column keys, returns a list of strings (e.g., ['col1', 'col2']).
#             - For composite keys, returns a list of tuples (e.g., [('col1', 'col2')]).
#             Returns an empty list if no key is found.
#     """
#     if value_col not in df.columns:
#         raise ValueError(f"Observation column '{value_col}' not found in DataFrame.")

#     dimension_columns = [col for col in df.columns if col != value_col]
#     df_cleaned = df.dropna(subset=dimension_columns, how='all')
    
#     if len(df_cleaned) == 0:
#         return []

#     for combination_size in range(1, len(dimension_columns) + 1):
#         unique_keys_found = []
        
#         for column_combination in combinations(dimension_columns, combination_size):
#             key_candidate = list(column_combination)
            
#             if not df_cleaned.duplicated(subset=key_candidate).any():
#                 # --- FIX IS HERE ---
#                 # If the key has only one column, store it as a string.
#                 # Otherwise, store it as a sorted tuple.
#                 if len(key_candidate) == 1:
#                     unique_keys_found.append(key_candidate[0])
#                 else:
#                     unique_keys_found.append(tuple(sorted(key_candidate)))

#         if unique_keys_found:
#             # Sort the final list for consistent output
#             # This handles cases where multiple keys of the same size are found
#             return sorted(unique_keys_found, key=lambda k: str(k))
            
#     return []

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


@typechecked
def build_multi_representation_map(
    df: pd.DataFrame,
    agency: str = "FAKE_AGENCY",
    id: Optional[str] = None,
    name: Optional[str] = None,
    source_cls: Optional[list[str]] = None,
    target_cls: Optional[list[str]] = None,
    version: str = "1.0",
    description: Optional[str] = None,
    source_cols: list[str] = ["source"],
    target_cols: list[str] = ["target"],
    valid_from_col: str = "valid_from",
    valid_to_col: str = "valid_to"
) -> MultiRepresentationMap:
    """Build a MultiRepresentationMap object from a pandas DataFrame using build_multi_value_map_list.

    Args:
        df (pd.DataFrame): DataFrame where each row represents a multi-mapping.
        agency (str): Agency maintaining the multi-representation map.
        id (Optional[str]): Identifier for the multi-representation map.
        name (Optional[str]): Name of the multi-representation map.
        source_cls (Optional[list[str]]): List of URNs or identifiers for source codelists or data types.
        target_cls (Optional[list[str]]): List of URNs or identifiers for target codelists or data types.
        version (str): Version of the multi-representation map. Defaults to "1.0".
        description (Optional[str]): Optional description of the multi-representation map.
        source_cols (list[str]): Column names for source values. Defaults to ["source"].
        target_cols (list[str]): Column names for target values. Defaults to ["target"].
        valid_from_col (str): Column name for validity start date. Defaults to "valid_from".
        valid_to_col (str): Column name for validity end date. Defaults to "valid_to".

    Returns:
        MultiRepresentationMap: A MultiRepresentationMap object containing the mappings.

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
        >>> mrm = build_multi_representation_map(df, id="MRM1", name="Country Multi Map", agency="ECB")
        >>> isinstance(mrm, MultiRepresentationMap)
        True
    """
    if df.empty:
        raise ValueError("Input DataFrame cannot be empty.")

    # Validate required columns
    required_cols = set(source_cols + target_cols)
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Missing required columns: {required_cols - set(df.columns)}")

    # Validate data types
    for col in source_cols + target_cols:
        if not all(isinstance(val, str) for val in df[col].dropna()):
            raise TypeError(f"Column '{col}' contains non-string values.")

    # Build multi-value maps
    multi_value_maps = build_multi_value_map_list(
        df,
        source_cols=source_cols,
        target_cols=target_cols,
        valid_from_col=valid_from_col,
        valid_to_col=valid_to_col
    )

    return MultiRepresentationMap(
        id=id,
        name=name,
        agency=agency,
        sources=source_cls,
        targets=target_cls,
        maps=multi_value_maps,
        description=description,
        version=version
    )


@typechecked
def build_single_component_map(
    df: pd.DataFrame,
    source_component: str,
    target_component: str,
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
) -> ComponentMap:
    """Build a ComponentMap mapping one source component to one target component using a RepresentationMap built from a pandas DataFrame.

    Args:
        df (pd.DataFrame): DataFrame where each row represents a mapping.
        source_component (str): ID of the source component.
        target_component (str): ID of the target component.
        agency (str): Agency maintaining the representation map. Defaults to "FAKE_AGENCY".
        id (Optional[str]): Identifier for the representation map.
        name (Optional[str]): Name of the representation map.
        source_cl (Optional[str]): URN or identifier for the source codelist or data type.
        target_cl (Optional[str]): URN or identifier for the target codelist or data type.
        version (str): Version of the representation map. Defaults to "1.0".
        description (Optional[str]): Optional description of the representation map.
        source_col (str): Column name for source values. Defaults to "source".
        target_col (str): Column name for target values. Defaults to "target".
        valid_from_col (str): Column name for validity start date. Defaults to "valid_from".
        valid_to_col (str): Column name for validity end date. Defaults to "valid_to".

    Returns:
        ComponentMap: A ComponentMap object mapping the source component to the target component.

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
        if not df[col].map(lambda x: isinstance(x, str) or pd.isna(x)).all():
            raise TypeError(f"Column '{col}' must contain only string values or NaN.")

    # Build RepresentationMap using the provided helper
    representation_map = build_representation_map(
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
        valid_to_col=valid_to_col
    )

    # Return ComponentMap
    return ComponentMap(source=source_component, target=target_component, values=representation_map)


# endregion

# region TESTING
def _sheet_to_df(wb: Workbook, sheet_name: str) -> pd.DataFrame:
    """Reads an openpyxl sheet into a pandas DataFrame."""
    if sheet_name not in wb.sheetnames:
        # Return empty DF with expected columns if sheet is missing to allow graceful failure handling
        return pd.DataFrame(columns=["source", "target", "valid_from", "valid_to"])
    
    ws = wb[sheet_name]
    data = list(ws.values)
    
    if not data:
        return pd.DataFrame(columns=["source", "target", "valid_from", "valid_to"])
        
    cols = data[0]
    rows = data[1:]
    
    return pd.DataFrame(rows, columns=cols)

@dataclass
class MappingDefinition:
    """Intermediate representation of a mapping rule parsed from the Excel file.

    It decouples the Excel parsing logic from the SDMX object construction.
    """
    target: str
    map_type: Literal["fixed", "implicit", "representation"]
    source: Optional[str] = None
    fixed_value: Optional[str] = None
    representation_df: Optional[pd.DataFrame] = None

@typechecked
def _read_comp_mapping_sheet(workbook: Workbook) -> pd.DataFrame:
    """Loads and validates the structure of the mandatory 'comp_mapping' sheet.

    Args:
        workbook (Workbook): The openpyxl Workbook object.

    Returns:
        pd.DataFrame: The validated DataFrame with normalized headers.

    Raises:
        KeyError: If 'comp_mapping' sheet is missing.
        ValueError: If the sheet is empty or headers are incorrect.
    """
    try:
        ws_comp = workbook["comp_mapping"]
    except KeyError:
        raise KeyError("Mandatory sheet 'comp_mapping' not found in workbook.")

    data = list(ws_comp.values)
    if not data or len(data) < 2:
        raise ValueError("The 'comp_mapping' sheet is empty or missing headers.")

    df_comp = pd.DataFrame(data[1:], columns=data[0])
    
    # Normalize headers
    df_comp.columns = [str(c).lower() for c in df_comp.columns]
    required_cols = {"source", "target", "mapping_rules"}
    if not required_cols.issubset(set(df_comp.columns)):
        raise ValueError(f"The 'comp_mapping' sheet must have columns: {required_cols}")
    
    
    # Remove rows where all values are NaN
    df_comp = df_comp.dropna(how='all')

    return df_comp.fillna("")


@typechecked
def _create_fixed_definition(row: pd.Series, target: str, mapping_rules: str) -> MappingDefinition:
    """Creates a MappingDefinition for a FixedValueMap."""
    fixed_val = mapping_rules[len("fixed:"):].strip()
    if not fixed_val:
        raise ValueError(f"Fixed value for target '{target}' cannot be empty.")
    
    return MappingDefinition(
        target=target,
        map_type="fixed",
        fixed_value=fixed_val
    )


@typechecked
def _create_implicit_definition(row: pd.Series, target: str, source: str) -> MappingDefinition:
    """Creates a MappingDefinition for an ImplicitComponentMap."""
    if not source:
        raise ValueError(f"Implicit map rule requires a 'source' for target '{target}'.")
    
    return MappingDefinition(
        target=target,
        map_type="implicit",
        source=source
    )


@typechecked
def _create_representation_definition(
    workbook: Workbook, target: str, source: str
) -> MappingDefinition:
    """Creates a MappingDefinition for a RepresentationMap by loading the dependent sheet."""
    # Load the referenced sheet immediately
    df_rep = _sheet_to_df(workbook, target)
    
    # Infer source if missing (Identity Map assumption: Source=Target)
    final_source = source if source else target
    
    return MappingDefinition(
        target=target,
        map_type="representation",
        source=final_source,
        representation_df=df_rep
    )

@typechecked
def extract_mapping_definitions(workbook: Workbook) -> list[MappingDefinition]:
    """Parses the workbook to extract a list of mapping definitions, delegating parsing logic to focused helper functions.

    Args:
        workbook (Workbook): The openpyxl Workbook object.

    Returns:
        list[MappingDefinition]: A list of intermediate objects describing the maps.

    Raises:
        KeyError: If 'comp_mapping' sheet is missing (from helper).
        ValueError: If sheet structure or rules are malformed (from helpers).
    """
    # 1. Load and Validate Main Mapping Sheet Structure
    df_comp = _read_comp_mapping_sheet(workbook)
    
    definitions: list[MappingDefinition] = []
    
    # 2. Iterate and Dispatch Parsing
    for _, row in df_comp.iterrows():
        source: str = str(row["source"]).strip()
        target: str = str(row["target"]).strip()
        mapping_rules: str = str(row["mapping_rules"]).strip()
        
        if not target:
            continue
        if not mapping_rules and not source:
            continue

        if mapping_rules.startswith("fixed:"):
            definitions.append(_create_fixed_definition(row, target, mapping_rules))
            
        elif mapping_rules == "implicit":
            definitions.append(_create_implicit_definition(row, target, source))
            
        elif mapping_rules == target and mapping_rules:
            definitions.append(_create_representation_definition(workbook, target, source))
            
        elif mapping_rules and not mapping_rules.startswith("=HYPERLINK"):
             raise ValueError(
                f"Unknown mapping rule for target '{target}': '{mapping_rules}'"
            )

    return definitions

@typechecked
def build_structure_map(
    workbook: Workbook, 
    default_agency: str = "DEFAULT_AGENCY"
) -> StructureMap:
    """Converts a populated Excel Workbook into a pysdmx StructureMap object.
    
    This function leverages `extract_mapping_definitions` to parse the Excel file
    into intermediate definitions, and then converts those definitions into
    pysdmx objects.
    """
    # 1. Parse Excel to Intermediate Definitions
    definitions = extract_mapping_definitions(workbook)
    
    maps_list = []
    
    # 2. Convert Definitions to pysdmx Objects
    for definition in definitions:
        if definition.map_type == "fixed":
            if definition.fixed_value is None:
                raise ValueError(f"Fixed value missing for {definition.target}")
            maps_list.append(
                build_fixed_map(target=definition.target, value=definition.fixed_value)
            )
            
        elif definition.map_type == "implicit":
            if definition.source is None:
                raise ValueError(f"Source missing for implicit map {definition.target}")
            maps_list.append(
                build_implicit_component_map(source=definition.source, target=definition.target)
            )
            
        elif definition.map_type == "representation":
            if definition.representation_df is None:
                raise ValueError(f"DataFrame missing for representation map {definition.target}")
            
            # Safe unwrapping of optional source (logic in extractor ensures it's set, but typing needs check)
            src = definition.source if definition.source else definition.target
            
            try:
                comp_map = build_single_component_map(
                    df=definition.representation_df,
                    source_component=src,
                    target_component=definition.target,
                    agency=default_agency,
                    id=f"REPMAP_{definition.target}",
                    name=f"Mapping for {definition.target}",
                    version="1.0"
                )
                maps_list.append(comp_map)
            except ValueError as e:
                # Log or handle empty DF errors if necessary
                if "empty" in str(e):
                    continue
                raise e

    # 3. Return Final Artifact
    return StructureMap(
        id="GENERATED_STRUCTURE_MAP",
        agency=default_agency, 
        version="1.0",
        name="Auto-generated Structure Map",
        maps=maps_list
    )
# endregion

# region create_schema_from_table()
@typechecked
def _infer_sdmx_type(dtype: object) -> DataType:
    """Infer the SDMX DataType from a pandas/numpy dtype.

    Args:
        dtype (object): The pandas/numpy data type.

    Returns:
        DataType: The corresponding SDMX DataType.
    """
    dtype_str = str(dtype)

    if "int" in dtype_str:
        return DataType.INTEGER
    elif "float" in dtype_str:
        return DataType.DOUBLE
    elif "bool" in dtype_str:
        return DataType.BOOLEAN
    elif "datetime" in dtype_str:
        return DataType.DATE_TIME
    else:
        return DataType.STRING


@typechecked
def _sanitize_sdmx_id(value: Any) -> str:
    """Sanitize a string to create a valid SDMX Identifier.

    Allowed characters: A-Z, a-z, 0-9, _, -, $, @.
    This function converts to uppercase and replaces invalid characters with underscores.

    Args:
        value (Any): The input value to sanitize.

    Returns:
        str: A valid SDMX ID string.
    """
    if value is None:
        return "UNKNOWN"
    
    # Convert to string, strip whitespace, and uppercase
    s = str(value).strip().upper()
    
    # Replace invalid characters with underscore
    # SDMX Common ID pattern: [A-Za-z0-9_@$-]+
    s = re.sub(r"[^A-Z0-9_@$-]", "_", s)
    
    # Ensure it doesn't start with a number or invalid char if that's a strict requirement,
    # though strictly the NCName pattern allows some flexibility. 
    # For robustness, if empty or starts with non-alpha, prefix.
    if not s or not s[0].isalpha():
        s = "ID_" + s
        
    return s


@typechecked
def _create_concept(concept_id: str, dtype: DataType) -> Concept:
    """Create a simple SDMX Concept with a specific ID and data type.

    Args:
        concept_id (str): The unique identifier for the concept.
        dtype (DataType): The data type of the concept.

    Returns:
        Concept: An immutable Concept object.
    """
    return Concept(
        id=concept_id,
        name=concept_id,
        dtype=dtype,
        description=f"Concept inferred from column {concept_id}",
    )


@typechecked
def _create_codelist_from_series(
    series: pd.Series, 
    col_name: str, 
    agency_id: str, 
    version: str
) -> Codelist:
    """Create an SDMX Codelist from the unique values in a pandas Series.

    Args:
        series (pd.Series): The data column.
        col_name (str): The name of the column (used for Codelist ID).
        agency_id (str): The maintenance agency ID.
        version (str): The version of the codelist.

    Returns:
        Codelist: A Codelist object populated with Codes.
    """
    unique_values = series.dropna().unique()
    codes: list[Code] = []
    
    for val in sorted(unique_values, key=lambda x: str(x)):
        # Generate a safe ID for the code
        code_id = _sanitize_sdmx_id(val)
        # Use the original value as the name
        code_name = str(val)
        codes.append(Code(id=code_id, name=code_name))

    # Generate a Codelist ID, typically prefixed with CL_
    codelist_id = f"CL_{_sanitize_sdmx_id(col_name)}"

    return Codelist(
        id=codelist_id,
        agency=agency_id,
        version=version,
        name=f"Codelist for {col_name}",
        items=codes
    )


@typechecked
def _create_component(
    component_id: str,
    role: Role,
    concept: Concept,
    required: bool = True,
    attachment_level: Optional[str] = None,
    codelist: Optional[Codelist] = None,
) -> Component:
    """Create an SDMX Component (Dimension, Measure, or Attribute).

    Args:
        component_id (str): The unique identifier for the component.
        role (Role): The role the component plays.
        concept (Concept): The Concept defining the component's semantics.
        required (bool): Whether the component value is mandatory.
        attachment_level (Optional[str]): Mandatory for Attributes.
        codelist (Optional[Codelist]): The Codelist restricting the component's values.

    Returns:
        Component: The constructed Component object.
    """
    # Determine the local data type
    # If a codelist is present, the type is typically STRING (codes are strings)
    local_dtype = DataType.STRING if codelist else concept.dtype

    return Component(
        id=component_id,
        required=required,
        role=role,
        concept=concept,
        attachment_level=attachment_level,
        local_codes=codelist,
        local_dtype=local_dtype,
        name=concept.name,
        description=concept.description
    )


@typechecked
def _create_time_period_component() -> Component:
    """Create the standard SDMX Cross Domain Time Period component.

    Returns:
        Component: The strictly defined TIME_PERIOD component.
    """
    time_concept = Concept(
        id="TIME_PERIOD",
        urn="urn:sdmx:org.sdmx.infomodel.conceptscheme.Concept=SDMX:CROSS_DOMAIN_CONCEPTS(2.0).TIME_PERIOD",
        name="Time period",
        description="Timespan or point in time to which the observation actually refers.",
        dtype=DataType.STRING,
    )
    
    return Component(
        id="TIME_PERIOD",
        required=True,
        role=Role.DIMENSION,
        concept=time_concept,
        local_dtype=DataType.PERIOD,
        name="Time period",
        description="Timespan or point in time to which the observation actually refers.",
    )


@typechecked
def create_schema_from_table(
    dataframe: pd.DataFrame,
    dimensions: list[str],
    measure: str,
    time_dimension: str,
    attributes: Optional[list[str]] = None,
    agency_id: str = "SDMX",
    schema_id: str = "GENERATED_SCHEMA",
    version: str = "1.0",
) -> Schema:
    """Create a pysdmx Schema object from a pandas DataFrame, including inferred Codelists.

    This function automatically maps the provided `time_dimension` column to the 
    standard SDMX `TIME_PERIOD` concept. For other dimensions, it infers a Codelist
    from the unique values present in the column.

    Args:
        dataframe (pd.DataFrame): The source data.
        dimensions (list[str]): List of column names to serve as Dimensions.
        measure (str): The column name to serve as the Measure.
        time_dimension (str): The column name to serve as the Time Dimension.
            The resulting component will always be ID='TIME_PERIOD'.
        attributes (Optional[list[str]]): List of column names to serve as Attributes.
            Defaults to None.
        agency_id (str): The Agency ID to assign to the Schema. Defaults to "SDMX".
        schema_id (str): The ID to assign to the Schema. Defaults to "GENERATED_SCHEMA".
        version (str): The version string. Defaults to "1.0".

    Returns:
        Schema: A pysdmx Schema object containing the generated Components and Codelists.

    Raises:
        ValueError: If specified columns are missing from the dataframe.

    Examples:
        >>> import pandas as pd
        >>> df = pd.DataFrame({
        ...     "FREQ": ["A", "A", "M"],
        ...     "Year": ["2020", "2021", "2021-01"],
        ...     "OBS_VALUE": [10.5, 20.0, 15.0],
        ...     "OBS_STATUS": ["A", "A", "E"]
        ... })
        >>> schema = create_schema_from_table(
        ...     df,
        ...     dimensions=["FREQ"],
        ...     time_dimension="Year",
        ...     measure="OBS_VALUE",
        ...     attributes=["OBS_STATUS"]
        ... )
        >>> # Verify Codelist creation for FREQ
        >>> freq_comp = schema.components["FREQ"]
        >>> len(freq_comp.local_codes.items)
        2
        >>> freq_comp.local_codes.items[0].id
        'A'
    """
    if attributes is None:
        attributes = []

    # Validate that all columns exist in the dataframe
    all_required_cols = dimensions + [measure] + [time_dimension] + attributes
    missing_cols = [col for col in all_required_cols if col not in dataframe.columns]
    
    if missing_cols:
        #logger.error("Missing columns in dataframe: %s", missing_cols)
        raise ValueError(f"Columns not found in dataframe: {missing_cols}")

    component_list: list[Component] = []

    # 1. Process Dimensions (with Codelist inference)
    for col in dimensions:
        # Determine strict type
        dtype = _infer_sdmx_type(dataframe[col].dtype)
        
        # Create Concept
        concept = _create_concept(col, dtype)
        
        # Generate Codelist for Dimensions (Standard practice is that dimensions are coded)
        # Note: We use the unique values to build the Codelist
        codelist = _create_codelist_from_series(dataframe[col], col, agency_id, version)
        
        # Create Component attaching the Codelist
        comp = _create_component(
            col, 
            Role.DIMENSION, 
            concept, 
            required=True,
            codelist=codelist
        )
        component_list.append(comp)

    # 2. Process Time Dimension (Standardized)
    # Time dimension usually does NOT have a simple enumerated codelist (it uses Period format)
    time_comp = _create_time_period_component()
    component_list.append(time_comp)

    # 3. Process Measure (Single, Uncoded)
    meas_dtype = _infer_sdmx_type(dataframe[measure].dtype)
    meas_concept = _create_concept(measure, meas_dtype)
    meas_comp = _create_component(
        measure, 
        Role.MEASURE, 
        meas_concept, 
        required=True
    )
    component_list.append(meas_comp)

    # 4. Process Attributes (Optional Codelist)
    # For this implementation, we will infer Codelists for attributes if they appear to be categorical (string)
    # However, to be safe and robust, we often allow attributes to be coded if they are strings.
    for col in attributes:
        dtype = _infer_sdmx_type(dataframe[col].dtype)
        concept = _create_concept(col, dtype)
        
        # Heuristic: If string type, create a Codelist. 
        # If numeric, leave as uncoded value (or user would need to specify).
        # We will assume string attributes are coded for consistency with Dimensions in this context.
        attr_codelist = None
        if dtype == DataType.STRING:
            attr_codelist = _create_codelist_from_series(dataframe[col], col, agency_id, version)

        comp = _create_component(
            col, 
            Role.ATTRIBUTE, 
            concept, 
            required=False, 
            attachment_level="O", # Default to Observation level
            codelist=attr_codelist
        )
        component_list.append(comp)

    # Construct the Components container
    components_obj = Components(component_list)

    return Schema(
        context="dataflow",
        agency=agency_id,
        id=schema_id,
        version=version,
        components=components_obj,
        generated=datetime.now(timezone.utc),
        name=f"Auto-generated schema for {schema_id}",
    )
# endregion

# region build_schema_from_wb_template
@typechecked
def _parse_info_sheet(sheets: dict[str, pd.DataFrame], sheet_name: str = "INFO") -> pd.DataFrame:
    """Parses the INFO sheet from a dictionary of DataFrames, extracting key-value metadata.

    This function extracts a specific DataFrame from the provided dictionary. It handles arbitrary
    layouts by treating headers as potential data, unless the headers appear to be auto-generated
    (RangeIndex). It filters for rows containing exactly two non-empty values.

    Args:
        sheets (dict[str, pd.DataFrame]): Dictionary containing DataFrames, typically from pd.read_excel.
        sheet_name (str): Name of the sheet to parse. Defaults to "INFO".

    Returns:
        pd.DataFrame: A DataFrame with columns ['Key', 'Value'] containing the extracted metadata.

    Raises:
        ValueError: If the specified sheet_name is not found in the dictionary.
    """
    if sheet_name not in sheets:
        raise ValueError(f"Sheet '{sheet_name}' not found in the provided dictionary.")

    df = sheets[sheet_name]

    # Normalize data extraction:
    # 1. If columns are a RangeIndex (0, 1, 2...), they are likely auto-generated by pandas
    #    (e.g., created via pd.DataFrame() without columns) and should be ignored.
    # 2. Otherwise, we treat columns as the first row of data, which covers cases where
    #    pd.read_excel(header=0) consumes the first row of actual metadata as the header.
    if isinstance(df.columns, pd.RangeIndex):
        all_rows = df.values.tolist()
    else:
        all_rows = [df.columns.tolist()] + df.values.tolist()
    
    cleaned_rows: list[list[str]] = []

    for row in all_rows:
        valid_cells = []
        for cell in row:
            # Basic validation: check for NaN/None
            if pd.isna(cell):
                continue
            
            s_cell = str(cell).strip()
            
            # Check for empty strings, 'nan' string literals, and pandas 'Unnamed' artifacts
            if (s_cell == "" or 
                s_cell.lower() == "nan" or 
                s_cell.startswith("Unnamed:")):
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
def _parse_comp_mapping_sheet(sheets: dict[str, pd.DataFrame], sheet_name: str = "COMP_MAPPING") -> pd.DataFrame:
    """Parses the COMP_MAPPING sheet, validating strict structure conformance.

    This function expects the sheet to contain specific headers: 'SOURCE', 'TARGET', 
    and 'MAPPING_RULES'. It extracts these columns, removes completely empty rows, 
    and returns the resulting DataFrame. Rows with partial data (e.g., missing 'SOURCE') 
    are preserved as they are valid mapping rules.

    Args:
        sheets (dict[str, pd.DataFrame]): Dictionary containing DataFrames, typically from pd.read_excel.
        sheet_name (str): Name of the sheet to parse. Defaults to "COMP_MAPPING".

    Returns:
        pd.DataFrame: A DataFrame containing 'SOURCE', 'TARGET', and 'MAPPING_RULES' columns.

    Raises:
        ValueError: If the sheet is missing or does not contain the required columns.
    """
    if sheet_name not in sheets:
        raise ValueError(f"Sheet '{sheet_name}' not found in the provided dictionary.")

    df = sheets[sheet_name]

    expected_columns = ["SOURCE", "TARGET", "MAPPING_RULES"]
    
    # Validate that required columns exist
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Sheet '{sheet_name}' is missing required columns: {missing_columns}. "
            f"Found: {df.columns.tolist()}"
        )

    # Extract only the relevant columns to ignore potential artifacts (e.g., 'Unnamed: X')
    result_df = df[expected_columns]

    # Remove rows where ALL columns are NaN/None (empty rows)
    # We do not use how='any' because some mapping rules might have an empty SOURCE
    result_df.dropna(how="all", inplace=True)

    return result_df

@typechecked
def _parse_rep_mapping_sheet(
    sheets: dict[str, pd.DataFrame], 
    sheet_name: str = "REP_MAPPING"
) -> dict[str, pd.DataFrame]:
    """Parses the REP_MAPPING sheet to split columns into Source and Target DataFrames.

    The function expects column headers to be prefixed with "S:" for source columns and 
    "T:" for target columns. Columns without these prefixes are ignored. The prefixes 
    are removed in the output DataFrames.

    Args:
        sheets (dict[str, pd.DataFrame]): Dictionary containing DataFrames.
        sheet_name (str): Name of the sheet to parse. Defaults to "REP_MAPPING".

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing (source_df, target_df).

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
        raise ValueError(f"No source columns (prefixed with 'S:') found in '{sheet_name}'.")
    
    if not target_cols:
        raise ValueError(f"No target columns (prefixed with 'T:') found in '{sheet_name}'.")

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
    structure_type: Literal["dataflow", "dsd", "provision-agreement"]
) -> str:
    """Extracts the SDMX ID for a specific structure type from the parsed INFO DataFrame.

    This function searches the provided DataFrame (output of `_parse_info_sheet`) for specific
    keys corresponding to the requested structure type. It handles standard SDMX reference 
    formats like 'Agency:ID(Version)' by parsing out just the 'ID' component.

    Args:
        info_df (pd.DataFrame): DataFrame containing metadata with 'Key' and 'Value' columns.
        structure_type (str): The type of artefact to extract. Must be one of:
                              'dataflow', 'dsd', 'provision-agreement'.

    Returns:
        str: The extracted SDMX ID.

    Raises:
        ValueError: If the `structure_type` is invalid, the key is not found, 
                    or the value is empty/null.
    """
    # Map friendly structure types to the actual keys found in the Excel/CSV
    # We use a case-insensitive match strategy in logic, but these are the expected targets.
    # Based on the file snippets: 
    # 'dsd' -> 'datastructure'
    # 'dataflow' -> 'dataflow'
    # 'provision-agreement' -> 'provisionagreement'
    type_map = {
        "dataflow": "dataflow",
        "dsd": "datastructure",
        "provision-agreement": "provisionagreement"
    }

    if structure_type not in type_map:
        raise ValueError(f"Invalid structure_type '{structure_type}'. Must be one of {list(type_map.keys())}.")

    target_key = type_map[structure_type]
    
    # Perform case-insensitive search for the key
    # Create a mask for matching keys
    mask = info_df["Key"].astype(str).str.strip().str.lower() == target_key.lower()
    
    if not mask.any():
        raise ValueError(f"Could not find metadata key '{target_key}' for structure type '{structure_type}'.")

    # Get the value associated with the key
    raw_value = info_df.loc[mask, "Value"].iloc[0]

    # Validate value is present and not empty/nan
    if pd.isna(raw_value) or str(raw_value).strip() == "":
        raise ValueError(f"Metadata for '{target_key}' is present but empty.")

    artefact_id = str(raw_value).strip()

    return artefact_id

@typechecked
def _match_column_name(target_name: str, available_columns: List[str]) -> str:
    """Matches a business name from COMP_MAPPING to the cleaned column names in REP_MAPPING.

    This handles discrepancies like 'Series code' (business name) vs 'Series' (Excel header).

    Args:
        target_name (str): The name to look for (e.g., 'Series code').
        available_columns (List[str]): The list of available column headers.

    Returns:
        str: The matching column name.

    Raises:
        ValueError: If no suitable match is found.
    """
    # 1. Exact match
    if target_name in available_columns:
        return target_name

    # 2. Normalized match (ignore case, spaces, underscores)
    norm_target = target_name.replace(" ", "").replace("_", "").lower()
    
    for col in available_columns:
        norm_col = col.replace(" ", "").replace("_", "").lower()
        # Check for containment (e.g., 'Series' in 'SeriesCode')
        if norm_col == norm_target or norm_col in norm_target or norm_target in norm_col:
            return col

    raise ValueError(f"Could not find a column in REP_MAPPING matching '{target_name}'. Available: {available_columns}")

# Region: Main Function

@typechecked
def build_structure_map_from_template_wb(
    mappings: Dict[str, pd.DataFrame],
    default_agency: str = "SDMX",
    default_structure_map_id: str = "WB_STRUCTURE_MAP"
) -> StructureMap:
    """Builds a complete StructureMap object by parsing a WB-format Excel template.
    
    Process:
    1.  Parses `INFO` sheet to extract Agency and Version context using `_extract_artefact_id`.
    2.  Parses `COMP_MAPPING` to determine mapping rules.
    3.  Parses `REP_MAPPING` using `_parse_rep_mapping_sheet` to get source/target data.
    4.  Constructs the specific SDMX Map objects (Fixed, Implicit, Component).

    Args:
        mappings (Dict[str, pd.DataFrame]): The dictionary of DataFrames containing all sheets.
        default_agency (str): Fallback agency ID if not found in INFO.
        default_structure_map_id (str): ID for the resulting StructureMap.

    Returns:
        StructureMap: A valid pysdmx StructureMap object.

    Raises:
        ValueError: If mandatory sheets/columns are missing or mapping rules are invalid.
    """
    # 1. Extract Metadata (Agency & Version)
    current_agency = default_agency
    current_version = "1.0"
    artefact_ref = None
    
    if "INFO" in mappings:
        try:
            info_df = _parse_info_sheet(mappings)
            
            # Try to find a defining artefact ID (Dataflow takes precedence)
            for type_key in ["dataflow", "dsd"]:
                try:
                    artefact_ref = _extract_artefact_id(info_df, type_key)
                    break 
                except ValueError:
                    continue
            
            if artefact_ref:
                # Use the helper to parse "Agency:ID(Version)"
                parsed_agency, _, parsed_version = parse_artefact_id(artefact_ref)
                current_agency = parsed_agency
                current_version = parsed_version
            elif "FMR_AGENCY" in info_df["Key"].values:
                 # Fallback to specific key if standard artefact not found
                 val = info_df.loc[info_df["Key"] == "FMR_AGENCY", "Value"].iloc[0]
                 if val:
                     current_agency = str(val).strip()

        except Exception:
            # Metadata parsing failed, proceeding with defaults.
            pass

    # 3. Parse Component Mappings Rules
    comp_df = _parse_comp_mapping_sheet(mappings)
    
    # 4. Prepare Representation Data
    rep_data: Dict[str, pd.DataFrame] = {}
    if "REP_MAPPING" in mappings:
        try:
            rep_data = _parse_rep_mapping_sheet(mappings)
        except ValueError:
             # Ignore if REP_MAPPING is present but structurally invalid, validation will fail 
             # only if a component actually attempts to use it.
             pass

    generated_maps: List[Union[FixedValueMap, ImplicitComponentMap, ComponentMap]] = []

    # 5. Iterate Logic
    for _, row in comp_df.iterrows():
        source_id = str(row["SOURCE"]).strip()
        target_id = str(row["TARGET"]).strip()
        rule = str(row["MAPPING_RULES"]).strip()

        # Skip empty rules or pandas artifacts
        if not target_id or not rule or rule.lower() == "nan":
            continue

        try:
            # --- Rule 1: Fixed Value (MAPPING_RULES starts with "fixed:") ---
            if rule.lower().startswith("fixed:"):
                parts = rule.split(":", 1)
                if len(parts) < 2 or not parts[1].strip():
                    raise ValueError(f"Invalid fixed rule format: {rule}")
                fixed_val = parts[1].strip()
                generated_maps.append(build_fixed_map(target_id, fixed_val))

            # --- Rule 2: Implicit (MAPPING_RULES is "implicit") ---
            elif rule.lower() == "implicit":
                if not source_id:
                     raise ValueError("Implicit map rule requires a non-empty 'SOURCE' component ID.")
                generated_maps.append(build_implicit_component_map(source_id, target_id))

            # --- Rule 3: Representation Map (MAPPING_RULES matches TARGET ID) ---
            elif rule == target_id:
                if not rep_data or rep_data["source"].empty or rep_data["target"].empty:
                    raise ValueError("Mapping rule requires 'REP_MAPPING' sheet with data, but it was invalid or empty.")
                if not source_id:
                     raise ValueError("Representation map rule requires a non-empty 'SOURCE' component ID.")
                
                source_dfs_map = rep_data["source"]
                target_dfs_map = rep_data["target"]

                # Resolve columns using fuzzy matching on the stripped headers
                actual_source_col = _match_column_name(source_id, source_dfs_map.columns.tolist())
                actual_target_col = _match_column_name(target_id, target_dfs_map.columns.tolist())

                # Combine into a single DF for the builder function, aligning on index
                combined_df = pd.DataFrame({
                    "source": source_dfs_map[actual_source_col],
                    "target": target_dfs_map[actual_target_col]
                })

                combined_df.dropna(subset=["source", "target"], how='any', inplace=True)
                combined_df.drop_duplicates(inplace=True)

                if combined_df.empty:
                    raise ValueError(f"No valid mapping rows found between source column '{actual_source_col}' and target column '{actual_target_col}'.")

                comp_map = build_single_component_map(
                    df=combined_df,
                    source_component=source_id,
                    target_component=target_id,
                    agency=current_agency,
                    id=f"MAP_{target_id}",
                    name=f"Mapping for {target_id}",
                    source_col="source",
                    target_col="target",
                    version=current_version
                )
                generated_maps.append(comp_map)
            
            else:
                 # Catch-all for non-matching strings
                 raise ValueError(f"Unknown mapping rule: '{rule}'")

        except ValueError as e:
            # Propagate detailed error
            raise ValueError(f"Error processing mapping for Target '{target_id}': {str(e)}") from e

    # 6. Construct Final Object
    name_suffix = artefact_ref if artefact_ref else default_structure_map_id
    return StructureMap(
        id=default_structure_map_id,
        agency=current_agency,
        version=current_version,
        name=f"Structure Map generated for {name_suffix}",
        maps=generated_maps
    )
# endregion
