"""Core: fetch schemas from FMR, standardise/map SDMX data."""

import json
import logging
import warnings
from typing import Literal
from urllib.parse import urljoin

import numpy as np
import pandas as pd
from pysdmx.api import fmr
from pysdmx.io.format import StructureFormat
from pysdmx.model import Schema
from typeguard import typechecked

from .qa_utils import qa_coerce_numeric, qa_remove_duplicates
from .utils import extract_component_ids

logger = logging.getLogger(__name__)


# NOTE: The following helpers are not part of the public API and are
# candidates for removal once confirmed unused by downstream consumers.


def _check_dict_keys(dict1: dict, dict2: dict) -> str | None:
    """Check whether the sorted keys of two dictionaries are the same."""
    keys1 = sorted(dict1.keys())
    keys2 = sorted(dict2.keys())
    if keys1 == keys2:
        return None
    diff1 = set(keys1) - set(keys2)
    diff2 = set(keys2) - set(keys1)
    return (
        f"The keys of both dictionaries should be the same.\n"
        f"Keys only in the first dictionary: {diff1}\n"
        f"Keys only in the second dictionary: {diff2}"
    )


def _remove_extension(key: str) -> str:
    """Remove the file extension from a key."""
    return key.rsplit(".", 1)[0]


def _modify_dict_keys(input_dict: dict) -> dict:
    """Create a new dictionary with file extensions removed from keys."""
    return {_remove_extension(key): value for key, value in input_dict.items()}


def _create_keys_dict(input_dict: dict) -> dict[str, str]:
    """Create a mapping from extension-stripped keys to original keys."""
    return {_remove_extension(key): key for key in input_dict}


# Keep old names as aliases for backwards compatibility with existing tests.
check_dict_keys = _check_dict_keys
remove_extension = _remove_extension
modify_dict_keys = _modify_dict_keys
create_keys_dict = _create_keys_dict


def fetch_dsd_schema(fmr_params: dict, env: str, dsd_id: str) -> Schema:
    """Fetch a DSD schema from a Fusion Metadata Registry (FMR).

    .. deprecated::
        Use :func:`fetch_schema` instead.

    Args:
        fmr_params: Base URL and endpoints to access FMR's API.
        env: FMR environment (e.g. ``'sandbox'``, ``'qa'``, ``'dev'``,
            ``'prod'``).
        dsd_id: The DSD identifier in the format ``"agency:id(version)"``.

    Returns:
        The schema of the requested Data Structure Definition.

    Raises:
        ValueError: If the URL is not syntactically valid.
    """
    warnings.warn(
        "fetch_dsd_schema is deprecated and will be removed in a future release. "
        "Please use fetch_schema instead.",
        FutureWarning,
        stacklevel=2,
    )

    structure_format = StructureFormat.FUSION_JSON
    fmr_url = fmr_params[env]["url"]
    base_url = urljoin(fmr_url, "/FMR/sdmx/v2/")

    client = fmr.RegistryClient(base_url, format=structure_format)

    agency, id_part, version = parse_dsd_id(dsd_id)
    return client.get_schema("datastructure", agency, id_part, version)


@typechecked
def fetch_schema(
    base_url: str,
    artefact_id: str,
    context: Literal["dataflow", "datastructure", "provisionagreement"],
) -> Schema:
    """Fetch the schema of a specified artefact from an SDMX registry.

    Args:
        base_url: The base URL of the FMR.
        artefact_id: The identifier of the artefact, typically in the format
            ``"agency:id(version)"``.
        context: The context of the artefact to fetch.

    Returns:
        The fetched schema object.
    """
    structure_format = StructureFormat.FUSION_JSON
    base_url = urljoin(base_url, "/FMR/sdmx/v2/")
    client = fmr.RegistryClient(base_url, format=structure_format)

    agency, id_part, version = parse_artefact_id(artefact_id)
    return client.get_schema(context, agency, id_part, version)


@typechecked
def parse_dsd_id(dsd_id: str) -> tuple[str, str, str]:
    """Parse a DSD identifier into its components.

    .. deprecated::
        Use :func:`parse_artefact_id` instead.

    Args:
        dsd_id: The DSD identifier in the format ``"agency:id(version)"``.

    Returns:
        A tuple containing the agency, id, and version.

    Raises:
        ValueError: If the dsd_id is not in the expected format.
    """
    warnings.warn(
        "parse_dsd_id is deprecated and will be removed in a future release. "
        "Please use parse_artefact_id instead.",
        FutureWarning,
        stacklevel=2,
    )

    try:
        agency, rest = dsd_id.split(":", 1)
        id_part, version_part = rest.split("(", 1)
        version = version_part.rstrip(")")
        return agency, id_part, version
    except (ValueError, AttributeError) as err:
        raise ValueError(
            "Invalid dsd_id format. Expected format: 'agency:id(version)'"
        ) from err


@typechecked
def parse_artefact_id(artefact_id: str) -> tuple[str, str, str]:
    """Parse an artefact identifier into its components: agency, id and version.

    Args:
        artefact_id: The identifier of the artefact, typically in the format
            ``"agency:id(version)"``.

    Returns:
        A tuple containing the agency, id, and version.

    Raises:
        ValueError: If the artefact_id is not in the expected format.
    """
    try:
        agency, rest = artefact_id.split(":", 1)
        id_part, version_part = rest.split("(", 1)
        version = version_part.rstrip(")")
        return agency, id_part, version
    except (ValueError, AttributeError) as err:
        raise ValueError(
            "Invalid artefact_id format. Expected format: 'agency:id(version)'"
        ) from err


@typechecked
def standardize_sdmx(
    df: pd.DataFrame,
    mapping: dict,
    cat_indicator: bool = False,
) -> pd.DataFrame:
    """Standardize a DataFrame by applying column and value transformations.

    Args:
        df: The input DataFrame with raw data.
        mapping: A dictionary containing the mapping DataFrame and other
            relevant information.
        cat_indicator: Whether OBS_VALUE is a categorical indicator.
            Default is False.

    Returns:
        The standardized DataFrame with columns transformed according to
        the mapping.
    """
    df = transform_source_to_target(df, mapping)
    df = map_to_sdmx(df, mapping)
    df = standardize_data_for_upload(
        df, dsd=mapping["dsd_id"], cat_indicator=cat_indicator
    )
    return df


@typechecked
def transform_source_to_target(
    df: pd.DataFrame,
    mapping: dict,
) -> pd.DataFrame:
    """Transform a raw DataFrame into the format defined by a components map.

    Creates a new DataFrame with columns as defined in
    ``mapping["components"]["TARGET"]`` and populates it with data from the
    source DataFrame based on the column names in ``["SOURCE"]``.

    Args:
        df: The input DataFrame with raw data.
        mapping: The master mapping dictionary containing a mapping between the
            input file columns and the columns defined in the schema.

    Returns:
        The transformed DataFrame with columns as defined in the components
        map's TARGET.

    Raises:
        KeyError: If the mapping does not contain a ``"components"`` key or its
            value is empty.
    """
    try:
        components_map = mapping["components"]

        if isinstance(components_map, list):
            components_map = pd.DataFrame(components_map)

        result_df = pd.DataFrame(columns=components_map["TARGET"].values)

        for _, row in components_map.iterrows():
            source_col = row["SOURCE"]
            target_col = row["TARGET"]

            if source_col in df.columns:
                result_df[target_col] = df[source_col]

        return result_df

    except KeyError as e:
        raise KeyError(
            "The mapping file should contain 'components' key or its value "
            "should not be empty. Please make sure the mapping file has this "
            "key and its value is not empty."
        ) from e


@typechecked
def vectorized_lookup_ordered_v1(
    series: pd.Series, mapping_df: pd.DataFrame
) -> pd.Series:
    """Apply ordered regex matching to a Pandas Series.

    For each regex pattern in mapping_df, check if the value in series matches
    the pattern. The corresponding TARGET is assigned when a match is found,
    and later rules are skipped. Any cell that does not match any pattern
    retains its original value.

    Args:
        series: The input data series (e.g., a DataFrame column).
        mapping_df: A DataFrame with at least two columns:

            - ``SOURCE``: regex patterns (ordered by priority)
            - ``TARGET``: corresponding replacement values

    Returns:
        A new series with values replaced according to the first matching
        regex, or the original value if no match is found.
    """
    series_str = series.astype(str)

    if mapping_df.empty:
        return series

    mapping_df = mapping_df.copy()
    mapping_df["SOURCE_LEN"] = mapping_df["SOURCE"].str.len()
    mapping_df = mapping_df.sort_values(by="SOURCE_LEN", ascending=False).drop(
        columns="SOURCE_LEN"
    )

    conditions = []
    choices = []

    for _, row in mapping_df.iterrows():
        conditions.append(series_str.str.contains(row["SOURCE"], regex=True))
        choices.append(row["TARGET"])

    default_value = series_str
    result = np.select(conditions, choices, default=default_value)

    return pd.Series(result, index=series.index)


@typechecked
def vectorized_lookup_ordered_v2(
    series: pd.Series, mapping_df: pd.DataFrame
) -> pd.Series:
    """Apply ordered matching (regex or exact) to a Pandas Series.

    For each row in mapping_df:

        - If ``IS_REGEX`` is True, perform regex matching.
        - If ``IS_REGEX`` is False, perform exact string matching.

    The corresponding TARGET is assigned when a match is found, and later
    rules are skipped. Any cell that does not match retains its original value.

    Args:
        series: The input data series (e.g., a DataFrame column).
        mapping_df: A DataFrame with at least three columns:

            - ``SOURCE``: regex patterns or exact strings (ordered by priority)
            - ``TARGET``: corresponding replacement values
            - ``IS_REGEX``: boolean indicating whether SOURCE is a regex

    Returns:
        A new series with values replaced according to the first matching
        rule, or the original value if no match is found.
    """
    series_str = series.astype(str)

    if mapping_df.empty:
        return series

    mapping_df = mapping_df.copy()
    mapping_df["SOURCE_LEN"] = mapping_df["SOURCE"].str.len()
    mapping_df = mapping_df.sort_values(by="SOURCE_LEN", ascending=False).drop(
        columns="SOURCE_LEN"
    )

    conditions = []
    choices = []

    for _, row in mapping_df.iterrows():
        source = row["SOURCE"]
        is_regex = row["IS_REGEX"]

        if is_regex:
            conditions.append(series_str.str.contains(source, regex=True))
        else:
            conditions.append(series_str == source)

        choices.append(row["TARGET"])

    default_value = series_str
    result = np.select(conditions, choices, default=default_value)

    return pd.Series(result, index=series.index)


@typechecked
def map_to_sdmx(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """Map DataFrame columns to SDMX values using a lookup mapping.

    This function transforms the given DataFrame columns to conform to the
    SDMX representation by applying either a fixed mapping or an ordered,
    regex-based mapping. For each key present in the DataFrame:

        - Fixed Mapping:
            If the mapping for a key contains a ``TARGET`` column but no
            ``SOURCE`` column, the entire column is replaced with the fixed
            value provided by ``TARGET``.

        - Regex-based Mapping:
            If the mapping for a key contains both ``SOURCE`` and ``TARGET``
            columns, the function applies ordered regex-based matching using
            a first-match-wins strategy.

    Args:
        df: The input DataFrame containing the data to be mapped.
        mapping: The lookup mapping as a dict. Each key represents an SDMX
            component and its value is expected to be either a list of
            dictionaries (with keys ``SOURCE`` and ``TARGET``) or a DataFrame
            with those columns.

    Returns:
        The transformed DataFrame with mapped column values.

    Raises:
        ValueError: If the schema version is unsupported.
    """
    df = df.copy()
    schema_version = mapping["schema_version"]
    representation_mapping = mapping.get("representation", {})
    total_items = len(representation_mapping)

    for index, (key, mapping_value) in enumerate(
        representation_mapping.items(), start=1
    ):
        logger.info("Processing %d/%d: %s", index, total_items, key)

        if not mapping_value:
            logger.debug("Skipping '%s' because mapping is empty", key)
            continue

        if key not in df.columns:
            logger.debug("Skipping '%s' because column not in DataFrame", key)
            continue

        if not isinstance(mapping_value, pd.DataFrame):
            mapping_value = pd.DataFrame(mapping_value)

        # Fixed mapping: no SOURCE column
        if "TARGET" in mapping_value.columns and "SOURCE" not in mapping_value.columns:
            df[key] = mapping_value["TARGET"].iloc[0]

        # Regex / ordered lookup mapping: both SOURCE and TARGET exist
        elif "SOURCE" in mapping_value.columns and "TARGET" in mapping_value.columns:
            if schema_version == "v1":
                df[key] = vectorized_lookup_ordered_v1(df[key], mapping_value)
            elif schema_version == "v2":
                df[key] = vectorized_lookup_ordered_v2(df[key], mapping_value)
            else:
                raise ValueError(f"Unsupported schema version: {schema_version}")

        else:
            logger.warning(
                "Skipping '%s': invalid mapping structure "
                "(expected SOURCE and TARGET columns)",
                key,
            )

    return df


@typechecked
def add_sdmx_reference_cols(
    df: pd.DataFrame,
    dsd: str,
    structure: str = "datastructure",
    action: str = "I",
) -> pd.DataFrame:
    """Add SDMX reference columns to a DataFrame.

    .. deprecated::
        Use :func:`standardize_output` instead.

    Args:
        df: The input DataFrame to which the columns will be added.
        dsd: The Data Structure Definition (DSD) identifier.
        structure: The structure type. Default is ``'datastructure'``.
        action: The action type. Default is ``'I'`` (Insert).

    Returns:
        The DataFrame with the added SDMX reference columns.
    """
    warnings.warn(
        "add_sdmx_reference_cols is deprecated and will be removed "
        "in a future release. Please use standardize_output instead.",
        FutureWarning,
        stacklevel=2,
    )
    df["STRUCTURE"] = structure
    df["STRUCTURE_ID"] = dsd
    df["ACTION"] = action

    return df


@typechecked
def standardize_indicator_id(df: pd.DataFrame) -> pd.DataFrame:
    """Fix the INDICATOR column to be uppercase and prefixed with dataset ID.

    Ensures all values in the ``INDICATOR`` column are upper case, prefixed
    with the dataset identifier, and have dots replaced with underscores.

    Args:
        df: The DataFrame to modify. Must contain an ``INDICATOR`` column
            and either a ``DATABASE_ID`` or ``DATASET_ID`` column.

    Returns:
        The modified DataFrame with corrected INDICATOR values.

    Raises:
        ValueError: If the database/dataset ID column contains more than
            one unique value.

    Examples:
        >>> df = pd.DataFrame({
        ...     "DATABASE_ID": ["WB.DATA360", "WB.DATA360"],
        ...     "INDICATOR": ["indicator.one", "indicator.two"],
        ... })
        >>> result = standardize_indicator_id(df)
        >>> list(result["INDICATOR"])
        ['WB_DATA360_INDICATOR_ONE', 'WB_DATA360_INDICATOR_TWO']
    """
    id_column = None
    for col in ["DATABASE_ID", "DATASET_ID"]:
        if col in df.columns:
            id_column = col
            break

    df = df.copy()
    dataset_id = df[id_column].unique()
    if len(dataset_id) != 1:
        raise ValueError(
            f"The '{id_column}' column has {len(dataset_id)} unique values. "
            "Expected exactly 1 unique value."
        )
    dataset_id = str(dataset_id[0])

    df["INDICATOR"] = df["INDICATOR"].astype(str)

    if not df["INDICATOR"].str.startswith(dataset_id).all():
        df["INDICATOR"] = dataset_id + "_" + df["INDICATOR"]
    if not df["INDICATOR"].str.isupper().all():
        df["INDICATOR"] = df["INDICATOR"].str.upper()
    df["INDICATOR"] = df["INDICATOR"].str.replace(r"\.+", "_", regex=True)

    return df


@typechecked
def standardize_data_for_upload(
    df: pd.DataFrame,
    dsd: str,
    structure: str = "datastructure",
    action: str = "I",
    cat_indicator: bool = False,
) -> pd.DataFrame:
    """Standardize a DataFrame for SDMX upload.

    .. deprecated::
        Use :func:`standardize_output` instead.

    Finalizes the DataFrame for upload by fixing INDICATOR values, adding
    reference columns, and reordering columns.

    Args:
        df: The input DataFrame to modify.
        dsd: The Data Structure Definition (DSD) identifier.
        structure: The structure type. Default is ``'datastructure'``.
            Options: ``'datastructure'``, ``'metadataflow'``, ``'dataflow'``.
        action: The action type. Default is ``'I'`` (Insert).
            Options: ``'I'``, ``'U'``, ``'D'``.
        cat_indicator: Whether OBS_VALUE is a categorical indicator.
            Default is False.

    Returns:
        The modified DataFrame with corrected INDICATOR values, added
        reference columns, and reordered columns.
    """
    warnings.warn(
        "standardize_data_for_upload is deprecated and will be removed "
        "in a future release. Please use standardize_output instead.",
        FutureWarning,
        stacklevel=2,
    )

    if not cat_indicator:
        df = qa_coerce_numeric(df, numeric_columns=["OBS_VALUE"])
    df = qa_remove_duplicates(df)

    df = standardize_indicator_id(df=df)
    df = add_sdmx_reference_cols(df=df, dsd=dsd, structure=structure, action=action)

    cols_to_move = ["STRUCTURE", "STRUCTURE_ID", "ACTION"]
    new_order = cols_to_move + [col for col in df.columns if col not in cols_to_move]
    df = df[new_order]

    return df


@typechecked
def standardize_output(
    df: pd.DataFrame,
    artefact_id: str,
    schema: Schema,
    action: Literal["I", "U", "D"] = "I",
) -> pd.DataFrame:
    """Standardize the output DataFrame by adding SDMX reference columns.

    Enriches the given DataFrame with SDMX-related metadata columns
    (``STRUCTURE``, ``STRUCTURE_ID``, ``ACTION``) based on the provided
    artefact ID and schema, then ensures these columns appear first.

    Args:
        df: Input DataFrame containing SDMX data.
        artefact_id: Unique identifier of the SDMX artefact (e.g.,
            Dataflow ID).
        schema: A pysdmx Schema object used to determine artefact type
            and filter columns.
        action: Action indicator for SDMX operations. Defaults to ``"I"``.
            Allowed values: ``"I"`` (Insert), ``"U"`` (Update),
            ``"D"`` (Delete).

    Returns:
        A new DataFrame with SDMX reference columns added and reordered.

    Raises:
        ValueError: If ``df`` is empty or ``artefact_id``/``schema`` is empty.
        TypeError: If ``df`` is not a pandas DataFrame.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input `df` must be a pandas DataFrame.")
    if df.empty:
        raise ValueError("Input DataFrame `df` cannot be empty.")
    if not artefact_id or not schema:
        raise ValueError("Parameters `artefact_id` and `schema` cannot be empty.")

    artefact_type = _extract_artefact_type(schema)

    components_to_keep = extract_component_ids(schema)
    df = df[[col for col in components_to_keep if col in df.columns]]

    df = _add_sdmx_reference_cols(
        df=df,
        artefact_id=artefact_id,
        artefact_type=artefact_type,
        action=action,
    )

    if artefact_type == "dataflow":
        cols_to_move = ["DATAFLOW", "DATAFLOW_ID", "ACTION"]
    elif artefact_type == "datastructure":
        cols_to_move = ["STRUCTURE", "STRUCTURE_ID", "ACTION"]
    else:
        cols_to_move = [
            "PROVISIONAGREEMENT",
            "PROVISION_AGREEMENT_ID",
            "ACTION",
        ]
    new_order = cols_to_move + [col for col in df.columns if col not in cols_to_move]
    df = df[new_order]

    return df


@typechecked
def _extract_artefact_type(
    schema: Schema,
) -> Literal["dataflow", "datastructure", "provisionagreement"]:
    """Extract the SDMX artefact type from a pysdmx Schema instance.

    Args:
        schema: A pysdmx Schema object representing allowed content within
            a context.

    Returns:
        The artefact type for which the schema applies.

    Raises:
        ValueError: If the schema context is not one of the expected values.

    Examples:
        >>> from pysdmx.model.dataflow import Schema, Components
        >>> from datetime import datetime, timezone
        >>> comps = Components([])
        >>> s = Schema("dataflow", "ECB", "EXR", comps, "1.0", [],
        ...            generated=datetime.now(timezone.utc))
        >>> _extract_artefact_type(s)
        'dataflow'
    """
    valid_contexts = {"dataflow", "datastructure", "provisionagreement"}
    if schema.context not in valid_contexts:
        raise ValueError(
            f"Invalid schema context '{schema.context}'. "
            f"Must be one of {valid_contexts}."
        )
    return schema.context


@typechecked
def _add_sdmx_reference_cols(
    df: pd.DataFrame,
    artefact_id: str,
    artefact_type: Literal["dataflow", "datastructure", "provisionagreement"],
    action: Literal["I", "U", "D"] = "I",
) -> pd.DataFrame:
    """Add SDMX reference columns to a DataFrame based on artefact type.

    Args:
        df: Input DataFrame.
        artefact_id: Identifier for the SDMX artefact.
        artefact_type: Artefact type.
        action: Action type. Defaults to ``"I"``.

    Returns:
        DataFrame with added SDMX reference columns.

    Raises:
        TypeError: If df is not a pandas DataFrame.

    Examples:
        >>> import pandas as pd
        >>> df = pd.DataFrame({"OBS_VALUE": [100, 200]})
        >>> result = _add_sdmx_reference_cols(df, "DF_EXAMPLE", "dataflow", "I")
        >>> print(result.columns)
        Index(['OBS_VALUE', 'DATAFLOW', 'DATAFLOW_ID', 'ACTION'], dtype='object')
    """
    df = df.copy()

    if artefact_type == "dataflow":
        structure_col = "DATAFLOW"
        structure_id_col = "DATAFLOW_ID"
    elif artefact_type == "datastructure":
        structure_col = "STRUCTURE"
        structure_id_col = "STRUCTURE_ID"
    else:
        structure_col = "PROVISIONAGREEMENT"
        structure_id_col = "PROVISION_AGREEMENT_ID"

    df.loc[:, structure_col] = artefact_type
    df.loc[:, structure_id_col] = artefact_id
    df.loc[:, "ACTION"] = action

    return df


# region Functions to handle mapping files


@typechecked
def read_mapping(path: str) -> dict:
    """Read a JSON mapping file and parse its content into DataFrames.

    The function processes JSON data with four main keys:

        1. ``schema_version``: The version of the mapping schema.
        2. ``dsd_id``: The Data Structure Definition ID.
        3. ``components``: A flat structure converted into a DataFrame.
        4. ``representation``: Multiple sub-keys, each converted into a
           separate DataFrame. Empty sub-keys are skipped.

    All occurrences of the string ``"NA"`` are converted to ``pd.NA``.

    Args:
        path: The file path to the JSON file to be parsed.

    Returns:
        A dictionary where:

            - ``schema_version`` is stored under key ``'schema_version'``.
            - ``dsd_id`` is stored under key ``'dsd_id'``.
            - The components DataFrame is stored under key ``'components'``.
            - Each valid representation sub-key is stored as a DataFrame
              under its corresponding key.

    Raises:
        ValueError: If required keys are missing or have unexpected formats.
    """
    with open(path) as file:
        data = json.load(file)

    result = {}

    schema_version = data.get("schema_version")
    if schema_version:
        result["schema_version"] = schema_version
    else:
        raise ValueError("Missing 'schema_version' key in JSON mapping file")

    dsd_id = data.get("dsd_id")
    if dsd_id:
        result["dsd_id"] = dsd_id
    else:
        raise ValueError("Missing 'dsd_id' key in JSON mapping file")

    components_data = data.get("components")
    if components_data:
        result["components"] = pd.DataFrame(components_data).replace("NA", pd.NA)
    else:
        raise ValueError("Missing 'components' key in JSON mapping file")

    representation_data = data.get("representation")
    if representation_data and isinstance(representation_data, dict):
        for sub_key, sub_value in representation_data.items():
            if isinstance(sub_value, list):
                result[sub_key] = pd.DataFrame(sub_value).replace("NA", pd.NA)
            elif not sub_value:
                continue
            else:
                raise ValueError(
                    f"Unexpected data format for representation sub-key: {sub_key}"
                )
    else:
        raise ValueError("Missing or invalid 'representation' key in JSON mapping file")

    return result


# endregion
