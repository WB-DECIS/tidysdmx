"""Kedro pipeline node wrappers for SDMX standardisation and validation."""

import logging

import pandas as pd
from pysdmx.model.dataflow import Schema

from .tidysdmx import (
    check_dict_keys,
    create_keys_dict,
    modify_dict_keys,
    read_mapping,
    standardize_sdmx,
)
from .utils import extract_validation_info
from .validation import validate_dataset_local

logger = logging.getLogger(__name__)


def kd_read_mappings(mapping_files: dict) -> dict:
    """Fetch multiple mappings from different files.

    Args:
        mapping_files: A dictionary where keys are dataset-specific keys
            and values are file paths to the mapping files.

    Returns:
        A dictionary where keys are dataset-specific keys and values
        are the mappings.
    """
    mappings = {}

    for dataset_key, file_path in mapping_files.items():
        mappings[dataset_key] = read_mapping(file_path)

    return mappings


def kd_standardize_sdmx(
    data: dict,
    mappings: dict,
    boolean: bool = True,
) -> dict:
    """Standardize a partitioned dataset into SDMX format.

    Applies transform_source_to_target to each input dataframe with its
    corresponding mapping.

    Args:
        data: A dictionary where keys are dataset-specific keys and values
            are input DataFrames.
        mappings: A dictionary where keys are dataset-specific keys and
            values are mapping DataFrames.
        boolean: A flag to force order execution in Kedro.

    Returns:
        A dictionary where keys are dataset-specific keys and values are
        transformed DataFrames.
    """
    # CASE 1: Single mapping file
    ## subcase 1.a: single mapping received as a dict of the mappings
    if len(mappings) == 1:
        single_mapping = next(iter(mappings.values()))
        data = standardize_sdmx(data, single_mapping)

    ## subcase 1.b: single mapping received directly (no higher level dict)
    elif "components" in mappings:
        single_mapping = mappings
        data = standardize_sdmx(data, single_mapping)

    # CASE 2: Multiple mapping files
    else:
        # Remove potential file extension from the keys
        # But keep track of the old keys
        bckup_keys = create_keys_dict(data)
        data = modify_dict_keys(data)

        # Ensure that the keys are the same for data and mappings dict
        check_dict_keys(data, mappings)

        partitioned_dataset = {}

        for key in mappings:
            if key in data:
                partition_data = data[key]()
                partition_mapping = mappings[key]
                partition_data = standardize_sdmx(partition_data, partition_mapping)
                partitioned_dataset[bckup_keys[key]] = partition_data

        # Combine all elements into a single dataframe
        data = pd.concat(partitioned_dataset.values(), ignore_index=True)

    return data


def kd_validate_dataset_local(
    df: pd.DataFrame,
    schema: Schema | None = None,
    valid: dict[str, object] | None = None,
) -> tuple[bool, dict]:
    """Validate a single DataFrame for SDMX compliance.

    Wrapper that calls validate_dataset_local to obtain a DataFrame of errors,
    then logs messages and returns a tuple of (success, errors).

    Args:
        df: The DataFrame to be validated.
        schema: The schema object containing validation information
            (optional if ``valid`` is provided).
        valid: Precomputed validation information (optional).

    Returns:
        A tuple where the first element is True if the dataset passed
        validation (no errors) and False otherwise, and the second element
        is an empty dict on success or a dict with key ``ValidationReport``
        mapping to the list of error messages.
    """
    errors_df = validate_dataset_local(df, schema=schema, valid=valid)

    if not errors_df.empty:
        logger.warning(
            "Validation finished with errors. "
            "JSON report will be exported to working directory."
        )
        error_list = errors_df["Error"].tolist()
        return False, {"ValidationReport": error_list}

    logger.info("Validation complete — no errors.")
    return True, {}


def kd_validate_datasets_local(
    datasets: dict,
    schema: Schema,
    boolean: bool,
) -> tuple[dict, dict]:
    """Validate multiple datasets for SDMX compliance.

    Ensures each dataset has ``STRUCTURE``, ``STRUCTURE_ID``, and ``ACTION``
    columns. See the `SDMX-CSV field guide
    <https://github.com/sdmx-twg/sdmx-csv/blob/master/data-message/docs/sdmx-csv-field-guide.md>`__
    for more details.

    Args:
        datasets: Dictionary of datasets to be validated.
        schema: Schema object containing validation information.
        boolean: A flag to force order execution in Kedro.

    Returns:
        A tuple of two dictionaries. The first maps each key to True/False,
        and the second maps each key to its error dictionary.
    """
    valid = extract_validation_info(schema)

    if boolean:
        logger.info("Validating files against DSD...")

        validated = {}
        error = {}
        for key in datasets:
            logger.info("Validating %s", key)
            temp_df = datasets[key]()
            temp_validated, temp_error = kd_validate_dataset_local(
                df=temp_df, valid=valid
            )
            validated[key] = temp_validated
            error[key] = temp_error

        return validated, error

    return {}, {}
