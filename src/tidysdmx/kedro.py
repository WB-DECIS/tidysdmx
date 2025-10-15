def kd_read_mappings(mapping_files):
    """
    Fetch multiple mappings from different files.

    Args:
        mapping_files (dict): A dictionary where keys are dataset specific keys and values are file paths to the mapping files.

    Returns:
        dict: A dictionary where the highest level keys are the dataset specific keys and values are the mappings.
    """
    mappings = {}

    for dataset_key, file_path in mapping_files.items():
        mappings[dataset_key] = read_mapping(file_path)

    return mappings

def kd_standardize_sdmx(data, mappings, boolean=True):
    """Standardize into SDMX format a partitioned dataset.
    
    Creates a partitioned dataset by applying transform_source_to_target to each input dataframe with its corresponding mapping.

    Args:
        mappings (dict): A dictionary where keys are dataset specific keys and values are mapping DataFrames.
        data (dict): A dictionary where keys are dataset specific keys and values are input DataFrames.
        boolean (bool): A boolean flag to force order execution in Kedro.

    Returns:
        dict: A dictionary where keys are dataset specific keys and values are transformed DataFrames.
    """
    # CASE 1: Single mapping file
    ## subcase 1.a: single mapping received as a dict of the mappings
    if len(mappings) == 1:
        # Extract the single element from the dictionaries
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

        # Initialize dictionary that is used to export the partitioned dataset
        partitioned_dataset = {}

        for key in mappings.keys():
            if key in data:
                partition_data = data[key]()
                partition_mapping = mappings[key]
                partition_data = standardize_sdmx(partition_data, partition_mapping)
                partitioned_dataset[bckup_keys[key]] = partition_data

        # Combine all elements of partitioned_dataset into a single dataframe
        data = pd.concat(partitioned_dataset.values(), ignore_index=True)

    # FINAL STEP: Return a partitioned dataset where each partition is an indicator code
    out = partition_formatted_data(data)

    return out

def kd_validate_dataset_local(df, schema=None, valid=None):
    """
    Production validation function for a DataFrame.

    This wrapper calls the interactive validator (validate_dataset_local) to obtain a DataFrame of errors, then logs messages and returns a tuple containing a boolean and an error dictionary, in the same format as the original function.

    Args:
        df (pd.DataFrame): The DataFrame to be validated.
        schema: The schema object containing validation information (optional if 'valid' is provided).
        valid: Precomputed validation information (optional).

    Returns:
        tuple: A tuple with two elements. The first element is a bool that indicates `True` if the dataset was validated successfully (i.e., no errors), and False otherwise. The second element is an empty dictionary if there are no errors, or a dictionary with key "ValidationReport" mapping to the list of error messages.
    """
    errors_df = validate_dataset_local(df, schema=schema, valid=valid)

    if not errors_df.empty:
        logger.info(
            "Validation finished with Errors! JSON report will be exported to working repository"
        )
        error_list = errors_df["Error"].tolist()  # Extract the error messages
        return False, {"ValidationReport": error_list}
    else:
        logger.info("Complete - no errors")
        return True, {}


def kd_validate_datasets_local(
    datasets: dict,
    schema,  #: px.model.dataflow.Schema,
    boolean: bool,
):
    """Function to validate multiple datasets for SDMX compliance.

    It ensures that each dataset has `STRUCTURE`, `STRUCTURE_ID`, and `ACTION` columns. See this `page for more details. <https://github.com/sdmx-twg/sdmx-csv/blob/master/data-message/docs/sdmx-csv-field-guide.md>`__

    Args:
        datasets (dict): Dictionary of datasets to be validated.
        schema (px.model.dataflow.Schema): Schema object containing validation information.
    
    Returns:
        tuple: Two dictionaries. The first dictionary returns True or False for each file, and the second dictionary contains errors for each file.
    """
    # Extract validation info from schema
    valid = extract_validation_info(schema)

    if boolean:
        logger.info("Validating files against DSD...")

        validated = {}
        error = {}
        for key in datasets.keys():
            logger.info(f"Validating {key}")
            temp_df = datasets[key]()
            temp_validated, temp_error = kd_validate_dataset_local(
                df=temp_df, valid=valid
            )
            validated[key] = temp_validated
            error[key] = temp_error

        return validated, error
