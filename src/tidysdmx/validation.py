from typing import Dict
import pandas as pd
from typeguard import typechecked

from tidysdmx.utils import extract_validation_info

# region Functions to validate formatted dataset

def validate_dataset_local(
    df: pd.DataFrame,
    schema=None,
    valid=None,
    sdmx_cols=["STRUCTURE", "STRUCTURE_ID", "ACTION"],
) -> pd.DataFrame:
    """Validate that a DataFrame is SDMX compliant and return a DataFrame of errors.

    Either a schema or a precomputed 'valid' object must be provided to avoid
    recomputing validation info for multiple datasets.

    Args:
        df (pd.DataFrame): The DataFrame to be validated.
        schema: The schema object (optional if 'valid' is provided).
        valid: Precomputed validation information (optional).
        sdmx_cols (list): SDMX reference columns expected in the dataset.

    Returns:
        pd.DataFrame: A DataFrame containing validation errors. Each row is one error.
    """
    # Define column names once for the returned dataframe
    error_columns = ["Validation", "Error"]

    # Compute validation info only if not provided
    if valid is None:
        if schema is None:
            raise ValueError("Either a schema or precomputed 'valid' must be provided.")
        valid = extract_validation_info(schema)

    error_records: list[dict[str, str]] = []

    # STEP 1: Validate components
    try:
        validate_columns(
			df, 
			valid_columns=valid["valid_comp"],
			sdmx_cols=sdmx_cols
		)
    except ValueError as e:
        error_records.append({error_columns[0]: "columns", error_columns[1]: str(e)})

    all_mandatory_comp_ok = True
    try:
        validate_mandatory_columns(
            df,
            mandatory_columns=valid["mandatory_comp"],
            sdmx_cols=sdmx_cols,
        )
    except ValueError as e:
        error_records.append({error_columns[0]: "mandatory_columns", error_columns[1]: str(e)})
        all_mandatory_comp_ok = False

    # STEP 2: If all mandatory components are present, continue with validation.
    if all_mandatory_comp_ok:
        try:
            validate_codelist_ids(df, valid["codelist_ids"])
        except ValueError as e:
            error_records.append({error_columns[0]: "codelist_ids", error_columns[1]: str(e)})

        try:
            validate_duplicates(df, dim_comp=valid["dim_comp"])
        except ValueError as e:
            error_records.append({error_columns[0]: "duplicates", error_columns[1]: str(e)})

        try:
            validate_no_missing_values(df, mandatory_columns=valid["mandatory_comp"])
        except ValueError as e:
            error_records.append({error_columns[0]: "missing_values", error_columns[1]: str(e)})

    # Always return a DataFrame with consistent columns
    return pd.DataFrame(error_records, columns=error_columns)



@typechecked
def validate_columns(
    df: pd.DataFrame,
    valid_columns: list[str],
    sdmx_cols: list[str] = ["STRUCTURE", "STRUCTURE_ID", "ACTION"],
) -> None:
    """Validate that all columns in the DataFrame are valid component or SDMX reference columns.

    Args:
        df: The DataFrame to validate.
        valid_columns: List of valid component names.
        sdmx_cols: List of additional allowed column names. Defaults to
            ``['STRUCTURE', 'STRUCTURE_ID', 'ACTION']``.

    Raises:
        ValueError: If any column in the DataFrame is not in ``valid_columns`` or ``sdmx_cols``.
    """
    for col in df.columns:
        if col not in sdmx_cols and col not in valid_columns:
            raise ValueError(f"Found unexpected column: {col}")


@typechecked
def validate_mandatory_columns(
    df: pd.DataFrame,
    mandatory_columns: list[str],
    sdmx_cols: list[str] = ["STRUCTURE", "STRUCTURE_ID", "ACTION"],
) -> None:
    """Validate that all mandatory columns are present in the DataFrame.

    Args:
        df: The DataFrame to validate.
        mandatory_columns: List of mandatory component names.
        sdmx_cols: List of additional mandatory column names. Defaults to
            ``['STRUCTURE', 'STRUCTURE_ID', 'ACTION']``.

    Raises:
        ValueError: If any mandatory column is absent from the DataFrame.
    """
    required_columns = set(mandatory_columns + sdmx_cols)
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing mandatory columns: {missing_columns}")


@typechecked
def validate_codelist_ids(df: pd.DataFrame, codelist_ids: Dict[str, list[str]]) -> None:
    """Validate that all values in coded columns are within the allowed codelist IDs.

    Args:
        df: The DataFrame to validate.
        codelist_ids: Mapping of column name to list of allowed code IDs.

    Raises:
        ValueError: If any value in a coded column is not in the allowed IDs.
    """
    for col, valid_ids in codelist_ids.items():
        if col in df.columns:
            # Convert to string for comparison only, without mutating the DataFrame
            col_as_str = df[col].astype(str)
            valid_ids_str = [str(id) for id in valid_ids]
            invalid_values = col_as_str[~col_as_str.isin(valid_ids_str)].unique()
            if len(invalid_values) > 0:
                raise ValueError(
                    f"Invalid values found in column '{col}': {invalid_values}"
                )


@typechecked
def validate_duplicates(df: pd.DataFrame, dim_comp: list[str]) -> None:
    """Validate that there are no duplicate rows for a given set of key columns.

    Args:
        df: The DataFrame to validate.
        dim_comp: List of column names that form the uniqueness key (typically dimensions).

    Raises:
        ValueError: If duplicate rows are found for the given combination of columns.
    """
    duplicates = df.duplicated(subset=dim_comp, keep=False)
    if duplicates.any():
        duplicate_rows = df[duplicates]
        raise ValueError(f"Duplicate rows found:\n{duplicate_rows}")


@typechecked
def validate_no_missing_values(df: pd.DataFrame, mandatory_columns: list[str]) -> None:
    """Validate that there are no missing values in mandatory columns.

    Args:
        df: The DataFrame to validate.
        mandatory_columns: List of mandatory column names to check.

    Raises:
        ValueError: If missing values are found in any mandatory column.
    """
    missing_values = df[mandatory_columns].isnull().any(axis=1)
    if missing_values.any():
        missing_rows = df[missing_values]
        raise ValueError(f"Missing values found in mandatory columns:\n{missing_rows}")
# endregion