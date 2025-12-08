from typing import Dict, List, Any
import pandas as pd
import pysdmx as px
from tidysdmx.utils import *
from typeguard import typechecked

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
        validate_columns(df, valid_columns=valid["valid_comp"])
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

	# STEP 2: If all mandatory components are present. Continue with validation
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



def validate_columns(
	df, valid_columns, sdmx_cols=["STRUCTURE", "STRUCTURE_ID", "ACTION"]
):
	"""Validate that all columns in the DataFrame are part of the specified components or sdmx_cols.

	Args:
		df (pd.DataFrame): The DataFrame to validate.
		valid_columns (list): List of valid component names.
		sdmx_cols (list, optional): List of additional valid column names. Defaults to ['STRUCTURE', 'STRUCTURE_ID', 'ACTION'].

	Raises:
		ValueError: If any column in the DataFrame is not in the list of valid components or sdmx_cols.
	"""
	cols = df.columns
	for col in cols:
		if col not in sdmx_cols and col not in valid_columns:
			raise ValueError(f"Found unexpected column: {col}")


def validate_mandatory_columns(
	df, mandatory_columns, sdmx_cols=["STRUCTURE", "STRUCTURE_ID", "ACTION"]
):
	"""Validate that all mandatory columns are present in the DataFrame.

	Args:
		df (pd.DataFrame): The DataFrame to validate.
		mandatory_columns (list): List of mandatory component names.
		sdmx_cols (list, optional): List of additional mandatory column names. Defaults to ['STRUCTURE', 'STRUCTURE_ID', 'ACTION'].

	Raises:
		ValueError: If any mandatory column is not present in the DataFrame.
	"""
	required_columns = set(mandatory_columns + sdmx_cols)
	missing_columns = required_columns - set(df.columns)
	if missing_columns:
		raise ValueError(f"Missing mandatory columns: {missing_columns}")


def get_codelist_ids(comp, coded_comp):
	"""Retrieve all codelist IDs for given coded components.

	Args:
		comp (list): List of components.
		coded_comp (list): List of coded components.

	Returns:
		dict: Dictionary with coded components as keys and list of codelist IDs as values.
	"""
	codelist_dict = {}
	for component in coded_comp:
		codes = comp[component].local_codes.items
		codelist_dict[component] = [code.id for code in codes]
	return codelist_dict


def validate_codelist_ids(df, codelist_ids):
	"""Validate that all values in specified columns of a DataFrame are within the allowed codelist IDs.

	Args:
		df (pd.DataFrame): The DataFrame to validate.
		codelist_ids (dict): A dictionary where keys are column names and values are lists of allowed IDs.

	Raises:
		ValueError: If any value in the specified columns is not in the allowed codelist IDs.
	"""
	for col, valid_ids in codelist_ids.items():
		if col in df.columns:
			# Convert all values to string before comparison
			df[col] = df[col].astype(str)
			valid_ids = [str(id) for id in valid_ids]
			invalid_values = df[~df[col].isin(valid_ids)][col].unique()
			if len(invalid_values) > 0:
				raise ValueError(
					f"Invalid values found in column '{col}': {invalid_values}"
				)


def validate_duplicates(df, dim_comp):
	"""Validate that there are no duplicate rows in the DataFrame for the given combination of columns.

	Args:
		df (pd.DataFrame): The DataFrame to validate.
		dim_comp (list): List of column names to check for duplicates.

	Raises:
		ValueError: If duplicate rows are found for the given combination of columns.
	"""
	# Check for duplicates
	duplicates = df.duplicated(subset=dim_comp, keep=False)
	if duplicates.any():
		duplicate_rows = df[duplicates]
		raise ValueError(f"Duplicate rows found:\n{duplicate_rows}")


def validate_no_missing_values(df, mandatory_columns):
	"""Validate that there are no missing values in the mandatory columns of the DataFrame.

	Args:
		df (pd.DataFrame): The DataFrame to validate.
		mandatory_columns (list): List of mandatory column names to check for missing values.

	Raises:
		ValueError: If missing values are found in any of the mandatory columns.
	"""
	missing_values = df[mandatory_columns].isnull().any(axis=1)
	if missing_values.any():
		missing_rows = df[missing_values]
		raise ValueError(f"Missing values found in mandatory columns:\n{missing_rows}")
# endregion