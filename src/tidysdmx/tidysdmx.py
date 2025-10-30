import pandas as pd
import numpy as np
import pysdmx as px
import json

from tidysdmx.qa_utils import *
import warnings

from pysdmx.io.format import StructureFormat # To extract json format
from pysdmx.api import fmr # CLient to connect to FMR
from urllib.parse import urljoin
from typing import Literal

def check_dict_keys(dict1, dict2):
	"""Checks whether the sorted keys of two dictionaries are the same.

	Args:
		dict1 (dict): The first dictionary.
		dict2 (dict): The second dictionary.

	Returns:
		`None` if the keys are the same or can be made the same by removing file extensions, or a formatted message highlighting the difference if the keys are not the same.
	"""

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


def remove_extension(key):
	"""Removes the file extension from a key by removing the last period and everything after it.

	Args:
		key (str): The key from which to remove the extension.

	Returns:
		str: The key without the file extension.
	"""
	return key.rsplit(".", 1)[0]


def modify_dict_keys(input_dict):
	"""Modifies the keys of a dictionary by removing the file extensions.

	Args:
		input_dict (dict): The input dictionary with keys that may contain file extensions.

	Returns:
		dict: A new dictionary with the modified keys.
	"""
	return {remove_extension(key): value for key, value in input_dict.items()}


def create_keys_dict(input_dict):
	"""Creates a dictionary where the keys are the new keys (with extensions removed) and the values are the old keys (with extensions).

	Args:
		input_dict (dict): The input dictionary with keys that may contain file extensions.

	Returns:
		dict: A new dictionary with the new keys as keys and the old keys as values.
	"""
	keys_dict = {remove_extension(key): key for key in input_dict.keys()}
	return keys_dict

def fetch_dsd_schema(fmr_params: dict, env: str, dsd_id):
	"""Fetches the Data Structure Definition (DSD) schema from a given Fusion Metadata Registry (FMR) URL.

	DEPRECATION:
		This function is deprecated and will be removed in a future release.
		Use `fetch_schema` instead.

	Args:
		fmr_params (dict): It has base url and endpoints to access FMR's API.
		env (str): FMR Environment to get the data from. It could be 'sandbox', 'qa', 'dev' or 'prod'.
		dsd_id (str): The identifier of the Data Structure Definition, typically in the format "agency:id(version)".

	Returns:
		dict: The schema of the requested Data Structure Definition.

	Raises:
		ValueError: If the URL is not syntactically valid.
		aiohttp.ClientError: If there is an issue with the HTTP request.
		px.io.exceptions.FormatError: If there is an issue with the format of the response.

	Examples:
		>>> schema = fetch_dsd_schema("https://example.com/fmr", "WB:WDI(1.0)")
	"""
	
	warnings.warn(
		"fetch_dsd_schema is deprecated and will be removed in a future release. "
		"Please use fetch_schema instead.",
		FutureWarning,
		stacklevel=2,
	)

	format = px.io.format.StructureFormat.FUSION_JSON

	fmr_url = fmr_params[env]["url"]

	# Ensure the URL is syntactically valid
	base_url = urljoin(fmr_url, "/FMR/sdmx/v2/")

	client = fmr.RegistryClient(
		base_url,
		format=format,
	)

	agency, id, version = parse_dsd_id(dsd_id)
	schema = client.get_schema("datastructure", agency, id, version)
	return schema

def fetch_schema(
		base_url:str,
		artefact_id: str,
		context: Literal["dataflow", "datastructure", "provisionagreement"]):
	"""Fetches the schema of a specified artefact from an SDMX registry.
	
	Args:
		base_url (str): The base URL of the FMR.
		artefact_id (str): The identifier of the artefact, typically in the format "agency:id(version)".
		context (Literal["dataflow", "datastructure", "provisionagreement"]): The context of the artefact to fetch.
		
	Returns:
		schema: The fetched schema object.
	"""
	format = StructureFormat.FUSION_JSON

	# Ensure the URL is syntactically valid
	base_url = urljoin(base_url, "/FMR/sdmx/v2/")

	# Initialize the client
	client = fmr.RegistryClient(
		base_url,
		format=format,
	)

	# Parse the artefact ID
	agency, id, version = parse_artefact_id(artefact_id)

	# Fetch the schema
	schema = client.get_schema(context, agency, id, version)
	
	return schema

def extract_validation_info(schema):
	"""
	Extract validation information from a given schema.

	Args:
		schema: The schema object containing validation information.

	Returns:
		dict: A dictionary containing validation information with the following keys:
			- valid_comp: List of valid component names.
			- mandatory_comp: List of mandatory component names.
			- coded_comp: List of coded component names.
			- codelist_ids: Dictionary with coded components as keys and list of codelist IDs as values.
			- dim_comp: List of dimension component names.
	"""
	comp = schema.components
	validation_info = {
		"valid_comp": [c.id for c in comp],
		"mandatory_comp": [c.id for c in comp if comp[c.id].required],
		"coded_comp": [c.id for c in comp if comp[c.id].local_codes is not None],
		"codelist_ids": get_codelist_ids(
			comp, [c.id for c in comp if comp[c.id].local_codes is not None]
		),
		"dim_comp": [c.id for c in comp if comp[c.id].role == px.model.Role.DIMENSION],
	}
	return validation_info


def parse_dsd_id(dsd_id):
	"""
	Parses the Data Structure Definition (DSD) identifier into its components.

	DEPRECATION:
		This function is deprecated and will be removed in a future release.
		Use `parse_artefact_id` instead.

	Args:
		dsd_id (str): The identifier of the Data Structure Definition, typically in the format "agency:id(version)".

	Returns:
		tuple: A tuple containing the agency, id, and version.

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
	except Exception:
		raise ValueError("Invalid dsd_id format. Expected format: 'agency:id(version)'")
	

def parse_artefact_id(artefact_id: str) -> tuple[str, str, str]:
	"""Parses artefact identifier (DSD, Dataflow, Codelist, etc) into its components: agency, id and version.

	Args:
		artefact_id (str): The identifier of the artefact, typically in the format "agency:id(version)".

	Returns:
		tuple[str, str, str]: A tuple containing the agency, id, and version.

	Raises:
		ValueError: If the artefact_id is not in the expected format.
	"""

	try:
		agency, rest = artefact_id.split(":", 1)
		id_part, version_part = rest.split("(", 1)
		version = version_part.rstrip(")")
		return agency, id_part, version
	except Exception:
		raise ValueError("Invalid artefact_id format. Expected format: 'agency:id(version)'")
	

def standardize_sdmx(data, mapping):
	"""Standardizes a DataFrame by applying transform_source_to_target and other transformations using the provided mapping.

	Args:
		data (pd.DataFrame): The input DataFrame with raw data.
		mapping (dict): A dictionary containing the mapping DataFrame and other relevant information.

	Returns:
		pd.DataFrame: The standardized DataFrame with columns transformed according to the mapping.
	"""
	data = transform_source_to_target(data, mapping)
	data = map_to_sdmx(data, mapping)
	data = standardize_data_for_upload(data, dsd=mapping["dsd_id"])

	return data

def transform_source_to_target(
		raw: pd.DataFrame, 
		mapping: dict
	) -> pd.DataFrame:
	"""Transforms raw DataFrame into the format defined by components_map.
	
	This function creates a new dataframe with columns as defined in components_map['TARGET'] and populates it with data from the raw DataFrame based on the columns names in the ['SOURCE'].

	Args:
		raw (pd.DataFrame): The input DataFrame with raw data.
		mapping (dict): The master mapping dictionary containing a mapping between the input file columns, and the columns defined in the schema.

	Returns:
		pd.DataFrame: The transformed DataFrame with columns as defined in components_map['TARGET'].
	"""
	# If there is no " components" key in the mapping, raise return None
	try: 
		# Create an empty DataFrame with columns as defined in components_map['TARGET']
		components_map = mapping["components"]

		# If the components_map is a list, create a dataframe with source and target columns
		if isinstance(components_map, list):
			components_map = pd.DataFrame(components_map)
		
		# Create an empty DataFrame with target columns
		result_df = pd.DataFrame(columns=components_map["TARGET"].values)

		# Iterate over the components_map DataFrame and map the columns
		for _, row in components_map.iterrows():
			source_col = row["SOURCE"]
			target_col = row["TARGET"]

			# If source_col exists in raw, populate the corresponding column in result_df
			if source_col in raw.columns:
				result_df[target_col] = raw[source_col]

		return result_df
	
	except KeyError as e:
		raise KeyError("The mapping file should contain 'components' key or its value should not be empty. Please make sure the mapping file has this key and its value is not empty.") from e


def vectorized_lookup_ordered_v1(series, mapping_df):
	"""Apply ordered regex matching to a Pandas Series.

	For each regex pattern (except the last one) in mapping_df,
	check if the value in series matches the pattern. The corresponding
	TARGET is assigned when a match is found, and later rules are skipped.
	Any cell that does not match any of the earlier patterns is assigned
	the last rule's TARGET (catch-all).

	Args:
		series (pd.Series): The input data series (e.g., a DataFrame column).
		mapping_df (pd.DataFrame): A DataFrame with at least two columns:

			- 'SOURCE': regex patterns (ordered by priority)
			- 'TARGET': corresponding replacement values

	Returns:
		pd.Series: A new series with values replaced according to the first matching regex, or the last rule's TARGET if no match is found.
	"""
	# Convert the series to strings for regex operations.
	series_str = series.astype(str)

	# If there are no mapping rules, return the original series.
	if mapping_df.empty:
		return series

	# Sort mapping by length of SOURCE descending
	mapping_df = mapping_df.copy()
	mapping_df["SOURCE_LEN"] = mapping_df["SOURCE"].str.len()
	mapping_df = mapping_df.sort_values(by="SOURCE_LEN", ascending=False).drop(columns="SOURCE_LEN")
	
	conditions = []
	choices = []

	# Build conditions for all rules.
	for _, row in mapping_df.iterrows():
		# Each condition is a boolean Series where the pattern is found.
		conditions.append(series_str.str.contains(row["SOURCE"], regex=True))
		choices.append(row["TARGET"])

	# Use the original value as the default if no match is found.
	default_value = series_str

	# np.select will choose the first condition that is True for each element,
	# and use the default_value if none match.
	result = np.select(conditions, choices, default=default_value)

	return pd.Series(result, index=series.index)


def vectorized_lookup_ordered_v2(series, mapping_df):
	"""Apply ordered matching (regex or exact) to a Pandas Series based on the "IS_REGEX" column.

	For each row in mapping_df:

		- If "IS_REGEX" is True, perform regex matching.
		- If "IS_REGEX" is False, perform exact string matching.
	
	The corresponding TARGET is assigned when a match is found, and later rules are skipped.
	Any cell that does not match any of the earlier rules is assigned the last rule's TARGET (catch-all).

	Args:
		series (pd.Series): The input data series (e.g., a DataFrame column).
		mapping_df (pd.DataFrame): A DataFrame with at least three columns:

			- 'SOURCE': regex patterns or exact strings (ordered by priority),
			- 'TARGET': corresponding replacement values,
			- 'IS_REGEX': boolean indicating whether 'SOURCE' is a regex pattern.

	Returns:
		pd.Series: A new series with values replaced according to the first matching rule, or the last rule's TARGET if no match is found.
	"""
	# Convert the series to strings for matching operations.
	series_str = series.astype(str)

	# If there are no mapping rules, return the original series.
	if mapping_df.empty:
		return series
	# Sort mapping by length of SOURCE descending
	mapping_df = mapping_df.copy()
	mapping_df["SOURCE_LEN"] = mapping_df["SOURCE"].str.len()
	mapping_df = mapping_df.sort_values(by="SOURCE_LEN", ascending=False).drop(columns="SOURCE_LEN")
	
	conditions = []
	choices = []

	# Build conditions for all rules.
	for _, row in mapping_df.iterrows():
		source = row["SOURCE"]
		is_regex = row["IS_REGEX"]

		if is_regex:
			# Regex matching
			conditions.append(series_str.str.contains(source, regex=True))
		else:
			# Exact string matching
			conditions.append(series_str == source)

		choices.append(row["TARGET"])

	# Use the original value as the default if no match is found.
	default_value = series_str

	# np.select will choose the first condition that is True for each element,
	# and use the default_value if none match.
	result = np.select(conditions, choices, default=default_value)

	return pd.Series(result, index=series.index)

def map_to_sdmx(df, mapping):
	"""Map DataFrame columns to SDMX values using a lookup mapping.

	This function transforms the given pandas DataFrame columns to conform
	to the SDMX representation by applying either a fixed mapping or an ordered,
	regex-based mapping. For each key present in the DataFrame:

		- Fixed Mapping: 
			If the mapping for a key contains a "TARGET" column but no "SOURCE" column, then the entire column is replaced with the fixed value provided by "TARGET".

		- Regex-based Mapping: 
			If the mapping for a key contains both "SOURCE" and "TARGET" columns, the function applies ordered regex-based matching. For each cell in the DataFrame column, the regex patterns are evaluated in order using the first-match-wins strategy. If no match is found in the earlier rules, the last rule's TARGET is applied.

	Args:
		df (pandas.DataFrame): The input DataFrame containing the data to be mapped.
		mapping (dict): The lookup mapping in JSON format (as a dict). Each key represents an SDMX component and its value is expected to be either a list of dictionaries (with keys "SOURCE" and "TARGET") or a pandas DataFrame with those columns.

	Returns:
		pandas.DataFrame: The transformed DataFrame with mapped column values.
	"""
	# Extract schema version
	schema_version = mapping["schema_version"]
	# Remove the "components" key from mapping if present.
	representation_mapping = mapping.get("representation", {})

	# Get the total number of items in the mapping.
	total_items = len(representation_mapping)

	# Iterate over each key in the mapping dictionary.
	for index, (key, mapping_value) in enumerate(representation_mapping.items(), start=1):
		print(f"Processing {index}/{total_items}: {key}")

		# Skip empty mappings
		if not mapping_value:
			print(f"Skipping '{key}' because mapping is empty")
			continue

		# Skip if column not in DataFrame
		if key not in df.columns:
			print(f"Skipping '{key}' because column not in DataFrame")
			continue

		# Ensure mapping_value is a DataFrame
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
			# This catches unexpected structures
			print(f"Skipping '{key}': invalid mapping structure (expected SOURCE and TARGET columns)")

	return df

def add_sdmx_reference_cols(df, dsd, structure="datastructure", action="I"):
	"""Adds necessary columns for a successful upload into an SDMX database.

	Args:
		df (pd.DataFrame): The input DataFrame to which the columns will be added.
		structure (str): The structure type. Default is 'datastructure'. Potential options accepted by SDMX for structure include:

			- 'datastructure': Represents a data structure definition.
			- 'metadataflow': Represents a metadata flow definition.
			- 'dataflow': Represents a data flow definition.
		dsd (str): The Data Structure Definition (DSD) identifier.
		action (str): The action type. Default is 'I'. Potential options accepted by SDMX for action include:
			
			- 'I': Insert
			- 'U': Update
			- 'D': Delete

	Returns:
		pd.DataFrame: The DataFrame with the added SDMX reference columns.
	"""
	df["STRUCTURE"] = structure
	df["STRUCTURE_ID"] = dsd
	df["ACTION"] = action

	return df

def standardize_indicator_id(df):
	"""Fixes the 'INDICATOR' column by ensuring all values are upper case and start with dataset_id.

	Args:
		df (pd.DataFrame): The DataFrame to modify.

	Returns:
		pd.DataFrame: The modified DataFrame with corrected 'INDICATOR' values.

	Examples:

		- Input DataFrame:
		
			| row |   DATABASE_ID   | INDICATOR                 |
			| --  | ------------------------ | ------------------------- |
			| 0   | WB.DATA360      | indicator.one  |
			| 1   | WB.DATA360      | indicator.two  |

		- Output DataFrame:
		
			| row |   DATABASE_ID   | INDICATOR                 |
			| --  | ------------------------ | --------------------------------------------------- |
			| 0   | WB.DATA360      | WB_DATA360_INDICATOR_ONE  |
			| 1   | WB.DATA360      | WB_DATA360_INDICATOR_TWO  |
			
	"""
	# Extract the unique values of the 'DATASET_ID'/'DATABASE_ID' column
	id_column = None
	for col in ["DATABASE_ID", "DATASET_ID"]:
		if col in df.columns:
			id_column = col
			break

	# Extract unique values
	dataset_id = df[id_column].unique()
	if len(dataset_id) != 1:
		raise ValueError(
			f"The 'DATABASE_ID' column has {len(dataset_id)} unique values. Expected exactly 1 unique value."
		)
	dataset_id = dataset_id[0]
	# Ensure INDICATOR IDs matches conventions
	df["INDICATOR"] = df["INDICATOR"].astype(str)
	dataset_id = str(dataset_id)

	if not df["INDICATOR"].str.startswith(dataset_id).all():
		df["INDICATOR"] = dataset_id + "_" + df["INDICATOR"]
	if not df["INDICATOR"].str.isupper().all():
		df["INDICATOR"] = df["INDICATOR"].str.upper()
	df["INDICATOR"] = df["INDICATOR"].str.replace(r"\.+", "_", regex=True)

	return df

def standardize_data_for_upload(df, dsd, structure="datastructure", action="I"):
	"""Standardizes the DataFrame for SDMX upload.

	Finalizes the DataFrame for a successful upload into an SDMX database by fixing the 'INDICATOR' values, adding necessary reference columns, and reordering the columns.

	Args:
		df (pd.DataFrame): The input DataFrame to modify.
		dataset_id (str): The dataset identifier to prepend to the 'INDICATOR' values.
		dsd (str): The Data Structure Definition (DSD) identifier.
		structure (str): The structure type. Default is 'datastructure'. Potential options accepted by SDMX for structure include:
			
			- 'datastructure': Represents a data structure definition.
			- 'metadataflow': Represents a metadata flow definition.
			- 'dataflow': Represents a data flow definition.
		
		action (str): The action type. Default is 'I'. Potential options accepted by SDMX for action include:
	
			- 'I': Insert
			- 'U': Update
			- 'D': Delete

	Returns:
		pd.DataFrame: The modified DataFrame with corrected 'INDICATOR' values, added reference columns, and reordered columns.
	"""

	# QUALITY ASSURANCE OPERATION
	# WILL BE MOVED INTO THERE OWN NODES
	df = qa_coerce_numeric(df, numeric_columns=["OBS_VALUE"])
	df = qa_remove_duplicates(df)

	df = standardize_indicator_id(df=df)
	df = add_sdmx_reference_cols(df=df, dsd=dsd, structure=structure, action=action)

	# Reorder columns because `STRUCTURE` and `STRUCTURE_ID` should always be first
	# Columns to move to the beginning
	cols_to_move = ["STRUCTURE", "STRUCTURE_ID", "ACTION"]

	# Create a new order for the columns
	new_order = cols_to_move + [col for col in df.columns if col not in cols_to_move]

	# Reindex the DataFrame to the new column order
	df = df[new_order]
	return df

# region Funtions to handle mapping files
def read_mapping(path):
	"""
	Reads a JSON file and parses its content into DataFrames.

	The function processes the JSON data with four main keys:

		1. "schema_version": This key contains the version of the schema.
		2. "dsd_id": This key contains the Data Structure Definition ID.
		3. "components": This key is expected to contain a flat structure, which is converted into a single DataFrame named "components".
		4. "representation": This key contains multiple sub-keys, each of which is associated with a flat structure. Each valid sub-key is converted into a separate DataFrame. Sub-keys with empty or invalid content are skipped.

	Additionally, all occurrences of the string "NA" in the JSON data are
	converted to missing values (pd.NA) in the resulting DataFrames.

	Args:
		path (str): The file path to the JSON file to be parsed.

	Returns:
		dict: A dictionary where:
			- The "schema_version" value is stored under the key 'schema_version'.
			- The "dsd_id" value is stored under the key 'dsd_id'.
			- The "components" DataFrame is stored under the key 'components'.
			- Each valid sub-key in "representation" is stored as a DataFrameunder its corresponding key.

	Raises:
		ValueError: If the "components" key is missing, the "representation" key is invalid, or a sub-key in "representation" has an unexpected format.
	"""
	# Load JSON data from file
	with open(path, "r") as file:
		data = json.load(file)

	# Initialize the result structure
	result = {}

	# Process 'schema_version' key
	schema_version = data.get("schema_version")
	if schema_version:
		result["schema_version"] = schema_version
	else:
		raise ValueError("Missing 'schema_version' key in JSON mapping file")

	# Process 'dsd_id' key
	dsd_id = data.get("dsd_id")
	if dsd_id:
		result["dsd_id"] = dsd_id
	else:
		raise ValueError("Missing 'dsd_id' key in JSON mapping file")

	# Process 'components' key into a DataFrame
	components_data = data.get("components")
	if components_data:
		df_components = pd.DataFrame(components_data).replace("NA", pd.NA)
		result["components"] = df_components
	else:
		raise ValueError("Missing 'components' key in JSON mapping file")

	# Process 'representation' key into multiple DataFrames
	representation_data = data.get("representation")
	if representation_data and isinstance(representation_data, dict):
		for sub_key, sub_value in representation_data.items():
			if isinstance(sub_value, list):
				# Convert list to DataFrame and replace "NA" with pd.NA
				df_representation = pd.DataFrame(sub_value).replace("NA", pd.NA)
				result[sub_key] = df_representation
			elif not sub_value:
				# Skip empty sub-keys
				continue
			else:
				raise ValueError(
					f"Unexpected data format for representation sub-key: {sub_key}"
				)
	else:
		raise ValueError("Missing or invalid 'representation' key in JSON mapping file")

	return result

# endregion

# region Functions to validate formatted dataset

def validate_dataset_local(
    df: pd.DataFrame,
    schema=None,
    valid=None,
    sdmx_cols=["STRUCTURE", "STRUCTURE_ID", "ACTION"]
) -> pd.DataFrame:
    """
    Validate that a DataFrame is SDMX compliant and return a DataFrame of errors.

    Either a schema or a precomputed 'valid' object must be provided. This design allows you to avoid
    re-computing the validation info for multiple datasets.

    Args:
        df (pd.DataFrame): The DataFrame to be validated.
        schema: The schema object (optional if 'valid' is provided).
        valid: Precomputed validation information (optional).

    Returns:
        pd.DataFrame: A DataFrame containing all validation errors. Each row represents one type of error. 
    """
    # Define column names once
    error_columns = ["Validation", "Error"]

    # Compute validation info only if not provided
    if valid is None:
        if schema is None:
            raise ValueError("Either a schema or precomputed 'valid' must be provided.")
        valid = extract_validation_info(schema)

    error_records: List[Dict[str, str]] = []

    # Run validations and collect errors
    try:
        validate_columns(df, valid_columns=valid["valid_comp"])
    except ValueError as e:
        error_records.append({error_columns[0]: "columns", error_columns[1]: str(e)})

    try:
        validate_mandatory_columns(df, mandatory_columns=valid["mandatory_comp"], sdmx_cols=sdmx_cols)
    except ValueError as e:
        error_records.append({error_columns[0]: "mandatory_columns", error_columns[1]: str(e)})

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
	"""
	Validate that all mandatory columns are present in the DataFrame.

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
	"""
	Retrieve all codelist IDs for given coded components.

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
	"""
	Validate that all values in specified columns of a DataFrame are within the allowed codelist IDs.

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
	"""
	Validate that there are no duplicate rows in the DataFrame for the given combination of columns.

	Args:
		df (pd.DataFrame): The DataFrame to validate.
		dim_columns (list): List of column names to check for duplicates.

	Raises:
		ValueError: If duplicate rows are found for the given combination of columns.
	"""
	# Check for duplicates
	duplicates = df.duplicated(subset=dim_comp, keep=False)
	if duplicates.any():
		duplicate_rows = df[duplicates]
		raise ValueError(f"Duplicate rows found:\n{duplicate_rows}")


def validate_no_missing_values(df, mandatory_columns):
	"""
	Validate that there are no missing values in the mandatory columns of the DataFrame.

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