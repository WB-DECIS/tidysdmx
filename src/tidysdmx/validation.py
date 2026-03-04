from typing import Dict, Optional
import pandas as pd
from typeguard import typechecked

from tidysdmx.utils import extract_validation_info

# Module-level constant to avoid mutable default arguments
_DEFAULT_SDMX_COLS: tuple[str, ...] = ("STRUCTURE", "STRUCTURE_ID", "ACTION")

# region Functions to validate formatted dataset

@typechecked
def validate_dataset_local(
    df: pd.DataFrame,
    schema: Optional[object] = None,
    valid: Optional[Dict[str, object]] = None,
    sdmx_cols: Optional[list[str]] = None,
    max_errors: int = 1000,
) -> pd.DataFrame:
    """Validate that a DataFrame is SDMX compliant and return a DataFrame of errors.

    Either a schema or a precomputed ``valid`` object must be provided to avoid
    recomputing validation info for multiple datasets.

    Args:
        df: The DataFrame to be validated.
        schema: The schema object (optional if ``valid`` is provided).
        valid: Precomputed validation information returned by
            :func:`~tidysdmx.utils.extract_validation_info` (optional).
        sdmx_cols: SDMX reference columns expected in the dataset. Defaults to
            ``['STRUCTURE', 'STRUCTURE_ID', 'ACTION']``.
        max_errors: Maximum number of individual errors to report per
            validation check. Defaults to ``1000``.

    Returns:
        A DataFrame containing validation errors. Each row is one error, with
        columns ``Validation`` and ``Error``.
    """
    if sdmx_cols is None:
        sdmx_cols = list(_DEFAULT_SDMX_COLS)
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
            sdmx_cols=sdmx_cols,
            max_errors=max_errors,
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
            validate_codelist_ids(df, valid["codelist_ids"], max_errors=max_errors)
        except ValueError as e:
            error_records.append({error_columns[0]: "codelist_ids", error_columns[1]: str(e)})

        try:
            validate_duplicates(df, dim_comp=valid["dim_comp"], max_errors=max_errors)
        except ValueError as e:
            error_records.append({error_columns[0]: "duplicates", error_columns[1]: str(e)})

        try:
            validate_no_missing_values(df, mandatory_columns=valid["mandatory_comp"], max_errors=max_errors)
        except ValueError as e:
            error_records.append({error_columns[0]: "missing_values", error_columns[1]: str(e)})

    # Always return a DataFrame with consistent columns
    return pd.DataFrame(error_records, columns=error_columns)



@typechecked
def validate_columns(
    df: pd.DataFrame,
    valid_columns: list[str],
    sdmx_cols: Optional[list[str]] = None,
    max_errors: int = 1000,
) -> None:
    """Validate that all columns in the DataFrame are valid component or SDMX reference columns.

    Args:
        df: The DataFrame to validate.
        valid_columns: List of valid component names.
        sdmx_cols: List of additional allowed column names. Defaults to
            ``['STRUCTURE', 'STRUCTURE_ID', 'ACTION']``.
        max_errors: Maximum number of unexpected columns to include in the
            error message. Defaults to ``1000``.

    Raises:
        ValueError: If any columns in the DataFrame are not in ``valid_columns``
            or ``sdmx_cols``, listing all offending names up to ``max_errors``.
    """
    if sdmx_cols is None:
        sdmx_cols = list(_DEFAULT_SDMX_COLS)
    allowed = set(valid_columns) | set(sdmx_cols)
    unexpected = [col for col in df.columns if col not in allowed]
    if unexpected:
        capped = unexpected[:max_errors]
        truncated = len(unexpected) - len(capped)
        msg = f"Found unexpected columns: {capped}"
        if truncated:
            msg += f" … and {truncated} more (max_errors={max_errors})"
        raise ValueError(msg)


@typechecked
def validate_mandatory_columns(
    df: pd.DataFrame,
    mandatory_columns: list[str],
    sdmx_cols: Optional[list[str]] = None,
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
    if sdmx_cols is None:
        sdmx_cols = list(_DEFAULT_SDMX_COLS)
    required_columns = set(mandatory_columns + sdmx_cols)
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing mandatory columns: {missing_columns}")


@typechecked
def validate_codelist_ids(
    df: pd.DataFrame,
    codelist_ids: Dict[str, list[str]],
    max_errors: int = 1000,
) -> None:
    """Validate that all values in coded columns are within the allowed codelist IDs.

    Reports violations across all coded columns in a single error, capped at
    ``max_errors`` entries.

    Args:
        df: The DataFrame to validate.
        codelist_ids: Mapping of column name to list of allowed code IDs.
        max_errors: Maximum number of invalid-value entries to include in the
            error message across all columns. Defaults to ``1000``.

    Raises:
        ValueError: If any value in a coded column is not in the allowed IDs,
            listing all offending values (up to ``max_errors``) with their column.
    """
    violations: list[str] = []
    for col, valid_ids in codelist_ids.items():
        if col not in df.columns or len(violations) >= max_errors:
            continue
        col_as_str = df[col].astype(str)
        valid_ids_str = [str(id) for id in valid_ids]
        invalid_values = col_as_str[~col_as_str.isin(valid_ids_str)].unique()
        for val in invalid_values:
            if len(violations) >= max_errors:
                break
            violations.append(f"'{col}': {val}")
    if violations:
        truncated = ""
        if len(violations) >= max_errors:
            truncated = f" (capped at max_errors={max_errors})"
        raise ValueError(
            f"Invalid codelist values found{truncated}:\n  "
            + "\n  ".join(violations)
        )


@typechecked
def validate_duplicates(
    df: pd.DataFrame,
    dim_comp: list[str],
    max_errors: int = 1000,
) -> None:
    """Validate that there are no duplicate rows for a given set of key columns.

    Args:
        df: The DataFrame to validate.
        dim_comp: List of column names that form the uniqueness key (typically dimensions).
        max_errors: Maximum number of duplicate key combinations to include in
            the error message. Defaults to ``1000``.

    Raises:
        ValueError: If duplicate rows are found, reporting the count and the
            offending key combinations (up to ``max_errors``).
    """
    duplicate_mask = df.duplicated(subset=dim_comp, keep=False)
    if duplicate_mask.any():
        dup_keys = (
            df.loc[duplicate_mask, dim_comp]
            .drop_duplicates()
            .head(max_errors)
        )
        total = df.loc[duplicate_mask, dim_comp].drop_duplicates().shape[0]
        truncated = f" (showing {len(dup_keys)} of {total})" if total > max_errors else ""
        raise ValueError(
            f"Found {duplicate_mask.sum()} duplicate rows across {total} key "
            f"combination(s) for {dim_comp}{truncated}:\n{dup_keys.to_string(index=False)}"
        )


@typechecked
def validate_no_missing_values(
    df: pd.DataFrame,
    mandatory_columns: list[str],
    max_errors: int = 1000,
) -> None:
    """Validate that there are no missing values in mandatory columns.

    Args:
        df: The DataFrame to validate.
        mandatory_columns: List of mandatory column names to check.
        max_errors: Maximum number of rows with missing values to include in
            the error message. Defaults to ``1000``.

    Raises:
        ValueError: If missing values are found in any mandatory column,
            reporting the count and the offending rows (up to ``max_errors``).
    """
    missing_mask = df[mandatory_columns].isnull().any(axis=1)
    if missing_mask.any():
        sample = df.loc[missing_mask].head(max_errors)
        total = missing_mask.sum()
        truncated = f" (showing {len(sample)} of {total})" if total > max_errors else ""
        raise ValueError(
            f"Found {total} row(s) with missing values in mandatory columns{truncated}:\n"
            + sample[mandatory_columns].to_string(index=True)
        )
# endregion