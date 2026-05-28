"""Validate SDMX datasets against schemas and codelists."""

import pandas as pd
from pysdmx.model.dataflow import Schema
from typeguard import typechecked

from .utils import extract_validation_info

_DEFAULT_SDMX_COLS: tuple[str, ...] = ("STRUCTURE", "STRUCTURE_ID", "ACTION")


def _get_unexpected_columns(
    df: pd.DataFrame,
    valid_columns: list[str],
    sdmx_cols: list[str],
) -> list[str]:
    """Return column names that are not in *valid_columns* or *sdmx_cols*.

    Args:
        df: The DataFrame whose columns are checked.
        valid_columns: Allowed component column names.
        sdmx_cols: Allowed SDMX reference column names.

    Returns:
        List of unexpected column names (preserving the order they appear in *df*).
    """
    allowed = set(valid_columns) | set(sdmx_cols)
    return [col for col in df.columns if col not in allowed]


def _get_codelist_violations(
    df: pd.DataFrame,
    codelist_ids: dict[str, list[str]],
    max_errors: int,
) -> list[tuple[str, str]]:
    """Return one ``(column, invalid_value)`` tuple per codelist violation.

    Args:
        df: The DataFrame to check.
        codelist_ids: Mapping of column name to list of allowed code IDs.
        max_errors: Maximum number of violations to return across all columns.

    Returns:
        List of ``(column_name, invalid_value_str)`` tuples, capped at *max_errors*.
    """
    violations: list[tuple[str, str]] = []
    for col, valid_ids in codelist_ids.items():
        if col not in df.columns or len(violations) >= max_errors:
            continue
        col_as_str = df[col].astype(str)
        valid_ids_set = {str(vid) for vid in valid_ids}
        invalid_values = col_as_str[~col_as_str.isin(valid_ids_set)].unique()
        for val in invalid_values:
            if len(violations) >= max_errors:
                break
            violations.append((col, val))
    return violations


@typechecked
def validate_dataset_local(
    df: pd.DataFrame,
    schema: Schema | None = None,
    valid: dict[str, object] | None = None,
    sdmx_cols: list[str] | None = None,
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
        sdmx_cols: SDMX reference columns expected in the dataset. When
            omitted, the columns are inferred from the schema's context
            (e.g. ``['DATAFLOW', 'DATAFLOW_ID', 'ACTION']`` for a dataflow
            schema, ``['STRUCTURE', 'STRUCTURE_ID', 'ACTION']`` for a
            datastructure schema).
        max_errors: Maximum number of individual errors to report per
            validation check. Defaults to ``1000``.

    Returns:
        A DataFrame containing validation errors. Each row is one error, with
        columns ``Validation`` and ``Error``.
    """
    if valid is None:
        if schema is None:
            raise ValueError("Either a schema or precomputed 'valid' must be provided.")
        valid = extract_validation_info(schema)

    if sdmx_cols is None:
        sdmx_cols = list(valid["sdmx_cols"])

    error_records: list[dict[str, str]] = []

    # Validate columns — one row per unexpected column
    unexpected = _get_unexpected_columns(df, valid["valid_comp"], sdmx_cols)
    for col in unexpected[:max_errors]:
        error_records.append(
            {"Validation": "columns", "Error": f"Unexpected column: '{col}'"}
        )

    # Check mandatory columns directly instead of catching ValueError
    required = set(valid["mandatory_comp"]) | set(sdmx_cols)
    missing = sorted(required - set(df.columns))
    if missing:
        error_records.append(
            {
                "Validation": "mandatory_columns",
                "Error": f"Missing mandatory columns: {missing}",
            }
        )

    # Only proceed with value-level checks if all mandatory columns are present
    if not missing:
        for col, val in _get_codelist_violations(df, valid["codelist_ids"], max_errors):
            error_records.append(
                {"Validation": "codelist_ids", "Error": f"'{col}': {val}"}
            )

        try:
            validate_duplicates(df, dim_comp=valid["dim_comp"], max_errors=max_errors)
        except ValueError as e:
            error_records.append({"Validation": "duplicates", "Error": str(e)})

        try:
            validate_no_missing_values(
                df,
                mandatory_columns=valid["mandatory_comp"],
                max_errors=max_errors,
            )
        except ValueError as e:
            error_records.append({"Validation": "missing_values", "Error": str(e)})

    return pd.DataFrame(error_records, columns=["Validation", "Error"])


@typechecked
def validate_columns(
    df: pd.DataFrame,
    valid_columns: list[str],
    sdmx_cols: list[str] | None = None,
    max_errors: int = 1000,
) -> None:
    """Validate that all DataFrame columns are valid components or SDMX references.

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
    unexpected = _get_unexpected_columns(df, valid_columns, sdmx_cols)
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
    sdmx_cols: list[str] | None = None,
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
    missing_columns = sorted(required_columns - set(df.columns))

    if missing_columns:
        raise ValueError(f"Missing mandatory columns: {missing_columns}")


@typechecked
def validate_codelist_ids(
    df: pd.DataFrame,
    codelist_ids: dict[str, list[str]],
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
    violations = _get_codelist_violations(df, codelist_ids, max_errors)
    if violations:
        truncated = ""
        if len(violations) >= max_errors:
            truncated = f" (capped at max_errors={max_errors})"
        raise ValueError(
            f"Invalid codelist values found{truncated}:\n  "
            + "\n  ".join(f"'{col}': {val}" for col, val in violations)
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
        dim_comp: Column names forming the uniqueness key (dimensions).
        max_errors: Maximum number of duplicate key combinations to include in
            the error message. Defaults to ``1000``.

    Raises:
        ValueError: If duplicate rows are found, reporting the count and the
            offending key combinations (up to ``max_errors``).
    """
    duplicate_mask = df.duplicated(subset=dim_comp, keep=False)
    if duplicate_mask.any():
        dup_keys = df.loc[duplicate_mask, dim_comp].drop_duplicates().head(max_errors)
        total = df.loc[duplicate_mask, dim_comp].drop_duplicates().shape[0]
        truncated = (
            f" (showing {len(dup_keys)} of {total})" if total > max_errors else ""
        )
        raise ValueError(
            f"Found {duplicate_mask.sum()} duplicate rows across {total} key "
            f"combination(s) for {dim_comp}{truncated}:\n"
            f"{dup_keys.to_string(index=False)}"
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
            f"Found {total} row(s) with missing values in "
            f"mandatory columns{truncated}:\n"
            + sample[mandatory_columns].to_string(index=True)
        )
