"""Validate SDMX datasets against schemas and codelists."""

import warnings
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Self

import numpy as np
import pandas as pd
from pysdmx.model.dataflow import Schema
from typeguard import typechecked

from .utils import extract_validation_info, sdmx_reference_cols_for

_DEFAULT_SDMX_COLS: tuple[str, ...] = ("STRUCTURE", "STRUCTURE_ID", "ACTION")

# Context-specific reference columns produced by tidysdmx <= 0.9.0; `valid`
# dicts cached under those versions may still carry them.
_LEGACY_CONTEXT_SDMX_COLS: tuple[list[str], ...] = (
    ["DATAFLOW", "DATAFLOW_ID", "ACTION"],
    ["PROVISIONAGREEMENT", "PROVISION_AGREEMENT_ID", "ACTION"],
)


def _truncation_note(shown: int, total: int, max_errors: int) -> str:
    """Return a canonical suffix describing truncated error output.

    Args:
        shown: Number of items included in the message.
        total: Total number of items found.
        max_errors: The cap that was applied.

    Returns:
        ``" … and N more (max_errors=...)"`` when items were dropped,
        otherwise an empty string.
    """
    if total <= shown:
        return ""
    return f" … and {total - shown} more (max_errors={max_errors})"


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


def _invalid_values(column: pd.Series, valid_ids: set[str]) -> list[str]:
    """Return the values of ``column`` outside ``valid_ids``, as text.

    Values compare as ``str``, in order of first appearance, so a missing
    value counts as (and is reported as) the text it prints as, ``nan``.
    Only the distinct values are converted to text, so a column costs its
    number of distinct values rather than its rows. An object column holding
    values that compare equal but print differently (``1``, ``1.0``,
    ``True``) reports the first of them only.

    Args:
        column: The coded column.
        valid_ids: The allowed code IDs, as ``str``.

    Returns:
        The printed form of each distinct invalid value.
    """
    as_str = pd.Series(column.unique()).astype(str)
    return [str(value) for value in as_str[~as_str.isin(valid_ids)]]


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
        valid_ids_set = {str(vid) for vid in valid_ids}
        for val in _invalid_values(df[col], valid_ids_set):
            if len(violations) >= max_errors:
                break
            violations.append((col, val))
    return violations


def _duplicate_keys(df: pd.DataFrame, dim_comp: list[str]) -> tuple[int, pd.DataFrame]:
    """Return how many rows share their key with another row, and those keys.

    Args:
        df: The DataFrame to check.
        dim_comp: Column names forming the uniqueness key (dimensions).

    Returns:
        The number of rows whose key occurs more than once, and the distinct
        keys that do, in order of first appearance (empty when there are none).
    """
    duplicate_mask = df.duplicated(subset=dim_comp, keep=False)
    if not duplicate_mask.any():
        return 0, df.iloc[:0][dim_comp]
    return int(duplicate_mask.sum()), df.loc[duplicate_mask, dim_comp].drop_duplicates()


def _duplicates_message(
    n_rows: int,
    shown: pd.DataFrame,
    total: int,
    dim_comp: list[str],
    max_errors: int,
) -> str:
    """Return the ``duplicates`` error message."""
    truncated = _truncation_note(len(shown), total, max_errors)
    return (
        f"Found {n_rows} duplicate rows across {total} key "
        f"combination(s) for {dim_comp}{truncated}:\n"
        f"{shown.to_string(index=False)}"
    )


def _missing_rows_mask(df: pd.DataFrame, columns: list[str]) -> np.ndarray:
    """Return, row by row, whether any of ``columns`` is missing.

    One column at a time, so the frame of every column's missingness is
    never built.
    """
    mask = np.zeros(len(df), dtype=bool)
    for col in columns:
        mask |= df[col].isna().to_numpy()
    return mask


def _missing_values_message(
    total: int,
    sample: pd.DataFrame,
    mandatory_columns: list[str],
    max_errors: int,
) -> str:
    """Return the ``missing_values`` error message."""
    truncated = _truncation_note(len(sample), total, max_errors)
    return (
        f"Found {total} row(s) with missing values in "
        f"mandatory columns{truncated}:\n"
        + sample[mandatory_columns].to_string(index=True)
    )


def _str_list(valid: Mapping[str, object], key: str) -> list[str]:
    """Narrow one list-of-str entry of a validation-info dict."""
    value = valid[key]
    if not isinstance(value, list | tuple) or not all(
        isinstance(item, str) for item in value
    ):
        raise TypeError(
            f"valid[{key!r}] must be a list of str, got {type(value).__name__}."
        )
    return list(value)


def _codelists(valid: Mapping[str, object]) -> dict[str, list[str]]:
    """Narrow the ``codelist_ids`` entry of a validation-info dict."""
    value = valid["codelist_ids"]
    if not isinstance(value, dict):
        raise TypeError(
            "valid['codelist_ids'] must be a dict of column to code IDs, got "
            f"{type(value).__name__}."
        )
    return {str(col): [str(code) for code in codes] for col, codes in value.items()}


@dataclass(frozen=True)
class _Rules:
    """What a schema requires of a dataset, as ``extract_validation_info`` reads it."""

    valid_comp: list[str]
    mandatory_comp: list[str]
    dim_comp: list[str]
    codelist_ids: dict[str, list[str]]

    @classmethod
    def from_info(cls, valid: Mapping[str, object]) -> Self:
        """Build the rules from a validation-info dict, checking its shape.

        The dict is a boundary: ``validate_dataset_local`` still accepts one a
        caller cached under an earlier release.

        Raises:
            KeyError: If an entry is missing.
            TypeError: If an entry has the wrong shape.
        """
        return cls(
            valid_comp=_str_list(valid, "valid_comp"),
            mandatory_comp=_str_list(valid, "mandatory_comp"),
            dim_comp=_str_list(valid, "dim_comp"),
            codelist_ids=_codelists(valid),
        )


@typechecked
class DatasetValidator:
    """Validate a dataset chunk by chunk and report once.

    Feed the chunks of one dataset to :meth:`update`, then call :meth:`errors`
    for the error DataFrame :func:`validate_dataset_local` would return for
    the whole dataset, with one difference: duplicate keys are found within
    each chunk, so a key that two chunks share is not reported. Chunk on a
    dimension to rule that out, or rely on the registry's own key check at
    upload.

    What is kept between chunks is bounded by ``max_errors``, not by rows:
    the unexpected and the missing mandatory columns seen, each coded
    column's invalid values, and the counts of rows with missing values or
    duplicate keys with a sample of each. Index labels in the messages are
    the chunks' own, so keep the index ``pd.read_csv(..., chunksize=...)``
    gives each chunk if file positions matter.

    Args:
        schema: The schema to validate against.
        sdmx_cols: SDMX reference columns expected in the dataset, as a
            list or tuple. When omitted, the standard SDMX-CSV columns
            ``['STRUCTURE', 'STRUCTURE_ID', 'ACTION']``.
        max_errors: Maximum number of individual errors to report per
            validation check, over the whole dataset. Defaults to ``1000``.

    Example:
        >>> validator = DatasetValidator(schema)  # doctest: +SKIP
        >>> for chunk in pd.read_csv(path, chunksize=1_000_000):  # doctest: +SKIP
        ...     validator.update(standardize_output(map_structures(chunk, sm), ...))
        >>> errors = validator.errors()  # doctest: +SKIP
    """

    def __init__(
        self,
        schema: Schema,
        sdmx_cols: list[str] | tuple[str, ...] | None = None,
        max_errors: int = 1000,
    ) -> None:
        rules = _Rules.from_info(extract_validation_info(schema))
        cols = (
            sdmx_reference_cols_for(schema.context)
            if sdmx_cols is None
            else list(sdmx_cols)
        )
        self._start(rules, cols, max_errors)

    @classmethod
    def _from_info(
        cls, valid: Mapping[str, object], sdmx_cols: Sequence[str], max_errors: int
    ) -> Self:
        """Build a validator from a precomputed ``extract_validation_info`` dict.

        Serves ``validate_dataset_local``'s deprecated ``valid=`` argument;
        the public constructor takes the schema.
        """
        validator = cls.__new__(cls)
        validator._start(_Rules.from_info(valid), list(sdmx_cols), max_errors)
        return validator

    def _start(self, rules: _Rules, sdmx_cols: list[str], max_errors: int) -> None:
        self._rules = rules
        self._sdmx_cols = sdmx_cols
        self._max_errors = max_errors
        self._valid_ids = {col: set(codes) for col, codes in rules.codelist_ids.items()}
        self._rows = 0
        self._unexpected: dict[str, None] = {}
        self._missing_columns: set[str] = set()
        self._invalid: dict[str, dict[str, None]] = {
            col: {} for col in rules.codelist_ids
        }
        self._duplicate_rows = 0
        self._duplicate_key_total = 0
        self._duplicate_keys: list[pd.DataFrame] = []
        self._missing_rows = 0
        self._missing_samples: list[pd.DataFrame] = []

    @property
    def rows(self) -> int:
        """The number of rows seen so far."""
        return self._rows

    def update(self, df: pd.DataFrame) -> None:
        """Validate one chunk and keep what the report needs.

        The column checks run on every chunk and their findings are merged.
        The value checks (codelists, duplicates, missing values) run on a
        chunk only when it carries every mandatory column, as
        :func:`validate_dataset_local` does.

        Args:
            df: A chunk of the dataset. Every chunk should carry the same
                columns.
        """
        self._rows += len(df)
        rules = self._rules
        for col in _get_unexpected_columns(df, rules.valid_comp, self._sdmx_cols):
            self._unexpected.setdefault(col)
        required = set(rules.mandatory_comp) | set(self._sdmx_cols)
        missing = required - set(df.columns)
        if missing:
            self._missing_columns |= missing
            return
        self._record_codelist_violations(df)
        self._record_duplicates(df)
        self._record_missing_values(df)

    def _record_codelist_violations(self, df: pd.DataFrame) -> None:
        for col, valid_ids in self._valid_ids.items():
            seen = self._invalid[col]
            if col not in df.columns or len(seen) >= self._max_errors:
                continue
            for value in _invalid_values(df[col], valid_ids):
                if len(seen) >= self._max_errors:
                    break
                seen.setdefault(value)

    def _record_duplicates(self, df: pd.DataFrame) -> None:
        n_rows, keys = _duplicate_keys(df, self._rules.dim_comp)
        if not n_rows:
            return
        self._duplicate_rows += n_rows
        self._duplicate_key_total += len(keys)
        kept = sum(len(sample) for sample in self._duplicate_keys)
        if kept < self._max_errors or not self._duplicate_keys:
            self._duplicate_keys.append(keys.head(max(self._max_errors - kept, 0)))

    def _record_missing_values(self, df: pd.DataFrame) -> None:
        missing_mask = _missing_rows_mask(df, self._rules.mandatory_comp)
        n_rows = int(missing_mask.sum())
        if not n_rows:
            return
        self._missing_rows += n_rows
        kept = sum(len(sample) for sample in self._missing_samples)
        if kept < self._max_errors or not self._missing_samples:
            sample = df.loc[missing_mask].head(max(self._max_errors - kept, 0))
            self._missing_samples.append(sample[self._rules.mandatory_comp])

    def errors(self) -> pd.DataFrame:
        """Return the errors found so far.

        Returns:
            A DataFrame with one error per row and the columns ``Validation``
            and ``Error``, as :func:`validate_dataset_local` returns. Empty
            when the chunks seen so far are valid. Calling this does not end
            the validator: more chunks may follow.
        """
        max_errors = self._max_errors
        records: list[dict[str, str]] = []

        unexpected = list(self._unexpected)
        capped_cols = unexpected[:max_errors]
        records.extend(
            {"Validation": "columns", "Error": f"Unexpected column: '{col}'"}
            for col in capped_cols
        )
        note = _truncation_note(len(capped_cols), len(unexpected), max_errors)
        if note:
            records.append({"Validation": "columns", "Error": note.strip()})

        if self._missing_columns:
            records.append(
                {
                    "Validation": "mandatory_columns",
                    "Error": f"Missing mandatory columns: "
                    f"{sorted(self._missing_columns)}",
                }
            )

        violations = [
            (col, value) for col, seen in self._invalid.items() for value in seen
        ]
        records.extend(
            {"Validation": "codelist_ids", "Error": f"'{col}': {value}"}
            for col, value in violations[:max_errors]
        )

        if self._duplicate_rows:
            shown = pd.concat(self._duplicate_keys).drop_duplicates().head(max_errors)
            records.append(
                {
                    "Validation": "duplicates",
                    "Error": _duplicates_message(
                        self._duplicate_rows,
                        shown,
                        self._duplicate_key_total,
                        self._rules.dim_comp,
                        max_errors,
                    ),
                }
            )

        if self._missing_rows:
            sample = pd.concat(self._missing_samples)
            records.append(
                {
                    "Validation": "missing_values",
                    "Error": _missing_values_message(
                        self._missing_rows,
                        sample,
                        self._rules.mandatory_comp,
                        max_errors,
                    ),
                }
            )

        return pd.DataFrame(records, columns=["Validation", "Error"])


@typechecked
def validate_dataset_local(
    df: pd.DataFrame,
    schema: Schema | None = None,
    valid: dict[str, object] | None = None,
    sdmx_cols: list[str] | tuple[str, ...] | None = None,
    max_errors: int = 1000,
) -> pd.DataFrame:
    """Validate that a DataFrame is SDMX compliant and return a DataFrame of errors.

    Pass ``schema`` so validation info (including the expected SDMX reference
    columns) can be inferred. The ``valid`` parameter is retained as a
    deprecated shim for callers that previously cached the output of
    :func:`~tidysdmx.utils.extract_validation_info`.

    For a dataset that does not fit in memory, feed its chunks to
    :class:`DatasetValidator` instead; this function is one chunk of it.

    Args:
        df: The DataFrame to be validated.
        schema: The schema object (optional if ``valid`` is provided).
        valid: **Deprecated.** Precomputed validation information returned by
            :func:`~tidysdmx.utils.extract_validation_info`. This argument
            will be removed in a future release; pass ``schema`` directly
            instead.
        sdmx_cols: SDMX reference columns expected in the dataset, as a
            list or tuple. When omitted, they resolve to the standard
            SDMX-CSV columns ``['STRUCTURE', 'STRUCTURE_ID', 'ACTION']``
            for every schema context; if only a ``valid`` dict is given,
            context-specific column sets cached under tidysdmx <= 0.9.0
            (e.g. ``['DATAFLOW', 'DATAFLOW_ID', 'ACTION']``) are likewise
            normalized to the standard columns.
        max_errors: Maximum number of individual errors to report per
            validation check. Defaults to ``1000``.

    Returns:
        A DataFrame containing validation errors. Each row is one error, with
        columns ``Validation`` and ``Error``.
    """
    if valid is not None:
        warnings.warn(
            "The `valid` argument of validate_dataset_local is deprecated "
            "and will be removed in a future release. Pass `schema` instead.",
            FutureWarning,
            stacklevel=2,
        )

    if valid is None:
        if schema is None:
            raise ValueError("Either a schema or precomputed 'valid' must be provided.")
        valid = extract_validation_info(schema)

    if sdmx_cols is not None:
        sdmx_cols = list(sdmx_cols)
    elif schema is not None:
        # The schema is authoritative: ignore `valid["sdmx_cols"]`, which may
        # carry stale context-specific names cached under tidysdmx <= 0.9.0.
        sdmx_cols = sdmx_reference_cols_for(schema.context)
    else:
        # TODO(deprecated): fallbacks for callers passing only a cached
        # `valid` dict. Remove once the deprecated `valid` parameter itself
        # is dropped.
        inferred = valid.get("sdmx_cols")
        sdmx_cols = list(inferred) if inferred is not None else list(_DEFAULT_SDMX_COLS)
        if sdmx_cols in _LEGACY_CONTEXT_SDMX_COLS:
            sdmx_cols = list(_DEFAULT_SDMX_COLS)

    validator = DatasetValidator._from_info(valid, sdmx_cols, max_errors)
    validator.update(df)
    return validator.errors()


@typechecked
def validate_columns(
    df: pd.DataFrame,
    valid_columns: list[str],
    sdmx_cols: list[str] | tuple[str, ...] | None = None,
    max_errors: int = 1000,
) -> None:
    """Validate that all DataFrame columns are valid components or SDMX references.

    Args:
        df: The DataFrame to validate.
        valid_columns: List of valid component names.
        sdmx_cols: Additional allowed column names, as a list or tuple.
            Defaults to ``['STRUCTURE', 'STRUCTURE_ID', 'ACTION']``.
        max_errors: Maximum number of unexpected columns to include in the
            error message. Defaults to ``1000``.

    Raises:
        ValueError: If any columns in the DataFrame are not in ``valid_columns``
            or ``sdmx_cols``, listing all offending names up to ``max_errors``.
    """
    sdmx_cols = list(_DEFAULT_SDMX_COLS) if sdmx_cols is None else list(sdmx_cols)
    unexpected = _get_unexpected_columns(df, valid_columns, sdmx_cols)
    if unexpected:
        capped = unexpected[:max_errors]
        msg = f"Found unexpected columns: {capped}"
        msg += _truncation_note(len(capped), len(unexpected), max_errors)
        raise ValueError(msg)


@typechecked
def validate_mandatory_columns(
    df: pd.DataFrame,
    mandatory_columns: list[str],
    sdmx_cols: list[str] | tuple[str, ...] | None = None,
) -> None:
    """Validate that all mandatory columns are present in the DataFrame.

    Args:
        df: The DataFrame to validate.
        mandatory_columns: List of mandatory component names.
        sdmx_cols: Additional mandatory column names, as a list or tuple.
            Defaults to ``['STRUCTURE', 'STRUCTURE_ID', 'ACTION']``.

    Raises:
        ValueError: If any mandatory column is absent from the DataFrame.
    """
    sdmx_cols = list(_DEFAULT_SDMX_COLS) if sdmx_cols is None else list(sdmx_cols)
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
    ``max_errors`` entries. Values are compared as ``str``, one distinct value
    at a time, so a missing value is reported as ``nan``.

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
            truncated = f" … capped (max_errors={max_errors})"
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
    n_rows, keys = _duplicate_keys(df, dim_comp)
    if n_rows:
        raise ValueError(
            _duplicates_message(
                n_rows, keys.head(max_errors), len(keys), dim_comp, max_errors
            )
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
    missing_mask = _missing_rows_mask(df, mandatory_columns)
    if missing_mask.any():
        sample = df.loc[missing_mask].head(max_errors)
        raise ValueError(
            _missing_values_message(
                int(missing_mask.sum()), sample, mandatory_columns, max_errors
            )
        )
