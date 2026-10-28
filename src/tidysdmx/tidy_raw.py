"""Filter SDMX DataFrames against schema codelist constraints."""

import numpy as np
import pandas as pd
from pysdmx.model import Schema
from typeguard import typechecked

from .utils import extract_validation_info


@typechecked
def filter_rows(
    df: pd.DataFrame,
    codelist_ids: dict[str, list[str]],
) -> pd.DataFrame:
    """Filter out rows where values are not in the allowed codelist.

    Compares as strings but does not change DataFrame dtypes. Only the
    distinct values of a column are compared, so a column costs its number
    of distinct values rather than its rows. Does not mutate the input
    DataFrame.

    Args:
        df: The input DataFrame.
        codelist_ids: A mapping of column names to allowed codelist IDs.

    Returns:
        A filtered copy of the DataFrame containing only selected rows.
    """
    if not codelist_ids:
        return df.copy()

    rows_to_drop = np.zeros(len(df), dtype=bool)

    for col, allowed in codelist_ids.items():
        if col not in df.columns:
            continue
        allowed_str = set(map(str, allowed))
        # Judge each distinct value once, then look the verdicts up by row.
        codes, uniques = pd.factorize(df[col])
        allowed_unique = pd.Series(uniques).astype(str).isin(allowed_str).to_numpy()
        present = codes >= 0  # a missing value (code -1) is never dropped
        unselected = np.zeros(len(df), dtype=bool)
        unselected[present] = ~allowed_unique[codes[present]]
        rows_to_drop |= unselected

    return df.loc[~rows_to_drop].copy()


@typechecked
def filter_tidy_raw(
    df: pd.DataFrame,
    schema: Schema,
) -> pd.DataFrame:
    """Filter an SDMX DataFrame by removing rows that violate codelist constraints.

    Args:
        df: The input DataFrame.
        schema: The SDMX schema to validate against.

    Returns:
        A filtered DataFrame with invalid code rows removed.
    """
    valid = extract_validation_info(schema)

    return filter_rows(
        df=df,
        codelist_ids=valid.get("codelist_ids", {}),
    )
