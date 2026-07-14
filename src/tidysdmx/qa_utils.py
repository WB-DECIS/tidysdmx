"""QA helpers: coerce numeric columns and remove duplicate rows."""

import logging

import pandas as pd
from typeguard import typechecked

from ._deprecation import deprecated

logger = logging.getLogger(__name__)


@deprecated()
@typechecked
def qa_coerce_numeric(
    df: pd.DataFrame,
    numeric_columns: list[str],
) -> pd.DataFrame:
    """Coerce specified columns to numeric, removing rows with invalid values.

    .. deprecated::
        This QA helper is part of the retiring standardisation pipeline and
        will be removed in a future release.

    Args:
        df: The input DataFrame.
        numeric_columns: Column names to coerce to numeric.

    Returns:
        A new DataFrame with numeric columns coerced and invalid rows removed.
    """
    df = df.copy()

    for column in numeric_columns:
        if column not in df.columns:
            continue

        df[column] = pd.to_numeric(df[column], errors="coerce")
        invalid_rows = df[df[column].isna()]

        if not invalid_rows.empty:
            logger.info(
                "Removing %d rows from column '%s' that cannot be coerced to numeric.",
                len(invalid_rows),
                column,
            )
            logger.debug("Invalid rows:\n%s", invalid_rows)
            df = df.dropna(subset=[column])

    return df


@typechecked
def qa_remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows from a DataFrame.

    Args:
        df: The input DataFrame.

    Returns:
        A new DataFrame with duplicate rows removed.
    """
    initial_length = len(df)
    df = df.drop_duplicates()
    duplicates_removed = initial_length - len(df)

    if duplicates_removed > 0:
        logger.info("Removed %d duplicate rows.", duplicates_removed)

    return df
