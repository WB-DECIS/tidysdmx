import pandas as pd

def qa_coerce_numeric(
        df: pd.DataFrame, 
        numeric_columns: list[str]
    ) -> pd.DataFrame:
    """Coerces values from specified numeric columns to numeric, and removes rows where values cannot be coerced to numeric.

    Args:
        df (pd.DataFrame): The input DataFrame.
        numeric_columns (list): A list of column names to be coerced to numeric.

    Returns:
        pd.DataFrame: The DataFrame with numeric columns coerced to numeric and invalid rows removed.
    """
    for column in numeric_columns:
        if column in df.columns:
            # Coerce column to numeric, setting errors='coerce' will replace invalid parsing with NaN
            df[column] = pd.to_numeric(df[column], errors="coerce")

            # Identify rows with NaN values in the numeric column
            invalid_rows = df[df[column].isna()]

            if not invalid_rows.empty:
                # Log information about the rows being deleted
                print(
                    f"Removing {len(invalid_rows)} rows from column '{column}' that cannot be coerced to numeric."
                )
                print(f"Invalid rows:\n{invalid_rows}")

                # Remove rows with NaN values in the numeric column
                df = df.dropna(subset=[column])

    return df


def qa_remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Removes duplicate rows from the DataFrame and logs information about the rows being removed.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with duplicate rows removed.
    """
    initial_length = len(df)

    # Remove duplicate rows
    df = df.drop_duplicates()

    final_length = len(df)
    duplicates_removed = initial_length - final_length

    if duplicates_removed > 0:
        # Log information about the rows being removed
        print(f"Removed {duplicates_removed} duplicate rows.")

    return df

