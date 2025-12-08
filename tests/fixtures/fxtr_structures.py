# tests/fixtures/fmr_api.py
import pytest
import pickle as pkl
from pathlib import Path
import pysdmx as px
from pysdmx.model.map import (
    RepresentationMap, 
    FixedValueMap, 
    ImplicitComponentMap, 
    DatePatternMap, 
    ValueMap, 
    MultiValueMap
    )
import pandas as pd

# Directory for cached responses
CACHE_DIR = Path(__file__).parent / "data/structures"
CACHE_DIR.mkdir(exist_ok=True)

@pytest.fixture(scope="session")
def value_map_df_mandatory_cols():
    """Session-scoped fixture returning a DataFrame with mandatory 'source' and 'target' columns.

    Caches the data to CSV on first run and reloads it on subsequent runs.
    """
    cache_file = CACHE_DIR / "value_map_df_mandatory_cols.csv"

    if cache_file.exists():
        # Load cached response
        df = pd.read_csv(cache_file)
        assert {"source", "target"}.issubset(df.columns)

    else:
        df = pd.DataFrame({
        "source": ["regex:^A", "UY", "FR"],
        "target": ["ARG", "URY", "FRA"]
        })
    
        # Cache as CSV
        df.to_csv(cache_file, index=False)

    return df


@pytest.fixture(scope="session")
def multi_value_map_df():
    """Session-scoped fixture returning a DataFrame for testing build_multi_value_map_list.

    Covers:
        - Normal case with multiple source columns and single target column.
        - Edge case with regex pattern in source.
        - Missing validity dates.
        - Validity dates present.
        - Empty strings in source.
        - Mixed valid and invalid rows for type checking.

    Caches the data to CSV on first run and reloads it on subsequent runs.
    """
    cache_file = CACHE_DIR / "multi_value_map_df.pkl"

    if cache_file.exists():   
    # Load cached DataFrame with preserved types
        with open(cache_file, "rb") as f:
            df = pkl.load(f)
        expected_cols = {"country", "currency", "iso_code", "valid_from", "valid_to"}
        assert expected_cols.issubset(df.columns)

    else:
        df = pd.DataFrame({
            "country": [
                "DE",          # Normal case
                "regex:^A",    # Regex pattern
                "",            # Empty string
                "FR",          # Normal case with validity
                123            # Invalid type for edge case
            ],
            "currency": [
                "LC",          # Normal case
                "LC",          # Regex case
                "LC",          # Empty source case
                "LC",          # Validity case
                "LC"           # Invalid type case
            ],
            "iso_code": [
                "EUR",         # Normal target
                "ARG",         # Regex target
                "CHF",         # Empty source case
                "FRA",         # Validity case
                "CHF"          # Invalid type case
            ],
            "valid_from": [
                None,          # No validity
                None,          # No validity
                None,          # No validity
                "2020-01-01",  # Validity start
                None           # Invalid type row
            ],
            "valid_to": [
                None,          # No validity
                None,          # No validity
                None,          # No validity
                "2025-12-31",  # Validity end
                None           # Invalid type row
            ]
        })
        
        # Cache using pickle
        with open(cache_file, "wb") as f:
            pkl.dump(df, f)


    return df
