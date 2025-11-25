# tests/fixtures/fmr_api.py
import pytest
#import pickle as pkl
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
        "source": ["AR", "UY", "FR"],
        "target": ["ARG", "URY", "FRA"]
        })
    
        # Cache as CSV
        df.to_csv(cache_file, index=False)

    return df