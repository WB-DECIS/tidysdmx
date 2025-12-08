import pytest
from pathlib import Path
import pandas as pd

# Global variables definitions
incorrect_ind_code = "INCORRECT_IND"

# Directory for cached responses
CACHE_DIR = Path(__file__).parent / "data"
CACHE_DIR.mkdir(exist_ok=True)

@pytest.fixture(scope="session")
def sample_df():
    cache_file = CACHE_DIR / "sample_df.csv"
    
    sample_df = pd.DataFrame({
        "code": ["1", "2", "3", None, "4"],
        "status": ["A", "B", "C", "D", None]
    })
    
    # Cache as CSV
    sample_df.to_csv(cache_file, index=False)

    return sample_df

@pytest.fixture(scope="session")
def sdmx_df():
    cache_file = CACHE_DIR / "sdmx_df.csv"
    
    sdmx_df = pd.DataFrame({
        "INDICATOR": ["IND1", "IND1", "IND1", "IND1", "IND1", "IND1", "IND2", "IND2", "IND2", "IND2", "IND2", "IND2", "IND3", "IND3", "IND3", "IND3", "IND3", "IND3", incorrect_ind_code],
        "TIME_PERIOD": [2010, 2010, 2010, 2020, 2020, 2020, 2010, 2010, 2010, 2020, 2020, 2020, 2010, 2010, 2010, 2020, 2020, 2020, 1000],
        "SEX": ["M", "F", "_T", "M", "F", "_T", "M", "F", "_T", "M", "F", "_T", "M", "F", "_T", "M", "F", "_T", "XXX"],
        "OBS_VALUE": [51, 49, 100, 50, 50, 100, 51, 49, 100, 50, 50, 100, 51, 49, 100, 50, 50, 100, 200 ]
    })
    
    # Cache as CSV
    sdmx_df.to_csv(cache_file, index=False)

    return sdmx_df

@pytest.fixture(scope="session")
def ifpri_asti_df():
    """Fixture to load a ifpri_asti sample CSV file into a pandas DataFrame."""
    data_path = CACHE_DIR / "ifpri_asti_sample.csv"
    df = pd.read_csv(data_path)
    
    return df



