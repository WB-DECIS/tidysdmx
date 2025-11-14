import pytest
from pathlib import Path
import pandas as pd

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
