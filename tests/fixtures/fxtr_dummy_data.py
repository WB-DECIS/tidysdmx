from pathlib import Path

import pandas as pd
import pytest

# Global variables definitions
incorrect_ind_code = "INCORRECT_IND"

# Directory for committed sample data read by fixtures below.
CACHE_DIR = Path(__file__).parent / "data"
CACHE_DIR.mkdir(exist_ok=True)


# Function-scoped (rebuilt per test) so a test that mutates the frame cannot
# leak state into another; the frames are tiny, so the cost is negligible.
@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {"code": ["1", "2", "3", None, "4"], "status": ["A", "B", "C", "D", None]}
    )


@pytest.fixture
def sdmx_df():
    return pd.DataFrame(
        {
            "INDICATOR": [
                "IND1",
                "IND1",
                "IND1",
                "IND1",
                "IND1",
                "IND1",
                "IND2",
                "IND2",
                "IND2",
                "IND2",
                "IND2",
                "IND2",
                "IND3",
                "IND3",
                "IND3",
                "IND3",
                "IND3",
                "IND3",
                incorrect_ind_code,
            ],
            "TIME_PERIOD": [
                2010,
                2010,
                2010,
                2020,
                2020,
                2020,
                2010,
                2010,
                2010,
                2020,
                2020,
                2020,
                2010,
                2010,
                2010,
                2020,
                2020,
                2020,
                1000,
            ],
            "SEX": [
                "M",
                "F",
                "_T",
                "M",
                "F",
                "_T",
                "M",
                "F",
                "_T",
                "M",
                "F",
                "_T",
                "M",
                "F",
                "_T",
                "M",
                "F",
                "_T",
                "XXX",
            ],
            "OBS_VALUE": [
                51,
                49,
                100,
                50,
                50,
                100,
                51,
                49,
                100,
                50,
                50,
                100,
                51,
                49,
                100,
                50,
                50,
                100,
                200,
            ],
        }
    )


@pytest.fixture
def ifpri_asti_df():
    """Fixture to load a ifpri_asti sample CSV file into a pandas DataFrame."""
    data_path = CACHE_DIR / "ifpri_asti_sample.csv"
    df = pd.read_csv(data_path)

    return df
