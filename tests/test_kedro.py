"""Tests for the deprecated Kedro node wrappers."""

import pandas as pd
import pytest

from tidysdmx.kedro import kd_standardize_sdmx


@pytest.mark.unit
class TestKdStandardizeSdmx:
    """Behaviour of kd_standardize_sdmx with multiple mapping files."""

    def test_key_mismatch_raises(self):
        """Mismatched data/mappings keys raise instead of dropping partitions."""
        data = {
            "ds1.csv": lambda: pd.DataFrame({"a": [1]}),
            "extra.csv": lambda: pd.DataFrame({"a": [2]}),
        }
        mappings = {"ds1": {}, "missing": {}}

        with (
            pytest.warns(FutureWarning, match="kd_standardize_sdmx is deprecated"),
            pytest.raises(
                ValueError, match="keys of both dictionaries should be the same"
            ),
        ):
            kd_standardize_sdmx(data, mappings)
