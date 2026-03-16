import re

import pandas as pd
import pytest

from tidysdmx.fake import generate_fake_dataset


@pytest.mark.unit
def test_generate_fake_dataset_columns_match_schema(fake_schema_basic):
    df = generate_fake_dataset(fake_schema_basic, n=10, seed=42)
    expected = {"FREQ", "REF_AREA", "TIME_PERIOD", "OBS_VALUE", "COMMENT"}
    assert set(df.columns) == expected


@pytest.mark.unit
def test_generate_fake_dataset_row_count(fake_schema_basic):
    df = generate_fake_dataset(fake_schema_basic, n=25, seed=42)
    assert len(df) == 25


@pytest.mark.unit
def test_generate_fake_dataset_seed_reproducibility(fake_schema_basic):
    df1 = generate_fake_dataset(fake_schema_basic, n=20, seed=42)
    df2 = generate_fake_dataset(fake_schema_basic, n=20, seed=42)
    pd.testing.assert_frame_equal(df1, df2)


@pytest.mark.unit
def test_generate_fake_dataset_coded_components(fake_schema_basic):
    df = generate_fake_dataset(fake_schema_basic, n=50, seed=42)
    assert set(df["FREQ"].unique()).issubset({"A", "M", "Q"})
    assert set(df["REF_AREA"].unique()).issubset({"USA", "FRA", "GBR"})


@pytest.mark.unit
def test_generate_fake_dataset_numeric_measure(fake_schema_basic):
    df = generate_fake_dataset(fake_schema_basic, n=20, seed=42)
    assert pd.api.types.is_float_dtype(df["OBS_VALUE"])


@pytest.mark.unit
def test_generate_fake_dataset_integer_measure(fake_schema_integer_measure):
    df = generate_fake_dataset(fake_schema_integer_measure, n=50, seed=42)
    values = df["OBS_VALUE"].dropna()
    assert all(0 <= v <= 1000 for v in values)


@pytest.mark.unit
def test_generate_fake_dataset_time_period_annual(fake_schema_annual_only):
    df = generate_fake_dataset(fake_schema_annual_only, n=30, seed=42)
    pattern = re.compile(r"^\d{4}$")
    for val in df["TIME_PERIOD"]:
        assert pattern.match(str(val)), f"Invalid annual period: {val}"


@pytest.mark.unit
def test_generate_fake_dataset_time_period_monthly(fake_schema_monthly):
    df = generate_fake_dataset(fake_schema_monthly, n=30, seed=42)
    pattern = re.compile(r"^\d{4}-M\d{2}$")
    for val in df["TIME_PERIOD"]:
        assert pattern.match(str(val)), f"Invalid monthly period: {val}"


@pytest.mark.unit
def test_generate_fake_dataset_time_period_quarterly(fake_schema_quarterly):
    df = generate_fake_dataset(fake_schema_quarterly, n=30, seed=42)
    pattern = re.compile(r"^\d{4}-Q\d$")
    for val in df["TIME_PERIOD"]:
        assert pattern.match(str(val)), f"Invalid quarterly period: {val}"


@pytest.mark.unit
def test_generate_fake_dataset_boolean_attribute(fake_schema_boolean):
    df = generate_fake_dataset(fake_schema_boolean, n=50, seed=42)
    non_null = df["IS_ESTIMATED"].dropna()
    assert all(isinstance(v, bool) for v in non_null)


@pytest.mark.unit
def test_generate_fake_dataset_optional_attribute_has_nulls(fake_schema_basic):
    df = generate_fake_dataset(fake_schema_basic, n=200, seed=42)
    assert df["COMMENT"].isna().any(), "Optional attribute should have nulls"


@pytest.mark.unit
def test_generate_fake_dataset_required_columns_no_nulls(fake_schema_basic):
    df = generate_fake_dataset(fake_schema_basic, n=50, seed=42)
    for col in ["FREQ", "REF_AREA", "TIME_PERIOD", "OBS_VALUE"]:
        assert not df[col].isna().any(), f"{col} should have no nulls"


@pytest.mark.unit
def test_generate_fake_dataset_returns_dataframe(fake_schema_basic):
    result = generate_fake_dataset(fake_schema_basic, n=5, seed=42)
    assert isinstance(result, pd.DataFrame)
