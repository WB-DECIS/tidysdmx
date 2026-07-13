import numpy as np
import pandas as pd
import pytest

from tidysdmx.validation import (
    validate_codelist_ids,
    validate_columns,
    validate_dataset_local,
    validate_duplicates,
    validate_mandatory_columns,
    validate_no_missing_values,
)


class TestValidateNoMissingValues:
    def test_no_missing(self):
        """Pass when no mandatory values are missing."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        validate_no_missing_values(df, mandatory_columns=["col1", "col2"])

    def test_missing_in_one_column(self):
        """Raise when one mandatory column has a null."""
        df = pd.DataFrame({"col1": [1, 2, None], "col2": [4, 5, 6]})
        with pytest.raises(
            ValueError, match="row\\(s\\) with missing values in mandatory columns"
        ):
            validate_no_missing_values(df, mandatory_columns=["col1", "col2"])

    def test_missing_in_multiple_columns(self):
        """Raise when multiple mandatory columns have nulls."""
        df = pd.DataFrame({"col1": [1, None, 3], "col2": [None, 5, 6]})
        with pytest.raises(
            ValueError, match="row\\(s\\) with missing values in mandatory columns"
        ):
            validate_no_missing_values(df, mandatory_columns=["col1", "col2"])

    def test_extra_columns_with_nulls_ignored(self):
        """Pass when non-mandatory columns contain nulls."""
        df = pd.DataFrame(
            {"col1": [1, 2, 3], "col2": [4, 5, 6], "col3": [None, None, None]}
        )
        validate_no_missing_values(df, mandatory_columns=["col1", "col2"])


class TestValidateDuplicates:
    def test_no_duplicates(self):
        """Pass when no duplicate key combinations exist."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        validate_duplicates(df, dim_comp=["col1", "col2"])

    def test_with_duplicates(self):
        """Raise when duplicate key combinations are found."""
        df = pd.DataFrame({"col1": [1, 2, 2], "col2": [4, 5, 5]})
        with pytest.raises(ValueError, match="duplicate rows"):
            validate_duplicates(df, dim_comp=["col1", "col2"])


class TestValidateMandatoryColumns:
    def test_all_present(self):
        """Pass when all mandatory columns are present."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        validate_mandatory_columns(df, mandatory_columns=["col1", "col2"], sdmx_cols=[])

    def test_missing(self):
        """Raise when a mandatory column is absent."""
        df = pd.DataFrame({"col1": [1, 2, 3]})
        with pytest.raises(ValueError, match="Missing mandatory columns"):
            validate_mandatory_columns(
                df, mandatory_columns=["col1", "col2"], sdmx_cols=[]
            )


class TestValidateColumns:
    @pytest.mark.parametrize(
        "df_columns, valid_columns, sdmx_cols",
        [
            (
                ["STRUCTURE", "STRUCTURE_ID", "ACTION"],
                ["COMP1", "COMP2"],
                ["STRUCTURE", "STRUCTURE_ID", "ACTION"],
            ),
            (
                ["COMP1", "COMP2"],
                ["COMP1", "COMP2"],
                ["STRUCTURE", "STRUCTURE_ID", "ACTION"],
            ),
            (
                ["COMP1", "STRUCTURE"],
                ["COMP1"],
                ["STRUCTURE", "STRUCTURE_ID", "ACTION"],
            ),
        ],
    )
    def test_valid_columns_pass(self, df_columns, valid_columns, sdmx_cols):
        """Tests that validate_columns passes when all columns are valid."""
        df = pd.DataFrame(columns=df_columns)
        validate_columns(df, valid_columns=valid_columns, sdmx_cols=sdmx_cols)

    @pytest.mark.parametrize(
        "df_columns, valid_columns, sdmx_cols, invalid_col",
        [
            (
                ["COMP1", "INVALID"],
                ["COMP1"],
                ["STRUCTURE", "STRUCTURE_ID", "ACTION"],
                "INVALID",
            ),
            (
                ["STRUCTURE", "BAD_COL"],
                ["COMP1", "COMP2"],
                ["STRUCTURE", "STRUCTURE_ID", "ACTION"],
                "BAD_COL",
            ),
        ],
    )
    def test_invalid_column_raises_value_error(
        self, df_columns, valid_columns, sdmx_cols, invalid_col
    ):
        """Tests that validate_columns raises ValueError for unexpected columns."""
        df = pd.DataFrame(columns=df_columns)
        with pytest.raises(ValueError) as exc_info:
            validate_columns(df, valid_columns=valid_columns, sdmx_cols=sdmx_cols)
        assert "Found unexpected columns" in str(exc_info.value)
        assert invalid_col in str(exc_info.value)

    def test_empty_dataframe_passes(self):
        """Tests that an empty DataFrame passes validation."""
        df = pd.DataFrame()
        validate_columns(
            df,
            valid_columns=["COMP1"],
            sdmx_cols=["STRUCTURE", "STRUCTURE_ID", "ACTION"],
        )

    def test_only_sdmx_columns_pass(self):
        """Tests that DataFrame with only SDMX columns passes validation."""
        df = pd.DataFrame(columns=["STRUCTURE", "STRUCTURE_ID"])
        validate_columns(
            df, valid_columns=[], sdmx_cols=["STRUCTURE", "STRUCTURE_ID", "ACTION"]
        )


class TestValidateCodelistIds:
    @pytest.fixture()
    def sample_codelist_ids(self):
        """Fixture that returns a dictionary of allowed IDs for columns."""
        return {
            "col1": ["A1", "A2"],
            "col2": ["B1", "B2"],
        }

    def test_valid_values_pass(self, sample_codelist_ids):
        """Tests that DataFrame with valid values passes without error."""
        df = pd.DataFrame({"col1": ["A1", "A2"], "col2": ["B1", "B2"]})
        validate_codelist_ids(df, sample_codelist_ids)

    def test_invalid_value_raises_error(self, sample_codelist_ids):
        """Tests that invalid values raise ValueError."""
        df = pd.DataFrame({"col1": ["A1", "INVALID"], "col2": ["B1", "B2"]})
        with pytest.raises(ValueError, match="Invalid codelist values found"):
            validate_codelist_ids(df, sample_codelist_ids)

    def test_multiple_invalid_values(self, sample_codelist_ids):
        """Tests that invalid values across multiple columns are all reported."""
        df = pd.DataFrame(
            {"col1": ["INVALID1", "INVALID2"], "col2": ["INVALID3", "B2"]}
        )
        with pytest.raises(ValueError) as excinfo:
            validate_codelist_ids(df, sample_codelist_ids)
        msg = str(excinfo.value)
        assert "col1" in msg and "col2" in msg

    def test_column_not_in_dataframe_is_ignored(self, sample_codelist_ids):
        """Tests that columns not present in DataFrame are ignored."""
        df = pd.DataFrame({"col1": ["A1", "A2"]})
        validate_codelist_ids(df, sample_codelist_ids)

    def test_empty_dataframe_passes(self, sample_codelist_ids):
        """Tests that an empty DataFrame passes without error."""
        df = pd.DataFrame(columns=["col1", "col2"])
        validate_codelist_ids(df, sample_codelist_ids)

    def test_missing_values_are_not_codelist_violations(self, sample_codelist_ids):
        """Missing cells (NaN or None) are not codelist violations (API-06).

        Missing values are the missing-values check's job; a NaN must not be
        stringified into a spurious ``'nan'`` code here, nor a None into a
        spurious ``'None'`` code.
        """
        df = pd.DataFrame({"col1": ["A1", np.nan, None], "col2": ["B1", "B2", "B1"]})
        # No ValueError, and in particular no 'nan'/'None' reported.
        validate_codelist_ids(df, sample_codelist_ids)

    def test_missing_value_alongside_real_violation(self, sample_codelist_ids):
        """NaN/None are ignored while a genuine out-of-codelist value still fails."""
        df = pd.DataFrame(
            {"col1": ["A1", np.nan, None, "WRONG"], "col2": ["B1", "B2", "B1", "B1"]}
        )
        with pytest.raises(ValueError, match="Invalid codelist values found") as exc:
            validate_codelist_ids(df, sample_codelist_ids)
        msg = str(exc.value)
        assert "WRONG" in msg
        assert "nan" not in msg
        assert "None" not in msg

    @pytest.mark.parametrize(
        "df_values,expected_error",
        [
            ({"col1": ["A1", "WRONG"], "col2": ["B1", "B2"]}, "col1"),
            ({"col1": ["A1", "A2"], "col2": ["WRONG", "B2"]}, "col2"),
        ],
    )
    def test_parametrized_invalid_values(
        self, df_values, expected_error, sample_codelist_ids
    ):
        """Tests invalid values in different columns using parametrization."""
        df = pd.DataFrame(df_values)
        with pytest.raises(ValueError) as excinfo:
            validate_codelist_ids(df, sample_codelist_ids)
        assert expected_error in str(excinfo.value)


class TestValidateDatasetLocal:
    # Existing tests pass a precomputed ``valid`` dict via fixture. That path
    # is deprecated (see #218 follow-up) and emits FutureWarning. Suppress it
    # at the class level so the legacy assertions stay focused on validation
    # output. Tests that specifically assert the warning override this with
    # ``pytest.warns``.
    pytestmark = pytest.mark.filterwarnings("ignore::FutureWarning")

    @pytest.fixture()
    def valid_info(self):
        """Fixture providing a mock valid dictionary for validation."""
        return {
            "valid_comp": ["TIME_PERIOD", "OBS_VALUE", "AREA", "INDICATOR"],
            "mandatory_comp": ["TIME_PERIOD", "OBS_VALUE", "AREA"],
            "codelist_ids": {
                "AREA": ["COL", "SWZ"],
                "INDICATOR": ["RES_FEMALE_TOT_FTE", "RES_MALE_TOT_FTE"],
            },
            "dim_comp": ["TIME_PERIOD", "AREA"],
            "sdmx_cols": ["STRUCTURE", "STRUCTURE_ID", "ACTION"],
        }

    def test_valid_dataset_returns_empty_df(self, valid_info):
        """Tests that a fully valid dataset returns an empty DataFrame."""
        df = pd.DataFrame(
            {
                "TIME_PERIOD": ["2020", "2021"],
                "OBS_VALUE": [100, 200],
                "AREA": ["COL", "SWZ"],
                "INDICATOR": ["RES_FEMALE_TOT_FTE", "RES_MALE_TOT_FTE"],
                "STRUCTURE": ["X", "X"],
                "STRUCTURE_ID": ["Y", "Y"],
                "ACTION": ["A", "A"],
            }
        )
        result = validate_dataset_local(df, valid=valid_info)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_unexpected_column_error(self, valid_info):
        """Tests that unexpected columns produce one error row per column."""
        df = pd.DataFrame(
            {
                "TIME_PERIOD": ["2020"],
                "OBS_VALUE": [100],
                "AREA": ["COL"],
                "INDICATOR": ["RES_FEMALE_TOT_FTE"],
                "EXTRA_COL": ["oops"],
            }
        )
        result = validate_dataset_local(df, valid=valid_info)
        col_errors = result[result["Validation"] == "columns"]
        assert not col_errors.empty
        assert any(
            "Unexpected column: 'EXTRA_COL'" in e for e in col_errors["Error"].values
        )

    def test_missing_mandatory_columns_error(self, valid_info):
        """Tests that missing mandatory columns produce an error record."""
        df = pd.DataFrame(
            {
                "TIME_PERIOD": ["2020"],
                "OBS_VALUE": [100],
                # Missing AREA
            }
        )
        result = validate_dataset_local(df, valid=valid_info)
        assert "mandatory_columns" in result["Validation"].values
        assert "Missing mandatory columns" in result["Error"].iloc[0]

    def test_invalid_codelist_values_error(self, valid_info):
        """Tests that invalid codelist values produce error rows."""
        df = pd.DataFrame(
            {
                "TIME_PERIOD": ["2020"],
                "OBS_VALUE": [100],
                "AREA": ["INVALID"],
                "INDICATOR": ["RES_FEMALE_TOT_FTE"],
                "STRUCTURE": ["X"],
                "STRUCTURE_ID": ["Y"],
                "ACTION": ["A"],
            }
        )
        result = validate_dataset_local(df, valid=valid_info)
        codelist_errors = result[result["Validation"] == "codelist_ids"]
        assert not codelist_errors.empty
        assert any("'AREA': INVALID" in e for e in codelist_errors["Error"].values)
        assert len(codelist_errors) == 1

    def test_duplicate_rows_error(self, valid_info):
        """Tests that duplicate rows produce an error record."""
        df = pd.DataFrame(
            {
                "TIME_PERIOD": ["2020", "2020"],
                "OBS_VALUE": [100, 200],
                "AREA": ["COL", "COL"],
                "INDICATOR": ["RES_FEMALE_TOT_FTE", "RES_FEMALE_TOT_FTE"],
                "STRUCTURE": ["X", "X"],
                "STRUCTURE_ID": ["Y", "Y"],
                "ACTION": ["A", "A"],
            }
        )
        result = validate_dataset_local(df, valid=valid_info)
        dup_errors = result[result["Validation"] == "duplicates"]
        assert not dup_errors.empty
        assert any("duplicate rows" in e for e in dup_errors["Error"].values)

    def test_missing_values_error(self, valid_info):
        """Tests that missing values in mandatory columns produce an error record."""
        df = pd.DataFrame(
            {
                "TIME_PERIOD": ["2020", None],
                "OBS_VALUE": [100, 200],
                "AREA": ["COL", "SWZ"],
                "INDICATOR": ["RES_FEMALE_TOT_FTE", "RES_MALE_TOT_FTE"],
                "STRUCTURE": ["X", "X"],
                "STRUCTURE_ID": ["Y", "Y"],
                "ACTION": ["A", "A"],
            }
        )
        result = validate_dataset_local(df, valid=valid_info)
        mv_errors = result[result["Validation"] == "missing_values"]
        assert not mv_errors.empty
        assert any(
            "missing values in mandatory columns" in e
            for e in mv_errors["Error"].values
        )

    def test_multiple_codelist_violations_produce_multiple_rows(self, valid_info):
        """Tests that violations in two columns produce two separate rows."""
        df = pd.DataFrame(
            {
                "TIME_PERIOD": ["2020"],
                "OBS_VALUE": [100],
                "AREA": ["BAD_AREA"],
                "INDICATOR": ["BAD_INDICATOR"],
                "STRUCTURE": ["X"],
                "STRUCTURE_ID": ["Y"],
                "ACTION": ["A"],
            }
        )
        result = validate_dataset_local(df, valid=valid_info)
        codelist_errors = result[result["Validation"] == "codelist_ids"]
        assert len(codelist_errors) == 2
        error_msgs = codelist_errors["Error"].tolist()
        assert any("'AREA': BAD_AREA" in m for m in error_msgs)
        assert any("'INDICATOR': BAD_INDICATOR" in m for m in error_msgs)

    def test_multiple_unexpected_columns_produce_multiple_rows(self, valid_info):
        """Tests that two unexpected columns produce two separate rows."""
        df = pd.DataFrame(
            {
                "TIME_PERIOD": ["2020"],
                "OBS_VALUE": [100],
                "AREA": ["COL"],
                "INDICATOR": ["RES_FEMALE_TOT_FTE"],
                "EXTRA1": ["x"],
                "EXTRA2": ["y"],
            }
        )
        result = validate_dataset_local(df, valid=valid_info)
        col_errors = result[result["Validation"] == "columns"]
        assert len(col_errors) == 2
        error_msgs = col_errors["Error"].tolist()
        assert any("'EXTRA1'" in m for m in error_msgs)
        assert any("'EXTRA2'" in m for m in error_msgs)

    def test_raises_error_if_no_schema_or_valid(self):
        """Tests that ValueError is raised if neither schema nor valid is provided."""
        df = pd.DataFrame({"TIME_PERIOD": ["2020"]})
        with pytest.raises(ValueError):
            validate_dataset_local(df)

    def test_dataflow_schema_accepts_dataflow_columns(self, sdmx_schema):
        """Dataflow-context schema infers DATAFLOW/DATAFLOW_ID reference columns.

        Regression test for issue #218: validation must not flag DATAFLOW /
        DATAFLOW_ID as unexpected or report STRUCTURE / STRUCTURE_ID as
        missing when the schema's context is ``dataflow``.
        """
        df = pd.DataFrame(
            {
                "INDICATOR": ["IND1", "IND3"],
                "TIME_PERIOD": ["2020", "2021"],
                "SEX": ["F", "M"],
                "OBS_VALUE": [100, 200],
                "DATAFLOW": ["dataflow", "dataflow"],
                "DATAFLOW_ID": ["tidysdmx:tx1(1.0)", "tidysdmx:tx1(1.0)"],
                "ACTION": ["I", "I"],
            }
        )
        result = validate_dataset_local(df, schema=sdmx_schema)
        assert isinstance(result, pd.DataFrame)
        assert result.empty, f"Expected no errors, got:\n{result}"

    def test_explicit_sdmx_cols_overrides_inference(self, sdmx_schema):
        """Caller-provided sdmx_cols win over schema-inferred ones."""
        df = pd.DataFrame(
            {
                "INDICATOR": ["IND1"],
                "TIME_PERIOD": ["2020"],
                "SEX": ["F"],
                "OBS_VALUE": [100],
                "STRUCTURE": ["datastructure"],
                "STRUCTURE_ID": ["tidysdmx:tx1(1.0)"],
                "ACTION": ["I"],
            }
        )
        result = validate_dataset_local(
            df,
            schema=sdmx_schema,
            sdmx_cols=["STRUCTURE", "STRUCTURE_ID", "ACTION"],
        )
        assert result.empty, f"Expected no errors, got:\n{result}"

    def test_legacy_valid_dict_without_sdmx_cols_uses_default(self):
        """Legacy `valid` dicts missing `sdmx_cols` fall back to STRUCTURE."""
        legacy_valid = {
            "valid_comp": ["TIME_PERIOD", "OBS_VALUE", "AREA"],
            "mandatory_comp": ["TIME_PERIOD", "OBS_VALUE", "AREA"],
            "codelist_ids": {},
            "dim_comp": ["TIME_PERIOD", "AREA"],
        }
        df = pd.DataFrame(
            {
                "TIME_PERIOD": ["2020"],
                "OBS_VALUE": [100],
                "AREA": ["COL"],
                "STRUCTURE": ["datastructure"],
                "STRUCTURE_ID": ["X"],
                "ACTION": ["I"],
            }
        )
        result = validate_dataset_local(df, valid=legacy_valid)
        assert result.empty, f"Expected no errors, got:\n{result}"

    def test_legacy_valid_dict_with_schema_uses_schema_context(self, sdmx_schema):
        """Legacy `valid` + schema infers reference columns from schema.context."""
        legacy_valid = {
            "valid_comp": ["INDICATOR", "TIME_PERIOD", "SEX", "OBS_VALUE"],
            "mandatory_comp": ["INDICATOR", "TIME_PERIOD", "SEX"],
            "codelist_ids": {},
            "dim_comp": ["INDICATOR", "TIME_PERIOD", "SEX"],
        }
        df = pd.DataFrame(
            {
                "INDICATOR": ["IND1"],
                "TIME_PERIOD": ["2020"],
                "SEX": ["F"],
                "OBS_VALUE": [100],
                "DATAFLOW": ["dataflow"],
                "DATAFLOW_ID": ["tidysdmx:tx1(1.0)"],
                "ACTION": ["I"],
            }
        )
        result = validate_dataset_local(df, schema=sdmx_schema, valid=legacy_valid)
        assert result.empty, f"Expected no errors, got:\n{result}"

    @pytest.mark.filterwarnings("default::FutureWarning")
    def test_passing_valid_emits_deprecation_warning(self, valid_info):
        """The `valid` argument is deprecated and must emit FutureWarning."""
        df = pd.DataFrame(
            {
                "TIME_PERIOD": ["2020"],
                "OBS_VALUE": [100],
                "AREA": ["COL"],
                "INDICATOR": ["RES_FEMALE_TOT_FTE"],
                "STRUCTURE": ["X"],
                "STRUCTURE_ID": ["Y"],
                "ACTION": ["A"],
            }
        )
        with pytest.warns(FutureWarning, match="`valid` argument"):
            validate_dataset_local(df, valid=valid_info)
