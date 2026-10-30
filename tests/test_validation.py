import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from typeguard import TypeCheckError

from tidysdmx.validation import (
    DatasetValidator,
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

    def test_sdmx_cols_accepts_tuple(self):
        """A tuple of reference columns is accepted (issue #255)."""
        df = pd.DataFrame({"col1": [1], "REF": ["x"]})
        validate_mandatory_columns(df, mandatory_columns=["col1"], sdmx_cols=("REF",))


class TestValidateColumns:
    @pytest.mark.parametrize(
        ("df_columns", "valid_columns", "sdmx_cols"),
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
        ("df_columns", "valid_columns", "sdmx_cols", "invalid_col"),
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

    def test_sdmx_cols_accepts_tuple(self):
        """A tuple of reference columns is accepted (issue #255)."""
        df = pd.DataFrame(columns=["COMP1", "STRUCTURE"])
        validate_columns(
            df, valid_columns=["COMP1"], sdmx_cols=("STRUCTURE", "STRUCTURE_ID")
        )

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
    @pytest.fixture
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
        assert "col1" in msg
        assert "col2" in msg

    def test_column_not_in_dataframe_is_ignored(self, sample_codelist_ids):
        """Tests that columns not present in DataFrame are ignored."""
        df = pd.DataFrame({"col1": ["A1", "A2"]})
        validate_codelist_ids(df, sample_codelist_ids)

    def test_empty_dataframe_passes(self, sample_codelist_ids):
        """Tests that an empty DataFrame passes without error."""
        df = pd.DataFrame(columns=["col1", "col2"])
        validate_codelist_ids(df, sample_codelist_ids)

    @pytest.mark.parametrize(
        ("df_values", "expected_error"),
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

    @pytest.fixture
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

    def test_dataflow_schema_accepts_structure_columns(self, sdmx_schema):
        """Dataflow-context schema accepts the standard SDMX reference columns.

        Regression test for issue #255: validation must not flag STRUCTURE /
        STRUCTURE_ID / ACTION as unexpected or report other reference columns
        as missing when the schema's context is ``dataflow``.
        """
        df = pd.DataFrame(
            {
                "INDICATOR": ["IND1", "IND3"],
                "TIME_PERIOD": ["2020", "2021"],
                "SEX": ["F", "M"],
                "OBS_VALUE": [100, 200],
                "STRUCTURE": ["dataflow", "dataflow"],
                "STRUCTURE_ID": ["tidysdmx:tx1(1.0)", "tidysdmx:tx1(1.0)"],
                "ACTION": ["I", "I"],
            }
        )
        result = validate_dataset_local(df, schema=sdmx_schema)
        assert isinstance(result, pd.DataFrame)
        assert result.empty, f"Expected no errors, got:\n{result}"

    def test_sdmx_cols_accepts_tuple(self, sdmx_schema):
        """A tuple of reference columns is accepted (issue #255)."""
        df = pd.DataFrame(
            {
                "INDICATOR": ["IND1"],
                "TIME_PERIOD": ["2020"],
                "SEX": ["F"],
                "OBS_VALUE": [100],
                "STRUCTURE": ["dataflow"],
                "STRUCTURE_ID": ["tidysdmx:tx1(1.0)"],
                "ACTION": ["I"],
            }
        )
        result = validate_dataset_local(
            df,
            schema=sdmx_schema,
            sdmx_cols=("STRUCTURE", "STRUCTURE_ID", "ACTION"),
        )
        assert result.empty, f"Expected no errors, got:\n{result}"

    def test_explicit_sdmx_cols_overrides_inference(self, sdmx_schema):
        """Caller-provided sdmx_cols win over schema-inferred ones.

        Datasets carrying non-standard reference columns (e.g. DATAFLOW /
        DATAFLOW_ID, as in issue #218) can still be validated by passing
        those columns explicitly.
        """
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
        result = validate_dataset_local(
            df,
            schema=sdmx_schema,
            sdmx_cols=["DATAFLOW", "DATAFLOW_ID", "ACTION"],
        )
        assert result.empty, f"Expected no errors, got:\n{result}"

    def test_stale_valid_dict_with_context_cols_normalized(self):
        """`valid` dicts cached under <=0.9.0 with DATAFLOW columns are normalized.

        Regression test for the PR #257 review: a stale cached dict must not
        resurrect the context-specific reference columns fixed in #255.
        """
        stale_valid = {
            "valid_comp": ["TIME_PERIOD", "OBS_VALUE", "AREA"],
            "mandatory_comp": ["TIME_PERIOD", "OBS_VALUE", "AREA"],
            "codelist_ids": {},
            "dim_comp": ["TIME_PERIOD", "AREA"],
            "sdmx_cols": ["DATAFLOW", "DATAFLOW_ID", "ACTION"],
        }
        df = pd.DataFrame(
            {
                "TIME_PERIOD": ["2020"],
                "OBS_VALUE": [100],
                "AREA": ["COL"],
                "STRUCTURE": ["dataflow"],
                "STRUCTURE_ID": ["tidysdmx:tx1(1.0)"],
                "ACTION": ["I"],
            }
        )
        result = validate_dataset_local(df, valid=stale_valid)
        assert result.empty, f"Expected no errors, got:\n{result}"

    def test_valid_dict_with_custom_sdmx_cols_respected(self):
        """Non-legacy custom sdmx_cols in a `valid` dict are still honored."""
        custom_valid = {
            "valid_comp": ["TIME_PERIOD", "OBS_VALUE", "AREA"],
            "mandatory_comp": ["TIME_PERIOD", "OBS_VALUE", "AREA"],
            "codelist_ids": {},
            "dim_comp": ["TIME_PERIOD", "AREA"],
            "sdmx_cols": ["REF_COL"],
        }
        df = pd.DataFrame(
            {
                "TIME_PERIOD": ["2020"],
                "OBS_VALUE": [100],
                "AREA": ["COL"],
                "REF_COL": ["x"],
            }
        )
        result = validate_dataset_local(df, valid=custom_valid)
        assert result.empty, f"Expected no errors, got:\n{result}"

    def test_schema_overrides_stale_valid_sdmx_cols(self, sdmx_schema):
        """When both schema and valid are passed, schema.context wins."""
        stale_valid = {
            "valid_comp": ["INDICATOR", "TIME_PERIOD", "SEX", "OBS_VALUE"],
            "mandatory_comp": ["INDICATOR", "TIME_PERIOD", "SEX"],
            "codelist_ids": {},
            "dim_comp": ["INDICATOR", "TIME_PERIOD", "SEX"],
            "sdmx_cols": ["DATAFLOW", "DATAFLOW_ID", "ACTION"],
        }
        df = pd.DataFrame(
            {
                "INDICATOR": ["IND1"],
                "TIME_PERIOD": ["2020"],
                "SEX": ["F"],
                "OBS_VALUE": [100],
                "STRUCTURE": ["dataflow"],
                "STRUCTURE_ID": ["tidysdmx:tx1(1.0)"],
                "ACTION": ["I"],
            }
        )
        result = validate_dataset_local(df, schema=sdmx_schema, valid=stale_valid)
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
                "STRUCTURE": ["dataflow"],
                "STRUCTURE_ID": ["tidysdmx:tx1(1.0)"],
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


class TestValidationOnDistinctValues:
    """The value checks judge each distinct value once; the reports are unchanged."""

    def test_validate_codelist_ids_compares_an_int_column_as_text(self):
        df = pd.DataFrame({"TIME_PERIOD": [2020, 2021, 2020]})
        validate_codelist_ids(df, {"TIME_PERIOD": ["2020", "2021"]})
        with pytest.raises(ValueError, match=r"'TIME_PERIOD': 2021"):
            validate_codelist_ids(df, {"TIME_PERIOD": ["2020"]})

    def test_validate_codelist_ids_reports_a_missing_value_as_nan(self):
        df = pd.DataFrame({"INDICATOR": ["IND1", None]})
        with pytest.raises(ValueError, match=r"'INDICATOR': nan"):
            validate_codelist_ids(df, {"INDICATOR": ["IND1"]})

    def test_validate_codelist_ids_keeps_first_appearance_order(self):
        df = pd.DataFrame({"CODE": ["B", "A", "B", "C", "A"]})
        with pytest.raises(ValueError, match=r"'CODE': B\n  'CODE': A\n  'CODE': C"):
            validate_codelist_ids(df, {"CODE": ["Z"]})

    @pytest.mark.parametrize(
        ("allowed", "reported"),
        [(["1"], "1.0"), (["1.0"], "1")],
        ids=["int-allowed", "float-allowed"],
    )
    def test_validate_codelist_ids_keeps_equal_values_that_print_apart(
        self, allowed, reported
    ):
        """1 and 1.0 compare equal but are compared as text, like before."""
        df = pd.DataFrame({"CODE": pd.Series([1, 1.0, 1], dtype=object)})
        with pytest.raises(ValueError, match="Invalid codelist values") as info:
            validate_codelist_ids(df, {"CODE": allowed})
        assert str(info.value).splitlines()[1:] == [f"  'CODE': {reported}"]

    def test_validate_codelist_ids_tells_negative_zero_apart(self):
        df = pd.DataFrame({"CODE": [0.0, -0.0]})
        with pytest.raises(ValueError, match=r"'CODE': -0.0"):
            validate_codelist_ids(df, {"CODE": ["0.0"]})

    def test_validate_codelist_ids_reports_a_categorical_by_its_values(self):
        """An unused category is not a value, so it is not a violation."""
        df = pd.DataFrame(
            {"CODE": pd.Categorical(["A", "A"], categories=["A", "UNUSED"])}
        )
        validate_codelist_ids(df, {"CODE": ["A"]})

    def test_validate_no_missing_values_lists_the_index_labels(self):
        df = pd.DataFrame({"col1": [1, None]}, index=[10, 11])
        with pytest.raises(ValueError, match=r"Found 1 row\(s\)[^\n]*\n[^\n]*\n11 "):
            validate_no_missing_values(df, mandatory_columns=["col1"])

    def test_validate_no_missing_values_samples_the_first_rows_in_order(self):
        # A repeated, unsorted index: the sample is taken by position, and must
        # list the same rows, labels and order as boolean selection did.
        df = pd.DataFrame({"col1": [None, 1, None, None]}, index=[5, 5, 7, 5])
        expected = df.loc[df["col1"].isna()].head(2)[["col1"]].to_string(index=True)
        with pytest.raises(ValueError, match="Found 3 row") as raised:
            validate_no_missing_values(df, mandatory_columns=["col1"], max_errors=2)
        assert str(raised.value).endswith(expected)

    def test_validate_duplicates_lists_each_key_once(self):
        df = pd.DataFrame({"K": ["a", "a", "a", "b"], "V": [1, 2, 3, 4]})
        with pytest.raises(
            ValueError, match=r"Found 3 duplicate rows across 1 key combination"
        ):
            validate_duplicates(df, dim_comp=["K"])

    def test_validate_duplicates_truncates_at_max_errors(self):
        df = pd.DataFrame({"K": ["a", "a", "b", "b"], "V": [1, 2, 3, 4]})
        with pytest.raises(ValueError, match=r"… and 1 more \(max_errors=1\)"):
            validate_duplicates(df, dim_comp=["K"], max_errors=1)


class TestDatasetValidator:
    """DatasetValidator accumulates validate_dataset_local's checks over chunks."""

    @staticmethod
    def _frame(**overrides):
        """A frame valid against the ``sdmx_schema`` fixture, with overrides."""
        data = {
            "STRUCTURE": ["dataflow"] * 4,
            "STRUCTURE_ID": ["tidysdmx:tx1(1.0)"] * 4,
            "ACTION": ["I"] * 4,
            "INDICATOR": ["IND1", "IND1", "IND3", "IND3"],
            "TIME_PERIOD": ["2020", "2021", "2020", "2021"],
            "SEX": ["M", "M", "F", "F"],
            "OBS_VALUE": [1, 2, 3, 4],
        }
        data.update(overrides)
        return pd.DataFrame(data)

    @staticmethod
    def _chunked(schema, chunks, **kwargs):
        validator = DatasetValidator(schema, **kwargs)
        for chunk in chunks:
            validator.update(chunk)
        return validator.errors()

    def test_dataset_validator_valid_chunks_give_an_empty_frame(self, sdmx_schema):
        df = self._frame()
        errors = self._chunked(sdmx_schema, [df.iloc[:2], df.iloc[2:]])
        assert errors.empty
        assert list(errors.columns) == ["Validation", "Error"]

    def test_dataset_validator_no_update_gives_an_empty_frame(self, sdmx_schema):
        errors = DatasetValidator(sdmx_schema).errors()
        assert errors.empty
        assert list(errors.columns) == ["Validation", "Error"]

    @pytest.mark.parametrize(
        "overrides",
        [
            pytest.param({}, id="valid"),
            pytest.param({"EXTRA": [1, 2, 3, 4]}, id="unexpected-column"),
            pytest.param(
                {"INDICATOR": ["IND1", "BAD", "IND3", "WORSE"]}, id="invalid-codes"
            ),
            pytest.param({"TIME_PERIOD": ["2020"] * 4}, id="duplicates"),
            pytest.param({"SEX": ["M", None, "F", None]}, id="missing-values"),
            pytest.param(
                {
                    "INDICATOR": ["BAD", "BAD", None, "IND3"],
                    "TIME_PERIOD": ["2020", "2020", "2020", "2021"],
                    "SEX": ["M", "M", None, "F"],
                    "EXTRA": [0, 0, 0, 0],
                },
                id="everything",
            ),
        ],
    )
    def test_dataset_validator_one_update_matches_validate_dataset_local(
        self, sdmx_schema, overrides
    ):
        df = self._frame(**overrides)
        assert_frame_equal(
            self._chunked(sdmx_schema, [df]),
            validate_dataset_local(df, schema=sdmx_schema),
        )

    def test_dataset_validator_one_update_matches_missing_mandatory_column(
        self, sdmx_schema
    ):
        df = self._frame().drop(columns=["SEX"])
        assert_frame_equal(
            self._chunked(sdmx_schema, [df]),
            validate_dataset_local(df, schema=sdmx_schema),
        )

    def test_dataset_validator_one_update_matches_max_errors(self, sdmx_schema):
        df = self._frame(
            INDICATOR=["A", "B", "C", "D"],
            TIME_PERIOD=["2020"] * 4,
            SEX=[None] * 4,
            EXTRA=[0] * 4,
            MORE=[0] * 4,
        )
        assert_frame_equal(
            self._chunked(sdmx_schema, [df], max_errors=1),
            validate_dataset_local(df, schema=sdmx_schema, max_errors=1),
        )

    def test_dataset_validator_merges_codelist_violations_across_chunks(
        self, sdmx_schema
    ):
        first = self._frame(INDICATOR=["BAD1", "IND1", "BAD1", "IND3"])
        second = self._frame(INDICATOR=["BAD2", "BAD1", "IND1", "IND3"])
        errors = self._chunked(sdmx_schema, [first, second])
        assert errors["Error"].tolist() == [
            "'INDICATOR': BAD1",
            "'INDICATOR': BAD2",
        ]

    def test_dataset_validator_sums_missing_value_counts(self, sdmx_schema):
        first = self._frame(SEX=["M", None, "F", "F"])
        second = self._frame(SEX=[None, None, "F", "F"])
        errors = self._chunked(sdmx_schema, [first, second])
        assert errors["Validation"].tolist() == ["missing_values"]
        message = errors["Error"].iloc[0]
        assert message.startswith("Found 3 row(s) with missing values")
        assert message.count("\n") == 4  # a header line plus three rows

    def test_dataset_validator_samples_at_most_max_errors_missing_rows(
        self, sdmx_schema
    ):
        mostly_missing = self._frame(SEX=[None, None, None, "F"])
        errors = self._chunked(sdmx_schema, [mostly_missing], max_errors=2)
        message = errors["Error"].iloc[0]
        assert message.startswith("Found 3 row(s) with missing values")
        assert message.count("\n") == 3  # a header line plus two rows

    def test_dataset_validator_reports_duplicates_within_a_chunk(self, sdmx_schema):
        duplicated = self._frame(TIME_PERIOD=["2020", "2020", "2020", "2021"])
        errors = self._chunked(sdmx_schema, [duplicated, self._frame()])
        assert errors["Validation"].tolist() == ["duplicates"]
        assert (
            errors["Error"]
            .iloc[0]
            .startswith("Found 2 duplicate rows across 1 key combination(s)")
        )

    def test_dataset_validator_does_not_see_a_key_shared_by_two_chunks(
        self, sdmx_schema
    ):
        """The documented limit: duplicates are found within each chunk only."""
        df = self._frame()
        assert self._chunked(sdmx_schema, [df, df]).empty

    def test_dataset_validator_caps_merged_errors_at_max_errors(self, sdmx_schema):
        first = self._frame(INDICATOR=["BAD1", "BAD2", "IND1", "IND3"])
        second = self._frame(INDICATOR=["BAD3", "BAD4", "IND1", "IND3"])
        errors = self._chunked(sdmx_schema, [first, second], max_errors=3)
        assert errors["Error"].tolist() == [
            "'INDICATOR': BAD1",
            "'INDICATOR': BAD2",
            "'INDICATOR': BAD3",
        ]

    def test_dataset_validator_reports_an_unexpected_column_once(self, sdmx_schema):
        extra = self._frame(EXTRA=[0] * 4)
        errors = self._chunked(sdmx_schema, [self._frame(), extra, extra])
        assert errors["Error"].tolist() == ["Unexpected column: 'EXTRA'"]

    def test_dataset_validator_skips_value_checks_without_mandatory_columns(
        self, sdmx_schema
    ):
        incomplete = self._frame(INDICATOR=["BAD"] * 4).drop(columns=["SEX"])
        complete = self._frame(INDICATOR=["WORSE"] * 4)
        errors = self._chunked(sdmx_schema, [incomplete, complete])
        assert errors["Validation"].tolist() == ["mandatory_columns", "codelist_ids"]
        assert errors["Error"].tolist() == [
            "Missing mandatory columns: ['SEX']",
            "'INDICATOR': WORSE",
        ]

    def test_dataset_validator_errors_can_be_read_between_updates(self, sdmx_schema):
        validator = DatasetValidator(sdmx_schema)
        validator.update(self._frame(INDICATOR=["BAD"] * 4))
        assert len(validator.errors()) == 1
        validator.update(self._frame(SEX=[None] * 4))
        assert validator.errors()["Validation"].tolist() == [
            "codelist_ids",
            "missing_values",
        ]

    def test_dataset_validator_counts_rows(self, sdmx_schema):
        validator = DatasetValidator(sdmx_schema)
        assert validator.rows == 0
        validator.update(self._frame())
        validator.update(self._frame().iloc[:1])
        assert validator.rows == 5

    def test_dataset_validator_accepts_an_empty_chunk(self, sdmx_schema):
        errors = self._chunked(sdmx_schema, [self._frame().iloc[:0], self._frame()])
        assert errors.empty

    def test_dataset_validator_takes_sdmx_cols(self, sdmx_schema):
        bare = self._frame().drop(columns=["STRUCTURE", "STRUCTURE_ID", "ACTION"])
        assert self._chunked(sdmx_schema, [bare], sdmx_cols=[]).empty
        errors = self._chunked(sdmx_schema, [bare])
        assert errors["Validation"].tolist() == ["mandatory_columns"]

    def test_dataset_validator_rejects_a_non_schema(self):
        with pytest.raises(TypeCheckError, match="schema"):
            DatasetValidator("not a schema")

    def test_dataset_validator_rejects_a_non_frame_chunk(self, sdmx_schema):
        with pytest.raises(TypeCheckError, match="df"):
            DatasetValidator(sdmx_schema).update(["not", "a", "frame"])
