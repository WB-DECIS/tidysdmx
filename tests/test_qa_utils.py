import pandas as pd
import pytest
from typeguard import TypeCheckError

from tidysdmx.qa_utils import qa_coerce_numeric, qa_remove_duplicates


class TestQaCoerceNumeric:
    """Tests for qa_coerce_numeric."""

    def test_converts_valid_numeric_strings(self):
        """Valid numeric strings are converted to float."""
        df = pd.DataFrame({"val": ["1", "2.5", "3"]})
        result = qa_coerce_numeric(df, ["val"])
        assert list(result["val"]) == [1.0, 2.5, 3.0]

    def test_removes_non_numeric_rows(self):
        """Rows with non-numeric values are dropped."""
        df = pd.DataFrame({"val": ["1", "abc", "3"], "other": ["a", "b", "c"]})
        result = qa_coerce_numeric(df, ["val"])
        assert len(result) == 2
        assert list(result["val"]) == [1.0, 3.0]
        assert list(result["other"]) == ["a", "c"]

    def test_does_not_mutate_input(self):
        """Input DataFrame remains unchanged."""
        df = pd.DataFrame({"val": ["1", "abc", "3"]})
        original = df.copy()
        qa_coerce_numeric(df, ["val"])
        pd.testing.assert_frame_equal(df, original)

    def test_skips_missing_columns(self):
        """Columns not present in the DataFrame are silently skipped."""
        df = pd.DataFrame({"val": ["1", "2"]})
        result = qa_coerce_numeric(df, ["nonexistent"])
        pd.testing.assert_frame_equal(result, df)

    def test_empty_dataframe(self):
        """Empty DataFrame returns empty DataFrame."""
        df = pd.DataFrame(columns=["val"])
        result = qa_coerce_numeric(df, ["val"])
        assert result.empty
        assert list(result.columns) == ["val"]

    def test_raises_on_invalid_df_type(self):
        """Non-DataFrame input raises TypeCheckError."""
        with pytest.raises(TypeCheckError):
            qa_coerce_numeric("not_a_df", ["val"])

    def test_raises_on_invalid_columns_type(self):
        """Non-list columns input raises TypeCheckError."""
        df = pd.DataFrame({"val": [1]})
        with pytest.raises(TypeCheckError):
            qa_coerce_numeric(df, "val")

    def test_multiple_numeric_columns(self):
        """Multiple columns are coerced independently."""
        df = pd.DataFrame({"a": ["1", "bad"], "b": ["bad", "2"]})
        result = qa_coerce_numeric(df, ["a", "b"])
        assert result.empty  # both rows have at least one invalid value


class TestQaRemoveDuplicates:
    """Tests for qa_remove_duplicates."""

    def test_removes_duplicate_rows(self):
        """Duplicate rows are removed."""
        df = pd.DataFrame({"a": [1, 1, 2], "b": ["x", "x", "y"]})
        result = qa_remove_duplicates(df)
        assert len(result) == 2

    def test_no_duplicates_unchanged(self):
        """DataFrame without duplicates is returned as-is."""
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = qa_remove_duplicates(df)
        assert len(result) == 3

    def test_does_not_mutate_input(self):
        """Input DataFrame remains unchanged."""
        df = pd.DataFrame({"a": [1, 1, 2]})
        original = df.copy()
        qa_remove_duplicates(df)
        pd.testing.assert_frame_equal(df, original)

    def test_empty_dataframe(self):
        """Empty DataFrame returns empty DataFrame."""
        df = pd.DataFrame(columns=["a"])
        result = qa_remove_duplicates(df)
        assert result.empty

    def test_raises_on_invalid_type(self):
        """Non-DataFrame input raises TypeCheckError."""
        with pytest.raises(TypeCheckError):
            qa_remove_duplicates("not_a_df")
