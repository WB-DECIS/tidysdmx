"""Tests for the internal @deprecated decorator."""

import warnings

import pytest
from typeguard import TypeCheckError, typechecked

import tidysdmx
from tidysdmx._deprecation import deprecated

# Every public function retired under Workstream 0. The coverage test below
# asserts each carries the @deprecated marker so none silently loses its
# warning during future refactors.
DEPRECATED_PUBLIC_FUNCTIONS = [
    # Already-deprecated (retrofitted onto the decorator)
    "fetch_dsd_schema",
    "parse_dsd_id",
    "add_sdmx_reference_cols",
    "standardize_data_for_upload",
    "fix_sdmx_xml_datatype_tags",
    # Engine A (bespoke JSON mapping pipeline)
    "standardize_sdmx",
    "transform_source_to_target",
    "map_to_sdmx",
    "vectorized_lookup_ordered_v1",
    "vectorized_lookup_ordered_v2",
    "standardize_indicator_id",
    "read_mapping",
    # Legacy Excel mapping-template writer
    "write_excel_mapping_template",
    "build_excel_workbook",
    "create_mapping_rules",
    # QA helper on the retiring standardisation path
    "qa_coerce_numeric",
    # Kedro node wrappers
    "kd_read_mappings",
    "kd_standardize_sdmx",
    "kd_validate_dataset_local",
    "kd_validate_datasets_local",
]


@pytest.mark.unit
class TestDeprecated:
    """Behaviour of the ``deprecated`` decorator."""

    def test_emits_future_warning_and_returns_value(self):
        """The wrapped function still runs and returns, but warns."""

        @deprecated(replacement="new_func")
        def old():
            return 42

        with pytest.warns(FutureWarning, match="old is deprecated"):
            assert old() == 42

    def test_message_includes_replacement(self):
        """A replacement is surfaced in the warning message."""

        @deprecated(replacement="new_func")
        def old():
            return None

        with pytest.warns(FutureWarning, match="Use new_func instead"):
            old()

    def test_message_omits_replacement_when_none(self):
        """With no replacement, the message has no 'Use ... instead' clause."""

        @deprecated()
        def old():
            return None

        with pytest.warns(FutureWarning) as record:
            old()

        assert "Use" not in str(record[0].message)

    def test_custom_removal_wording(self):
        """The removal description is interpolated into the message."""

        @deprecated(removal="version 0.11")
        def old():
            return None

        with pytest.warns(FutureWarning, match="removed in version 0.11"):
            old()

    def test_preserves_name_and_docstring(self):
        """functools.wraps keeps the wrapped function's identity."""

        @deprecated(replacement="new_func")
        def old():
            """Old docstring."""
            return None

        assert old.__name__ == "old"
        assert old.__doc__ == "Old docstring."

    def test_sets_deprecated_attribute(self):
        """The wrapper exposes __deprecated__ for cheap introspection."""

        @deprecated()
        def old():
            return None

        assert hasattr(old, "__deprecated__")

    def test_forwards_args_and_kwargs(self):
        """Positional and keyword arguments pass through unchanged."""

        @deprecated()
        def add(a, b, c=0):
            return a + b + c

        with pytest.warns(FutureWarning):
            assert add(1, 2, c=3) == 6

    def test_composes_with_typechecked_happy_path(self):
        """A typechecked function below the decorator still runs and warns."""

        @deprecated(replacement="new")
        @typechecked
        def strict(x: int) -> int:
            return x * 2

        with pytest.warns(FutureWarning):
            assert strict(3) == 6

    def test_typechecking_still_enforced(self):
        """Type errors are still raised through the deprecation wrapper."""

        @deprecated(replacement="new")
        @typechecked
        def strict(x: int) -> int:
            return x * 2

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with pytest.raises(TypeCheckError):
                strict("not an int")


@pytest.mark.unit
@pytest.mark.parametrize("name", DEPRECATED_PUBLIC_FUNCTIONS)
def test_public_function_is_marked_deprecated(name):
    """Every retiring public function carries the @deprecated marker."""
    func = getattr(tidysdmx, name)
    assert getattr(func, "__deprecated__", None), (
        f"{name} should be decorated with @deprecated but has no __deprecated__ marker"
    )


@pytest.mark.unit
def test_deprecated_orchestrator_emits_single_warning(tmp_path):
    """A deprecated function calling deprecated helpers warns exactly once (DEP-02).

    write_excel_mapping_template internally calls build_excel_workbook and
    create_mapping_rules (both deprecated); routing those through .__wrapped__
    must suppress the misleading cascade so the user sees only the outer warning.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        tidysdmx.write_excel_mapping_template(
            ["FREQ", "REF_AREA"], None, tmp_path / "template.xlsx"
        )
    future = [w for w in caught if issubclass(w.category, FutureWarning)]
    assert len(future) == 1
    assert "write_excel_mapping_template" in str(future[0].message)
