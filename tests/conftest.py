"""Shared pytest fixtures.

Fixtures are imported here rather than registered through ``pytest_plugins``.
pytest only honours ``pytest_plugins`` in the *rootdir* conftest; this file is
``tests/conftest.py``, and the previous declaration worked only because
``testpaths`` happened to make this directory resolve as top level — behaviour
that is not guaranteed across pytest versions. Importing the fixture functions
into this module registers them for the whole test session with no such
dependency.

The re-exports below are deliberate: ruff would otherwise flag them as unused,
hence ``__all__``.
"""

from tests.fixtures.fxtr_dummy_data import ifpri_asti_df, sample_df, sdmx_df
from tests.fixtures.fxtr_mapping import api_params_sm, ifpri_asti_sm
from tests.fixtures.fxtr_schemas import (
    api_params_schema,
    ifpri_asti_schema,
    sdmx_schema,
)
from tests.fixtures.fxtr_structures import (
    multi_value_map_df,
    value_map_df_mandatory_cols,
)

__all__ = [
    "api_params_schema",
    "api_params_sm",
    "ifpri_asti_df",
    "ifpri_asti_schema",
    "ifpri_asti_sm",
    "multi_value_map_df",
    "sample_df",
    "sdmx_df",
    "sdmx_schema",
    "value_map_df_mandatory_cols",
]
