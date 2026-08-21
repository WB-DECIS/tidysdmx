"""Shared pytest fixtures.

Fixtures are imported here rather than registered through ``pytest_plugins``.
The previous declaration listed bare module paths (``fixtures.fxtr_schemas``),
which stopped resolving when the suite moved to ``--import-mode=importlib`` with
``tests/`` as a real package — it raises ``ModuleNotFoundError: fixtures``.
Importing the fixture functions directly keeps them registered for the whole
session without depending on the shape of ``sys.path``.

``pytest_plugins`` itself would be legal in this file: pytest rejects it only in
conftests imported *after* configuration (``_pytest/config/__init__.py``,
``_check_non_top_pytest_plugins``), and ``tests/conftest.py`` is loaded as an
initial conftest. It is rejected one level down, e.g. in ``tests/fixtures/``.

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
