"""Build SDMX structure artefacts (StructureMap, ValueMap, Codelist, etc.)."""

from ._ids import (
    sanitize_variable,
)
from .map_builders import (
    build_date_pattern_map,
    build_fixed_map,
    build_implicit_component_map,
    build_multi_component_map,
    build_multi_representation_map,
    build_multi_representation_map_from_df,
    build_multi_value_map_list,
    build_representation_map,
    build_representation_map_from_df,
    build_single_component_map,
    build_value_map,
    build_value_map_list,
)
from .schema_from_table import (
    SchemaComponents,
    create_schema_from_table,
)
from .template import (
    STRUCTURE_TYPE_TO_ARTEFACT,
    build_structure_map_from_template_wb,
)
from .urn import (
    SDMX_PACKAGE_MAP,
    gen_urn,
)

# Only the public surface is advertised via ``__all__`` (so ``from
# tidysdmx.structures import *`` does not leak the ``_``-prefixed helpers).
# The private helpers remain importable by their explicit name for tests and
# internal callers; import them from their defining submodule where practical.
__all__ = [
    "SDMX_PACKAGE_MAP",
    "STRUCTURE_TYPE_TO_ARTEFACT",
    "SchemaComponents",
    "build_date_pattern_map",
    "build_fixed_map",
    "build_implicit_component_map",
    "build_multi_component_map",
    "build_multi_representation_map",
    "build_multi_representation_map_from_df",
    "build_multi_value_map_list",
    "build_representation_map",
    "build_representation_map_from_df",
    "build_single_component_map",
    "build_structure_map_from_template_wb",
    "build_value_map",
    "build_value_map_list",
    "create_schema_from_table",
    "gen_urn",
    "sanitize_variable",
]
