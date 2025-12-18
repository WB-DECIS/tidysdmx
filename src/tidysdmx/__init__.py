# read version from installed package
from importlib.metadata import version
__version__ = version("tidysdmx")

from .tidysdmx import (
    fetch_dsd_schema,
    fetch_schema,
    parse_dsd_id,
    parse_artefact_id,
    standardize_sdmx,
    transform_source_to_target,
    vectorized_lookup_ordered_v1,
    vectorized_lookup_ordered_v2,
    map_to_sdmx,
    add_sdmx_reference_cols,
    standardize_indicator_id,
    standardize_data_for_upload,
    read_mapping,
    standardize_output
)
from .qa_utils import qa_coerce_numeric, qa_remove_duplicates
from .kedro import kd_read_mappings, kd_standardize_sdmx, kd_validate_dataset_local, kd_validate_datasets_local
from .tidy_raw import filter_rows, filter_tidy_raw
from .utils import (
    extract_validation_info, 
    get_codelist_ids, 
    extract_component_ids, 
    create_mapping_rules,
    build_excel_workbook,
    write_excel_mapping_template,
    parse_mapping_template_wb
)
from .mapping import map_structures, apply_fixed_value_maps, apply_implicit_component_maps, apply_multi_component_map
from .validation import (
    validate_dataset_local, 
    validate_columns, 
    validate_mandatory_columns, 
    validate_codelist_ids, 
    validate_duplicates, 
    validate_no_missing_values
    ) 
from .structures import (
    build_fixed_map, 
    build_implicit_component_map, 
    build_date_pattern_map, 
    build_value_map,
    build_value_map_list,
    build_multi_value_map_list,
    build_representation_map,
    build_multi_representation_map,
    build_single_component_map,
    build_structure_map,
    create_schema_from_table,
    build_structure_map_from_template_wb
    )

__all__ = [
    "fetch_dsd_schema",
    "fetch_schema", 
    "extract_validation_info",
    "parse_dsd_id",
    "parse_artefact_id",
    "standardize_sdmx",
    "transform_source_to_target",
    "vectorized_lookup_ordered_v1",
    "vectorized_lookup_ordered_v2",
    "map_to_sdmx",
    "add_sdmx_reference_cols",
    "standardize_indicator_id",
    "standardize_data_for_upload",
    "read_mapping",
    "validate_dataset_local",
    "validate_columns",
    "validate_mandatory_columns",
    "get_codelist_ids",
    "validate_codelist_ids",
    "validate_duplicates",
    "validate_no_missing_values",
    "qa_coerce_numeric",
    "qa_remove_duplicates",
    "kd_read_mappings",
    "kd_standardize_sdmx",
    "kd_validate_dataset_local",
    "kd_validate_datasets_local",
    "filter_tidy_raw",
    "map_structures",
    "filter_rows",
    "infer_schema",
    "infer_role_dimension",
    "apply_fixed_value_maps",
    "apply_implicit_component_maps",
    "build_fixed_map",
    "build_implicit_component_map",
    "build_date_pattern_map",
    "build_value_map",
    "build_value_map_list",
    "build_multi_value_map_list",
    "build_representation_map",
    "build_multi_representation_map",
    "build_single_component_map",
    "extract_component_ids",
    "create_mapping_rules",
    "build_excel_workbook",
    "write_excel_mapping_template",
    "build_structure_map",
    "create_schema_from_table",
    "build_structure_map_from_template_wb",
    "apply_multi_component_map",
    "standardize_output",
    "parse_mapping_template_wb"
    ]