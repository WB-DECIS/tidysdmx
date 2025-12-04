from pathlib import Path
from typing import Optional, Union, Literal, List, Dict, Tuple
import pandas as pd
from typeguard import typechecked
from pysdmx.model.map import (
    StructureMap, 
    FixedValueMap, 
    ImplicitComponentMap, 
    ComponentMap
)

# Importing helpers from the provided structures.py context
from .structures import (
    _parse_info_sheet, 
    _parse_comp_mapping_sheet, 
    _parse_rep_mapping_sheet,
    _extract_artefact_id,
    build_fixed_map, 
    build_implicit_component_map, 
    build_single_component_map
)

from .tidysdmx import parse_artefact_id

# Region: New Helper Functions

@typechecked
def _match_column_name(target_name: str, available_columns: List[str]) -> str:
    """Matches a business name from COMP_MAPPING to the cleaned column names in REP_MAPPING.

    This handles discrepancies like 'Series code' (business name) vs 'Series' (Excel header).

    Args:
        target_name (str): The name to look for (e.g., 'Series code').
        available_columns (List[str]): The list of available column headers.

    Returns:
        str: The matching column name.

    Raises:
        ValueError: If no suitable match is found.
    """
    # 1. Exact match
    if target_name in available_columns:
        return target_name

    # 2. Normalized match (ignore case, spaces, underscores)
    norm_target = target_name.replace(" ", "").replace("_", "").lower()
    
    for col in available_columns:
        norm_col = col.replace(" ", "").replace("_", "").lower()
        # Check for containment (e.g., 'Series' in 'SeriesCode')
        if norm_col == norm_target or norm_col in norm_target or norm_target in norm_col:
            return col

    raise ValueError(f"Could not find a column in REP_MAPPING matching '{target_name}'. Available: {available_columns}")

# Region: Main Function

@typechecked
def build_structure_map_from_template_wb(
    mappings: Dict[str, pd.DataFrame],
    default_agency: str = "SDMX",
    default_structure_map_id: str = "WB_STRUCTURE_MAP"
) -> StructureMap:
    """Builds a complete StructureMap object by parsing a WB-format Excel template.
    
    Process:
    1.  Parses `INFO` sheet to extract Agency and Version context using `_extract_artefact_id`.
    2.  Parses `COMP_MAPPING` to determine mapping rules.
    3.  Parses `REP_MAPPING` using `_parse_rep_mapping_sheet` to get source/target data.
    4.  Constructs the specific SDMX Map objects (Fixed, Implicit, Component).

    Args:
        mappings (Dict[str, pd.DataFrame]): The dictionary of DataFrames containing all sheets.
        default_agency (str): Fallback agency ID if not found in INFO.
        default_structure_map_id (str): ID for the resulting StructureMap.

    Returns:
        StructureMap: A valid pysdmx StructureMap object.

    Raises:
        ValueError: If mandatory sheets/columns are missing or mapping rules are invalid.
    """
    # 1. Extract Metadata (Agency & Version)
    current_agency = default_agency
    current_version = "1.0"
    artefact_ref = None
    
    if "INFO" in mappings:
        try:
            info_df = _parse_info_sheet(mappings)
            
            # Try to find a defining artefact ID (Dataflow takes precedence)
            for type_key in ["dataflow", "dsd"]:
                try:
                    artefact_ref = _extract_artefact_id(info_df, type_key)
                    break 
                except ValueError:
                    continue
            
            if artefact_ref:
                # Use the helper to parse "Agency:ID(Version)"
                parsed_agency, _, parsed_version = parse_artefact_id(artefact_ref)
                current_agency = parsed_agency
                current_version = parsed_version
            elif "FMR_AGENCY" in info_df["Key"].values:
                 # Fallback to specific key if standard artefact not found
                 val = info_df.loc[info_df["Key"] == "FMR_AGENCY", "Value"].iloc[0]
                 if val:
                     current_agency = str(val).strip()

        except Exception:
            # Metadata parsing failed, proceeding with defaults.
            pass

    # 3. Parse Component Mappings Rules
    comp_df = _parse_comp_mapping_sheet(mappings)
    
    # 4. Prepare Representation Data
    rep_data: Dict[str, pd.DataFrame] = {}
    if "REP_MAPPING" in mappings:
        try:
            rep_data = _parse_rep_mapping_sheet(mappings)
        except ValueError:
             # Ignore if REP_MAPPING is present but structurally invalid, validation will fail 
             # only if a component actually attempts to use it.
             pass

    generated_maps: List[Union[FixedValueMap, ImplicitComponentMap, ComponentMap]] = []

    # 5. Iterate Logic
    for _, row in comp_df.iterrows():
        source_id = str(row["SOURCE"]).strip()
        target_id = str(row["TARGET"]).strip()
        rule = str(row["MAPPING_RULES"]).strip()

        # Skip empty rules or pandas artifacts
        if not target_id or not rule or rule.lower() == "nan":
            continue

        try:
            # --- Rule 1: Fixed Value (MAPPING_RULES starts with "fixed:") ---
            if rule.lower().startswith("fixed:"):
                parts = rule.split(":", 1)
                if len(parts) < 2 or not parts[1].strip():
                    raise ValueError(f"Invalid fixed rule format: {rule}")
                fixed_val = parts[1].strip()
                generated_maps.append(build_fixed_map(target_id, fixed_val))

            # --- Rule 2: Implicit (MAPPING_RULES is "implicit") ---
            elif rule.lower() == "implicit":
                if not source_id:
                     raise ValueError("Implicit map rule requires a non-empty 'SOURCE' component ID.")
                generated_maps.append(build_implicit_component_map(source_id, target_id))

            # --- Rule 3: Representation Map (MAPPING_RULES matches TARGET ID) ---
            elif rule == target_id:
                if not rep_data or rep_data["source"].empty or rep_data["target"].empty:
                    raise ValueError("Mapping rule requires 'REP_MAPPING' sheet with data, but it was invalid or empty.")
                if not source_id:
                     raise ValueError("Representation map rule requires a non-empty 'SOURCE' component ID.")
                
                source_dfs_map = rep_data["source"]
                target_dfs_map = rep_data["target"]

                # Resolve columns using fuzzy matching on the stripped headers
                actual_source_col = _match_column_name(source_id, source_dfs_map.columns.tolist())
                actual_target_col = _match_column_name(target_id, target_dfs_map.columns.tolist())

                # Combine into a single DF for the builder function, aligning on index
                combined_df = pd.DataFrame({
                    "source": source_dfs_map[actual_source_col],
                    "target": target_dfs_map[actual_target_col]
                })

                combined_df.dropna(subset=["source", "target"], how='any', inplace=True)
                combined_df.drop_duplicates(inplace=True)

                if combined_df.empty:
                    raise ValueError(f"No valid mapping rows found between source column '{actual_source_col}' and target column '{actual_target_col}'.")

                comp_map = build_single_component_map(
                    df=combined_df,
                    source_component=source_id,
                    target_component=target_id,
                    agency=current_agency,
                    id=f"MAP_{target_id}",
                    name=f"Mapping for {target_id}",
                    source_col="source",
                    target_col="target",
                    version=current_version
                )
                generated_maps.append(comp_map)
            
            else:
                 # Catch-all for non-matching strings
                 raise ValueError(f"Unknown mapping rule: '{rule}'")

        except ValueError as e:
            # Propagate detailed error
            raise ValueError(f"Error processing mapping for Target '{target_id}': {str(e)}") from e

    # 6. Construct Final Object
    name_suffix = artefact_ref if artefact_ref else default_structure_map_id
    return StructureMap(
        id=default_structure_map_id,
        agency=current_agency,
        version=current_version,
        name=f"Structure Map generated for {name_suffix}",
        maps=generated_maps
    )