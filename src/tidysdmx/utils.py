from typing import Dict, List, Optional
from typeguard import typechecked
from pysdmx.model import Schema
from pathlib import Path
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import pysdmx as px
import pandas as pd

@typechecked
def extract_validation_info(schema: px.model.dataflow.Schema) -> Dict[str, object]:
    """Extract validation information from a given schema.

    Args:
        schema (pysdmx.model.dataflow.Schema object.): The schema object contains all necessary validation information.

    Returns:
        dict: A dictionary containing validation information with the following keys:
            - valid_comp: List of valid component names.
            - mandatory_comp: List of mandatory component names.
            - coded_comp: List of coded component names.
            - codelist_ids: Dictionary with coded components as keys and list of codelist IDs as values.
            - dim_comp: List of dimension component names.
    """
    comp = schema.components
    # Precompute reusable objects
    valid_comp = [c.id for c in comp]
    mandatory_comp = [c.id for c in comp if comp[c.id].required]
    coded_comp = [c.id for c in comp if comp[c.id].local_codes is not None]
    dim_comp = [c.id for c in comp if comp[c.id].role == px.model.Role.DIMENSION]

    out = {
        "valid_comp": valid_comp,
        "mandatory_comp": mandatory_comp,
        "coded_comp": coded_comp,
        "codelist_ids": get_codelist_ids(comp, coded_comp),
        "dim_comp": dim_comp,
    }

    return out

@typechecked
def get_codelist_ids(comp: px.model.dataflow.Components, coded_comp: List) -> Dict[str, list[str]]:
    """Retrieve all codelist IDs for given coded components.

    Args:
        comp (list): List of components.
        coded_comp (list): List of coded components.

    Returns:
        dict: Dictionary with coded components as keys and list of codelist IDs as values.
    """
    codelist_dict = {}
    for component in coded_comp:
        codes = comp[component].local_codes.items
        codelist_dict[component] = [code.id for code in codes]
    
    return codelist_dict


@typechecked
def extract_component_ids(schema: Schema) -> list[str]:
    """Retrieve all component IDs from a given pysdmx Schema.

    Args:
        schema (Schema): A pysdmx Schema object representing an SDMX structure.

    Returns:
        list[str]: A list of component IDs contained in the schema.

    Raises:
        TypeError: If the input is not a Schema instance.
        ValueError: If the schema has no components.

    Examples:
        >>> from pysdmx.model import Schema, Components, Component
        >>> comp1 = Component(id="FREQ")
        >>> comp2 = Component(id="TIME_PERIOD")
        >>> schema = Schema(context="datastructure", agency="ECB", id_="EXR",
        ...                 components=Components([comp1, comp2]),
        ...                 version="1.0.0", urns=[])
        >>> get_component_ids(schema)
        ['FREQ', 'TIME_PERIOD']
    """
    if not isinstance(schema, Schema):
        raise TypeError("Input must be a pysdmx Schema instance.")
    if not schema.components or len(schema.components) == 0:
        raise ValueError("Schema contains no components.")
    return [component.id for component in schema.components]




typechecked
def create_excel_mapping(
    components: List[str],
    rep_maps: Optional[List[str]] = None,
    output_path: Path = Path("mapping.xlsx")
) -> Workbook:
    """Create an Excel file with a default mapping tab and optional representation mapping tabs.

    The default tab contains a table with columns: source, target, mapping_rules.
    - 'source' column is empty.
    - 'target' column is populated with values from `components`.
    - 'mapping_rules' column is empty unless `rep_maps` is provided.
      If `rep_maps` is provided, each row in 'mapping_rules' will contain the corresponding
      rep_map name and a hyperlink to its tab.

    If `rep_maps` is provided, a new tab is created for each element in the list.
    Each tab contains a table with columns: source, target, valid_from, valid_to.

    Args:
        components (List[str]): List of target component names for the default tab.
        rep_maps (Optional[List[str]]): List of names for additional tabs.
        output_path (Path): Path where the Excel file will be saved.

    Returns:
        Path: Path to the saved Excel file.

    Raises:
        ValueError: If `components` is empty.
        FileNotFoundError: If the output directory does not exist.

    Examples:
        >>> from pathlib import Path
        >>> create_excel_mapping(
        ...     components=["comp1", "comp2"],
        ...     rep_maps=["rep1", "rep2"],
        ...     output_path=Path("mapping.xlsx")
        ... )
        PosixPath('mapping.xlsx')
    """
    # Validate inputs
    if not components:
        raise ValueError("components cannot be empty.")
    if not output_path.parent.exists():
        raise FileNotFoundError(f"Directory {output_path.parent} does not exist.")

    # Create workbook
    wb = Workbook()
    default_sheet = wb.active
    default_sheet.title = "comp_mapping"

    
# Prepare mapping_rules column
    mapping_rules = []
    if rep_maps and len(rep_maps) > 0:
        for comp in components:
            if comp in rep_maps:
                # Add hyperlink only if component matches a rep_map name
                mapping_rules.append(f'=HYPERLINK("#{comp}!A1","{comp}")')
            else:
                mapping_rules.append("")
    else:
        mapping_rules = ["" for _ in components]


    # Prepare default mapping DataFrame
    comp_mapping_df = pd.DataFrame({
        "source": ["" for _ in components],
        "target": components,
        "mapping_rules": mapping_rules
    })

    # Write default mapping to sheet
    for row in dataframe_to_rows(comp_mapping_df, index=False, header=True):
        default_sheet.append(row)

    # Add representation mapping tabs if provided
    if rep_maps:
        for tab_name in rep_maps:
            ws = wb.create_sheet(title=tab_name)
            # Prepare empty DataFrame for representation mapping
            df_rep = pd.DataFrame(columns=["source", "target", "valid_from", "valid_to"])
            for row in dataframe_to_rows(df_rep, index=False, header=True):
                ws.append(row)

    # Save workbook
    wb.save(output_path)
    return output_path


