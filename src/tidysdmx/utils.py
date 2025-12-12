from typing import Dict, List, Optional, Sequence, AbstractSet, Literal, Any
from typeguard import typechecked
from pathlib import Path
from openpyxl import Workbook, load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from pathlib import Path
from datetime import datetime
from dateutil.parser import parse as parse_date
import pysdmx as px
import pandas as pd

# --- Import Official pysdmx Classes ---
from pysdmx.model import (
    StructureMap,
    ComponentMap,
    FixedValueMap,
    ImplicitComponentMap,
    RepresentationMap,
    ValueMap,
    Schema
)

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
    if not schema.components or len(schema.components) == 0:
        raise ValueError("Schema contains no components.")
    return [component.id for component in schema.components]


@typechecked
def write_excel_mapping_template(
    components: Sequence[str],
    rep_maps: Sequence[str] | None = None,
    output_path: Path = Path("mapping.xlsx")
) -> Path:
    """Generates an Excel file containing a default component mapping tab and optional tabs for representation maps by saving a Workbook object.

    Args:
        components (Sequence[str]): An ordered list of unique target component IDs.
        rep_maps (Sequence[str] | None): A sequence of unique names for which
            dedicated representation mapping tabs should be created.
        output_path (Path): The full path where the Excel file will be saved.

    Returns:
        Path: The file path to the saved Excel workbook.

    Raises:
        ValueError: If `components` validation fails (delegated to helper).
        FileNotFoundError: If the parent directory for `output_path` does not exist.
        RuntimeError: If saving the workbook fails due to I/O issues or if
            workbook creation fails (delegated to helper).

    Examples:
        >>> from pathlib import Path
        >>> # Setup a temporary file path
        >>> file_path = Path("temp_mapping_final.xlsx")
        >>> try:
        ...     write_excel_mapping_template(["C1", "C2"], ["C1"], file_path)
        ...     print(f"File created: {file_path.exists()}")
        ... finally:
        ...     if file_path.exists():
        ...         file_path.unlink() # Clean up
        File created: True
    """
    # 1. Validate environment
    if output_path.parent != Path(".") and not output_path.parent.exists():
        raise FileNotFoundError(
            f"Directory {output_path.parent} does not exist. Please create it first."
        )

    # 2. Build Workbook
    wb = build_excel_workbook(components, rep_maps)

    # 3. Save Workbook
    try:
        wb.save(str(output_path))
    except Exception as e:
        # Catch I/O error or other unexpected save failures
        raise RuntimeError(f"Failed to save Excel workbook to {output_path}: {e}") from e

    return output_path

@typechecked
def create_mapping_rules(
    components: Sequence[str],
    rep_maps: AbstractSet[str] | None = None,
) -> list[str]:
    """Create Excel-style hyperlink formulas for SDMX dataflow components.

    Creates a list of Excel-style hyperlink formulas for SDMX dataflow components
    that have corresponding representation maps, or an empty string otherwise.

    This utility is typically used when generating structured metadata output (e.g.,
    a DSD specification to Excel) where a component linking to a separate sheet
    (named after the component) indicates a custom mapping exists.

    Args:
        components (Sequence[str]): A list or sequence of SDMX component IDs.
        rep_maps (AbstractSet[str] | None): A set of component IDs for which a
            representation map exists and a hyperlink should be generated.

    Returns:
        list[str]: A list of strings, where each element is either an Excel
        formula string (=HYPERLINK("#COMPONENT_ID!A1","COMPONENT_ID")) or an
        empty string ("").

    Raises:
        TypeError: If any input argument fails type validation via @typechecked.

    Examples:
        >>> components = ["FREQ", "REF_AREA", "SEX", "OBS_VALUE"]
        >>> rep_maps = {"REF_AREA", "SEX"}
        >>> create_mapping_rules(components, rep_maps)
        ['', '=HYPERLINK("#REF_AREA!A1","REF_AREA")', '=HYPERLINK("#SEX!A1","SEX")', '']

        >>> create_mapping_rules(components, None)
        ['', '', '', '']

        >>> create_mapping_rules([], {"ANY"})
        []
    """
    # Defensive check: ensure all components are non-empty strings before processing,
    # though strict string type is covered by typechecked.
    if not all(isinstance(c, str) and c for c in components):
        invalid_components = [c for c in components if not (isinstance(c, str) and c)]
        if invalid_components:
            # Note: This is a defensive check; the typechecker handles non-string types.
            # This catches non-truthy strings like "".
            raise ValueError(
                f"Component IDs must be non-empty strings, but found invalid values: {invalid_components}"
            )

    # Simplified logic using a list comprehension. Handles None and empty set/list
    # for rep_maps efficiently.
    if not rep_maps:
        return [""] * len(components)

    return [
        f'=HYPERLINK("#{comp}!A1","{comp}")' if comp in rep_maps else ""
        for comp in components
    ]

@typechecked
def build_excel_workbook(
    components: Sequence[str],
    rep_maps: Sequence[str] | None = None,
) -> Workbook:
    """Construct an openpyxl Workbook containing the default component mapping sheet and optional template sheets for representation maps.

    The primary sheet 'comp_mapping' contains three columns: 'source', 'target', and 'mapping_rules', with hyperlinks for components having a rep_map.

    Args:
        components (Sequence[str]): An ordered list of unique target component IDs.
        rep_maps (Sequence[str] | None): A sequence of names (matching component
            IDs) for which dedicated representation mapping tabs should be created.
            The list is internally deduplicated via conversion to a set.

    Returns:
        Workbook: An openpyxl Workbook object populated with sheets and headers.

    Raises:
        ValueError: If 'components' validation fails (delegated to helper).
        TypeCheckError: If any input argument fails type validation.
        RuntimeError: If sheet creation fails due to invalid sheet names.
    """
    # 1. Prepare Data
    # Convert rep_maps to a set early for efficient lookup and guaranteed unique sheet names.
    rep_map_set: AbstractSet[str] = set(rep_maps) if rep_maps else set()
    
    # Leverage helper function
    mapping_rules: list[str] = create_mapping_rules(components, rep_map_set)
    
    comp_mapping_df = pd.DataFrame({
        "source": [""] * len(components),
        "target": components,
        "mapping_rules": mapping_rules
    })

    # 2. Create and Populate Workbook
    wb = Workbook()

    # a. Create sheet for components map
    default_sheet = wb.active
    default_sheet.title = "comp_mapping"

    # Write the header and data rows
    for row in dataframe_to_rows(comp_mapping_df, index=False, header=True):
        default_sheet.append(row)

    # b. Create optional sheets for representation maps
    if rep_map_set:
        REP_MAP_HEADERS = ["source", "target", "valid_from", "valid_to"]
        df_rep = pd.DataFrame(columns=REP_MAP_HEADERS)

        for tab_name in rep_map_set:
            try:
                ws = wb.create_sheet(title=tab_name)
                # Write header row only for the empty DataFrame
                for row in dataframe_to_rows(df_rep, index=False, header=True):
                    ws.append(row)
            except Exception as e:
                 # Openpyxl raises ValueError/KeyError for invalid or duplicate names.
                 raise RuntimeError(
                     f"Failed to create sheet with name: '{tab_name}'. "
                     f"Check for invalid characters or excessively long names: {e}"
                 )

    return wb

