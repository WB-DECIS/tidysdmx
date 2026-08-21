"""SDMX component extraction, mapping rules, and Excel helpers."""

import zipfile
from collections.abc import Sequence, Set
from pathlib import Path
from typing import Literal

import pandas as pd
import pysdmx as px
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from pysdmx.model import Schema
from typeguard import typechecked

_STANDARD_SDMX_REFERENCE_COLS: tuple[str, ...] = ("STRUCTURE", "STRUCTURE_ID", "ACTION")

# Per-context dispatch retained so context-specific columns can be
# reintroduced later without touching call sites.
_SDMX_REFERENCE_COLS_BY_CONTEXT: dict[str, tuple[str, ...]] = {
    "dataflow": _STANDARD_SDMX_REFERENCE_COLS,
    "datastructure": _STANDARD_SDMX_REFERENCE_COLS,
    "provisionagreement": _STANDARD_SDMX_REFERENCE_COLS,
}


@typechecked
def sdmx_reference_cols_for(
    context: Literal["dataflow", "datastructure", "provisionagreement"],
) -> list[str]:
    """Return the SDMX reference columns for a given schema context.

    Per the SDMX-CSV specification the reference column names are the same
    for every context — only the values carried in the ``STRUCTURE`` column
    differ (``dataflow``, ``datastructure``, ``provisionagreement``) — so
    every context currently resolves to
    ``["STRUCTURE", "STRUCTURE_ID", "ACTION"]``.

    Args:
        context: The SDMX schema context.

    Returns:
        The ``["STRUCTURE", "STRUCTURE_ID", "ACTION"]`` column names that an
        SDMX-CSV dataset is expected to carry for the given context.
    """
    return list(_SDMX_REFERENCE_COLS_BY_CONTEXT[context])


@typechecked
def extract_validation_info(schema: px.model.dataflow.Schema) -> dict[str, object]:
    """Extract validation information from a given schema.

    Args:
        schema: The schema object containing all necessary validation
            information.

    Returns:
        A dictionary containing validation information with the following keys:
            - valid_comp: List of valid component names.
            - mandatory_comp: List of mandatory component names.
            - coded_comp: List of coded component names.
            - codelist_ids: Dictionary with coded components as keys and
              list of codelist IDs as values.
            - dim_comp: List of dimension component names.
            - sdmx_cols: SDMX reference columns expected in the dataset
              (``STRUCTURE`` / ``STRUCTURE_ID`` / ``ACTION`` for every
              schema context), resolved via the schema's context.
    """
    comp = schema.components
    valid_comp = [c.id for c in comp]
    mandatory_comp = [c.id for c in comp if c.required]
    coded_comp = [c.id for c in comp if c.local_codes is not None]
    dim_comp = [c.id for c in comp if c.role == px.model.Role.DIMENSION]

    return {
        "valid_comp": valid_comp,
        "mandatory_comp": mandatory_comp,
        "coded_comp": coded_comp,
        "codelist_ids": get_codelist_ids(comp, coded_comp),
        "dim_comp": dim_comp,
        "sdmx_cols": sdmx_reference_cols_for(schema.context),
    }


@typechecked
def get_codelist_ids(
    comp: px.model.dataflow.Components, coded_comp: list[str]
) -> dict[str, list[str]]:
    """Retrieve all codelist IDs for given coded components.

    Args:
        comp: A pysdmx Components collection.
        coded_comp: List of coded component IDs.

    Returns:
        Dictionary with coded component IDs as keys and lists of codelist
        IDs as values.
    """
    return {
        component: [code.id for code in comp[component].local_codes.items]
        for component in coded_comp
    }


@typechecked
def extract_component_ids(schema: Schema) -> list[str]:
    """Retrieve all component IDs from a given pysdmx Schema.

    Args:
        schema: A pysdmx Schema object representing an SDMX structure.

    Returns:
        A list of component IDs contained in the schema.

    Raises:
        TypeError: If the input is not a Schema instance.
        ValueError: If the schema has no components.

    Examples:
        >>> from pysdmx.model import Schema, Components, Component
        >>> comp1 = Component(id="FREQ")
        >>> comp2 = Component(id="TIME_PERIOD")
        >>> schema = Schema(
        ...     context="datastructure",
        ...     agency="ECB",
        ...     id_="EXR",
        ...     components=Components([comp1, comp2]),
        ...     version="1.0.0",
        ...     urns=[],
        ... )
        >>> extract_component_ids(schema)
        ['FREQ', 'TIME_PERIOD']
    """
    if not schema.components or len(schema.components) == 0:
        raise ValueError("Schema contains no components.")
    return [component.id for component in schema.components]


@typechecked
def write_excel_mapping_template(
    components: Sequence[str],
    rep_maps: Sequence[str] | None = None,
    output_path: Path = Path("mapping.xlsx"),
) -> Path:
    """Generate an Excel mapping template with component and representation tabs.

    Args:
        components: An ordered list of unique target component IDs.
        rep_maps: A sequence of unique names for which dedicated
            representation mapping tabs should be created.
        output_path: The full path where the Excel file will be saved.

    Returns:
        The file path to the saved Excel workbook.

    Raises:
        ValueError: If `components` validation fails (delegated to helper).
        FileNotFoundError: If the parent directory for `output_path` does not
            exist.
        RuntimeError: If saving the workbook fails due to I/O issues.
    """
    if output_path.parent != Path(".") and not output_path.parent.exists():
        raise FileNotFoundError(
            f"Directory {output_path.parent} does not exist. Please create it first."
        )

    wb = build_excel_workbook(components, rep_maps)

    try:
        wb.save(str(output_path))
    except OSError as e:
        raise RuntimeError(
            f"Failed to save Excel workbook to {output_path}: {e}"
        ) from e

    return output_path


@typechecked
def create_mapping_rules(
    components: Sequence[str],
    rep_maps: Set[str] | None = None,
) -> list[str]:
    """Create Excel hyperlink formulas for components with representation maps.

    Args:
        components: A list or sequence of SDMX component IDs.
        rep_maps: A set of component IDs for which a representation map
            exists and a hyperlink should be generated.

    Returns:
        A list of strings, where each element is either an Excel hyperlink
        formula or an empty string.

    Raises:
        TypeError: If any input argument fails type validation via
            @typechecked.

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
    """Build a Workbook with component mapping and representation map sheets.

    The primary sheet ``comp_mapping`` contains three columns: ``source``,
    ``target``, and ``mapping_rules``, with hyperlinks for components that
    have a representation map.

    Args:
        components: An ordered list of unique target component IDs.
        rep_maps: A sequence of names (matching component IDs) for which
            dedicated representation mapping tabs should be created.
            Internally deduplicated via conversion to a set.

    Returns:
        An openpyxl Workbook object populated with sheets and headers.

    Raises:
        ValueError: If ``components`` validation fails (delegated to helper).
        TypeCheckError: If any input argument fails type validation.
        RuntimeError: If sheet creation fails due to invalid sheet names.
    """
    rep_map_set: Set[str] = set(rep_maps) if rep_maps else set()

    mapping_rules = create_mapping_rules(components, rep_map_set)

    comp_mapping_df = pd.DataFrame(
        {
            "source": [""] * len(components),
            "target": list(components),
            "mapping_rules": mapping_rules,
        }
    )

    wb = Workbook()
    default_sheet = wb.active
    default_sheet.title = "comp_mapping"

    for row in dataframe_to_rows(comp_mapping_df, index=False, header=True):
        default_sheet.append(row)

    if rep_map_set:
        rep_map_headers = ["source", "target", "valid_from", "valid_to"]
        df_rep = pd.DataFrame(columns=rep_map_headers)

        for tab_name in rep_map_set:
            try:
                ws = wb.create_sheet(title=tab_name)
                for row in dataframe_to_rows(df_rep, index=False, header=True):
                    ws.append(row)
            except ValueError as e:
                # openpyxl raises ValueError for invalid/duplicate/too-long
                # sheet titles.
                raise RuntimeError(
                    f"Failed to create sheet '{tab_name}'. "
                    f"Check for invalid characters or long names: {e}"
                ) from e

    return wb


@typechecked
def parse_mapping_template_wb(path: str | Path) -> dict[str, pd.DataFrame]:
    """Read an Excel mapping template and return all sheets as DataFrames.

    Args:
        path: Path to the Excel file.

    Returns:
        A dictionary where keys are sheet names and values are DataFrames.

    Raises:
        FileNotFoundError: If the provided file path does not exist.
        ValueError: If the file is not an Excel file (.xlsx or .xls).
        RuntimeError: If reading the Excel file fails.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    if path.suffix.lower() not in (".xlsx", ".xls"):
        raise ValueError(
            f"Invalid file type: {path.suffix}. Expected an Excel file (.xlsx or .xls)."
        )

    try:
        return pd.read_excel(path, sheet_name=None, dtype="string", engine="openpyxl")
    except (ValueError, OSError, zipfile.BadZipFile) as e:
        raise RuntimeError(f"Failed to read Excel file: {e}") from e


@typechecked
def fix_sdmx_xml_datatype_tags(
    input_path: str | Path,
    output_path: str | Path | None = None,
) -> Path:
    """Fix incorrect SourceCodelist/TargetCodelist tags in SDMX-ML.

    The pysdmx XML writer emits ``<str:SourceCodelist>String</str:SourceCodelist>``
    and ``<str:TargetCodelist>String</str:TargetCodelist>`` when a
    RepresentationMap uses a plain DataType. The correct SDMX 3.0 tags are
    ``<str:SourceDataType>`` and ``<str:TargetDataType>``.

    Args:
        input_path: Path to the SDMX-ML XML file to fix.
        output_path: Path to write the corrected XML. If ``None``, the input
            file is overwritten in place.

    Returns:
        The path to the written output file.

    Raises:
        FileNotFoundError: If ``input_path`` does not exist.
    """
    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"File not found: {input_path}")

    output_path = Path(output_path) if output_path is not None else input_path

    content = input_path.read_text(encoding="utf-8")
    content = content.replace(
        "<str:SourceCodelist>String</str:SourceCodelist>",
        "<str:SourceDataType>String</str:SourceDataType>",
    )
    content = content.replace(
        "<str:TargetCodelist>String</str:TargetCodelist>",
        "<str:TargetDataType>String</str:TargetDataType>",
    )
    output_path.write_text(content, encoding="utf-8")
    return output_path
