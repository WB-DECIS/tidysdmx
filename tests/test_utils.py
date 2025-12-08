from typeguard import TypeCheckError
from pysdmx.model import Component, Components, Schema
from openpyxl import load_workbook
from pathlib import Path
import pytest
import os
# Import tidysdmx functions
from tidysdmx.utils import (
    get_codelist_ids, 
    extract_validation_info, 
    extract_component_ids, 
    create_mapping_rules,
    build_excel_workbook,
    write_excel_mapping_template
)

# Define fixtures
@pytest.fixture
def test_workbook_data() -> tuple[list[str], list[str]]:
    """Common test data for components and rep_maps."""
    components = ["C_FREQ", "C_REF_AREA", "C_UNIT", "C_OBS_VALUE"]
    rep_maps = ["C_REF_AREA", "C_UNIT", "C_REF_AREA"] # Duplicate to test deduplication
    return components, rep_maps

# region Testing extract_validation_info()
class TestExtractValidationInfo:
    @pytest.mark.parametrize("invalid_input", [
        None,
        {},
        [],
        "not_a_schema",
        123,
    ])

    def test_extract_validation_info(self, invalid_input):
        """Check TypeError raised when input is not of the expected type."""
        with pytest.raises(TypeCheckError):
            extract_validation_info(invalid_input)

    def test_extract_validation_has_expected_structure(self, ifpri_asti_schema):
        """Ensure the returned object has the expected type and structure."""
        result = extract_validation_info(ifpri_asti_schema)

        assert isinstance(result, dict)
        expected_keys = {"valid_comp", "mandatory_comp", "coded_comp", 
                         "codelist_ids", "dim_comp"}
        assert set(result.keys()) == expected_keys
        assert all(isinstance(item, list) for key, item in result.items() 
                   if key != "codelist_ids")
        assert isinstance(result["codelist_ids"], dict)

    def test_extract_validation_has_expected_structure2(self, sdmx_schema):
        """Ensure the returned object has the expected type and structure."""
        result = extract_validation_info(sdmx_schema)

        assert isinstance(result, dict)
        expected_keys = {"valid_comp", "mandatory_comp", "coded_comp", 
                         "codelist_ids", "dim_comp"}
        assert set(result.keys()) == expected_keys
        assert all(isinstance(item, list) for key, item in result.items() 
                   if key != "codelist_ids")
        assert isinstance(result["codelist_ids"], dict)
# endregion

# region Testing get_codelist_ids()
class TestGetCodelistIds:
    def test_get_codelist_ids_has_expected_structure(self, ifpri_asti_schema):
        """Ensure the returned object has the expected type and structure."""
        comp = ifpri_asti_schema.components
        coded_comp = [c.id for c in comp if comp[c.id].local_codes is not None]

        result = get_codelist_ids(comp, coded_comp)
        assert isinstance(result, dict)
        for key, value in result.items():
            assert key in coded_comp
            assert isinstance(value, list)
            assert all(isinstance(code_id, str) for code_id in value)

    # Test get_codelist_ids()
    @pytest.mark.skip(reason="Test needs to be modified to use correct inputs")
    def test_get_codelist_ids(self):
        comp = {"dim1": "Dimension 1", "dim2": "Dimension 2"}
        coded_comp = {"dim1": ["A", "B"], "dim2": ["C", "D"]}
        expected_output = {"dim1": ["A", "B"], "dim2": ["C", "D"]}
        assert get_codelist_ids(comp, coded_comp) == expected_output
# endregion

class TestExtractCodelistIds:  # noqa: D101
    @pytest.mark.skip(reason="Temporary skipping to generate a coverage report")
    def test_extract_component_ids_normal(self):
        """Retrieve IDs from a valid schema with multiple components."""
        comp1 = Component(id="FREQ")
        comp2 = Component(id="TIME_PERIOD")
        schema = Schema(context="datastructure", agency="ECB", id_="EXR",
                        components=Components([comp1, comp2]),
                        version="1.0.0", urns=[])
        result = extract_component_ids(schema)
        assert result == ["FREQ", "TIME_PERIOD"]
        assert all(isinstance(cid, str) for cid in result)

    @pytest.mark.skip(reason="Temporary skipping to generate a coverage report")
    def test_extract_component_ids_single_component(self):
        """Schema with a single component returns a list with one ID."""
        comp = Component(id="OBS_VALUE")
        schema = Schema(context="datastructure", agency="ECB", id_="EXR",
                        components=Components([comp]),
                        version="1.0.0", urns=[])
        result = extract_component_ids(schema)
        assert result == ["OBS_VALUE"]
        assert len(result) == 1

    @pytest.mark.skip(reason="Temporary skipping to generate a coverage report")
    def test_extract_component_ids_empty(self):
        """Schema with no components raises ValueError."""
        schema = Schema(context="datastructure", agency="ECB", id_="EXR",
                        components=Components([]),
                        version="1.0.0", urns=[])
        with pytest.raises(ValueError):
            extract_component_ids(schema)

    def test_extract_component_ids_invalid_type(self):
        """Non-Schema input raises TypeError."""
        with pytest.raises(TypeCheckError):
            extract_component_ids("not_a_schema")

    @pytest.mark.skip(reason="Temporary skipping to generate a coverage report")
    def test_extract_component_ids_component_without_id(self):
        """Component without an ID should raise Error."""
        comp = Component(id=None)  # Simulate missing ID
        schema = Schema(context="datastructure", agency="ECB", id_="EXR",
                        components=Components([comp]),
                        version="1.0.0", urns=[])
        with pytest.raises(TypeError):
            extract_component_ids(schema)

class TestCreateMappingRules:  # noqa: D101
    def test_create_mapping_rules_normal_case(self):
        """Tests a mix of matching and non-matching components."""
        components = ["D1", "D2", "D3", "D4"]
        rep_maps = {"D2", "D4"}
        expected = [
            "",
            '=HYPERLINK("#D2!A1","D2")',
            "",
            '=HYPERLINK("#D4!A1","D4")',
        ]
        result = create_mapping_rules(components, rep_maps)
        assert result == expected
        assert isinstance(result, list)
        assert all(isinstance(r, str) for r in result)


    def test_create_mapping_rules_no_matches(self):
        """Tests case where no component is present in the rep_maps."""
        components = ["D1", "D2", "D3"]
        rep_maps = {"D4", "D5"}
        expected = ["", "", ""]
        result = create_mapping_rules(components, rep_maps)
        assert result == expected


    def test_create_mapping_rules_all_matches(self):
        """Tests case where all components are present in the rep_maps."""
        components = ["D1", "D2", "D3"]
        rep_maps = {"D1", "D2", "D3", "D4"}
        expected = [
            '=HYPERLINK("#D1!A1","D1")',
            '=HYPERLINK("#D2!A1","D2")',
            '=HYPERLINK("#D3!A1","D3")',
        ]
        result = create_mapping_rules(components, rep_maps)
        assert result == expected


    def test_create_mapping_rules_empty_components(self):
        """Tests passing an empty list for components."""
        components: list[str] = []
        rep_maps = {"D1", "D2"}
        expected: list[str] = []
        result = create_mapping_rules(components, rep_maps)
        assert result == expected


    def test_create_mapping_rules_none_rep_maps(self):
        """Tests passing None for rep_maps (should result in all empty strings)."""
        components = ["D1", "D2", "D3"]
        rep_maps = None
        expected = ["", "", ""]
        result = create_mapping_rules(components, rep_maps)
        assert result == expected


    def test_create_mapping_rules_empty_rep_maps(self):
        """Tests passing an empty set for rep_maps (should result in all empty strings)."""
        components = ["D1", "D2", "D3"]
        rep_maps: AbstractSet[str] = set()
        expected = ["", "", ""]
        result = create_mapping_rules(components, rep_maps)
        assert result == expected


    def test_create_mapping_rules_type_error_for_components(self):
        """Tests for TypeCheckError when an incorrect type is passed for components."""
        with pytest.raises(TypeCheckError):
            # Incorrect type for components: int instead of Sequence[str]
            create_mapping_rules(123, {"D2"})  # type: ignore


    def test_create_mapping_rules_type_error_for_rep_maps(self):
        """Tests for TypeError when an incorrect type is passed for rep_maps."""
        components = ["D1", "D2"]
        with pytest.raises(TypeCheckError):
            # Incorrect type for rep_maps: list[int] instead of AbstractSet[str] or None
            create_mapping_rules(components, [1, 2])  # type: ignore


    def test_create_mapping_rules_value_error_for_empty_component_id(self):
        """Tests for ValueError when an empty string is in the components list (non-truthy string)."""
        components = ["D1", "", "D3"]
        rep_maps = {"D1", "D3"}
        with pytest.raises(ValueError) as excinfo:
            create_mapping_rules(components, rep_maps)
        assert "Component IDs must be non-empty strings" in str(excinfo.value)

class TestBuildExcelWorkbook:  # noqa: D101
    def test_build_excel_workbook_content_and_sheets(self, test_workbook_data: tuple[list[str], list[str]]):
        """Tests successful workbook creation and core content structure."""
        components, rep_maps = test_workbook_data
        
        wb = build_excel_workbook(components, rep_maps)
        
        # 1. Check sheet names and count (2 unique rep_maps + 1 default sheet)
        expected_sheet_titles = {"comp_mapping", "C_REF_AREA", "C_UNIT"}
        actual_sheet_titles = set(wb.sheetnames)
        assert actual_sheet_titles == expected_sheet_titles
        
        # 2. Check default sheet content (comp_mapping)
        main_sheet = wb["comp_mapping"]
        
        # Check Header (Row 1)
        header = [cell.value for cell in main_sheet[1]]
        assert header == ["source", "target", "mapping_rules"]
        
        # Check Data for mapping_rules column (Column C, Rows 2-5)
        rules_cells = [main_sheet[f"C{i}"].value for i in range(2, 6)]
        expected_rules = [
            "",
            '=HYPERLINK("#C_REF_AREA!A1","C_REF_AREA")',
            '=HYPERLINK("#C_UNIT!A1","C_UNIT")',
            "",
        ]
        assert rules_cells == expected_rules
        
        # 3. Check rep_map sheets content
        rep_sheet = wb["C_REF_AREA"]
        rep_header = [cell.value for cell in rep_sheet[1]]
        assert rep_header == ["source", "target", "valid_from", "valid_to"]


    def test_build_excel_workbook_no_rep_maps(self, test_workbook_data: tuple[list[str], list[str]]):
        """Tests workbook creation when rep_maps is None."""
        components, _ = test_workbook_data
        wb = build_excel_workbook(components, None)
        
        # Should only contain the default sheet
        assert wb.sheetnames == ["comp_mapping"]
        
        # Check mapping_rules column is all empty strings
        main_sheet = wb["comp_mapping"]
        rules_cells = [main_sheet[f"C{i}"].value for i in range(2, 6)]
        assert rules_cells == ["", "", "", ""]

class TestWriteExcelMappingTemplate:  # noqa: D101
    # Note: pytest automatically cleans up files created under tmp_path
    def test_write_excel_mapping_template_success(self, test_workbook_data: tuple[list[str], list[str]], tmp_path: Path):
        """Scenario: Tests successful file creation and ensures the file exists and is not empty."""
        components, rep_maps = test_workbook_data
        # Use tmp_path to ensure temporary file creation
        output_path = tmp_path / "test_saved_file.xlsx"

        result_path: Path = write_excel_mapping_template(components, rep_maps, output_path)

        assert result_path == output_path
        assert output_path.exists()
        assert output_path.name == "test_saved_file.xlsx"
        assert os.path.getsize(output_path) > 100 # Ensure file is not empty


    def test_write_excel_mapping_template_non_existent_directory_raises_filenotfounderror(self, test_workbook_data: tuple[list[str], list[str]], tmp_path: Path):
        """Scenario: Tests if non-existent parent directory raises FileNotFoundError."""
        components, rep_maps = test_workbook_data
        # Create a Path object pointing to a subdirectory that does not exist
        non_existent_dir = tmp_path / "sub_dir" 
        output_path = non_existent_dir / "test_missing_dir.xlsx"

        # Crucially, we do NOT create the non_existent_dir beforehand
        assert not non_existent_dir.exists()

        with pytest.raises(FileNotFoundError) as excinfo:
            write_excel_mapping_template(components, rep_maps, output_path)

        assert "does not exist" in str(excinfo.value)
        assert not output_path.exists() # Ensure no file was created


    def test_write_excel_mapping_template_integrity_check(self, test_workbook_data: tuple[list[str], list[str]], tmp_path: Path):
        """Scenario: Verifies that the saved file content is correct."""
        components, rep_maps = test_workbook_data
        output_path = tmp_path / "test_integrity.xlsx"

        write_excel_mapping_template(components, rep_maps, output_path)

        # Load the saved file
        wb = load_workbook(output_path)
    
        # Check sheet names
        expected_sheet_titles = {"comp_mapping", "C_REF_AREA", "C_UNIT"}
        assert set(wb.sheetnames) == expected_sheet_titles
        
        # Check a specific hyperlink cell
        main_sheet = wb["comp_mapping"]
        # Row 3, Column C should be the 'C_UNIT' hyperlink
        cell_value = main_sheet["C3"].value 
        assert cell_value == '=HYPERLINK("#C_REF_AREA!A1","C_REF_AREA")'