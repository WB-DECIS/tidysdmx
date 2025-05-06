from tidysdmx import tidysdmx
import pytest

def test_parse_dsd_id_valid_input():
    # Test with a valid DSD ID
    dsd_id = "WB:WDI(1.0)"
    expected_result = ("WB", "WDI", "1.0")
    assert tidysdmx.parse_dsd_id(dsd_id) == expected_result

def test_parse_dsd_id_missing_colon():
    # Test with a DSD ID missing the colon
    dsd_id = "WBWDI(1.0)"
    try:
        tidysdmx.parse_dsd_id(dsd_id)
    except ValueError as e:
        assert str(e) == "Invalid dsd_id format. Expected format: 'agency:id(version)'"

def test_parse_dsd_id_missing_parentheses():
    # Test with a DSD ID missing the parentheses
    dsd_id = "WB:WDI1.0"
    try:
        tidysdmx.parse_dsd_id(dsd_id)
    except ValueError as e:
        assert str(e) == "Invalid dsd_id format. Expected format: 'agency:id(version)'"

def test_parse_dsd_id_empty_string():
    # Test with an empty string
    dsd_id = ""
    try:
        tidysdmx.parse_dsd_id(dsd_id)
    except ValueError as e:
        assert str(e) == "Invalid dsd_id format. Expected format: 'agency:id(version)'"

def test_parse_dsd_id_extra_colon():
    # Test with an extra colon in the DSD ID
    dsd_id = "WB:WDI:Extra(1.0)"
    try:
        tidysdmx.parse_dsd_id(dsd_id)
    except ValueError as e:
        assert str(e) == "Invalid dsd_id format. Expected format: 'agency:id(version)'"