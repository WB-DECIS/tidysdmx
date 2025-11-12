# python
import importlib
import pytest
from typeguard import TypeCheckError

# Now import the module under test (reload if already imported)
import tidysdmx.validation as v
importlib.reload(v)

# extract_validation_info()
def test_extract_validation_info():
    """Check TypeError raised when input is not of the expected type."""
    with pytest.raises(TypeCheckError):
        schema = "string_input"
        v.extract_validation_info(schema)