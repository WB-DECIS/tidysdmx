
import pytest
import json
import os
from pathlib import Path
import requests
# from .fixtures.fmr_api import dsd_schema

# Import fixtures define under .tests/fixtures
pytest_plugins = [
    "fixtures.fxtr_schemas",
    "fixtures.fxtr_dummy_data",
    "fixtures.fxtr_structures",
    "fixtures.fxtr_mapping"
]




