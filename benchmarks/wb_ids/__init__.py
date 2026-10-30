"""A committed stand-in for the World Bank IDS bronze dataset and its pipeline.

The real input, a 15 GB CSV of International Debt Statistics with its mapping
template, cannot be committed. This package records what that input looks like
and rebuilds a faithful mock of it at any scale, so the notebook pipeline can be
run, measured and optimised without the real file:

- ``profile`` measures an IDS-shaped CSV (real or mock) into a JSON profile.
- ``template`` scrubs the real mapping workbook into the committed mock one.
- ``schema`` captures the DS_IDS target schema from FMR as JSON, and loads it.
- ``generate`` writes a mock CSV of any size from the committed profile.
- ``pipeline`` runs the notebook pipeline on a CSV and measures every step.

The committed inputs live in ``benchmarks/wb_ids/data/``. The dataset, its
size and how to regenerate everything are described in
``docs/datasets/wb-ids.md``.
"""

from pathlib import Path

DATA_DIR: Path = Path(__file__).parent / "data"
PROFILE_JSON: Path = DATA_DIR / "profile.json"
TEMPLATE_XLSX: Path = DATA_DIR / "WB_IDS_mock.xlsx"
SCHEMA_JSON: Path = DATA_DIR / "ds_ids_schema.json"
SAMPLE_CSV: Path = DATA_DIR / "WB_IDS_BRONZE_mock.csv"
# About 400 KB, under the check-added-large-files hook's 500 KB limit.
SAMPLE_ROWS: int = 4_000

# The bronze CSV's header, in file order. Here rather than in ``profile``, so a
# module that only needs the names does not import pyarrow with them.
COLUMNS: tuple[str, ...] = (
    "Country_Code",
    "Counterpart_Area_Code",
    "Series",
    "Series_Name",
    "Year",
    "Value",
    "country_series_metadata",
    "footnote",
)
CODE_COLUMNS: tuple[str, ...] = ("Country_Code", "Counterpart_Area_Code", "Series")
TEXT_COLUMNS: tuple[str, ...] = ("country_series_metadata", "footnote")
