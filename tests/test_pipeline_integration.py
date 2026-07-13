"""Integration tests for the data pipeline workflow.

These tests mirror the pipeline documented in
``notebooks/tidysdmx/DEV-pipeline-iterative-development-wb-template.ipynb``
(Steps 1 through 6), exercising the end-to-end flow without FMR access.

The pipeline steps are:
  1. Load raw data
  2. Reshape raw data into tidy format + sanitize variables
  3. Describe tidy raw data with an SDMX schema
  4. Filter / apply constraints (optional)
  5. Validate tidy raw data against its schema
  5b. Create structure map from mapping template
  5c. Map data using the structure map
  5d. Standardize output for upload
  6. Final validation of mapped output against dissemination schema
  7. Collect artifacts for FMR upload
"""

import os
import pickle as pkl
import re
from pathlib import Path

import pandas as pd
import pytest
from pysdmx.api import fmr
from pysdmx.model import Schema
from pysdmx.model.map import (
    ComponentMap,
    FixedValueMap,
    ImplicitComponentMap,
    StructureMap,
)

from tidysdmx import (
    build_structure_map_from_template_wb,
    collect_structure_map_artifacts,
    create_schema_from_table,
    map_structures,
    parse_mapping_template_wb,
    sanitize_variable,
    standardize_output,
    validate_dataset_local,
)

DATA_DIR = Path(__file__).parent / "fixtures" / "data"
CACHE_DIR = Path(__file__).parent / "fixtures" / "cassettes"
RAW_CSV = DATA_DIR / "pipeline_raw_sample.csv"
MAPPING_XLSX = DATA_DIR / "pipeline_mapping_template.xlsx"

# Dissemination schema FMR parameters (matches the notebook)
DIS_FMR_URL = "https://fmrqa.worldbank.org/FMR/sdmx/v2"
DIS_AGENCY = "WB.GGH.HSP"
DIS_STRUCTURE_ID = "DS_ASPIRE"
DIS_STRUCTURE_VERSION = "1.0.0"
DIS_ARTEFACT_ID = f"{DIS_AGENCY}:{DIS_STRUCTURE_ID}({DIS_STRUCTURE_VERSION})"
DIS_SCHEMA_CACHE = CACHE_DIR / "pipeline_dis_schema.pkl"


def _load_dis_schema() -> Schema:
    """Load the dissemination schema from cache, or fetch from FMR.

    On first run (no cache file), calls the FMR API and pickles the
    response for subsequent runs.  This mirrors the notebook's
    ``client.get_schema(...)`` call.

    To generate the cache file, run the following snippet with FMR
    network access::

        import pickle as pkl
        import pysdmx as px
        from pathlib import Path

        client = fmr.RegistryClient(
            "https://fmrqa.worldbank.org/FMR/sdmx/v2"
        )
        schema = client.get_schema(
            "datastructure",
            agency="WB.GGH.HSP",
            id="DS_ASPIRE",
            version="1.0.0",
        )

        cache = Path("tests/fixtures/cassettes/pipeline_dis_schema.pkl")
        cache.parent.mkdir(exist_ok=True)
        with open(cache, "wb") as f:
            pkl.dump(schema, f)
    """
    if DIS_SCHEMA_CACHE.exists():
        with open(DIS_SCHEMA_CACHE, "rb") as f:
            schema = pkl.load(f)
        assert isinstance(schema, Schema)
        return schema

    # Cache missing. Under CI, fail loudly rather than silently skipping and
    # live-fetching from FMR (which would mask a missing or renamed cassette).
    if os.environ.get("CI"):
        raise RuntimeError(
            f"Cassette {DIS_SCHEMA_CACHE.name} missing under CI; refusing to "
            f"live-fetch FMR. Regenerate it locally (see this function's docstring) "
            f"and commit it."
        )

    # Local dev only — attempt real API call, requires FMR network access.
    try:
        client = fmr.RegistryClient(DIS_FMR_URL)
        schema = client.get_schema(
            "datastructure",
            agency=DIS_AGENCY,
            id=DIS_STRUCTURE_ID,
            version=DIS_STRUCTURE_VERSION,
        )
    except Exception as exc:
        pytest.skip(
            f"Dissemination schema cache not found at "
            f"{DIS_SCHEMA_CACHE} and FMR is unreachable: {exc}"
        )

    CACHE_DIR.mkdir(exist_ok=True)
    with open(DIS_SCHEMA_CACHE, "wb") as f:
        pkl.dump(schema, f)

    return schema


# Deliberately NOT marked `integration` (the marker was already removed once,
# in 54144b7): under CI these tests are hermetic — the dissemination schema
# loads from the committed cassette and `_load_dis_schema` refuses to
# live-fetch when CI is set — and ci.yml deselects integration tests, so the
# marker would drop the only end-to-end pipeline test from CI. FMR is reached
# solely as a local-dev fallback to regenerate a missing cassette.
class TestPipelineWorkflow:
    """End-to-end pipeline integration test mirroring the notebook workflow.

    Each step builds on the output of the previous one. Pipeline state flows
    through *fixtures* (each fixture depends on the previous step's fixture),
    so the tests are order-independent and safe under randomisation / xdist —
    there is no shared class-attribute state, and a step whose prerequisite
    skips (e.g. the dissemination schema cache is unavailable) skips
    automatically via its fixture chain.
    """

    # -- Pipeline fixtures (each depends on the previous step) ---------------

    @pytest.fixture
    def raw_df(self):
        """Step 1: the raw CSV."""
        return pd.read_csv(RAW_CSV)

    @pytest.fixture
    def tidy_raw_df(self, raw_df):
        """Step 2: melt year columns, clean, uppercase, sanitize."""
        year_cols = [c for c in raw_df.columns if c.startswith("YR")]
        id_cols = [c for c in raw_df.columns if c not in year_cols]

        tidy = raw_df.melt(
            id_vars=id_cols,
            var_name="year",
            value_name="value",
        )
        tidy["year"] = tidy["year"].str.replace("YR", "", regex=False)
        tidy = tidy.dropna(subset=["value"])

        # Rename to match SDMX conventions
        tidy = tidy.rename(
            columns={
                "Series Code": "Series",
                "Country Code": "Country_Code",
                "year": "TIME_PERIOD",
            }
        )
        tidy.columns = [c.upper() for c in tidy.columns]

        # Drop columns not needed for SDMX pipeline
        tidy = tidy.drop(
            columns=[c for c in ["COUNTRY NAME", "SERIES NAME"] if c in tidy.columns]
        )

        # Sanitize dimension variables for SDMX compliance
        for dim in ["SERIES"]:
            tidy[dim] = tidy[dim].map(sanitize_variable)

        return tidy.reset_index(drop=True)

    @pytest.fixture
    def tidy_raw_schema(self, tidy_raw_df):
        """Step 3: schema components inferred from the tidy raw data."""
        return create_schema_from_table(
            tidy_raw_df,
            dimensions=["SERIES", "COUNTRY_CODE"],
            time_dimension="TIME_PERIOD",
            measure="VALUE",
        )

    @pytest.fixture
    def constrained_df(self, tidy_raw_df):
        """Step 4: tidy frame filtered to the target series."""
        constraints = {"SERIES": ["SPL_TR_AMT_RD"]}
        constrained = tidy_raw_df.copy()
        for column, valid_values in constraints.items():
            if column in constrained.columns:
                constrained = constrained[constrained[column].isin(valid_values)]
        return constrained.reset_index(drop=True)

    @pytest.fixture
    def structure_map(self):
        """Step 5b: the StructureMap built from the Excel mapping template."""
        mappings = parse_mapping_template_wb(MAPPING_XLSX)
        return build_structure_map_from_template_wb(
            mappings,
            target_structure_id=DIS_ARTEFACT_ID,
            source_structure_id="WB.DP:DP_SCHEMA(1.0)",
        )

    @pytest.fixture
    def mapped_df(self, constrained_df, structure_map):
        """Step 5c: constrained frame mapped through the structure map."""
        return map_structures(df=constrained_df, structure_map=structure_map)

    @pytest.fixture
    def dis_schema(self):
        """The dissemination schema (cached FMR response; skips if unavailable)."""
        return _load_dis_schema()

    @pytest.fixture
    def standardized_df(self, mapped_df, dis_schema):
        """Step 5d: mapped frame with SDMX reference columns added."""
        return standardize_output(
            df=mapped_df,
            artefact_id=DIS_ARTEFACT_ID,
            schema=dis_schema,
            action="I",
        )

    # -- Step 1: Load raw data -----------------------------------------------

    def test_step1_load_raw_data(self, raw_df):
        """Step 1: Load the raw CSV and verify its structure."""
        assert not raw_df.empty
        assert "Country Code" in raw_df.columns
        assert "Series Code" in raw_df.columns
        year_cols = [c for c in raw_df.columns if c.startswith("YR")]
        assert len(year_cols) > 0

    # -- Step 2: Reshape raw data into tidy format ---------------------------

    def test_step2_reshape_raw_data(self, tidy_raw_df):
        """Step 2: Melt year columns, clean, uppercase, sanitize."""
        assert "VALUE" in tidy_raw_df.columns
        assert "TIME_PERIOD" in tidy_raw_df.columns
        assert tidy_raw_df["VALUE"].notna().all()
        for col in tidy_raw_df.columns:
            assert col == col.upper(), f"Column '{col}' is not uppercase"
        for val in tidy_raw_df["SERIES"].dropna().unique():
            assert re.fullmatch(r"[A-Z0-9_]+", val), (
                f"'{val}' is not a valid SDMX code ID"
            )

    # -- Step 3: Create SDMX schema from tidy raw data ----------------------

    def test_step3_create_schema(self, tidy_raw_schema):
        """Step 3: Describe the tidy raw data with an SDMX schema."""
        assert tidy_raw_schema.dsd is not None
        assert tidy_raw_schema.concept_scheme is not None
        assert isinstance(tidy_raw_schema.codelists, list)

        comp_ids = [c.id for c in tidy_raw_schema.dsd.components]
        assert "SERIES" in comp_ids
        assert "COUNTRY_CODE" in comp_ids
        assert "TIME_PERIOD" in comp_ids
        assert "VALUE" in comp_ids

        assert len(tidy_raw_schema.codelists) >= 1
        cl_ids = [cl.id for cl in tidy_raw_schema.codelists]
        assert "CL_SERIES" in cl_ids

        schema = tidy_raw_schema.dsd.to_schema()
        assert isinstance(schema, Schema)

    # -- Step 4: Apply constraints (optional) --------------------------------

    def test_step4_apply_constraints(self, constrained_df):
        """Step 4: Filter data to keep only the target series."""
        assert not constrained_df.empty
        assert set(constrained_df["SERIES"].unique()) == {"SPL_TR_AMT_RD"}

    # -- Step 5: Validate tidy raw data against its schema -------------------

    def test_step5_validate_raw_data(self, tidy_raw_schema, constrained_df):
        """Step 5: Tidy raw data passes validation against its own schema."""
        raw_schema = tidy_raw_schema.dsd.to_schema()
        errors = validate_dataset_local(
            df=constrained_df,
            schema=raw_schema,
            sdmx_cols=[],
        )
        assert errors.empty, f"Unexpected validation errors:\n{errors.to_string()}"

    # -- Step 5b: Create structure map from mapping template -----------------

    def test_step5b_create_structure_map(self, structure_map):
        """Step 5b: Parse the Excel mapping template and build a StructureMap."""
        assert isinstance(structure_map, StructureMap)
        assert len(structure_map.maps) > 0

        map_types = {type(m) for m in structure_map.maps}
        assert FixedValueMap in map_types
        assert ImplicitComponentMap in map_types
        assert ComponentMap in map_types

    # -- Step 5c: Map data using the structure map ---------------------------

    def test_step5c_map_data(self, mapped_df, constrained_df):
        """Step 5c: Apply the structure map to transform source to target."""
        # Target columns present
        assert "INDICATOR" in mapped_df.columns
        assert "REF_AREA" in mapped_df.columns
        assert "OBS_VALUE" in mapped_df.columns
        assert "FREQ" in mapped_df.columns

        # Fixed value applied
        assert (mapped_df["FREQ"] == "A").all()

        # Implicit maps copy values
        assert mapped_df["OBS_VALUE"].notna().all()
        assert mapped_df["TIME_PERIOD"].notna().all()

        # Component map transforms values
        assert mapped_df["INDICATOR"].notna().all()
        assert set(mapped_df["INDICATOR"].unique()) == {"SPL_TR_AMT_RD"}

        # No rows lost during mapping
        assert len(mapped_df) == len(constrained_df)

    # -- Step 5d: Standardize output for upload ------------------------------

    def test_step5d_standardize_output(self, standardized_df):
        """Step 5d: Add SDMX reference columns and reorder for upload.

        The dissemination schema is loaded from a cached FMR response
        (see ``_load_dis_schema``). This is the same schema the notebook
        fetches via ``client.get_schema("datastructure", ...)``.
        """
        # SDMX reference columns added and positioned first
        assert list(standardized_df.columns[:3]) == [
            "STRUCTURE",
            "STRUCTURE_ID",
            "ACTION",
        ]
        assert (standardized_df["ACTION"] == "I").all()
        assert (standardized_df["STRUCTURE_ID"] == DIS_ARTEFACT_ID).all()

    # -- Step 6: Final validation --------------------------------------------

    def test_step6_final_validation(self, standardized_df, dis_schema):
        """Step 6: Standardized output passes dissemination schema validation.

        This validates against the pre-existing dissemination schema
        (from FMR), not a schema derived from the output. This catches
        real issues like missing dimensions, unexpected columns, or
        codelist violations.
        """
        errors = validate_dataset_local(
            df=standardized_df,
            schema=dis_schema,
        )
        assert errors.empty, f"Final validation errors:\n{errors.to_string()}"

    # -- Step 7: Collect artifacts for FMR upload ----------------------------

    def test_step7_collect_artifacts(self, structure_map):
        """Step 7: Collect the StructureMap and its RepresentationMaps."""
        artifacts = collect_structure_map_artifacts(structure_map)

        # At least RepresentationMap(s) + the StructureMap itself
        assert len(artifacts) >= 2
        assert isinstance(artifacts[-1], StructureMap)

    # -- End-to-end invariants -----------------------------------------------

    def test_end_to_end_row_count(self, standardized_df):
        """Pipeline preserves the expected number of observations (6)."""
        assert len(standardized_df) == 6

    def test_end_to_end_no_raw_columns_in_output(self, standardized_df):
        """Final output has no leftover raw source columns."""
        raw_cols = {"COUNTRY_NAME", "COUNTRY_CODE", "SERIES_NAME", "SERIES"}
        remaining = raw_cols & set(standardized_df.columns)
        assert not remaining, f"Unexpected raw columns in output: {remaining}"
