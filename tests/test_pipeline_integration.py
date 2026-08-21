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

        client = fmr.RegistryClient("https://fmrqa.worldbank.org/FMR/sdmx/v2")
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

    # Attempt real API call — requires FMR network access
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


class TestPipelineWorkflow:
    """End-to-end pipeline integration test mirroring the notebook workflow.

    Each step builds on the output of the previous one, just like in
    the notebook. Pipeline state is passed between tests via class
    attributes so the data flow is explicit and readable.
    """

    # -- Step 1: Load raw data -----------------------------------------------

    def test_step1_load_raw_data(self):
        """Step 1: Load the raw CSV and verify its structure."""
        raw_df = pd.read_csv(RAW_CSV)

        assert not raw_df.empty
        assert "Country Code" in raw_df.columns
        assert "Series Code" in raw_df.columns
        year_cols = [c for c in raw_df.columns if c.startswith("YR")]
        assert len(year_cols) > 0

        TestPipelineWorkflow.raw_df = raw_df

    # -- Step 2: Reshape raw data into tidy format ---------------------------

    def test_step2_reshape_raw_data(self):
        """Step 2: Melt year columns, clean, uppercase, sanitize."""
        raw_df = TestPipelineWorkflow.raw_df

        # Melt year columns into tidy format (one observation per row)
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

        tidy = tidy.reset_index(drop=True)

        # Verify tidy format
        assert "VALUE" in tidy.columns
        assert "TIME_PERIOD" in tidy.columns
        assert tidy["VALUE"].notna().all()
        for col in tidy.columns:
            assert col == col.upper(), f"Column '{col}' is not uppercase"
        for val in tidy["SERIES"].dropna().unique():
            assert re.fullmatch(r"[A-Z0-9_]+", val), (
                f"'{val}' is not a valid SDMX code ID"
            )

        TestPipelineWorkflow.tidy_raw_df = tidy

    # -- Step 3: Create SDMX schema from tidy raw data ----------------------

    def test_step3_create_schema(self):
        """Step 3: Describe the tidy raw data with an SDMX schema."""
        tidy_raw_df = TestPipelineWorkflow.tidy_raw_df

        tidy_raw_schema = create_schema_from_table(
            tidy_raw_df,
            dimensions=["SERIES", "COUNTRY_CODE"],
            time_dimension="TIME_PERIOD",
            measure="VALUE",
        )

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

        TestPipelineWorkflow.tidy_raw_schema = tidy_raw_schema

    # -- Step 4: Apply constraints (optional) --------------------------------

    def test_step4_apply_constraints(self):
        """Step 4: Filter data to keep only the target series."""
        tidy_raw_df = TestPipelineWorkflow.tidy_raw_df

        constraints = {"SERIES": ["SPL_TR_AMT_RD"]}
        constrained = tidy_raw_df.copy()
        for column, valid_values in constraints.items():
            if column in constrained.columns:
                constrained = constrained[constrained[column].isin(valid_values)]
        constrained = constrained.reset_index(drop=True)

        assert not constrained.empty
        assert set(constrained["SERIES"].unique()) == {"SPL_TR_AMT_RD"}

        TestPipelineWorkflow.constrained_df = constrained

    # -- Step 5: Validate tidy raw data against its schema -------------------

    def test_step5_validate_raw_data(self):
        """Step 5: Tidy raw data passes validation against its own schema."""
        raw_schema = TestPipelineWorkflow.tidy_raw_schema.dsd.to_schema()
        errors = validate_dataset_local(
            df=TestPipelineWorkflow.constrained_df,
            schema=raw_schema,
            sdmx_cols=[],
        )
        assert errors.empty, f"Unexpected validation errors:\n{errors.to_string()}"

    # -- Step 5b: Create structure map from mapping template -----------------

    def test_step5b_create_structure_map(self):
        """Step 5b: Parse the Excel mapping template and build a StructureMap."""
        mappings = parse_mapping_template_wb(MAPPING_XLSX)
        target_id = DIS_ARTEFACT_ID
        source_id = "WB.DP:DP_SCHEMA(1.0)"

        sm = build_structure_map_from_template_wb(
            mappings,
            target_structure_id=target_id,
            source_structure_id=source_id,
        )

        assert isinstance(sm, StructureMap)
        assert len(sm.maps) > 0

        map_types = {type(m) for m in sm.maps}
        assert FixedValueMap in map_types
        assert ImplicitComponentMap in map_types
        assert ComponentMap in map_types

        TestPipelineWorkflow.structure_map = sm

    # -- Step 5c: Map data using the structure map ---------------------------

    def test_step5c_map_data(self):
        """Step 5c: Apply the structure map to transform source to target."""
        mapped = map_structures(
            df=TestPipelineWorkflow.constrained_df,
            structure_map=TestPipelineWorkflow.structure_map,
        )

        # Target columns present
        assert "INDICATOR" in mapped.columns
        assert "REF_AREA" in mapped.columns
        assert "OBS_VALUE" in mapped.columns
        assert "FREQ" in mapped.columns

        # Fixed value applied
        assert (mapped["FREQ"] == "A").all()

        # Implicit maps copy values
        assert mapped["OBS_VALUE"].notna().all()
        assert mapped["TIME_PERIOD"].notna().all()

        # Component map transforms values
        assert mapped["INDICATOR"].notna().all()
        assert set(mapped["INDICATOR"].unique()) == {"SPL_TR_AMT_RD"}

        # No rows lost during mapping
        assert len(mapped) == len(TestPipelineWorkflow.constrained_df)

        TestPipelineWorkflow.mapped_df = mapped

    # -- Step 5d: Standardize output for upload ------------------------------

    def test_step5d_standardize_output(self):
        """Step 5d: Add SDMX reference columns and reorder for upload.

        The dissemination schema is loaded from a cached FMR response
        (see ``_load_dis_schema``). This is the same schema the notebook
        fetches via ``client.get_schema("datastructure", ...)``.
        """
        mapped_df = TestPipelineWorkflow.mapped_df

        # Load pre-existing dissemination schema (cached FMR response)
        dis_schema = _load_dis_schema()

        out = standardize_output(
            df=mapped_df,
            artefact_id=DIS_ARTEFACT_ID,
            schema=dis_schema,
            action="I",
        )

        # SDMX reference columns added and positioned first
        assert list(out.columns[:3]) == [
            "STRUCTURE",
            "STRUCTURE_ID",
            "ACTION",
        ]
        assert (out["ACTION"] == "I").all()
        assert (out["STRUCTURE_ID"] == DIS_ARTEFACT_ID).all()

        TestPipelineWorkflow.standardized_df = out
        TestPipelineWorkflow.dis_schema = dis_schema

    # -- Step 6: Final validation --------------------------------------------

    def test_step6_final_validation(self):
        """Step 6: Standardized output passes dissemination schema validation.

        This validates against the pre-existing dissemination schema
        (from FMR), not a schema derived from the output. This catches
        real issues like missing dimensions, unexpected columns, or
        codelist violations.
        """
        if not hasattr(TestPipelineWorkflow, "standardized_df"):
            pytest.skip("Step 5d was skipped (no dissemination schema cache)")
        errors = validate_dataset_local(
            df=TestPipelineWorkflow.standardized_df,
            schema=TestPipelineWorkflow.dis_schema,
        )
        assert errors.empty, f"Final validation errors:\n{errors.to_string()}"

    # -- Step 7: Collect artifacts for FMR upload ----------------------------

    def test_step7_collect_artifacts(self):
        """Step 7: Collect the StructureMap and its RepresentationMaps."""
        sm = TestPipelineWorkflow.structure_map
        artifacts = collect_structure_map_artifacts(sm)

        # At least RepresentationMap(s) + the StructureMap itself
        assert len(artifacts) >= 2
        assert isinstance(artifacts[-1], StructureMap)

    # -- End-to-end invariants -----------------------------------------------

    def test_end_to_end_row_count(self):
        """Pipeline preserves the expected number of observations (6)."""
        if not hasattr(TestPipelineWorkflow, "standardized_df"):
            pytest.skip("Step 5d was skipped (no dissemination schema cache)")
        assert len(TestPipelineWorkflow.standardized_df) == 6

    def test_end_to_end_no_raw_columns_in_output(self):
        """Final output has no leftover raw source columns."""
        if not hasattr(TestPipelineWorkflow, "standardized_df"):
            pytest.skip("Step 5d was skipped (no dissemination schema cache)")
        raw_cols = {"COUNTRY_NAME", "COUNTRY_CODE", "SERIES_NAME", "SERIES"}
        remaining = raw_cols & set(TestPipelineWorkflow.standardized_df.columns)
        assert not remaining, f"Unexpected raw columns in output: {remaining}"
