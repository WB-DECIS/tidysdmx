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
  6. Final validation of mapped output
"""

from pathlib import Path

import pandas as pd
import pytest
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
RAW_CSV = DATA_DIR / "pipeline_raw_sample.csv"
MAPPING_XLSX = DATA_DIR / "pipeline_mapping_template.xlsx"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def raw_df():
    """Step 1: Load the raw CSV data."""
    return pd.read_csv(RAW_CSV)


@pytest.fixture(scope="module")
def tidy_raw_df(raw_df):
    """Step 2: Reshape raw data into tidy format and sanitize variables."""
    # Melt year columns
    year_cols = [c for c in raw_df.columns if c.startswith("YR")]
    id_cols = [c for c in raw_df.columns if c not in year_cols]

    tidy = raw_df.melt(
        id_vars=id_cols,
        var_name="year",
        value_name="value",
    )

    # Clean year prefix
    tidy["year"] = tidy["year"].str.replace("YR", "", regex=False)

    # Drop missing observations
    tidy = tidy.dropna(subset=["value"])

    # Rename to match SDMX conventions
    tidy = tidy.rename(
        columns={
            "Series Code": "Series",
            "Country Code": "Country_Code",
            "year": "TIME_PERIOD",
        }
    )

    # Uppercase all column names
    tidy.columns = [c.upper() for c in tidy.columns]

    # Drop columns not needed for SDMX pipeline
    tidy = tidy.drop(
        columns=[c for c in ["COUNTRY NAME", "SERIES NAME"] if c in tidy.columns]
    )

    # Sanitize dimension variables
    for dim in ["SERIES"]:
        tidy[dim] = tidy[dim].map(sanitize_variable)

    return tidy.reset_index(drop=True)


@pytest.fixture(scope="module")
def tidy_raw_schema(tidy_raw_df):
    """Step 3: Create an SDMX schema from the tidy raw DataFrame."""
    return create_schema_from_table(
        tidy_raw_df,
        dimensions=["SERIES", "COUNTRY_CODE"],
        time_dimension="TIME_PERIOD",
        measure="VALUE",
    )


@pytest.fixture(scope="module")
def constrained_df(tidy_raw_df):
    """Step 4: Apply constraints to filter the data (optional)."""
    constraints = {"SERIES": ["SPL_COV_TOT"]}
    df = tidy_raw_df.copy()
    for column, valid_values in constraints.items():
        if column in df.columns:
            df = df[df[column].isin(valid_values)]
    return df.reset_index(drop=True)


@pytest.fixture(scope="module")
def structure_map():
    """Step 5b: Build a StructureMap from the mapping template."""
    mappings = parse_mapping_template_wb(MAPPING_XLSX)
    target_id = "TEST.AGENCY:DS_TEST(1.0.0)"
    source_id = "WB.DP:DP_SCHEMA(1.0)"
    return build_structure_map_from_template_wb(
        mappings,
        target_structure_id=target_id,
        source_structure_id=source_id,
    )


@pytest.fixture(scope="module")
def mapped_df(constrained_df, structure_map):
    """Step 5c: Apply the structure map to the constrained data."""
    return map_structures(df=constrained_df, structure_map=structure_map)


@pytest.fixture(scope="module")
def dis_schema(mapped_df):
    """Create a dissemination schema from the mapped data for final validation.

    In the notebook this is fetched from FMR. Here we create it from the
    mapped DataFrame so the test is self-contained.
    """
    return create_schema_from_table(
        mapped_df,
        dimensions=["INDICATOR", "REF_AREA"],
        time_dimension="TIME_PERIOD",
        measure="OBS_VALUE",
        agency_id="TEST.AGENCY",
        schema_id="DS_TEST",
        version="1.0.0",
    )


@pytest.fixture(scope="module")
def standardized_df(mapped_df, dis_schema):
    """Step 5d: Standardize the mapped output for upload."""
    artefact_id = "TEST.AGENCY:DS_TEST(1.0.0)"
    schema = dis_schema.dsd.to_schema()
    return standardize_output(
        df=mapped_df,
        artefact_id=artefact_id,
        schema=schema,
        action="I",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestStep1LoadRawData:  # noqa: D101
    def test_raw_data_loaded(self, raw_df):
        """Raw CSV is loaded and contains expected columns."""
        assert not raw_df.empty
        assert "Country Code" in raw_df.columns
        assert "Series Code" in raw_df.columns

    def test_raw_data_has_year_columns(self, raw_df):
        """At least one year column (YR*) exists."""
        year_cols = [c for c in raw_df.columns if c.startswith("YR")]
        assert len(year_cols) > 0


@pytest.mark.integration
class TestStep2ReshapeRawData:  # noqa: D101
    def test_tidy_format_one_obs_per_row(self, tidy_raw_df):
        """After reshaping each row is a single observation."""
        assert "VALUE" in tidy_raw_df.columns
        assert "TIME_PERIOD" in tidy_raw_df.columns
        assert tidy_raw_df["VALUE"].notna().all()

    def test_columns_are_uppercase(self, tidy_raw_df):
        """All column names are uppercased to match SDMX convention."""
        for col in tidy_raw_df.columns:
            assert col == col.upper(), f"Column '{col}' is not uppercase"

    def test_sanitized_variables_sdmx_safe(self, tidy_raw_df):
        """Sanitized dimension values contain only SDMX-safe characters."""
        import re

        for val in tidy_raw_df["SERIES"].dropna().unique():
            assert re.fullmatch(r"[A-Z0-9_]+", val), (
                f"'{val}' is not a valid SDMX code ID"
            )

    def test_no_missing_values_after_dropna(self, tidy_raw_df):
        """All rows have a non-null VALUE after dropna."""
        assert tidy_raw_df["VALUE"].notna().all()


@pytest.mark.integration
class TestStep3CreateSchema:  # noqa: D101
    def test_schema_created(self, tidy_raw_schema):
        """create_schema_from_table returns a SchemaComponents namedtuple."""
        assert tidy_raw_schema.dsd is not None
        assert tidy_raw_schema.concept_scheme is not None
        assert isinstance(tidy_raw_schema.codelists, list)

    def test_dsd_has_expected_components(self, tidy_raw_schema):
        """DSD contains the dimension, time, and measure components."""
        comp_ids = [c.id for c in tidy_raw_schema.dsd.components]
        assert "SERIES" in comp_ids
        assert "COUNTRY_CODE" in comp_ids
        assert "TIME_PERIOD" in comp_ids
        assert "VALUE" in comp_ids

    def test_schema_generates_codelists_for_dimensions(self, tidy_raw_schema):
        """A codelist is generated for each dimension."""
        assert len(tidy_raw_schema.codelists) >= 1
        cl_ids = [cl.id for cl in tidy_raw_schema.codelists]
        assert "CL_SERIES" in cl_ids

    def test_dsd_to_schema_returns_schema(self, tidy_raw_schema):
        """DSD.to_schema() produces a pysdmx Schema object."""
        schema = tidy_raw_schema.dsd.to_schema()
        assert isinstance(schema, Schema)


@pytest.mark.integration
class TestStep4Constraints:  # noqa: D101
    def test_constraints_filter_data(self, constrained_df):
        """Applying constraints keeps only the requested series."""
        assert set(constrained_df["SERIES"].unique()) == {"SPL_COV_TOT"}

    def test_constrained_df_not_empty(self, constrained_df):
        """Filtered DataFrame still has data."""
        assert not constrained_df.empty


@pytest.mark.integration
class TestStep5ValidateRawData:  # noqa: D101
    def test_validate_tidy_raw_no_errors(self, constrained_df, tidy_raw_schema):
        """Step 5: Tidy raw data passes validation against its own schema."""
        raw_schema = tidy_raw_schema.dsd.to_schema()
        errors = validate_dataset_local(
            df=constrained_df, schema=raw_schema, sdmx_cols=[]
        )
        assert errors.empty, f"Unexpected validation errors:\n{errors.to_string()}"


@pytest.mark.integration
class TestStep5bCreateStructureMap:  # noqa: D101
    def test_structure_map_is_valid(self, structure_map):
        """build_structure_map_from_template_wb returns a StructureMap."""
        assert isinstance(structure_map, StructureMap)

    def test_structure_map_has_maps(self, structure_map):
        """StructureMap contains at least one mapping rule."""
        assert len(structure_map.maps) > 0

    def test_structure_map_contains_expected_types(self, structure_map):
        """StructureMap includes FixedValueMap, ImplicitComponentMap, ComponentMap."""
        types = {type(m) for m in structure_map.maps}
        assert FixedValueMap in types
        assert ImplicitComponentMap in types
        assert ComponentMap in types


@pytest.mark.integration
class TestStep5cMapData:  # noqa: D101
    def test_mapped_df_has_target_columns(self, mapped_df):
        """Mapped DataFrame contains columns from the structure map."""
        assert "INDICATOR" in mapped_df.columns
        assert "REF_AREA" in mapped_df.columns
        assert "OBS_VALUE" in mapped_df.columns
        assert "FREQ" in mapped_df.columns

    def test_fixed_value_applied(self, mapped_df):
        """Fixed value map sets FREQ to 'A' for all rows."""
        assert (mapped_df["FREQ"] == "A").all()

    def test_implicit_map_copies_values(self, mapped_df):
        """Implicit maps copy source values to target columns."""
        assert mapped_df["OBS_VALUE"].notna().all()
        assert mapped_df["TIME_PERIOD"].notna().all()

    def test_component_map_transforms_values(self, mapped_df):
        """ComponentMap maps source SERIES values to target INDICATOR values."""
        assert mapped_df["INDICATOR"].notna().all()
        assert set(mapped_df["INDICATOR"].unique()) == {"TEST_PIPELINE_SPL_COV_TOT"}


@pytest.mark.integration
class TestStep5dStandardizeOutput:  # noqa: D101
    def test_sdmx_reference_columns_added(self, standardized_df):
        """standardize_output adds STRUCTURE, STRUCTURE_ID, and ACTION."""
        assert "STRUCTURE" in standardized_df.columns
        assert "STRUCTURE_ID" in standardized_df.columns
        assert "ACTION" in standardized_df.columns

    def test_sdmx_reference_columns_first(self, standardized_df):
        """STRUCTURE, STRUCTURE_ID, ACTION are the first three columns."""
        first_three = list(standardized_df.columns[:3])
        assert first_three == ["STRUCTURE", "STRUCTURE_ID", "ACTION"]

    def test_action_column_value(self, standardized_df):
        """All rows have ACTION='I'."""
        assert (standardized_df["ACTION"] == "I").all()

    def test_structure_id_value(self, standardized_df):
        """STRUCTURE_ID matches the provided artefact_id."""
        assert (standardized_df["STRUCTURE_ID"] == "TEST.AGENCY:DS_TEST(1.0.0)").all()


@pytest.mark.integration
class TestStep6FinalValidation:  # noqa: D101
    def test_final_validation_no_errors(self, standardized_df, dis_schema):
        """Standardized output passes validation against dissemination schema."""
        schema = dis_schema.dsd.to_schema()
        errors = validate_dataset_local(df=standardized_df, schema=schema)
        assert errors.empty, f"Final validation errors:\n{errors.to_string()}"


@pytest.mark.integration
class TestStep7CollectArtifacts:  # noqa: D101
    def test_collect_structure_map_artifacts(self, structure_map):
        """Artifacts include the StructureMap and its dependencies."""
        artifacts = collect_structure_map_artifacts(structure_map)
        assert len(artifacts) >= 1
        # StructureMap should be the last artifact
        assert isinstance(artifacts[-1], StructureMap)

    def test_artifacts_include_representation_maps(self, structure_map):
        """Embedded RepresentationMaps are extracted as separate artifacts."""
        artifacts = collect_structure_map_artifacts(structure_map)
        # At least one RepresentationMap + the StructureMap itself
        assert len(artifacts) >= 2


@pytest.mark.integration
class TestEndToEndPipeline:  # noqa: D101
    def test_full_pipeline_row_count(self, raw_df, standardized_df):
        """Pipeline preserves the expected number of observations.

        Raw data has 6 rows with 3 non-null values each across 3 year columns,
        resulting in 6 observations after melt + dropna.
        """
        assert len(standardized_df) == 6

    def test_full_pipeline_no_data_loss(self, constrained_df, mapped_df):
        """No rows are lost during mapping (row count unchanged)."""
        assert len(mapped_df) == len(constrained_df)

    def test_full_pipeline_columns_clean(self, standardized_df):
        """Final output has no leftover raw source columns."""
        raw_cols = {"COUNTRY_NAME", "COUNTRY_CODE", "SERIES_NAME", "SERIES"}
        remaining = raw_cols & set(standardized_df.columns)
        assert not remaining, f"Unexpected raw columns in output: {remaining}"
