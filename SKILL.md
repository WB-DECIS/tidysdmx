---
name: tidysdmx
description: >-
  A toolbox to work with SDMX data, built on pysdmx. Use when fetching SDMX
  schemas from an FMR registry, building or applying structure maps, validating
  datasets against codelists, or preparing data for SDMX dissemination.
---

# tidysdmx

A toolbox to work with SDMX data. It wraps [pysdmx](https://py.sdmx.io) and adds
higher-level functionality pysdmx does not provide: Excel mapping templates,
DataFrame-driven artefact builders, dataset validation, and publish-readiness
checks.

Everything public is re-exported from the top-level package, so
`from tidysdmx import fetch_schema` is the supported import path. Reaching into
submodules (`tidysdmx.structures`, `tidysdmx.tidysdmx`) is not part of the
public contract.

## Installation

```bash
pip install tidysdmx
```

## The main pipeline

The canonical flow is fetch → build a map → apply it → validate → standardise.

```python
import pandas as pd

from tidysdmx import (
    build_structure_map_from_template_wb,
    fetch_schema,
    map_structures,
    parse_mapping_template_wb,
    standardize_output,
    validate_dataset_local,
)

# 1. Fetch the target schema from an FMR registry.
schema = fetch_schema(
    base_url="https://fmr.example.org",
    artefact_id="WB:WDI(1.0.0)",
    context="dataflow",
)

# 2. Read an Excel mapping template and turn it into a pysdmx StructureMap.
sheets = parse_mapping_template_wb("mapping_template.xlsx")
structure_map = build_structure_map_from_template_wb(sheets)

# 3. Apply the map to raw data.
raw = pd.read_csv("raw_data.csv")
mapped = map_structures(raw, structure_map)

# 4. Validate against the schema's codelists. Returns a DataFrame of errors;
#    an empty frame means the dataset is clean.
errors = validate_dataset_local(mapped, schema=schema)
if not errors.empty:
    raise ValueError(errors["Error"].tolist())

# 5. Add the SDMX reference columns an SDMX-CSV message needs.
final = standardize_output(mapped, artefact_id="WB:WDI(1.0.0)", schema=schema)
```

## Key APIs by task

| Task | Functions |
|---|---|
| Fetch schemas from FMR | `fetch_schema`, `parse_artefact_id`, `create_schema_from_table` |
| Read Excel mapping templates | `parse_mapping_template_wb`, `build_structure_map_from_template_wb` |
| Build map rules by hand | `build_fixed_map`, `build_implicit_component_map`, `build_date_pattern_map`, `build_value_map`, `build_single_component_map`, `build_multi_component_map` |
| Apply maps to DataFrames | `map_structures`, `apply_fixed_value_maps`, `apply_implicit_component_maps`, `apply_multi_component_map` |
| Validate datasets | `validate_dataset_local` (returns errors), `validate_columns`, `validate_codelist_ids`, `validate_duplicates`, `validate_mandatory_columns`, `validate_no_missing_values` (these raise) |
| Build artefacts for publication | `build_codelist`, `build_concept_scheme`, `build_dataflow`, `build_data_structure_definition`, `build_agency_scheme`, `build_category_scheme`, `build_hierarchy` |
| Check publish-readiness | `validate`, `validate_many`, `raise_if_invalid`, `ValidationIssue`, `ValidationError` |
| Prepare a map for FMR upload | `collect_structure_map_artifacts`, `validate_structure_map_references`, `prepare_structure_map_for_upload` |
| Standardise output | `standardize_output`, `standardize_sdmx`, `standardize_indicator_id`, `sanitize_variable` |

## Conventions worth knowing

- **DataFrame in, DataFrame out.** Functions return new objects; inputs are not
  mutated.
- **Column names are UPPER_SNAKE_CASE**, matching SDMX dimension IDs
  (`INDICATOR`, `TIME_PERIOD`, `OBS_VALUE`).
- **Two validation vocabularies.** `validate_dataset_local` checks *data* against
  a schema and returns an error DataFrame. `validate` / `raise_if_invalid` check
  *artefacts* for publish-readiness and raise `ValidationError`.
- **Deprecated functions emit `FutureWarning`.** `fetch_dsd_schema`,
  `parse_dsd_id`, `standardize_data_for_upload` and `add_sdmx_reference_cols`
  are retained for compatibility only; each names its replacement in its
  docstring.
