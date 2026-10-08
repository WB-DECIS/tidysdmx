---
name: tidysdmx
description: >-
  A toolbox to work with SDMX data, built on pysdmx. Use when fetching SDMX
  schemas from an FMR registry, connecting to an FMR behind single sign-on
  (bearer tokens, refresh), uploading artefacts, building or applying structure
  maps, validating datasets against codelists, or preparing data for SDMX
  dissemination.
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

The canonical flow is fetch → build a map → apply it → standardise → validate.

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

# 3. Apply the map to raw data. The result keeps the raw columns alongside
#    every target column.
raw = pd.read_csv("raw_data.csv")
mapped = map_structures(raw, structure_map)

# 4. Keep only the schema's components and add the SDMX-CSV reference columns
#    (STRUCTURE, STRUCTURE_ID, ACTION). ACTION defaults to "I"; pass
#    action="A", "R" or "D" for the other SDMX-CSV actions.
final = standardize_output(mapped, artefact_id="WB:WDI(1.0.0)", schema=schema)

# 5. Validate against the schema's columns and codelists. Returns a DataFrame
#    of errors; an empty frame means the dataset is clean.
errors = validate_dataset_local(final, schema=schema)
if not errors.empty:
    raise ValueError(errors["Error"].tolist())
```

Validate *after* `standardize_output`, never straight after `map_structures`:
given a `schema` and no `sdmx_cols`, `validate_dataset_local` requires the three
reference columns and rejects every column that is not a schema component.

## Key APIs by task

| Task | Functions |
|---|---|
| Fetch schemas from FMR | `fetch_schema`, `parse_artefact_id` |
| Fetch artefacts from FMR (any client, signed in or not) | `FmrClient.fetch_artefact`, `fetch_codelist`, `fetch_hierarchy`, `fetch_concept_scheme`, `fetch_category_scheme`, `fetch_categorisation`, `fetch_dataflow`, `fetch_data_structure_definition`, `fetch_provision_agreement`, `fetch_metadataflow`, `fetch_metadata_structure`, `fetch_metadata_provision_agreement`, `fetch_structure_map`, `fetch_representation_map`, `fetch_transformation_scheme`; `ArtefactType` lists the type names, `RegistryArtefact` is what `fetch_artefact` returns |
| Other registry reads (every pysdmx `RegistryClient` getter is wrapped) | `FmrClient.fetch_schema`, `fetch_dataflow_info`, `fetch_agencies`, `fetch_data_providers`, `fetch_metadata_providers`, `fetch_metadata_report`, `fetch_metadata_reports` |
| Connect to FMR with authentication and token refresh | `FmrClient`, `AzureTokenProvider`, `StaticTokenProvider`, `TokenProvider`, `BearerToken` |
| Describe a tidy DataFrame as SDMX structures | `create_schema_from_table` — returns `SchemaComponents(dsd, concept_scheme, codelists)`; `.dsd.to_schema()` gives the pysdmx `Schema` that validation takes |
| Read Excel mapping templates | `parse_mapping_template_wb`, `build_structure_map_from_template_wb` |
| Build map rules by hand | `build_fixed_map`, `build_implicit_component_map`, `build_date_pattern_map`, `build_value_map`, `build_value_map_list`, `build_multi_value_map_list`, `build_representation_map`, `build_multi_representation_map`, `build_single_component_map`, `build_multi_component_map` |
| Apply maps to DataFrames | `map_structures`, `apply_fixed_value_maps`, `apply_implicit_component_maps`, `apply_multi_component_map` |
| Validate datasets | `validate_dataset_local` (returns errors), `validate_columns`, `validate_codelist_ids`, `validate_duplicates`, `validate_mandatory_columns`, `validate_no_missing_values` (these raise) |
| Build artefacts for publication | `build_codelist`, `build_concept_scheme`, `build_dataflow`, `build_data_structure_definition`, `build_agency_scheme`, `build_category_scheme`, `build_hierarchy` |
| Check publish-readiness | `validate`, `validate_many`, `raise_if_invalid`, `ValidationIssue`, `ValidationError` |
| Prepare a map for FMR upload | `collect_structure_map_artifacts`, `validate_structure_map_references`, `prepare_structure_map_for_upload` — return pysdmx artefacts for `FmrClient.put_structures` or `pysdmx.io.write_sdmx` |
| Standardise output | `standardize_output`, `standardize_indicator_id`, `sanitize_variable` |

## Conventions worth knowing

- **DataFrame in, DataFrame out.** Functions return new objects; inputs are not
  mutated.
- **Column names are UPPER_SNAKE_CASE**, matching SDMX dimension IDs
  (`INDICATOR`, `TIME_PERIOD`, `OBS_VALUE`).
- **Two validation vocabularies.** `validate_dataset_local` checks *data* against
  a schema and returns an error DataFrame. `validate` / `raise_if_invalid` check
  *artefacts* for publish-readiness and raise `ValidationError`.
- **Allowed codes come from the schema.** Codelist checks (`validate_dataset_local`,
  `filter_tidy_raw`, `extract_validation_info`) use each component's local
  (constrained) codes when the schema has them, otherwise its concept's codelist;
  when FMR returns a hierarchy, every code at every level is valid.
- **`map_structures` needs embedded representation maps.** A `ComponentMap`
  whose representation map is only a URN string raises `TypeError`, as does a
  `DatePatternMap`. Fetch a structure map with
  `FmrClient.fetch_structure_map("AGENCY:ID(VERSION)")`, which wraps pysdmx's
  `RegistryClient.get_mapping()` and embeds them.
- **`FmrClient` is the only stateful object.** Build one per registry and reuse it:
  `client.registry` is pysdmx's `RegistryClient`, `client.maintenance` its
  `RegistryMaintenanceClient`, both sending a bearer token that refreshes itself.
  Anything with `get_token() -> BearerToken` works as a `token_provider`.
  Its `fetch_*` methods take `"AGENCY:ID(VERSION)"` or a full or short URN of
  the matching class (so `client.fetch_data_structure_definition(dataflow.structure)`
  works), refuse wildcards and lists, and return pysdmx objects unchanged.
- **Deprecated functions emit `FutureWarning`.** `fetch_dsd_schema`,
  `parse_dsd_id`, `standardize_data_for_upload`, `add_sdmx_reference_cols`,
  and `fix_sdmx_xml_datatype_tags` are retained for compatibility only; each names
  its replacement in its docstring (pysdmx now writes SDMX-ML data types
  correctly, so `fix_sdmx_xml_datatype_tags` is simply dropped). The `valid`
  argument of `validate_dataset_local` is deprecated too: pass `schema`.
  `standardize_sdmx` and `kd_standardize_sdmx` call `standardize_data_for_upload`,
  so they warn on every call; use `standardize_output` for new code.
