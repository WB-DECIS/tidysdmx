# Architecture: How tidysdmx Maps onto pysdmx

## Design Philosophy

pysdmx and tidysdmx solve related but different problems. Understanding the gap between them is the key to understanding why tidysdmx exists.

**pysdmx** is a faithful Python representation of the SDMX Information Model. Every class, attribute, and method corresponds to an SDMX artefact or operation defined in the SDMX standard. The primary audience is developers who need to work with SDMX metadata as a first-class domain. Working with pysdmx means thinking in terms of Data Structure Definitions, Components, Codelists, StructureMaps, and RepresentationMaps. The abstractions are precise and correct, but they require SDMX knowledge to use.

**tidysdmx** is a task-oriented library for data engineers and data analysts who need to prepare data for SDMX systems, but whose primary mental model is the data pipeline — raw files in, clean DataFrames out. The abstractions are named after the analyst's tasks: "fetch the schema", "standardize the data", "validate the dataset", "map the values". The primary data structure is a pandas DataFrame, not an SDMX object. pysdmx objects are used internally but are threaded through as opaque handles, not unpacked and manipulated directly.

The table below captures the philosophical gap at each stage of the workflow:

| Stage | pysdmx concept | tidysdmx concept |
|---|---|---|
| Structure | `Schema` object | The schema is fetched once, passed around, and never directly queried by the analyst |
| Components | `Components` / `Component` (typed SDMX artefacts) | A list of column names; a dict of allowed values |
| Mapping specification | `StructureMap` (SDMX artefact with typed sub-maps) | A JSON file with `components` and `representation` dicts; or an Excel workbook |
| Applying mappings | Iterate `StructureMap.maps`, dispatch by type | `map_structures(df, smap)` or `map_to_sdmx(df, mapping)` — a single function call |
| Validation | `Component.required`, `Component.local_codes.items` | `validate_dataset_local(df, schema)` — returns a DataFrame of error messages |
| Output preparation | No equivalent | `standardize_output(df, artefact_id, schema)` — adds metadata columns and reorders |
| Production use | No equivalent | Kedro-compatible wrapper functions |

---

## Layer Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          tidysdmx                                       │
│                                                                         │
│  fetch_schema()  →  standardize_output()  →  validate_dataset_local()  │
│  read_mapping()  →  map_to_sdmx()         →  kd_validate_datasets_*()  │
│  map_structures()   build_*()               filter_tidy_raw()           │
│  create_schema_from_table()                                             │
│  write_excel_mapping_template()                                         │
│  build_structure_map_from_template_wb()                                 │
│                                                                         │
│  Primary types: pd.DataFrame, dict, str, list                           │
├─────────────────────────────────────────────────────────────────────────┤
│                   Thin translation layer                                │
│                                                                         │
│  extract_validation_info()   — Schema → plain dict                      │
│  extract_component_ids()     — Schema → list[str]                       │
│  build_value_map_list()          — pd.DataFrame → list[ValueMap]        │
│  build_representation_map_from_df()— pd.DataFrame → RepresentationMap    │
│  build_single_component_map()    — pd.DataFrame + str → ComponentMap     │
├─────────────────────────────────────────────────────────────────────────┤
│                          pysdmx                                         │
│                                                                         │
│  Schema / Components / Component / Role / DataType                      │
│  Codelist / Code / Concept                                              │
│  StructureMap / FixedValueMap / ImplicitComponentMap                    │
│  ComponentMap / RepresentationMap / ValueMap                            │
│  MultiComponentMap / MultiRepresentationMap / MultiValueMap             │
│  DatePatternMap                                                         │
│  fmr.RegistryClient                                                     │
│                                                                         │
│  Primary types: pysdmx dataclasses                                      │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Functional Areas

### 1. Schema Fetching

**pysdmx view:** Instantiate a `RegistryClient` with a base URL and format, call `get_schema(context, agency, id, version)`, receive a `Schema` object. Four separate arguments, each derived from the SDMX artefact reference.

**tidysdmx view:** Call `fetch_schema(base_url, artefact_id, context)` with a single artefact ID string in the `"AGENCY:ID(VERSION)"` format — the format analysts already have in their config files. tidysdmx parses it, builds the client, and returns the schema.

```python
# pysdmx — developer builds the client and parses the ID manually
from pysdmx.api import fmr
from pysdmx.io.format import StructureFormat

client = fmr.RegistryClient("https://fmr.example.com/FMR/sdmx/v2/",
                             format=StructureFormat.FUSION_JSON)
schema = client.get_schema("dataflow", "WB", "WDI", "1.0.0")

# tidysdmx — analyst passes the ID they already have
from tidysdmx import fetch_schema

schema = fetch_schema(
    base_url="https://fmr.example.com",
    artefact_id="WB:WDI(1.0.0)",
    context="dataflow"
)
```

**What tidysdmx hides:** URL construction, `RegistryClient` instantiation, `StructureFormat` selection, ID string parsing. The analyst only needs to know the three things they already know: where the FMR is, what the artefact ID is, and whether it's a dataflow or a DSD.

---

### 2. Schema Introspection

**pysdmx view:** The `Schema` object is a rich tree of typed SDMX objects. To find out which columns are required, iterate `Components` and test `component.required`. To get allowed values, access `component.local_codes.items`. To identify dimensions, test `component.role == Role.DIMENSION`.

**tidysdmx view:** Call `extract_validation_info(schema)` once and get a plain Python `dict` with everything needed to validate a DataFrame. No pysdmx attributes are accessed after this point.

```python
# pysdmx — analyst must understand Component structure
mandatory = [c.id for c in schema.components if schema.components[c.id].required]
coded     = [c.id for c in schema.components if schema.components[c.id].local_codes is not None]
dims      = [c.id for c in schema.components
             if schema.components[c.id].role == px.model.Role.DIMENSION]

# tidysdmx — one call, plain dict
from tidysdmx import extract_validation_info
valid = extract_validation_info(schema)
# valid["mandatory_comp"] → ["FREQ", "REF_AREA", "TIME_PERIOD", "OBS_VALUE", ...]
# valid["coded_comp"]     → ["FREQ", "REF_AREA", "INDICATOR"]
# valid["codelist_ids"]   → {"FREQ": ["A", "M", "Q"], "REF_AREA": ["US", "GB", ...]}
# valid["dim_comp"]       → ["FREQ", "REF_AREA", "INDICATOR", "TIME_PERIOD"]
# valid["valid_comp"]     → all component IDs
```

The `valid` dict is the analyst's entire interface to schema knowledge. It is passed as a pre-computed argument to all downstream functions (`validate_dataset_local`, `kd_validate_datasets_local`, `filter_tidy_raw`) to avoid re-parsing the schema for each dataset.

---

### 3. Mapping Specification

This is where the philosophical difference is sharpest. pysdmx has a formal SDMX artefact for mappings; tidysdmx offers two analyst-facing formats.

#### 3a. JSON Mapping File (Legacy / Simple Pipelines)

The analyst writes a JSON file. No SDMX knowledge is required — no `StructureMap`, no `ComponentMap`, no `RepresentationMap`. The two concepts they need are:

- **`components`**: which source column maps to which target SDMX column (column rename)
- **`representation`**: for each column, which source values map to which target SDMX codes

```json
{
  "schema_version": "v2",
  "dsd_id": "WB:WDI(1.0.0)",
  "components": [
    {"SOURCE": "Country", "TARGET": "REF_AREA"},
    {"SOURCE": "Series",  "TARGET": "INDICATOR"},
    {"SOURCE": "Year",    "TARGET": "TIME_PERIOD"},
    {"SOURCE": "Value",   "TARGET": "OBS_VALUE"}
  ],
  "representation": {
    "REF_AREA": [
      {"SOURCE": "United States", "TARGET": "US"},
      {"SOURCE": ".*",            "TARGET": "ZZ", "IS_REGEX": true}
    ]
  }
}
```

`read_mapping(path)` parses this into a Python dict where DataFrames replace the lists, ready for `map_to_sdmx()`.

**pysdmx equivalent:** A `StructureMap` containing `ImplicitComponentMap`s (for the column renames), `ComponentMap`s with `RepresentationMap`s (for the value mappings), and `FixedValueMap`s — each a typed SDMX object constructed in Python code.

#### 3b. Excel Mapping Template (Accessible / Business Analyst Workflows)

For non-programmers or mixed technical/non-technical teams, tidysdmx generates an Excel workbook from a schema:

```python
from tidysdmx import fetch_schema, extract_component_ids, write_excel_mapping_template

schema     = fetch_schema(base_url, "WB:WDI(1.0.0)", "dataflow")
components = extract_component_ids(schema)
write_excel_mapping_template(components, rep_maps=["REF_AREA", "INDICATOR"],
                              output_path=Path("mapping.xlsx"))
```

The workbook has a `COMP_MAPPING` sheet (source component → target component, with a `MAPPING_RULES` column accepting `"implicit"`, `"fixed:<VALUE>"`, `"representation"`, or `"multi_representation"`) and a `REP_MAPPING` sheet holding value-level mappings (source columns prefixed `S:`, target columns prefixed `T:`).

For a **single** coded component, use `"representation"` with one component ID in `SOURCE`:

| SOURCE | TARGET | MAPPING_RULES |
|--------|--------|---------------|
| SERIES | INDICATOR | representation |

For an **N→1 multi-component** mapping (a tuple of source components jointly determining one target), use `"multi_representation"` and join the source component IDs in the `SOURCE` cell with `|`. The matching `S:`/`T:` columns in `REP_MAPPING` supply the value tuples:

`COMP_MAPPING`

| SOURCE | TARGET | MAPPING_RULES |
|--------|--------|---------------|
| FREQ\|REF_AREA | INDICATOR | multi_representation |

`REP_MAPPING`

| S:FREQ | S:REF_AREA | T:INDICATOR |
|--------|------------|-------------|
| A | US | GDP_ANNUAL |
| Q | US | GDP_QUARTERLY |

This produces a pysdmx `MultiComponentMap` whose `source` is the ordered tuple `(FREQ, REF_AREA)` and `target` is `[INDICATOR]`. (`|` is used purely as a separator inside the Excel template — it never appears in the emitted SDMX artefact, whose `MultiComponentMap` uses the parsed component IDs, so it does not need to be SDMX-compliant. A `multi_representation` rule needs at least two source components; source codelists are not yet read for multi rules, so sources map as plain strings while `TARGET_CL` is still honoured.)

`build_structure_map_from_template_wb(mappings)` reads the filled-in workbook and returns a pysdmx `StructureMap`. The analyst fills in Excel cells; pysdmx objects are the implementation detail.

**pysdmx equivalent:** A developer constructs the `StructureMap` programmatically in Python.

---

### 4. Applying Mappings

tidysdmx provides two parallel paths for applying mappings, corresponding to the two specification formats.

#### Path A — JSON mapping dict → `map_to_sdmx()`

```python
from tidysdmx import read_mapping, transform_source_to_target, map_to_sdmx

mapping = read_mapping("mapping.json")

# Step 1: rename columns (SOURCE → TARGET)
df = transform_source_to_target(raw_df, mapping)

# Step 2: recode values using representation rules
df = map_to_sdmx(df, mapping)
```

`map_to_sdmx` iterates the `representation` dict and applies vectorised pandas operations (`np.select` with regex or exact matching). There are no pysdmx objects involved at this point.

#### Path B — pysdmx `StructureMap` → `map_structures()`

```python
from tidysdmx import map_structures, build_structure_map_from_template_wb, parse_mapping_template_wb

mappings = parse_mapping_template_wb("mapping.xlsx")
smap     = build_structure_map_from_template_wb(mappings, agency="WB")

result_df = map_structures(df, smap)
```

`map_structures` dispatches by map type — it calls `apply_fixed_value_maps()`, `apply_implicit_component_maps()`, `apply_component_map()`, and `apply_multi_component_map()` depending on what the `StructureMap` contains. Each function operates on a DataFrame and returns a DataFrame.

**pysdmx view vs tidysdmx view:**

```
pysdmx StructureMap.maps           tidysdmx function called
───────────────────────────────    ─────────────────────────────
FixedValueMap                  →   apply_fixed_value_maps()
ImplicitComponentMap           →   apply_implicit_component_maps()
ComponentMap (+ RepresentationMap) apply_component_map()
MultiComponentMap              →   apply_multi_component_map()
```

Each `apply_*` function signature takes a DataFrame and returns a DataFrame. The pysdmx object is an argument, not the working data.

---

### 5. Building pysdmx Objects from DataFrames

The `build_*` functions in `tidysdmx.structures` are the explicit translation layer. They accept pandas DataFrames (and plain strings) and return pysdmx objects. They are the bridge that lets analysts specify mappings in tabular form and then obtain the correctly-typed SDMX artefacts pysdmx requires.

```
Analyst input (tabular)          →   pysdmx object
────────────────────────────────     ─────────────────────────────────────
(target: str, value: str)        →   FixedValueMap
(source: str, target: str)       →   ImplicitComponentMap
(source, target, pattern, freq)  →   DatePatternMap
(source: str, target: str)       →   ValueMap
pd.DataFrame[source, target, ...]→   list[ValueMap]         (via build_value_map_list)
pd.DataFrame + agency/id/...     →   RepresentationMap      (via build_representation_map_from_df)
pd.DataFrame + source/target_comp→   ComponentMap           (via build_single_component_map)
Workbook (openpyxl)              →   StructureMap           (via build_structure_map)
dict[str, pd.DataFrame]          →   StructureMap           (via build_structure_map_from_template_wb)
```

This layer is intentionally thin. Each builder function validates its inputs, then calls the pysdmx constructor. There is no business logic here — the translation is structural, not semantic.

---

### 6. Schema Creation from Data

`create_schema_from_table()` runs in the opposite direction: it infers the components of an SDMX schema — returned as a `SchemaComponents` namedtuple bundling the `dsd`, `concept_scheme`, and `codelists` — from a pandas DataFrame. It is used when no SDMX registry is available and the structural artefacts need to be inferred from data alone.

```python
from tidysdmx import create_schema_from_table

schema_components = create_schema_from_table(
    dataframe=df,
    dimensions=["FREQ", "REF_AREA"],
    time_dimension="YEAR",         # mapped to standard TIME_PERIOD component
    measure="OBS_VALUE",
    attributes=["OBS_STATUS"],
    agency_id="WB",
    schema_id="INFERRED_WDI"
)
```

The function:
1. Infers `DataType` from pandas dtypes
2. Builds a `Codelist` from unique values in each dimension column
3. Creates `Component` objects with the correct `Role` and `local_codes`
4. Maps the `time_dimension` column to the standardised `TIME_PERIOD` concept (with `DataType.PERIOD`)
5. Returns a `SchemaComponents` namedtuple (`dsd`, `concept_scheme`, `codelists`) holding the inferred artefacts

**Why this matters:** You get the inferred DSD, concept scheme, and codelists as first-class pysdmx artefacts, ready to publish to a registry via the `build_*` builders — without writing a line of XML. Note that the returned bundle is *not* itself a pysdmx `Schema`: the schema-consuming helpers (`validate_dataset_local()`, `standardize_output()`, `filter_tidy_raw()`) require a pysdmx `Schema` instance — however it was obtained — so validating inferred structures against themselves is not yet a one-call round-trip.

---

### 7. Validation

**pysdmx view:** Validation means navigating the object model: iterate `Components`, check `component.required`, compare data values against `component.local_codes.items`. Each check is a custom loop over the DataFrame.

**tidysdmx view:** `validate_dataset_local(df, schema)` is a single call that returns a DataFrame of errors. If the DataFrame is empty, the data is valid. Each validation failure is a row. The analyst can sort, filter, and export the error DataFrame like any other.

```python
from tidysdmx import validate_dataset_local, extract_validation_info

# Either pass the schema directly
errors = validate_dataset_local(df, schema=schema)

# Or pre-compute validation info for reuse across many datasets
valid  = extract_validation_info(schema)
errors = validate_dataset_local(df, valid=valid)

# errors is a plain pd.DataFrame:
# ┌──────────────────────┬──────────────────────────────────────────────┐
# │ Validation           │ Error                                        │
# ├──────────────────────┼──────────────────────────────────────────────┤
# │ codelist_ids         │ Invalid values found in 'REF_AREA': ['XYZ'] │
# │ missing_values       │ Missing values found in mandatory columns... │
# └──────────────────────┴──────────────────────────────────────────────┘
```

The five validation checks and their pysdmx source:

| tidysdmx check | pysdmx attribute used | Task-level meaning |
|---|---|---|
| `validate_columns` | `component.id` (all) | No unexpected columns exist |
| `validate_mandatory_columns` | `component.required` | All required columns are present |
| `validate_codelist_ids` | `component.local_codes.items` | All coded values are in the allowed list |
| `validate_duplicates` | `component.role == Role.DIMENSION` | No duplicate observations (same key) |
| `validate_no_missing_values` | `component.required` | No nulls in mandatory columns |

---

### 8. Output Standardisation

pysdmx has no concept of "preparing a DataFrame for upload". That is a data engineering task, not an SDMX IM concept. tidysdmx wraps it in `standardize_output()`.

```python
from tidysdmx import standardize_output

result = standardize_output(df, artefact_id="WB:WDI(1.0.0)", schema=schema, action="I")
# Adds reference columns, drops non-schema columns, moves metadata columns to front
```

Internally, `standardize_output` reads `schema.context` to determine the correct column names (`STRUCTURE` / `DATAFLOW` / `PROVISIONAGREEMENT`), then calls `_add_sdmx_reference_cols()`. The analyst specifies the action (`"I"`, `"U"`, `"D"`) in plain English terms; the SDMX column value is identical.

---

### 9. Production Integration (Kedro)

The `kd_*` functions are thin wrappers that adapt tidysdmx's single-dataset functions to Kedro's partitioned dataset pattern (dict-of-callables). They exist at the production layer, not the SDMX layer.

```
kd_read_mappings()          →  calls read_mapping() for each partition
kd_standardize_sdmx()       →  calls standardize_sdmx() for each partition
kd_validate_dataset_local() →  calls validate_dataset_local(), returns (bool, dict)
kd_validate_datasets_local()→  calls kd_validate_dataset_local() for each partition
```

There is no new pysdmx usage in the Kedro layer. It is purely an orchestration adapter.

---

### 10. FMR Publication Layer (`tidysdmx.fmr`)

pysdmx's FMR clients are CRUD: `RegistryClient` reads, and the (experimental) `RegistryMaintenanceClient` uploads whatever it is given. The `tidysdmx.fmr` subpackage adds the workflow layer real pipelines need — change detection, automated versioning, and a dry-run-able upsert — and supersedes the notebook-only `RegistryMaintenanceClient` upload recipe.

```
tidysdmx/fmr/
├── client.py      ← FmrClient: unified read/write facade
│                    env-var credentials (TIDYSDMX_FMR_URL/_USER/_PASSWORD/_TOKEN),
│                    registry-agnostic URLs (no hardcoded /FMR/ path — PYSDMX-04),
│                    lazy write client, generic get_artefact() dispatch
├── diff.py        ← compare_artefacts(existing, updated) → ArtefactDiff
│                    typed ArtefactChange records classified by impact
├── versioning.py  ← SDMX version algebra: parse/compare/bump/suggest_version
│                    two-part "1.0" and semver "1.0.0"/"1.0.0-draft" schemes
├── publish.py     ← plan_publication() → PublicationPlan → execute_plan()
│                    CREATE / UPDATE-at-bumped-version / SKIP-unchanged
└── report.py      ← pandas DataFrame views (the ONLY fmr module using pandas)
```

**Change impact taxonomy** — every detected change carries one of three impacts, which drive the version bump:

| Impact | Meaning | Examples | Default bump |
|---|---|---|---|
| `BREAKING` | consumers can break | item/component removed, representation narrowed, reference changed | major |
| `ADDITIVE` | new capability, consumers fine | item added, optional attribute added | minor |
| `COSMETIC` | presentation only | names, descriptions, annotations, ordering | patch (minor on two-part) |

Unknown fields and unregistered artefact types fall through to a generic field walk that classifies conservatively as breaking — new pysdmx model fields may therefore over-trigger major bumps until a specialized differ handles them (watch the pysdmx changelog).

**Version policy** — `suggest_version(diff, current_version, policy)` auto-detects the version scheme from the registry's current version and never migrates schemes. `VersionPolicy` controls the impact→bump mapping, draft handling (`finalize` drops the `-draft` extension without a numeric bump), and `replace_non_final` (in-place republish of non-final versions; note two-part versions are never final per SDMX 3.0, so this disables bumping entirely on two-part registries).

**Plan → execute flow** —

```
plan_publication(client, artefacts)          # read-only
  1. order dependencies first (codelists → rep maps/hierarchies → DSDs
     → dataflows/structure maps → PAs/categorisations)
  2. per artefact: get_existing() → compare_artefacts() →
     CREATE | UPDATE @ suggest_version() | SKIP
  3. validate publish-readiness; detect version conflicts (registry
     ahead of local baseline → blocking P002)
  4. propagate bumps to intra-batch references (a Dataflow pointing at
     a bumped DSD is rewritten and promoted from SKIP to UPDATE)
print(plan.summary())                        # or plan_to_dataframe(plan)
execute_plan(client, plan, dry_run=..., batch=...)
  - blocking issues raise BEFORE any network call
  - batch=True: one transactional FMR submission (default)
  - batch=False: per-artefact, fail-fast, dependents never attempted
```

**Import boundary (extraction rule)** — `fmr` core modules import only pysdmx, the stdlib, typeguard, and each other; the single tidysdmx-internal import is `artefact_validation` (itself pysdmx-only). pandas is quarantined in `report.py`. This keeps the subpackage extractable into a standalone package if adoption outside tidysdmx warrants it.

**Known limitations (v1)** — references *from* registry artefacts outside the submitted batch are not updated; the plan is trusted at execute time (the P002 pre-flight is the mitigation); `PublicationResult.submission` is reserved but always `None` until pysdmx's maintenance client stops discarding the FMR response (upstream candidate, together with an async maintenance client).

---

## Key Design Decisions

### pysdmx objects as opaque handles

When tidysdmx functions accept a `schema` parameter, they treat it as an opaque handle. The analyst passes the schema through the pipeline without ever needing to understand its internal structure. The schema is unpacked exactly once — inside `extract_validation_info()` — and the result is a plain dict that the analyst can inspect, cache, and pass around freely.

### DataFrames as the universal currency

Every function that touches data accepts a `pd.DataFrame` and returns a `pd.DataFrame`. Mapping specifications are DataFrames. Validation results are DataFrames. Error reports are DataFrames. This means the analyst never needs to switch mental models: everything is a table.

### Two mapping paths, same pysdmx destination

The JSON mapping dict (`read_mapping` → `map_to_sdmx`) and the Excel mapping template (`parse_mapping_template_wb` → `build_structure_map_from_template_wb` → `map_structures`) both ultimately apply the same logical transformations. The JSON path is older, faster, and simpler. The Excel path produces a pysdmx `StructureMap` as an intermediate, enabling richer mapping types (e.g. `DatePatternMap`, `MultiComponentMap`) and formal SDMX artefact compliance.

### Validation pre-computation

`extract_validation_info(schema)` is designed to be called **once** per run and reused. The `valid` dict is passed as an argument to all validation functions, allowing batch validation of hundreds of partitions without re-parsing the schema on each call. This is the pattern used in `kd_validate_datasets_local()`.

### Deprecation pattern

Early versions of tidysdmx used function names tied to SDMX jargon (`fetch_dsd_schema`, `parse_dsd_id`, `add_sdmx_reference_cols`, `standardize_data_for_upload`). These have been deprecated in favour of names that describe the analyst's task (`fetch_schema`, `parse_artefact_id`, `standardize_output`). The renamed functions also dropped DSD-specific semantics in favour of generic artefact handling.

---

## Module Responsibilities

```
tidysdmx/
├── tidysdmx.py     ← End-to-end pipeline functions: fetch, standardize, map, output
│                     Owns the JSON mapping format (read_mapping, map_to_sdmx)
│                     Wraps fmr.RegistryClient (fetch_schema)
│
├── structures/     ← Translation layer: DataFrames → pysdmx objects (package)
│                     All build_*() functions live here (map_builders.py,
│                     template.py) and are re-exported from tidysdmx.structures
│                     Also create_schema_from_table() (DataFrame → SchemaComponents)
│
├── mapping.py      ← DataFrame-level application of pysdmx map objects
│                     map_structures(), apply_fixed_value_maps(), etc.
│                     Each function: (DataFrame, pysdmx map) → DataFrame
│
├── validation.py   ← Schema-driven DataFrame validation
│                     validate_dataset_local() and individual check functions
│                     Returns DataFrames of errors, not exceptions
│
├── utils.py        ← Schema introspection and Excel tooling
│                     extract_validation_info() — the pysdmx → dict bridge
│                     Excel template generation and parsing
│
├── tidy_raw.py     ← Codelist-based row filtering
│                     filter_tidy_raw(df, schema) — pre-processing before mapping
│
├── qa_utils.py     ← Data quality operations independent of SDMX
│                     qa_coerce_numeric(), qa_remove_duplicates()
│
├── fmr/            ← FMR publication layer (see Functional Area 10)
│                     FmrClient facade, artefact diffing, version bumping,
│                     plan/execute upsert workflow, DataFrame reporting
│
└── kedro.py        ← Production/Kedro adapter layer
                      kd_* wrappers for partitioned dataset patterns
```