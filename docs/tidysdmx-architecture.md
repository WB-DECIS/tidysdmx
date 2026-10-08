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
| Applying mappings | Group `StructureMap.maps` by type (`fixed_value_maps`, `component_maps`, ...) and apply each to the data yourself | `map_structures(df, smap)` or `map_to_sdmx(df, mapping)` — a single function call |
| Validation | `Component.required`, `Component.enumeration` | `validate_dataset_local(df, schema)` — returns a DataFrame of error messages |
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
│  FmrClient  — authenticated registry access (reads and uploads)         │
│                                                                         │
│  Primary types: pd.DataFrame, dict, str, list (FmrClient is the one     │
│  stateful object)                                                       │
├─────────────────────────────────────────────────────────────────────────┤
│                   Thin translation layer                                │
│                                                                         │
│  extract_validation_info()   — Schema → plain dict                      │
│  extract_component_ids()     — Schema → list[str]                       │
│  build_value_map_list()      — pd.DataFrame → list[ValueMap]            │
│  build_representation_map()  — pd.DataFrame → RepresentationMap         │
│  build_single_component_map()— pd.DataFrame + str → ComponentMap        │
├─────────────────────────────────────────────────────────────────────────┤
│                          pysdmx                                         │
│                                                                         │
│  Schema / Components / Component / Role / DataType                      │
│  Codelist / Code / Concept                                              │
│  StructureMap / FixedValueMap / ImplicitComponentMap                    │
│  ComponentMap / RepresentationMap / ValueMap                            │
│  MultiComponentMap / MultiRepresentationMap / MultiValueMap             │
│  DatePatternMap                                                         │
│  fmr.RegistryClient / fmr.maintenance.RegistryMaintenanceClient         │
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

client = fmr.RegistryClient(
    "https://fmr.example.com/FMR/sdmx/v2/", format=StructureFormat.FUSION_JSON
)
schema = client.get_schema("dataflow", "WB", "WDI", "1.0.0")

# tidysdmx — analyst passes the ID they already have
from tidysdmx import fetch_schema

schema = fetch_schema(
    base_url="https://fmr.example.com", artefact_id="WB:WDI(1.0.0)", context="dataflow"
)
```

**What tidysdmx hides:** URL construction, `RegistryClient` instantiation, `StructureFormat` selection, ID string parsing. The analyst only needs to know the three things they already know: where the FMR is, what the artefact ID is, and whether it's a dataflow or a DSD.

---

### 2. Schema Introspection

**pysdmx view:** The `Schema` object is a rich tree of typed SDMX objects. To find out which columns are required, iterate `Components` and test `component.required`. To get allowed values, read `component.enumeration` — the component's local (constrained) codes if the schema carries any, otherwise the codes of its concept's core representation — which is a `Codelist`, or a `Hierarchy` when FMR resolves a hierarchy association. To identify dimensions, test `component.role == Role.DIMENSION`.

**tidysdmx view:** Call `extract_validation_info(schema)` once and get a plain Python `dict` with everything needed to validate a DataFrame. A component counts as coded when its `enumeration` is set, and a `Hierarchy` is flattened to every code at every level (`Hierarchy.all_codes()`). No pysdmx attributes are accessed after this point.

```python
# pysdmx — analyst must understand Component structure
from pysdmx.model import Role

mandatory = [c.id for c in schema.components if schema.components[c.id].required]
coded = [
    c.id for c in schema.components if schema.components[c.id].enumeration is not None
]
dims = [
    c.id for c in schema.components if schema.components[c.id].role == Role.DIMENSION
]

# tidysdmx — one call, plain dict
from tidysdmx import extract_validation_info

valid = extract_validation_info(schema)
# valid["mandatory_comp"] → ["FREQ", "REF_AREA", "TIME_PERIOD", "OBS_VALUE", ...]
# valid["coded_comp"]     → ["FREQ", "REF_AREA", "INDICATOR"]
# valid["codelist_ids"]   → {"FREQ": ["A", "M", "Q"], "REF_AREA": ["US", "GB", ...]}
# valid["dim_comp"]       → ["FREQ", "REF_AREA", "INDICATOR", "TIME_PERIOD"]
# valid["valid_comp"]     → all component IDs
# valid["sdmx_cols"]      → ["STRUCTURE", "STRUCTURE_ID", "ACTION"]
```

The `valid` dict is the analyst's entire interface to schema knowledge. The downstream functions (`validate_dataset_local`, `kd_validate_datasets_local`, `filter_tidy_raw`) take the `schema` itself and call `extract_validation_info` internally; passing a pre-computed dict as `validate_dataset_local(valid=...)` is deprecated (see [Validation pre-computation](#validation-pre-computation)). `get_codelist_ids` raises `ValueError` if asked for a component that is uncoded or not in the schema.

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

`read_mapping(path)` parses this into a Python dict where DataFrames replace the lists: `schema_version`, `dsd_id`, `components`, and one top-level key per `representation` sub-key (`mapping["REF_AREA"]`, not `mapping["representation"]["REF_AREA"]`).

**Known defect — the output is not ready for `map_to_sdmx()`.** `map_to_sdmx` reads only `mapping["representation"]`, which `read_mapping` never produces, so on a mapping loaded this way it recodes nothing and returns the values unchanged. `standardize_sdmx` and `kd_standardize_sdmx` inherit this. None of these four functions has a test yet (TEST-03 / TEST-05, backlog B4 in `docs/reviews/2026-06-architecture-review.md`); only a hand-built dict that keeps the nested `representation` key gets recoded.

**pysdmx equivalent:** A `StructureMap` containing `ImplicitComponentMap`s (for the column renames), `ComponentMap`s with `RepresentationMap`s (for the value mappings), and `FixedValueMap`s — each a typed SDMX object constructed in Python code.

#### 3b. Excel Mapping Template (Accessible / Business Analyst Workflows)

For non-programmers or mixed technical/non-technical teams, the mapping can be written in an Excel workbook.

**Known defect — the template writer and reader disagree (ARCH-01, open backlog item A3).** tidysdmx has a writer that generates a workbook from a schema, but the reader rejects what it writes:

```python
from tidysdmx import fetch_schema, extract_component_ids, write_excel_mapping_template

schema = fetch_schema(base_url, "WB:WDI(1.0.0)", "dataflow")
components = extract_component_ids(schema)
write_excel_mapping_template(
    components, rep_maps=["REF_AREA", "INDICATOR"], output_path=Path("mapping.xlsx")
)
```

The writer emits a legacy layout: `build_excel_workbook` (which `write_excel_mapping_template` saves) produces a lowercase `comp_mapping` sheet with `source` / `target` / `mapping_rules` columns, plus one tab per `rep_maps` entry with `source` / `target` / `valid_from` / `valid_to` headers. The reader, `build_structure_map_from_template_wb` (via `_validate_mapping_template_wb`), requires `INFO`, `COMP_MAPPING` and `REP_MAPPING` sheets and fails with `Missing required sheet` for all three. There is no working write→read round trip today; a workbook the reader accepts has to be laid out by hand in the format below.

The format the reader accepts has an `INFO` sheet (key/value metadata such as the agency), a `COMP_MAPPING` sheet (`SOURCE` / `TARGET` / `MAPPING_RULES` columns, plus optional `SOURCE_CL` / `TARGET_CL` / `DEFAULT_VALUE`; `MAPPING_RULES` accepts `"implicit"`, `"fixed:<VALUE>"`, `"representation"`, or `"multi_representation"`) and a `REP_MAPPING` sheet holding value-level mappings (source columns prefixed `S:`, target columns prefixed `T:`).

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

`parse_mapping_template_wb(path)` reads the filled-in workbook into a dict of DataFrames (one per sheet), and `build_structure_map_from_template_wb(mappings)` turns that dict into a pysdmx `StructureMap`. The analyst fills in Excel cells; pysdmx objects are the implementation detail. Those four rules are the only ones the template accepts, so a template-built `StructureMap` holds only `FixedValueMap`, `ImplicitComponentMap`, `ComponentMap` and `MultiComponentMap` — never a `DatePatternMap`.

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

`map_to_sdmx` iterates the `representation` dict and applies vectorised pandas operations (`np.select` with regex or exact matching). There are no pysdmx objects involved at this point. As written, though, step 2 is a no-op: the dict `read_mapping` returns has no `representation` key (see the known defect in §3a).

#### Path B — pysdmx `StructureMap` → `map_structures()`

```python
from tidysdmx import (
    map_structures,
    build_structure_map_from_template_wb,
    parse_mapping_template_wb,
)

mappings = parse_mapping_template_wb("mapping.xlsx")
smap = build_structure_map_from_template_wb(mappings, agency="WB")

result_df = map_structures(df, smap)
```

`map_structures` dispatches by map type — it groups the maps with pysdmx's own typed views (`StructureMap.fixed_value_maps`, `implicit_component_maps`, `component_maps`, `multi_component_maps`, which keep stored order) and calls `apply_fixed_value_maps()`, `apply_implicit_component_maps()`, `apply_component_map()`, and `apply_multi_component_map()` depending on what the `StructureMap` contains. Each function operates on a DataFrame and returns a DataFrame. Every map reads its source columns from the DataFrame passed in, never from another map's output, so one source column can feed several targets — including a target with the same name.

`map_structures` raises `TypeError` in two cases:

- the `StructureMap` holds any other map type — in practice a `DatePatternMap`, which tidysdmx can build (`build_date_pattern_map`) but not apply;
- a `ComponentMap` or `MultiComponentMap` references its representation map by URN string instead of embedding it, so there are no value maps to apply. Fetch the structure map with pysdmx's `RegistryClient.get_mapping()`, which resolves the representation maps, rather than passing one whose `values` is still a URN.

**pysdmx view vs tidysdmx view:**

```
pysdmx StructureMap.maps           tidysdmx function called
───────────────────────────────    ─────────────────────────────
FixedValueMap                  →   apply_fixed_value_maps()
ImplicitComponentMap           →   apply_implicit_component_maps()
ComponentMap (+ RepresentationMap) apply_component_map()
MultiComponentMap              →   apply_multi_component_map()
DatePatternMap                 →   (not supported — TypeError)
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
pd.DataFrame + agency/id/...     →   RepresentationMap      (via build_representation_map)
pd.DataFrame + source/target_comp→   ComponentMap           (via build_single_component_map)
pd.DataFrame + source/target_cols→   list[MultiValueMap]    (via build_multi_value_map_list)
pd.DataFrame + agency/id/...     →   MultiRepresentationMap (via build_multi_representation_map)
pd.DataFrame + source/target_comps→  MultiComponentMap      (via build_multi_component_map)
dict[str, pd.DataFrame]          →   StructureMap           (via build_structure_map_from_template_wb)
```

This layer is intentionally thin. Each builder function validates its inputs, then calls the pysdmx constructor. There is no business logic here — the translation is structural, not semantic.

A second family of builders lives in `tidysdmx.artefact_builder`: `build_codelist`, `build_concept_scheme`, `build_category_scheme`, `build_agency_scheme`, `build_hierarchy`, `build_data_structure_definition` and `build_dataflow` take plain values and pysdmx objects rather than DataFrames, and run the publish-readiness rules in `tidysdmx.artefact_validation` before returning (raising `ValidationError`). That module also defines value-driven `build_representation_map` / `build_multi_representation_map` with different signatures from the DataFrame-driven ones above; only the `structures` pair is exported from `tidysdmx` (CONS-20, backlog B7).

---

### 6. Schema Creation from Data

`create_schema_from_table()` runs in the opposite direction: it builds SDMX structures from a pandas DataFrame. It is used when no SDMX registry is available and a schema needs to be inferred from data alone.

```python
from tidysdmx import create_schema_from_table

parts = create_schema_from_table(
    dataframe=df,
    dimensions=["FREQ", "REF_AREA"],
    time_dimension="YEAR",  # mapped to standard TIME_PERIOD component
    measure="OBS_VALUE",
    attributes=["OBS_STATUS"],
    agency_id="WB",
    schema_id="INFERRED_WDI",
)
schema = parts.dsd.to_schema()  # pysdmx Schema, context "datastructure"
```

The function:
1. Infers `DataType` from pandas dtypes
2. Builds a `Codelist` from unique values in each dimension column (and each string-typed attribute column)
3. Creates `Component` objects with the correct `Role` and `local_codes`
4. Maps the `time_dimension` column to the standardised `TIME_PERIOD` concept (with `DataType.PERIOD`)
5. Returns a `SchemaComponents(dsd, concept_scheme, codelists)` named tuple — a pysdmx `DataStructureDefinition`, its `ConceptScheme`, and the generated `Codelist`s — not a `Schema`

**Why this matters:** Once converted with `parts.dsd.to_schema()`, the schema can be passed to `validate_dataset_local()`, `standardize_output()`, or `filter_tidy_raw()` like a registry-fetched one. The analyst's workflow is the same regardless of whether the schema came from a registry or was inferred.

---

### 7. Validation

**pysdmx view:** Validation means navigating the object model: iterate `Components`, check `component.required`, compare data values against the codes of `component.enumeration`. Each check is a custom loop over the DataFrame.

**tidysdmx view:** `validate_dataset_local(df, schema)` is a single call that returns a DataFrame of errors. If the DataFrame is empty, the data is valid. Each validation failure is a row. The analyst can sort, filter, and export the error DataFrame like any other.

```python
from tidysdmx import validate_dataset_local

errors = validate_dataset_local(df, schema=schema)

# errors is a plain pd.DataFrame:
# ┌──────────────────────┬──────────────────────────────────────────────┐
# │ Validation           │ Error                                        │
# ├──────────────────────┼──────────────────────────────────────────────┤
# │ columns              │ Unexpected column: 'FOO'                     │
# │ codelist_ids         │ 'REF_AREA': XYZ                              │
# │ missing_values       │ Found 2 row(s) with missing values in ...    │
# └──────────────────────┴──────────────────────────────────────────────┘
```

The `Validation` column takes one of `columns`, `mandatory_columns`, `codelist_ids`, `duplicates` or `missing_values`. The value-level checks (codelist, duplicates, missing values) run only when no mandatory column is missing. The deprecated `valid=` argument still works but emits a `FutureWarning`; with neither `schema` nor `valid`, the call raises `ValueError`.

The five validation checks and their pysdmx source. Each is also public on its own; unlike `validate_dataset_local`, the individual `validate_*` functions return `None` and raise `ValueError` when their check fails:

| tidysdmx check | pysdmx attribute used | Task-level meaning |
|---|---|---|
| `validate_columns` | `component.id` (all) | No unexpected columns exist |
| `validate_mandatory_columns` | `component.required` | All required columns are present |
| `validate_codelist_ids` | `component.enumeration` (a `Hierarchy` flattened to all its codes) | All coded values are in the allowed list |
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

Internally, `standardize_output` reads `schema.context` and calls `_add_sdmx_reference_cols()`, which adds the standard SDMX-CSV reference columns (`STRUCTURE`, `STRUCTURE_ID`, `ACTION`). `STRUCTURE` carries the SDMX-CSV name of the context: `dataflow`, `datastructure`, or `dataprovision` for a provision-agreement schema (pysdmx's SDMX-CSV reader rejects `provisionagreement`). `action` is one of the SDMX-CSV codes pysdmx reads and writes — `"I"` (Information, the default), `"A"` (Append), `"R"` (Replace), `"D"` (Delete) — and is written to the `ACTION` column as given.

---

### 9. Production Integration (Kedro)

The `kd_*` functions are thin wrappers that adapt tidysdmx's single-dataset functions to Kedro's partitioned dataset pattern (dict-of-callables). They exist at the production layer, not the SDMX layer.

```
kd_read_mappings()          →  calls read_mapping() for each partition
kd_standardize_sdmx()       →  calls standardize_sdmx() for each partition
kd_validate_dataset_local() →  calls validate_dataset_local(), returns (bool, dict)
kd_validate_datasets_local()→  calls kd_validate_dataset_local() for each partition
```

There is no new pysdmx usage in the Kedro layer. It is purely an orchestration adapter. Two known bugs pass through it: `kd_standardize_sdmx` inherits the JSON-path defect in §3a (no values are recoded), and `kd_validate_datasets_local(datasets, schema, boolean)` computes `extract_validation_info(schema)` once and passes it down as the deprecated `valid=` argument, so it triggers the `FutureWarning` from `validate_dataset_local` itself.

### 10. Registry Access and Authentication

**pysdmx view:** Two clients with two different URL conventions. `RegistryClient` reads and takes no credentials at all; `RegistryMaintenanceClient` writes and takes a *static* `access_token` string that it never refreshes. Against a registry behind single sign-on, the developer acquires the token (`azure-identity`), watches `expires_on`, and rebuilds the client when it expires.

**tidysdmx view:** One `FmrClient` per registry, built from the registry root and a `TokenProvider`. It hands out pysdmx's own clients — `client.registry` *is* a `RegistryClient`, `client.maintenance` *is* a `RegistryMaintenanceClient` — with a bearer token that is acquired lazily, cached, refreshed before it expires, and sent on every request, reads included.

```python
# pysdmx — token wiring is the caller's problem
from azure.identity import DefaultAzureCredential
from pysdmx.api.fmr.maintenance import RegistryMaintenanceClient

token = DefaultAzureCredential().get_token("api://<fmr-app-id>/.default")
client = RegistryMaintenanceClient(
    "https://fmr.example.org/FMR", access_token=token.token
)
# ... and again in an hour, and reads stay anonymous

# tidysdmx — one object, refresh included
from tidysdmx import AzureTokenProvider, FmrClient

provider = AzureTokenProvider.from_default_credential("api://<fmr-app-id>/.default")
client = FmrClient("https://fmr.example.org/FMR", token_provider=provider)
schema = client.fetch_schema("WB:WDI(1.0.0)", "dataflow")
regions = client.fetch_codelist("WB:CL_REF_AREA(1.0)")
dsd = client.fetch_artefact("WB:IFPRI_ASTI(1.0)", "datastructure")
client.put_structures(artefacts)
```

**What tidysdmx hides:** token acquisition and refresh, the `Authorization` header on reads (which pysdmx has no hook for), the two URL conventions, client construction, and the split of an `"AGENCY:ID(VERSION)"` string into pysdmx's three getter arguments. The `fetch_*` methods add only what pysdmx leaves to the caller: one generic `fetch_artefact` keyed by the SDMX REST resource name and driven by one table of pysdmx getters, with each typed method a one-line call to it; a single result where pysdmx only lists (dataflows, data structures, metadataflows, metadata structures); URNs accepted as well as `"AGENCY:ID(VERSION)"`, with the URN's class checked against what is fetched; and a refusal of wildcards, lists and characters pysdmx would put into the URL unescaped, which its single-artefact readers would otherwise mishandle silently (`PYSDMX-READ-01`, `-02`). `TokenProvider` is the only extension point: anything with `get_token() -> BearerToken` plugs in, so the design is not tied to Azure.

**What it does not hide:** the pysdmx clients themselves. Every pysdmx method stays reachable, and the seams tidysdmx uses to get the token onto the wire are guarded, wire-tested and registered in `docs/pysdmx-shortcomings.md` so they can be deleted when upstream adds an auth hook.

---

## Key Design Decisions

### pysdmx objects as opaque handles

When tidysdmx functions accept a `schema` parameter, they treat it as an opaque handle. The analyst passes the schema through the pipeline without ever needing to understand its internal structure. The schema is unpacked inside tidysdmx — for validation and filtering by `extract_validation_info()` — and the result is a plain dict that the analyst can inspect.

### DataFrames as the universal currency

Every function that transforms data accepts a `pd.DataFrame` and returns a `pd.DataFrame`. Mapping specifications are DataFrames. Validation results are DataFrames. Error reports are DataFrames. This means the analyst never needs to switch mental models: everything is a table.

### Two mapping paths, same pysdmx destination

The JSON mapping dict (`read_mapping` → `map_to_sdmx`) and the Excel mapping template (`parse_mapping_template_wb` → `build_structure_map_from_template_wb` → `map_structures`) are meant to apply the same logical transformations. The JSON path is older and simpler, but today it recodes nothing when fed by `read_mapping` (the known defect in §3a). The Excel path produces a pysdmx `StructureMap` as an intermediate, enabling `MultiComponentMap` and formal SDMX artefact compliance; its writer half is broken (ARCH-01, §3b), so its workbooks are authored by hand. Neither path handles `DatePatternMap`: the template has no rule for it, and `map_structures` raises `TypeError` on one.

### Validation pre-computation

`extract_validation_info(schema)` was designed to be called once per run, with the `valid` dict passed to every validation call so that hundreds of partitions could be validated without re-parsing the schema. That pattern is now deprecated: `validate_dataset_local(valid=...)` emits a `FutureWarning`, and the parameter will be removed. Pass `schema`; `validate_dataset_local`, `filter_tidy_raw` and `kd_validate_datasets_local` all take it and derive the dict themselves. `kd_validate_datasets_local()` still pre-computes the dict and passes `valid=` internally — a known bug (§9), not a pattern to copy.

### A stateful client object

Apart from its own token types (`TokenProvider`, `BearerToken` and the two providers) and the artefact-validation types (`ValidationIssue`, `ValidationError`), every other public name in the package is a function. `FmrClient` is a class because a token cache has a lifetime: it must outlive a single call and be shared by every request against the same registry. Hold one instance per registry. Everything it returns is still a pysdmx object.

### Guarded pysdmx seams

pysdmx 1.19.0 offers no way to put an `Authorization` header on reads and no way to refresh the token on writes. `tidysdmx.fmr` reaches into two private hooks to do both. Each seam is (1) isolated in one private class, (2) guarded by a runtime check that raises `RuntimeError` naming the register entry if the hook is gone, (3) covered by a wire-level test that fails on any pysdmx bump that changes the behaviour, and (4) recorded in `docs/pysdmx-shortcomings.md` with the upstream fix and the trigger for deleting the workaround. Adding a third seam means adding all four.

### Deprecation pattern

Early versions of tidysdmx used function names tied to SDMX jargon (`fetch_dsd_schema`, `parse_dsd_id`, `add_sdmx_reference_cols`, `standardize_data_for_upload`). These have been deprecated in favour of names that describe the analyst's task (`fetch_schema`, `parse_artefact_id`, `standardize_output`). The renamed functions also dropped DSD-specific semantics in favour of generic artefact handling.

Deprecations also retire workarounds once pysdmx catches up: `fix_sdmx_xml_datatype_tags` emits a `FutureWarning` because pysdmx 1.14.0 and later write `SourceDataType`/`TargetDataType` correctly, so the call is no longer needed. Every deprecation in the package, the `valid=` argument included, warns with `FutureWarning` (shown to end users by default, unlike `DeprecationWarning`).

---

## Module Responsibilities

```
tidysdmx/
├── tidysdmx.py     ← End-to-end pipeline functions: fetch, standardize, map, output
│                     Owns the JSON mapping format (read_mapping, map_to_sdmx)
│                     Wraps fmr.RegistryClient (fetch_schema)
│
├── structures.py   ← Translation layer: DataFrames → pysdmx objects
│                     The DataFrame-driven build_*() map builders, and
│                     build_structure_map_from_template_wb() (Excel reader)
│                     gen_urn() — URNs under the SDMX class name
│                     (MultiRepresentationMap → RepresentationMap=,
│                     DataStructureDefinition → DataStructure=)
│                     Also create_schema_from_table()
│                     (DataFrame → SchemaComponents; .dsd.to_schema())
│
├── artefact_builder.py ← Value-driven build_*() builders (codelist, concept,
│                     category and agency schemes, hierarchy, DSD, dataflow,
│                     representation maps), validated before returning
│
├── artefact_validation.py ← Publish-readiness rules for artefacts
│                     validate(), validate_many(), raise_if_invalid()
│                     Raises ValidationError (a ValueError subclass)
│
├── structure_map_writer.py ← StructureMap → upload-ready artefact list
│                     collect_structure_map_artifacts(),
│                     validate_structure_map_references(),
│                     prepare_structure_map_for_upload()
│
├── mapping.py      ← DataFrame-level application of pysdmx map objects
│                     map_structures(), apply_fixed_value_maps(), etc.
│                     Each function: (DataFrame, pysdmx map) → DataFrame
│                     TypeError on DatePatternMap or URN-only map values
│
├── validation.py   ← Schema-driven DataFrame validation
│                     validate_dataset_local() returns a DataFrame of errors
│                     Individual validate_*() checks raise ValueError
│
├── utils.py        ← Schema introspection and Excel tooling
│                     extract_validation_info() — the pysdmx → dict bridge
│                     Excel template writer (build_excel_workbook,
│                     write_excel_mapping_template — legacy layout the
│                     reader rejects, ARCH-01) and parse_mapping_template_wb()
│                     fix_sdmx_xml_datatype_tags() (deprecated)
│
├── fmr.py          ← Registry access with authentication
│                     FmrClient wraps RegistryClient + RegistryMaintenanceClient
│                     TokenProvider / AzureTokenProvider / StaticTokenProvider
│                     Future home of fetch_schema (backlog B2)
│
├── tidy_raw.py     ← Codelist-based row filtering
│                     filter_tidy_raw(df, schema) — pre-processing before mapping
│
├── qa_utils.py     ← Data quality operations independent of SDMX
│                     qa_coerce_numeric(), qa_remove_duplicates()
│
└── kedro.py        ← Production/Kedro adapter layer
                      kd_* wrappers for partitioned dataset patterns
```
