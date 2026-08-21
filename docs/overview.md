# pysdmx Overview for tidysdmx Developers

**Purpose:** This document describes the `pysdmx` library (v1.8.1+), explains how its objects map to the SDMX Information Model, and documents the subset of the API used by `tidysdmx`. It is intended to orient AI agents and developers so they can leverage existing pysdmx functionality rather than reimplementing it.

---

## 1. What is pysdmx?

`pysdmx` is an opinionated Python library for working with SDMX metadata and data. It provides:

- **Model classes** — Pythonic dataclasses representing SDMX artefacts (Schema, Component, Codelist, StructureMap, etc.)
- **I/O support** — Serialisation/deserialisation in SDMX-ML (XML), SDMX-JSON, and Fusion JSON formats
- **Registry client** — An HTTP client (`fmr.RegistryClient`) for querying SDMX registries (specifically the Fusion Metadata Registry, FMR)

The library is format-neutral at the model layer: all formats parse into the same Python objects.

**Key dependency versions used by tidysdmx:**
- `pysdmx >= 1.13.0`
- Internally uses `httpx` (HTTP/2), `msgspec` (fast serialisation), `parsy` (parsing)

---

## 2. pysdmx Module Map

```
pysdmx
├── model/                     # Core SDMX model classes
│   ├── __init__.py            # Re-exports: Schema, Component, Codelist, Code,
│   │                          #   Concept, Role, DataType, StructureMap,
│   │                          #   ComponentMap, FixedValueMap, ImplicitComponentMap,
│   │                          #   RepresentationMap, ValueMap
│   ├── dataflow.py            # Schema, Components, Component
│   └── map.py                 # StructureMap and all Map types
├── io/
│   └── format.py              # StructureFormat enum (e.g. FUSION_JSON)
└── api/
    └── fmr.py                 # RegistryClient — HTTP client for FMR
```

All public model symbols can be imported from `pysdmx.model` directly:
```python
from pysdmx.model import Schema, Component, Components, Role, DataType
from pysdmx.model import Codelist, Code, Concept
from pysdmx.model import StructureMap, ComponentMap, FixedValueMap
from pysdmx.model import ImplicitComponentMap, RepresentationMap, ValueMap
from pysdmx.model import MultiValueMap, MultiRepresentationMap, DatePatternMap
from pysdmx.model.dataflow import Schema, Components, Component  # also here
from pysdmx.model.map import StructureMap, ComponentMap, ...     # also here
```

---

## 3. Core Model Classes and SDMX IM Mapping

### 3.1 `Schema`

**SDMX IM equivalent:** Represents the resolved structure of a dataset — combining a DSD (Data Structure Definition), Dataflow, or ProvisionAgreement with its full component set. It is the result of calling `GET /schema/{context}/{agency}/{id}/{version}` on an SDMX REST API.

| pysdmx attribute | Type | Description |
|---|---|---|
| `context` | `str` | One of `"dataflow"`, `"datastructure"`, `"provisionagreement"` |
| `agency` | `str` | Maintenance agency ID (e.g. `"ECB"`) |
| `id` | `str` | Artefact ID (e.g. `"EXR"`) |
| `version` | `str` | Semver string (e.g. `"1.0.0"`) |
| `components` | `Components` | All components (dimensions, measures, attributes) |
| `urns` | `list[str]` | URNs of artefacts that generated this schema |
| `generated` | `datetime` | Timestamp of schema generation |
| `name` | `str \| None` | Human-readable name |

**Key usage in tidysdmx:**
```python
# Fetch a schema from FMR
schema = client.get_schema("dataflow", "WB", "WDI", "1.0.0")

# Access components
schema.components  # Components container (iterable)
schema.context  # "dataflow" | "datastructure" | "provisionagreement"
```

---

### 3.2 `Components`

**SDMX IM equivalent:** The combined DimensionList + MeasureList + AttributeList of a DSD, flattened into a single ordered collection.

`Components` is an **iterable, dict-like container** of `Component` objects:

```python
comp = schema.components

# Iteration gives Component objects
for c in comp:
    print(c.id)

# Dict-style lookup by component ID
freq_component = comp["FREQ"]

# Length
len(comp)
```

---

### 3.3 `Component`

**SDMX IM equivalent:** A single DSD component — a Dimension, Measure, or Attribute.

| pysdmx attribute | Type | Description |
|---|---|---|
| `id` | `str` | Component identifier (e.g. `"FREQ"`, `"OBS_VALUE"`) |
| `role` | `Role` | Component role: `DIMENSION`, `MEASURE`, or `ATTRIBUTE` |
| `required` | `bool` | `True` if the component is mandatory |
| `concept` | `Concept` | The underlying SDMX Concept |
| `local_codes` | `Codelist \| None` | Allowed codes (if the component is coded), else `None` |
| `local_dtype` | `DataType \| None` | The data type (if not coded) |
| `attachment_level` | `str \| None` | For attributes: attachment level (e.g. `"O"` for observation) |
| `name` | `str \| None` | Human-readable name |
| `description` | `str \| None` | Description |

**Key patterns used in tidysdmx:**
```python
# Check if a component is coded (has a codelist)
if comp["FREQ"].local_codes is not None:
    codes = comp["FREQ"].local_codes.items  # list of Code objects

# Check role
from pysdmx.model import Role

comp["FREQ"].role == Role.DIMENSION  # True for dimensions
comp["FREQ"].role == Role.MEASURE  # True for measures
comp["FREQ"].role == Role.ATTRIBUTE  # True for attributes

# Check if mandatory
comp["FREQ"].required  # True or False
```

---

### 3.4 `Role` (Enum)

**SDMX IM equivalent:** The component role within a DSD.

| Value | SDMX IM concept |
|---|---|
| `Role.DIMENSION` | Standard dimension (forms part of the series key) |
| `Role.MEASURE_DIMENSION` | Measure dimension (used in cross-sectional datasets) |
| `Role.MEASURE` | Measure (observed value, e.g. `OBS_VALUE`) |
| `Role.ATTRIBUTE` | Descriptive attribute (not part of the key) |

> **Note:** In the SDMX 3.0 IM, `TIME_PERIOD` is modelled as a special dimension. In pysdmx, it appears as `Role.DIMENSION` with `local_dtype=DataType.PERIOD`.

---

### 3.5 `DataType` (Enum)

**SDMX IM equivalent:** Facet/representation type for uncoded components.

Common values used in tidysdmx:

| Value | Description |
|---|---|
| `DataType.STRING` | Plain text (default for coded components) |
| `DataType.INTEGER` | Whole number |
| `DataType.DOUBLE` | Floating-point number |
| `DataType.BOOLEAN` | Boolean flag |
| `DataType.DATE_TIME` | ISO 8601 datetime |
| `DataType.PERIOD` | SDMX time period (used for `TIME_PERIOD`) |

---

### 3.6 `Concept`

**SDMX IM equivalent:** An SDMX Concept from a ConceptScheme — the semantic definition of what a component represents.

| pysdmx attribute | Type | Description |
|---|---|---|
| `id` | `str` | Concept ID (e.g. `"FREQ"`, `"TIME_PERIOD"`) |
| `urn` | `str \| None` | Full SDMX URN for the concept |
| `name` | `str \| None` | Human-readable name |
| `description` | `str \| None` | Description |
| `dtype` | `DataType \| None` | Core representation type |

---

### 3.7 `Codelist` and `Code`

**SDMX IM equivalent:** A Codelist (enumeration) and individual Code items within it.

**`Codelist`:**

| pysdmx attribute | Type | Description |
|---|---|---|
| `id` | `str` | Codelist identifier (e.g. `"CL_FREQ"`) |
| `agency` | `str` | Maintenance agency |
| `version` | `str` | Version string |
| `name` | `str \| None` | Human-readable name |
| `items` | `list[Code]` | The list of codes in the codelist |

**`Code`:**

| pysdmx attribute | Type | Description |
|---|---|---|
| `id` | `str` | Code identifier (e.g. `"A"` for annual) |
| `name` | `str \| None` | Human-readable label |
| `description` | `str \| None` | Description |

**Key pattern:**
```python
# Get valid codes for a component
codelist = schema.components["FREQ"].local_codes
code_ids = [code.id for code in codelist.items]  # e.g. ["A", "M", "Q"]
```

---

## 4. Mapping Model Classes

These classes live in `pysdmx.model.map` and represent the SDMX **StructureMap** artefact family. They describe how to transform data from one structure to another.

### 4.1 `StructureMap`

**SDMX IM equivalent:** A StructureMap artefact — a named, versioned container of mapping rules.

| pysdmx attribute | Type | Description |
|---|---|---|
| `id` | `str` | Identifier for this structure map |
| `agency` | `str` | Maintenance agency |
| `version` | `str` | Version string |
| `name` | `str \| None` | Human-readable name |
| `maps` | `list[...]` | List of map objects (any mix of types below) |

```python
from pysdmx.model.map import StructureMap

smap = StructureMap(
    id="MY_MAP",
    agency="ECB",
    version="1.0",
    name="My mapping",
    maps=[fixed_map, implicit_map, component_map],
)
```

### 4.2 `FixedValueMap`

Assigns a **constant value** to a target component regardless of source data.

| attribute | Type | Description |
|---|---|---|
| `target` | `str` | Target component ID |
| `value` | `str` | The fixed value to assign |
| `located_in` | `str` | `"source"` or `"target"` (default: `"target"`) |

```python
FixedValueMap(target="CONF_STATUS", value="F", located_in="target")
# → Sets the CONF_STATUS column to "F" for all rows
```

### 4.3 `ImplicitComponentMap`

**Copies** values from a source component to a target component, applying no value transformation (same values, different column name).

| attribute | Type | Description |
|---|---|---|
| `source` | `str` | Source component ID (column name in input data) |
| `target` | `str` | Target component ID (column name in output data) |

```python
ImplicitComponentMap(source="FREQ", target="FREQUENCY")
# → df["FREQUENCY"] = df["FREQ"]
```

### 4.4 `ComponentMap`

Maps values from one component to another using a **`RepresentationMap`** (explicit value lookup table).

| attribute | Type | Description |
|---|---|---|
| `source` | `str` | Source component ID |
| `target` | `str` | Target component ID |
| `values` | `RepresentationMap` | The value-level lookup table |

```python
ComponentMap(source="COUNTRY", target="REF_AREA", values=rep_map)
# → df["REF_AREA"] = df["COUNTRY"].map({"GB": "UK", "DE": "DEU", ...})
```

### 4.5 `MultiComponentMap`

Maps values from **multiple source components** to one (or more) target components, supporting regex pattern matching.

| attribute | Type | Description |
|---|---|---|
| `source` | `list[str]` | Source component IDs |
| `target` | `list[str]` | Target component IDs |
| `values` | `MultiRepresentationMap` | Multi-column lookup rules |

Pattern prefix `"regex:"` in source values triggers regex matching vs. exact matching.

### 4.6 `RepresentationMap`

A named container of `ValueMap` pairs — a simple source→target lookup table for a single component.

| attribute | Type | Description |
|---|---|---|
| `id` | `str \| None` | Identifier |
| `name` | `str \| None` | Human-readable name |
| `agency` | `str` | Maintenance agency |
| `source` | `str \| None` | URN/ID of the source codelist |
| `target` | `str \| None` | URN/ID of the target codelist |
| `maps` | `list[ValueMap]` | Individual value pair mappings |
| `version` | `str` | Version string |

### 4.7 `ValueMap`

A single source→target code mapping pair, optionally scoped to a validity period.

| attribute | Type | Description |
|---|---|---|
| `source` | `str` | Source code/value |
| `target` | `str` | Target code/value |
| `valid_from` | `datetime \| None` | Start of business validity |
| `valid_to` | `datetime \| None` | End of business validity |

```python
ValueMap(source="GB", target="UK")
ValueMap(source="DE", target="DEU", valid_from=datetime(2020, 1, 1))
```

### 4.8 `MultiValueMap`

Like `ValueMap` but maps **tuples** of source values to tuples of target values.

| attribute | Type | Description |
|---|---|---|
| `source` | `tuple[str, ...]` | Source values (one per source column) |
| `target` | `tuple[str, ...]` | Target values (one per target column) |
| `valid_from` | `datetime \| None` | Start of business validity |
| `valid_to` | `datetime \| None` | End of business validity |

### 4.9 `MultiRepresentationMap`

Container of `MultiValueMap` pairs, used inside `MultiComponentMap`.

| attribute | Type | Description |
|---|---|---|
| `id` | `str \| None` | Identifier |
| `agency` | `str` | Maintenance agency |
| `source` | `list[str]` | URNs/IDs of source codelists |
| `target` | `list[str]` | URNs/IDs of target codelists |
| `maps` | `list[MultiValueMap]` | The multi-column mapping rules |

### 4.10 `DatePatternMap`

Transforms a date column from a source format/pattern into the SDMX `TIME_PERIOD` format.

| attribute | Type | Description |
|---|---|---|
| `source` | `str` | Source component ID |
| `target` | `str` | Target component ID (typically `"TIME_PERIOD"`) |
| `pattern` | `str` | Source date pattern (e.g. `"MMM yy"`) |
| `frequency` | `str` | SDMX frequency code (`"M"`) or reference to a frequency dimension (`"FREQ"`) |
| `id` | `str \| None` | Optional map ID |
| `locale` | `str` | Locale for date parsing (default `"en"`) |
| `pattern_type` | `str` | `"fixed"` (frequency is a literal code) or `"variable"` (frequency is a dimension reference) |
| `resolve_period` | `str \| None` | `"startOfPeriod"`, `"endOfPeriod"`, or `"midPeriod"` |

---

## 5. API and I/O Classes

### 5.1 `fmr.RegistryClient`

An HTTP client for querying an SDMX **Fusion Metadata Registry (FMR)** — the most common SDMX registry implementation.

```python
from pysdmx.api import fmr
from pysdmx.io.format import StructureFormat

client = fmr.RegistryClient(
    base_url="https://your-fmr-host/FMR/sdmx/v2/",
    format=StructureFormat.FUSION_JSON,  # recommended format
)
```

**Key method — `get_schema()`:**

```python
schema = client.get_schema(
    context,  # "dataflow" | "datastructure" | "provisionagreement"
    agency,  # e.g. "WB"
    id,  # e.g. "WDI"
    version,  # e.g. "1.0.0"
)
# Returns: Schema object
```

The `get_schema()` call hits the SDMX REST endpoint:
```
GET {base_url}/schema/{context}/{agency}/{id}/{version}
```
and returns a fully-resolved `Schema` containing all components with their codelists and data types pre-resolved.

### 5.2 `StructureFormat`

An enum specifying the wire format for FMR communication.

| Value | Description |
|---|---|
| `StructureFormat.FUSION_JSON` | Fusion Metadata Registry's extended JSON format (recommended for tidysdmx) |
| `StructureFormat.SDMX_JSON_2_0` | Standard SDMX-JSON 2.0 |
| `StructureFormat.SDMX_ML_3_0` | Standard SDMX-ML (XML) 3.0 |

---

## 6. SDMX Artefact ID Format

pysdmx follows the SDMX convention for artefact identification. Artefact IDs are strings in the format:

```
"AGENCY:ID(VERSION)"
```

Examples:
- `"WB:WDI(1.0.0)"` — World Bank WDI dataflow, version 1.0.0
- `"ECB:EXR(1.0)"` — ECB exchange rate DSD
- `"SDMX:CL_FREQ(2.0)"` — SDMX cross-domain frequency codelist

**Parsing in tidysdmx:**
```python
from tidysdmx import parse_artefact_id

agency, id, version = parse_artefact_id("WB:WDI(1.0.0)")
# → ("WB", "WDI", "1.0.0")
```

---

## 7. How tidysdmx Uses pysdmx

tidysdmx is a **thin wrapper** that bridges pysdmx's object model with pandas DataFrames and Excel-based workflows. The table below shows where pysdmx objects are used directly and what tidysdmx adds on top.

| Task | pysdmx provides | tidysdmx adds |
|---|---|---|
| **Fetch schema** | `fmr.RegistryClient.get_schema()` | `fetch_schema()` — simplified wrapper with URL building and ID parsing |
| **Schema introspection** | `Schema`, `Components`, `Component`, `Role`, `Codelist` | `extract_validation_info()` — extracts validation dict from schema; `extract_component_ids()` — list of component IDs |
| **Column validation** | Component `required` flag, `local_codes` | `validate_dataset_local()` — full validation pipeline; `validate_columns()`, `validate_mandatory_columns()`, `validate_codelist_ids()`, `validate_duplicates()`, `validate_no_missing_values()` |
| **Apply structure maps** | `StructureMap`, `FixedValueMap`, `ImplicitComponentMap`, `ComponentMap`, `MultiComponentMap` | `map_structures()` — applies a StructureMap to a DataFrame; individual `apply_*` functions |
| **Build structure maps** | Raw pysdmx map constructors | `build_fixed_map()`, `build_implicit_component_map()`, `build_date_pattern_map()`, `build_value_map()`, `build_representation_map()`, `build_multi_representation_map()`, `build_single_component_map()`, `build_structure_map_from_template_wb()` |
| **Create schema from data** | `Schema`, `Components`, `Component`, `Codelist`, `Code`, `Concept`, `DataType`, `Role` | `create_schema_from_table()` — infers an SDMX Schema from a pandas DataFrame |
| **Output standardisation** | None | `standardize_output()` — adds SDMX reference columns (`STRUCTURE`, `STRUCTURE_ID`, `ACTION`) and reorders columns |
| **Excel mapping workflow** | None | `write_excel_mapping_template()`, `parse_mapping_template_wb()`, `build_structure_map_from_template_wb()` |

---

## 8. Working with pysdmx Schema Objects — Key Patterns

### Extracting validation info from a Schema
```python
from tidysdmx import fetch_schema, extract_validation_info

schema = fetch_schema(
    base_url="https://fmr.example.com", artefact_id="WB:WDI(1.0.0)", context="dataflow"
)

valid = extract_validation_info(schema)
# valid = {
#   "valid_comp":     ["FREQ", "REF_AREA", "INDICATOR", "TIME_PERIOD", "OBS_VALUE"],
#   "mandatory_comp": ["FREQ", "REF_AREA", "INDICATOR", "TIME_PERIOD", "OBS_VALUE"],
#   "coded_comp":     ["FREQ", "REF_AREA", "INDICATOR"],
#   "codelist_ids":   {"FREQ": ["A", "M", "Q"], "REF_AREA": ["US", "GB", ...], ...},
#   "dim_comp":       ["FREQ", "REF_AREA", "INDICATOR", "TIME_PERIOD"]
# }
```

### Iterating components
```python
for component in schema.components:
    print(component.id, component.role, component.required)

# Access by ID
obs_val = schema.components["OBS_VALUE"]
obs_val.role  # Role.MEASURE
obs_val.required  # True
obs_val.local_codes  # None (numeric measures are typically uncoded)
```

### Checking codes
```python
freq = schema.components["FREQ"]
if freq.local_codes is not None:
    codes = [c.id for c in freq.local_codes.items]
```

---

## 9. Building Mapping Objects — Quick Reference

```python
from tidysdmx import (
    build_fixed_map,
    build_implicit_component_map,
    build_date_pattern_map,
    build_value_map,
    build_value_map_list,
    build_representation_map,
    build_single_component_map,
    build_structure_map,
    build_structure_map_from_template_wb,
    map_structures,
)
import pandas as pd

# 1. Fixed value
fmap = build_fixed_map(target="CONF_STATUS", value="F")

# 2. Implicit (column rename with no value change)
imap = build_implicit_component_map(source="SourceFreq", target="FREQ")

# 3. Date pattern
dpm = build_date_pattern_map(
    source="DATE", target="TIME_PERIOD", pattern="MMM yy", frequency="M"
)

# 4. Value-level representation map from DataFrame
mapping_df = pd.DataFrame({"source": ["GB", "US"], "target": ["GBR", "USA"]})
rep_map = build_representation_map(mapping_df, agency="ECB", id="RM_COUNTRY")

# 5. Single component map (wraps representation map)
cm = build_single_component_map(
    df=mapping_df,
    source_component="COUNTRY_SRC",
    target_component="REF_AREA",
    agency="ECB",
)

# 6. Apply a StructureMap to a DataFrame
from pysdmx.model.map import StructureMap

smap = StructureMap(id="MY_MAP", agency="ECB", version="1.0", maps=[fmap, imap, cm])
result_df = map_structures(df, smap)
```

---

## 10. SDMX Reference Columns Added by tidysdmx

When a dataset is ready for upload, `standardize_output()` adds reference columns that identify which SDMX artefact the data belongs to and what operation to perform. Per the SDMX-CSV specification, the column names are the same for every artefact type — the artefact type is carried as the *value* of the `STRUCTURE` column:

| Artefact type | Col 1 | Col 2 | Col 3 |
|---|---|---|---|
| `"datastructure"` | `STRUCTURE` | `STRUCTURE_ID` | `ACTION` |
| `"dataflow"` | `STRUCTURE` | `STRUCTURE_ID` | `ACTION` |
| `"provisionagreement"` | `STRUCTURE` | `STRUCTURE_ID` | `ACTION` |

`ACTION` values follow SDMX conventions: `"I"` (Insert), `"U"` (Update), `"D"` (Delete).

---

## 11. What NOT to Reimplement in tidysdmx

The following capabilities already exist in pysdmx and should be used directly rather than re-coded:

| Don't reimplement | Use instead |
|---|---|
| SDMX artefact identity parsing | `parse_artefact_id()` (tidysdmx thin wrapper over standard parsing) |
| HTTP schema fetching | `fmr.RegistryClient.get_schema()` via `fetch_schema()` |
| Component role checking | `component.role == Role.DIMENSION` etc. |
| Codelist access | `component.local_codes.items` |
| Mandatory field checking | `component.required` |
| FixedValueMap, ImplicitComponentMap, etc. construction | pysdmx constructors directly, or the `build_*` helpers in `tidysdmx.structures` |
| StructureMap application | `map_structures()` in `tidysdmx.mapping` |
| Schema creation from DataFrame | `create_schema_from_table()` in `tidysdmx.structures` |

---

## 12. Glossary: pysdmx ↔ SDMX IM ↔ tidysdmx

| pysdmx class/attribute | SDMX IM concept | tidysdmx usage |
|---|---|---|
| `Schema` | Resolved DSD/Dataflow/PA structure | Central object; passed to `validate_dataset_local()`, `standardize_output()`, `extract_validation_info()` |
| `Schema.context` | Artefact type (DSD, Dataflow, PA) | Determines reference column names in `standardize_output()` |
| `Schema.components` | DimensionList + MeasureList + AttributeList | Iterated to extract component IDs, roles, codelists |
| `Component` | Dimension / Measure / Attribute | Each DataFrame column maps to a Component |
| `Component.role` | Component role in DSD | Used to identify dimensions for duplicate-checking |
| `Component.required` | Mandatory in data | Used for mandatory column validation |
| `Component.local_codes` | Codelist (allowed values) | Used for codelist validation |
| `Role.DIMENSION` | Dimension component | Identifies key columns for `validate_duplicates()` |
| `Role.MEASURE` | Measure component | Not currently used for special treatment |
| `Role.ATTRIBUTE` | Attribute component | Not required by default in validation |
| `DataType` | Facet/representation type | Used in `create_schema_from_table()` |
| `Codelist.items` | Code list items | List of Code objects; `.id` used for validation |
| `Code.id` | Code identifier | The actual allowed string value in the data |
| `StructureMap` | SDMX StructureMap artefact | Input to `map_structures()` |
| `FixedValueMap` | Fixed-value component mapping | Applied to add constant columns |
| `ImplicitComponentMap` | Implicit component mapping (rename) | Applied to rename columns |
| `ComponentMap` | Component mapping with value translation | Applied to recode column values |
| `RepresentationMap` | Code-level lookup table | Attached to `ComponentMap`; built from DataFrames |
| `ValueMap` | Single source→target code pair | Items in `RepresentationMap.maps` |
| `fmr.RegistryClient` | SDMX REST API client | Used in `fetch_schema()` |
| `StructureFormat.FUSION_JSON` | Wire format for FMR | Default format in all tidysdmx registry calls |
