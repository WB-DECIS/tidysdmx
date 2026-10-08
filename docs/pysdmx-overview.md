# pysdmx Overview for tidysdmx Developers

**Purpose:** This document describes the `pysdmx` library, explains how its objects map to the SDMX Information Model, and documents the subset of the API used by `tidysdmx`. It is intended to orient AI agents and developers so they can leverage existing pysdmx functionality rather than reimplementing it.

**Verified against:** pysdmx **1.20.0** (the version `uv.lock` pins). tidysdmx requires `pysdmx>=1.19.0,<2`. When the lock moves, follow §13 before trusting the details below.

---

## 1. What is pysdmx?

`pysdmx` is an opinionated Python library for working with SDMX metadata and data. It provides:

- **Model classes** — `msgspec` structs representing SDMX artefacts (Schema, Component, Codelist, StructureMap, etc.). They are immutable: to "change" one, build a new one (`msgspec.structs.replace` or the constructor).
- **I/O support** — `read_sdmx` / `write_sdmx` for SDMX-ML (XML), SDMX-JSON and SDMX-CSV, behind optional extras (`xml`, `json`, `data`).
- **Registry clients** — `RegistryClient` (reads) and `RegistryMaintenanceClient` (uploads) for the Fusion Metadata Registry (FMR).
- **Utilities** — URN parsers (`pysdmx.util`) and converters (`pysdmx.toolkit`).

The library is format-neutral at the model layer: all formats parse into the same Python objects.

**Dependencies:** tidysdmx installs plain `pysdmx` (no extras), whose core pulls in `httpx` (HTTP/2), `msgspec` and `parsy`. Anything behind an extra — `pysdmx.io` readers/writers, `PandasDataset`, `pysdmx.toolkit.pd` — is not guaranteed to be importable in a tidysdmx install (the `data` extra needs `pyarrow`, which tidysdmx does not depend on).

---

## 2. pysdmx Module Map

```
pysdmx
├── model/                     # Core SDMX model classes, all re-exported from pysdmx.model
│   ├── dataflow.py            # Schema, Components, Component, DataStructureDefinition, Dataflow, Role
│   ├── code.py                # Codelist, Code, Hierarchy, HierarchicalCode
│   ├── concept.py             # ConceptScheme, Concept, DataType
│   └── map.py                 # StructureMap and all Map types
├── io/                        # read_sdmx / write_sdmx (extras: xml, json, data)
│   └── format.py              # StructureFormat, Format enums
├── util/                      # parse_urn, parse_short_urn, find_by_urn, convert_dpm, is_final
├── toolkit/                   # pd (to_pandas_schema, to_pyarrow_schema), sqlsrv, vtl
└── api/
    └── fmr/
        ├── __init__.py        # RegistryClient, AsyncRegistryClient — read metadata from FMR
        └── maintenance.py     # RegistryMaintenanceClient — upload metadata (EXPERIMENTAL)
```

All public model symbols can be imported from `pysdmx.model` directly. Import by name — never `import pysdmx as px` followed by `px.model...`, which only resolves when some other module has already imported the submodule (see CLAUDE.md):

```python
from pysdmx.model import Schema, Component, Components, Concept, Role, DataType
from pysdmx.model import Codelist, Code, Hierarchy, HierarchicalCode
from pysdmx.model import Agency, ItemReference
from pysdmx.model import StructureMap, ComponentMap, MultiComponentMap
from pysdmx.model import FixedValueMap, ImplicitComponentMap, DatePatternMap
from pysdmx.model import RepresentationMap, MultiRepresentationMap
from pysdmx.model import ValueMap, MultiValueMap
```

`MaintainableArtefact` and `ItemScheme` are **not** re-exported (still private in `pysdmx.model.__base` as of 1.20). tidysdmx imports them from there with a comment saying so; keep such imports to those two names.

---

## 3. Core Model Classes and SDMX IM Mapping

### 3.1 `Schema`

**SDMX IM equivalent:** The resolved structure of a dataset — a DSD, Dataflow or ProvisionAgreement with its full component set and any constraints already applied. It is the result of `GET /schema/{context}/{agency}/{id}/{version}` on an SDMX REST API.

| pysdmx attribute | Type | Description |
|---|---|---|
| `context` | `str` | One of `"dataflow"`, `"datastructure"`, `"provisionagreement"` |
| `agency` | `str` | Maintenance agency ID (e.g. `"ECB"`) |
| `id` | `str` | Artefact ID (e.g. `"EXR"`) |
| `components` | `Components` | All components (dimensions, measures, attributes) |
| `version` | `str` | The version **as requested** — `get_schema` copies the caller's string, so it is only meaningful when you passed an explicit version |
| `artefacts` | `Sequence[str]` | URNs of the artefacts the schema was built from (renamed from `urns`) |
| `generated` | `datetime` | Timestamp of schema generation |
| `name` | `str \| None` | Human-readable name |
| `groups` | `Sequence[GroupDimension] \| None` | DSD groups |
| `keys` | `Sequence[str] \| None` | Allowed series keys from constraint key sets, wildcarded (`"*.USD.CHF.*"`), since 1.12 |
| `excluded_keys` | `Sequence[str] \| None` | Excluded series keys (exclusive key sets), since 1.13 |

Cube-region constraints are already reflected in each component's `local_codes`. tidysdmx does **not** yet check `keys` / `excluded_keys` (backlog).

```python
schema = client.get_schema("dataflow", "WB", "WDI", "1.0.0")
schema.components  # Components container
schema.context  # "dataflow" | "datastructure" | "provisionagreement"
```

A DSD built locally becomes a `Schema` through `DataStructureDefinition.to_schema()` (no constraints applied) — this is how a `create_schema_from_table()` result is validated against.

---

### 3.2 `Components`

**SDMX IM equivalent:** The combined DimensionList + MeasureList + AttributeList of a DSD, flattened into a single ordered collection.

```python
comp = schema.components

for c in comp:  # Component objects, in DSD order
    print(c.id)

freq = comp["FREQ"]  # lookup by ID; returns None if absent
comp.dimensions  # typed views — use these instead of filtering on role
comp.measures
comp.attributes
len(comp)
```

---

### 3.3 `Component`

**SDMX IM equivalent:** A single DSD component — a Dimension, Measure, or Attribute.

| pysdmx attribute | Type | Description |
|---|---|---|
| `id` | `str` | Component identifier (e.g. `"FREQ"`, `"OBS_VALUE"`) |
| `required` | `bool` | `True` if the component is mandatory |
| `role` | `Role` | `DIMENSION`, `MEASURE`, or `ATTRIBUTE` |
| `concept` | `Concept \| ItemReference` | The underlying SDMX Concept |
| `local_codes` | `Codelist \| Hierarchy \| None` | Codes set on the component itself (constrained by the schema) |
| `local_dtype`, `local_facets` | | Local representation, if any |
| `attachment_level` | `str \| None` | For attributes (mandatory for them): e.g. `"O"` for observation |
| `local_enum_ref` | `str \| None` | URN of the enumeration when its codes are not in the message (1.20) |

Properties resolve the local representation first, then the concept's core representation:

| Property | Returns |
|---|---|
| `enumeration` | `local_codes`, else `concept.codes`, else `None` |
| `dtype` | `local_dtype`, else `concept.dtype`, else `DataType.STRING` |
| `facets` | `local_facets`, else `concept.facets` |
| `enum_ref` | URN of the enumeration |

**Read codes through `enumeration`, not `local_codes`.** With Fusion-JSON, a component whose codes come from its concept has `local_codes=None` and the codes on `concept.codes`. And `get_schema` for a dataflow or provision agreement replaces `local_codes` with a `Hierarchy` when the component has a hierarchy association — a `Hierarchy` has no `.items`:

```python
enum = schema.components["REF_AREA"].enumeration
if isinstance(enum, Hierarchy):
    code_ids = [c.id for c in enum.all_codes()]  # every level, flattened
elif enum is not None:
    code_ids = [c.id for c in enum]  # Codelist iterates its codes
```

This is what `tidysdmx.utils.get_codelist_ids` does (it also de-duplicates hierarchy codes attached under several parents).

---

### 3.4 `Role` (Enum)

| Value | SDMX IM concept |
|---|---|
| `Role.DIMENSION` | Dimension (part of the series key; includes the time dimension) |
| `Role.MEASURE` | Measure (observed value, e.g. `OBS_VALUE`) |
| `Role.ATTRIBUTE` | Attribute (not part of the key) |

There are no other members.

---

### 3.5 `DataType` (Enum)

**SDMX IM equivalent:** Facet/representation type for uncoded components. 41 members as of 1.20; 1.17 added `EXC_VAL_RANGE`, `INC_VAL_RANGE`, `GEO_INFO`, `REP_TIME_PERIOD` and `TIME_RANGE`, so code that matches exhaustively over `DataType` must be revisited on upgrades.

Common values used in tidysdmx:

| Value | Description |
|---|---|
| `DataType.STRING` | Plain text (the default `dtype`) |
| `DataType.INTEGER` | Whole number |
| `DataType.DOUBLE` | Floating-point number |
| `DataType.BOOLEAN` | Boolean flag |
| `DataType.DATE_TIME` | ISO 8601 datetime |
| `DataType.PERIOD` | SDMX observational time period |

---

### 3.6 `Concept`

**SDMX IM equivalent:** An SDMX Concept from a ConceptScheme — the semantic definition of what a component represents.

| pysdmx attribute | Type | Description |
|---|---|---|
| `id` | `str` | Concept ID (e.g. `"FREQ"`, `"TIME_PERIOD"`) |
| `urn` | `str \| None` | Full SDMX URN for the concept |
| `name`, `description` | `str \| None` | Labels |
| `dtype`, `facets` | | Core representation type and facets |
| `codes` | `Codelist \| None` | Core representation codes |
| `enum_ref` | `str \| None` | URN of the core representation enumeration |

---

### 3.7 `Codelist`, `Code` and `Hierarchy`

| `Codelist` | Description |
|---|---|
| `id`, `agency`, `version`, `name` | Identity |
| `items` (alias property `codes`) | The `Code` objects |
| `sdmx_type` | `"codelist"` or `"valuelist"` — a ValueList is a `Codelist` too |
| `short_urn` | `Codelist=AG:ID(v)` / `ValueList=AG:ID(v)` |

A `Codelist` iterates its codes, supports `"A" in codelist` and `codelist["A"]` (returns `None` when absent), and since 1.10 `codelist.search(query, use_regex=False, fields="all")` to find codes by name or description.

A `Hierarchy` holds nested `HierarchicalCode`s in `codes`. `in` and `[]` only see top-level codes (or dotted paths); use `all_codes()` for the flat list.

---

## 4. Mapping Model Classes

These classes live in `pysdmx.model.map` (re-exported from `pysdmx.model`) and represent the SDMX **StructureMap** artefact family.

### 4.1 `StructureMap`

**SDMX IM equivalent:** A StructureMap artefact — a named, versioned container of mapping rules between a source and a target structure.

| pysdmx attribute | Type | Description |
|---|---|---|
| `id`, `agency`, `version`, `name` | | Identity (`name` is required to write SDMX-JSON) |
| `source`, `target` | `str` | URNs of the source and target structures |
| `maps` | `Sequence[...]` | Any mix of the map types below |

Typed views filter `maps` in stored order — use them instead of `isinstance` loops:
`fixed_value_maps`, `implicit_component_maps`, `component_maps`, `multi_component_maps`, `date_pattern_maps`. (`tidysdmx.mapping` does.)

Avoid `structure_map["X"]`: `__getitem__` matches substrings of a `str` source, so `sm["AREA"]` also returns the map whose source is `REF_AREA`.

### 4.2 `FixedValueMap`

Assigns a **constant value** to a component.

| attribute | Type | Description |
|---|---|---|
| `target` | `str` | Component ID |
| `value` | `Any` | The fixed value |
| `located_in` | `str` | `"source"` or `"target"` (default) |

### 4.3 `ImplicitComponentMap`

Copies values from a source component to a target component unchanged.

```python
ImplicitComponentMap(source="FREQ", target="FREQUENCY")
```

### 4.4 `ComponentMap`

Maps values from one component to another through a `RepresentationMap`.

| attribute | Type | Description |
|---|---|---|
| `source`, `target` | `str` | Component IDs |
| `values` | `RepresentationMap \| str` | The embedded map, **or only its URN** |

`values` is a URN string when the map was built or read without resolving the reference. `RegistryClient.get_mapping()` resolves them (it fetches children and embeds each representation map). `tidysdmx.mapping` raises a `TypeError` saying so when it meets a URN, because there are no value maps to apply.

### 4.5 `MultiComponentMap`

Maps values from **several source components** to one or more targets.

| attribute | Type | Description |
|---|---|---|
| `source`, `target` | `Sequence[str]` | Component IDs |
| `values` | `MultiRepresentationMap \| str` | The embedded map or its URN |

### 4.6 `RepresentationMap`

A named, versioned set of `ValueMap`s for one source and one target representation.

| attribute | Type | Description |
|---|---|---|
| `id`, `agency`, `version`, `name` | | Identity (`name` is required to write SDMX-JSON) |
| `source`, `target` | `str \| None` | A Codelist/ValueList URN, or a data type name (`"String"`) |
| `maps` | `Sequence[ValueMap]` | The value pairs |

Since **1.17** the constructor raises `pysdmx.errors.Invalid` when `maps` is non-empty and `source` or `target` is `None` (an empty string still passes). pysdmx's writers decide between a codelist reference and a data type by whether the string contains `Codelist` or `ValueList`.

### 4.7 `ValueMap`

A single source→target pair, optionally scoped to a validity period. **Keyword-only.**

```python
ValueMap(source="GB", target="UK")
ValueMap(source="regex:^D.*", target="DEU", valid_from=datetime(2020, 1, 1))
```

A source prefixed `"regex:"` is a regular expression; `typed_source` returns it compiled. tidysdmx does not use `typed_source`: it applies regexes with `fullmatch` semantics and ranks literal maps before regex maps and the catch-all `"regex:.*"` last (`mapping._value_map_rank`), which `typed_source` alone does not express.

### 4.8 `MultiValueMap`

Like `ValueMap` but for tuples (`source` / `target` are sequences, one entry per component). Keyword-only.

### 4.9 `MultiRepresentationMap`

Container of `MultiValueMap`s used by a `MultiComponentMap`. `source` / `target` are sequences of URNs or data type names; since 1.17 their lengths must match the first map's tuples.

**There is no `MultiRepresentationMap` class in SDMX** — both single and multi representation maps are SDMX `RepresentationMap`s, and `short_urn` says `RepresentationMap=AG:ID(v)`. Never put `MultiRepresentationMap=` in a URN; `tidysdmx.gen_urn` writes such names under the SDMX class.

### 4.10 `DatePatternMap`

Transforms a date column from a source pattern into an SDMX time period.

| attribute | Type | Description |
|---|---|---|
| `source`, `target` | `str` | Component IDs (target typically `"TIME_PERIOD"`) |
| `pattern` | `str` | Source date pattern (e.g. `"MMM yy"`) |
| `frequency` | `str` | A frequency code (`"M"`) or the ID of a frequency dimension (`"FREQ"`) |
| `pattern_type` | `str` | `"fixed"` (frequency is a code) or `"variable"` (it names a dimension) |
| `id`, `locale`, `resolve_period` | | Optional |

`py_pattern` converts the SDMX pattern to a Python `strftime` pattern (`pysdmx.util.convert_dpm`). tidysdmx can build these maps but `map_structures` does not apply them yet (it raises `TypeError`).

---

## 5. API and I/O Classes

### 5.1 `RegistryClient`

An HTTP client for querying an FMR.

```python
from pysdmx.api.fmr import RegistryClient
from pysdmx.io.format import StructureFormat

client = RegistryClient(
    api_endpoint="https://your-fmr-host/FMR/sdmx/v2/",  # must include /sdmx/v2
    format=StructureFormat.FUSION_JSON,  # what tidysdmx uses
)
schema = client.get_schema("dataflow", "WB", "WDI", "1.0.0")  # version is required
```

Methods tidysdmx uses or should reach for: `get_schema(context, agency, id, version)`, `get_mapping(agency, id, version="~")` (StructureMap with representation maps resolved), `get_code_map(...)` (one representation map), `get_codes(...)` (codelist, falling back to a valuelist), `get_hierarchy(...)`, `get_concepts(...)`, `get_data_structures(...)`, `get_dataflow_details(...)`. `FmrClient` wraps all 21 getters (§7), and `tests/test_fmr.py` fails when a pysdmx release adds one it does not wrap. `get_metadata_providers` is annotated `Sequence[DataProvider]` but returns `MetadataProvider`s (`docs/pysdmx-shortcomings.md`, PYSDMX-READ-03). `AsyncRegistryClient` has the same methods; `FmrClient` has no async counterpart yet.

**Version defaults.** Since **1.15** every method's default `version` is `"~"` — the latest version, **including non-final ones**; before 1.15 it was `"+"` (latest stable). Pass an explicit version (tidysdmx always does), or `"+"` when you want only final releases. Since 1.16 semver strings work throughout.

The client has **no authentication hook** — it cannot send an `Authorization` header. `tidysdmx.fmr.FmrClient` works around it; see §5.3 and `docs/pysdmx-shortcomings.md` (PYSDMX-AUTH-01).

Errors: 404 → `pysdmx.errors.NotFound`; any other 4xx, including 401/403 → `Invalid`; 5xx → `InternalError`; transport failures → `Unavailable`.

### 5.2 `StructureFormat`

| Value | Description |
|---|---|
| `StructureFormat.FUSION_JSON` | FMR's extended JSON (what tidysdmx uses) |
| `StructureFormat.SDMX_JSON_2_0_0` | Standard SDMX-JSON 2.0 (the client's default) |
| `StructureFormat.SDMX_JSON_1_0_0` | SDMX-JSON 1.0 |
| `StructureFormat.SDMX_ML_2_1`, `SDMX_ML_3_0`, `SDMX_ML_3_1` | SDMX-ML |

`RegistryClient` accepts only `FUSION_JSON` and `SDMX_JSON_2_0_0`; anything else raises `pysdmx.errors.NotImplemented`. File I/O uses the separate `pysdmx.io.format.Format` enum (e.g. `Format.STRUCTURE_SDMX_ML_3_0`).

### 5.3 `RegistryMaintenanceClient` (EXPERIMENTAL)

Uploads maintainable artefacts to an FMR. Lives in `pysdmx.api.fmr.maintenance`, takes the registry **root** (it strips `/sdmx/v2` itself), and authenticates with either basic auth or a static bearer token (`access_token`, since 1.16):

```python
from pysdmx.api.fmr.maintenance import RegistryMaintenanceClient, StructureAction

client = RegistryMaintenanceClient("https://your-fmr-host/FMR", access_token=token)
# POSTs SDMX-JSON 2.0.0 to {root}/ws/secure/sdmxapi/rest
client.put_structures([codelist], action=StructureAction.Replace)
```

`StructureAction` is `Append`, `Merge` or `Replace`. Pass constructor arguments by keyword: 1.16 inserted `access_token` before `pem`. The body is SDMX-JSON 2.0, which has no `isFinal`, so finality follows the version string (pysdmx's `is_final()` treats `1.0.0` as final and `1.0` as not). Since 1.20 an `AvailabilityConstraint` in the list is skipped with a `UserWarning`.

The class is marked experimental by pysdmx, so its API may change between minor releases; it does not acquire or refresh tokens, and a rejected token surfaces as `pysdmx.errors.Invalid("Client error 401")`, never as `Unauthorized`. `tidysdmx.fmr.FmrClient` wraps both clients, adds token acquisition and refresh, and authenticates reads; the pysdmx gaps it works around are catalogued in `docs/pysdmx-shortcomings.md`.

### 5.4 Reading and writing SDMX files

`pysdmx.io.read_sdmx` / `write_sdmx(objects, Format.…, output_path=...)` handle structures in SDMX-ML 2.1/3.0/3.1 and SDMX-JSON 2.0/2.1, behind the `xml` / `json` extras. tidysdmx does not call them itself; `collect_structure_map_artifacts` and `prepare_structure_map_for_upload` return pysdmx objects for the caller to write or upload. Points that matter for structure maps:

- Write maps as **SDMX-ML 3.0/3.1 or SDMX-JSON**; StructureMap and RepresentationMap are SDMX 3.0 artefacts.
- The SDMX-ML writer emits no `validFrom`/`validTo` and no regex flag on value maps: a `regex:` source is written as literal text. SDMX-JSON keeps both.
- The SDMX-ML writer writes `ComponentMap.values` verbatim, so it must be a URN string, and the representation map must be passed as a top-level artefact. SDMX-JSON converts an embedded map to its URN itself.
- The SDMX-JSON writer raises `Invalid` when a StructureMap or RepresentationMap has no `name`.
- pysdmx ≥1.14 writes `SourceDataType`/`TargetDataType` correctly; `tidysdmx.fix_sdmx_xml_datatype_tags` is deprecated.

---

## 6. SDMX Artefact ID Format

tidysdmx identifies artefacts with the SDMX short form `"AGENCY:ID(VERSION)"`:

- `"WB:WDI(1.0.0)"` — World Bank WDI dataflow, version 1.0.0
- `"SDMX:CL_FREQ(2.0)"` — SDMX cross-domain frequency codelist

```python
from tidysdmx import parse_artefact_id

agency, id, version = parse_artefact_id("WB:WDI(1.0.0)")
# → ("WB", "WDI", "1.0.0")
```

`parse_artefact_id` is tidysdmx's own parser. pysdmx's public parsers (`parse_urn`, `parse_short_urn`, `parse_maintainable_urn`) all expect a `Type=` prefix; `pysdmx.util.parse_flow_urn` accepts this form but is not in `pysdmx.util.__all__`, raises `pysdmx.errors.Invalid` rather than `ValueError`, and always reports the type as `Dataflow`.

`FmrClient`'s fetch methods take either form: `parse_artefact_id` splits `"AGENCY:ID(VERSION)"`, pysdmx's `parse_urn` a full or short URN, whose class must match what is fetched.

Full URNs: pysdmx has parsers but **no URN builder**, so `tidysdmx.gen_urn` builds them. For an artefact you already hold, prefer `f"urn:sdmx:org.sdmx.infomodel.<package>.{artefact.short_urn}"`, which gets the SDMX class name right.

---

## 7. How tidysdmx Uses pysdmx

tidysdmx is a **thin wrapper** that bridges pysdmx's object model with pandas DataFrames and Excel-based workflows.

| Task | pysdmx provides | tidysdmx adds |
|---|---|---|
| **Fetch schema** | `RegistryClient.get_schema()` | `FmrClient.fetch_schema()` — one ID string or URN, endpoint and token from the client (the module-level `fetch_schema()` is deprecated) |
| **Fetch artefacts** | `RegistryClient.get_codes()`, `get_hierarchy()`, `get_concepts()`, `get_categories()`, `get_categorisation()`, `get_dataflows()`, `get_data_structures()`, `get_provision_agreement()`, `get_metadataflows()`, `get_metadata_structures()`, `get_metadata_provision_agreement()`, `get_mapping()`, `get_code_map()`, `get_vtl_transformation_scheme()` — separate agency, id and version; dataflows, DSDs, metadataflows and MSDs only as lists | `FmrClient.fetch_artefact()`, driven by one table of the 14 getters, and one typed `fetch_*` method per type that calls it — one `"AGENCY:ID(VERSION)"` string or URN, a single result, wildcards refused |
| **Other registry reads** | `RegistryClient.get_dataflow_details()`, `get_agencies()`, `get_providers()`, `get_metadata_providers()`, `get_report()`, `get_reports()` | `FmrClient.fetch_dataflow_info()`, `fetch_agencies()`, `fetch_data_providers()`, `fetch_metadata_providers()`, `fetch_metadata_report()`, `fetch_metadata_reports()` — same reference rules; with `fetch_schema` and the artefact fetchers they wrap every `RegistryClient` getter |
| **Registry access with authentication** | `RegistryClient` (no auth), `RegistryMaintenanceClient(access_token=...)` (static token) | `FmrClient` — one root URL, `TokenProvider`-based acquisition and refresh, bearer token on reads and writes |
| **Schema introspection** | `Components.dimensions`, `Component.required`, `Component.enumeration`, `Hierarchy.all_codes()` | `extract_validation_info()`, `get_codelist_ids()`, `extract_component_ids()` |
| **Column validation** | The schema's rules (no DataFrame validator exists in pysdmx) | `validate_dataset_local()` and the individual `validate_*` checks |
| **Apply structure maps** | The map model and its typed views (no map applier exists in pysdmx) | `map_structures()` and the `apply_*` functions |
| **Build structure maps** | Map constructors | `build_*` helpers, `build_structure_map_from_template_wb()` |
| **Create schema from data** | `DataStructureDefinition`, `Codelist`, `ConceptScheme` constructors | `create_schema_from_table()` — infers a DSD (+ concepts, codelists) from a DataFrame |
| **Publish-readiness checks** | Constructor invariants only | `artefact_validation.validate()` / `raise_if_invalid()` |
| **Output standardisation** | SDMX-CSV conventions (its writer needs the `data` extra) | `standardize_output()` — adds `STRUCTURE`, `STRUCTURE_ID`, `ACTION` |
| **Excel mapping workflow** | None | `write_excel_mapping_template()`, `parse_mapping_template_wb()`, `build_structure_map_from_template_wb()` |

---

## 8. Working with pysdmx Schema Objects — Key Patterns

### Extracting validation info from a Schema
```python
from tidysdmx import FmrClient, extract_validation_info

client = FmrClient("https://fmr.example.com/FMR")
schema = client.fetch_schema("WB:WDI(1.0.0)", "dataflow")

valid = extract_validation_info(schema)
# valid = {
#   "valid_comp":     ["FREQ", "REF_AREA", "INDICATOR", "TIME_PERIOD", "OBS_VALUE"],
#   "mandatory_comp": ["FREQ", "REF_AREA", "INDICATOR", "TIME_PERIOD", "OBS_VALUE"],
#   "coded_comp":     ["FREQ", "REF_AREA", "INDICATOR"],
#   "codelist_ids":   {"FREQ": ["A", "M", "Q"], "REF_AREA": ["US", "GB", ...], ...},
#   "dim_comp":       ["FREQ", "REF_AREA", "INDICATOR", "TIME_PERIOD"],
#   "sdmx_cols":      ["STRUCTURE", "STRUCTURE_ID", "ACTION"],
# }
```

A component is in `coded_comp` when its `enumeration` is set — local codes, a hierarchy, or its concept's codes.

### Iterating components
```python
for component in schema.components:
    print(component.id, component.role, component.required)

obs_val = schema.components["OBS_VALUE"]
obs_val.role  # Role.MEASURE
obs_val.enumeration  # None (numeric measures are typically uncoded)
obs_val.dtype  # e.g. DataType.DOUBLE
```

---

## 9. Building Mapping Objects — Quick Reference

```python
import pandas as pd
from pysdmx.model import StructureMap

from tidysdmx import (
    build_date_pattern_map,
    build_fixed_map,
    build_implicit_component_map,
    build_representation_map,
    build_single_component_map,
    map_structures,
)

# 1. Fixed value
fmap = build_fixed_map(target="CONF_STATUS", value="F")

# 2. Implicit (column rename with no value change)
imap = build_implicit_component_map(source="SourceFreq", target="FREQ")

# 3. Date pattern (buildable; map_structures does not apply it yet)
dpm = build_date_pattern_map(
    source="DATE", target="TIME_PERIOD", pattern="MMM yy", frequency="M"
)

# 4. Value-level representation map from a DataFrame
mapping_df = pd.DataFrame({"source": ["GB", "US"], "target": ["GBR", "USA"]})
rep_map = build_representation_map(mapping_df, agency="ECB", id="RM_COUNTRY")

# 5. Single component map (embeds a representation map)
cm = build_single_component_map(
    df=mapping_df,
    source_component="COUNTRY_SRC",
    target_component="REF_AREA",
    agency="ECB",
    id="RM_COUNTRY",
    name="Country map",
)

# 6. Apply a StructureMap to a DataFrame
smap = StructureMap(
    id="MY_MAP", agency="ECB", version="1.0", name="My map", maps=[fmap, imap, cm]
)
result_df = map_structures(df, smap)
```

---

## 10. SDMX Reference Columns Added by tidysdmx

`standardize_output()` adds the SDMX-CSV reference columns. The column names are the same for every artefact type; the type is carried as the *value* of `STRUCTURE`, using SDMX-CSV's names — which differ from the schema context for a provision agreement:

| `Schema.context` | `STRUCTURE` value |
|---|---|
| `"datastructure"` | `datastructure` |
| `"dataflow"` | `dataflow` |
| `"provisionagreement"` | `dataprovision` |

`ACTION` takes the SDMX-CSV codes pysdmx reads and writes (`pysdmx.model.dataset.ActionType`): `"I"` (Information), `"A"` (Append), `"R"` (Replace), `"D"` (Delete).

---

## 11. What NOT to Reimplement in tidysdmx

| Don't reimplement | Use instead |
|---|---|
| HTTP schema fetching | `RegistryClient.get_schema()` via `FmrClient.fetch_schema()` |
| Artefact fetching | `RegistryClient.get_codes()`, `get_hierarchy()`, `get_dataflows()`, ... via `FmrClient.fetch_*()` |
| Artefact upload | `RegistryMaintenanceClient.put_structures()` via `FmrClient.put_structures()` |
| Resolving map references | `RegistryClient.get_mapping()` / `get_code_map()`; `pysdmx.util.find_by_urn` over objects you hold |
| Filtering components by role | `Components.dimensions` / `.measures` / `.attributes` |
| Reading a component's codes | `Component.enumeration` (+ `Hierarchy.all_codes()`) |
| Mandatory field checking | `Component.required` |
| Grouping a StructureMap's maps by type | `StructureMap.fixed_value_maps`, `.implicit_component_maps`, `.component_maps`, `.multi_component_maps`, `.date_pattern_maps` |
| SDMX class names in URNs | `artefact.short_urn` |
| Parsing full or `Type=`-prefixed short URNs | `pysdmx.util.parse_urn` / `parse_short_urn` |
| Converting DatePatternMap patterns | `DatePatternMap.py_pattern` |
| Finding codes by name or description | `ItemScheme.search()` |
| Map constructors | pysdmx constructors directly, or the `build_*` helpers |

### Deliberately not used (so you don't re-litigate)

| pysdmx feature | Why tidysdmx doesn't use it |
|---|---|
| `ValueMap.typed_source` | tidysdmx needs `fullmatch` semantics and literal-before-regex-before-catch-all ranking (§4.7). |
| `StructureMap.__getitem__` | Substring matching on sources (§4.1). |
| `parse_flow_urn` for `AG:ID(v)` | Not exported; different exception type (§6). |
| `PandasDataset`, `to_pandas_schema`, SDMX-CSV writer | Need the `data` extra (pyarrow), which tidysdmx does not depend on; `PandasDataset` also casts and mutates the caller's DataFrame. |
| A dataset-vs-schema validator | Does not exist in pysdmx as of 1.20, so `validation.py` is not a reimplementation. |
| A StructureMap applier | Does not exist in pysdmx as of 1.20, so `mapping.py` is not a reimplementation. |

---

## 12. Glossary: pysdmx ↔ SDMX IM ↔ tidysdmx

| pysdmx class/attribute | SDMX IM concept | tidysdmx usage |
|---|---|---|
| `Schema` | Resolved DSD/Dataflow/PA structure | Passed to `validate_dataset_local()`, `standardize_output()`, `extract_validation_info()` |
| `Schema.context` | Artefact type (DSD, Dataflow, PA) | Sets the SDMX-CSV `STRUCTURE` value in `standardize_output()` |
| `Schema.components` | DimensionList + MeasureList + AttributeList | Iterated to extract component IDs, roles, codes |
| `Component` | Dimension / Measure / Attribute | Each DataFrame column maps to a Component |
| `Components.dimensions` | Dimension descriptor | Key columns for `validate_duplicates()` |
| `Component.required` | Mandatory in data | Mandatory column validation |
| `Component.enumeration` | Enumerated representation (codelist or hierarchy) | Codelist validation |
| `Code.id` | Code identifier | The allowed string value in the data |
| `DataType` | Facet/representation type | Used in `create_schema_from_table()` |
| `StructureMap` | StructureMap artefact | Input to `map_structures()` |
| `FixedValueMap` | Fixed-value mapping | Adds constant columns |
| `ImplicitComponentMap` | Implicit component mapping | Copies columns under a new name |
| `ComponentMap` / `MultiComponentMap` | Component mapping with value translation | Recodes column values |
| `RepresentationMap` / `MultiRepresentationMap` | RepresentationMap | Built from DataFrames; URN class is always `RepresentationMap` |
| `ValueMap` / `MultiValueMap` | RepresentationMapping | Items in a representation map's `maps` |
| `RegistryClient` | SDMX REST client | `FmrClient.registry`, behind every `FmrClient.fetch_*()` |
| `RegistryMaintenanceClient` | SDMX REST maintenance client | `FmrClient.maintenance` / `put_structures()` |
| `StructureFormat.FUSION_JSON` | FMR wire format | Format of all tidysdmx registry reads |

---

## 13. Keeping Up With pysdmx

pysdmx ships a minor release roughly monthly. When `uv.lock` moves:

1. Read every release note since the locked version: <https://github.com/bis-med-it/pysdmx/releases>.
2. `uv lock --upgrade-package pysdmx`, `uv sync --all-groups --all-extras`, then run the **whole** suite — including `-m integration`, which loads the pickled cassettes in `tests/fixtures/cassettes/` (pickled msgspec structs break if pysdmx renames or drops fields).
3. Re-verify each seam in `docs/pysdmx-shortcomings.md` and update its "Verified against" line; `tests/test_fmr.py` exercises them on the wire.
4. Check for changed defaults (version wildcards), new constructor invariants (they can turn tidysdmx's own checks into dead code), new `DataType` members, and newly exported names that let a private `pysdmx.model.__base` import go.
5. Update the "Verified against" line at the top of this document and anything below that changed. Raise the floor in `pyproject.toml` only when tidysdmx starts relying on a newer release.

Changes since 1.13 that shaped the current code:

| Release | Change | Effect on tidysdmx |
|---|---|---|
| 1.10 | `ItemScheme.search()` | Available; nothing to replace |
| 1.12 / 1.13 | `Schema.keys` / `excluded_keys` | Not yet validated (backlog) |
| 1.14 | SDMX-ML writer uses `SourceDataType`/`TargetDataType` | `fix_sdmx_xml_datatype_tags` deprecated |
| 1.15 | FMR default version `"+"` → `"~"` | tidysdmx passes explicit versions; test fixtures calling `get_mapping` without one now get the latest, possibly non-final, version |
| 1.15 | `data` extra moved to PyArrow dtypes; data writers require a `Schema` | Not used by tidysdmx |
| 1.16 | `access_token` on `RegistryMaintenanceClient`; semver versions in FMR clients | Used by `FmrClient` |
| 1.17 | RepresentationMap requires source/target when maps are set; MultiRepresentationMap length checks; 5 new `DataType`s | Some tidysdmx `None` checks are now unreachable for maps with values |
| 1.19 | Stub artefact parsing; `Dataflow.structure` may be `None` | Covered by `artefact_validation` |
| 1.20 | Availability constraints and time ranges; empty messages read as empty; `local_enum_ref` kept when the codelist is absent | `enumeration` can be `None` while `enum_ref` is set — such a component is treated as uncoded |
