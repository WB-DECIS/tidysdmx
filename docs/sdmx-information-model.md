# SDMX Information Model Reference
**Source:** SDMX Technical Standards Section 1 (Framework) & Section 2 (Information Model), Version 3.0, October 2021
**Authoritative reference:** https://www.sdmx.org | https://github.com/sdmx-twg

---

## 1. What is the SDMX Information Model?

The SDMX Information Model (IM) is a standardised, format-neutral object model for representing statistical data and metadata. It is defined as a UML conceptual design (Section 2 of the Technical Specifications) and serves as the **common semantic layer** that guarantees lossless transformation between all SDMX transmission formats (SDMX-ML, SDMX-JSON, SDMX-CSV).

Every SDMX transmission format is a syntax-bound expression of this shared model. Code written against the IM is therefore format-agnostic by design.

---

## 2. Core Concepts

### 2.1 Statistical Data

SDMX models statistical data as **multi-dimensional observations** organised into a "cube":

- **Observation**: a single measured value, typically numeric, usually associated with a time period and a measured concept (e.g. GDP, unemployment rate).
- **Dimension**: a named concept that acts as an axis of the cube (e.g. `FREQ`, `REF_AREA`, `INDICATOR`). Each series has a **key** — the combination of values for all non-time dimensions.
- **Measure**: the observed value itself. SDMX 3.0 supports multiple measures per DSD.
- **Attribute**: descriptive metadata attached to a dataset, series, or observation (e.g. `UNIT_MULT`, `OBS_STATUS`). Attributes do not form part of the key.
- **Series**: a slice of the cube identified by its key, containing one or more observations over time.
- **Group**: an optional intermediate grouping of series sharing common dimension values.
- **Dataset**: a collection of series (or observations) conforming to a single Data Structure Definition, covering a fixed time period.

### 2.2 Metadata Taxonomy

SDMX distinguishes two broad categories of metadata:

| Type | Description | Examples |
|------|-------------|---------|
| **Structural metadata** | Describes the structure of data and metadata sets; machine-readable | DSD, Codelist, ConceptScheme, Dataflow |
| **Reference metadata** | Describes and qualifies data sets more generally; often human-readable | Quality reports, methodology, release calendars |

Reference metadata in SDMX 3.0 can be attached directly to a dataset as attributes (e.g. footnotes), or reported separately via MetadataSets linked to structural objects.

---

## 3. Major Artefact Types

All SDMX artefacts are **identifiable objects** with a unique identity composed of:
- **Agency ID** — the maintenance agency (e.g. `ECB`, `SDMX`, `IMF`)
- **ID** — the artefact's local identifier (e.g. `CL_FREQ`)
- **Version** — semantic version string (e.g. `1.0.0`) — SDMX 3.0 adopts semver (https://semver.org)

The full reference is expressed as a **URN**: `urn:sdmx:org.sdmx.infomodel.<type>=<AgencyID>:<ID>(<Version>)`.
In SDMX 3.0, URN is the **exclusive** mechanism for non-local cross-artefact referencing (agency+ID+version tuple references are deprecated).

### 3.1 Data Structure Definition (DSD)

The central structural artefact. Defines the structure of a dataset.

| Component | Role |
|-----------|------|
| **DimensionList** | Ordered list of dimensions forming the series key |
| **MeasureList** | One or more measures (SDMX 3.0 supports multiple) |
| **AttributeList** | Attributes with attachment levels (dataset / group / series / observation) |
| **GroupList** | Optional grouping dimensions |

Each dimension, measure, and attribute references a **Concept** (from a ConceptScheme) and optionally specifies a **representation** (a Codelist, ValueList, or facet like `String`, `Integer`, `Date`).

The DSD may optionally reference a **Metadata Structure Definition (MSD)** for reporting reference metadata alongside the data.

> **SDMX 3.0 change:** DSDs now support multiple measures and array-valued attributes/measures to better support microdata.

### 3.2 Codelist

An enumeration of codes used to represent dimension or attribute values.

- Each **Code** has an ID, name, and optional description.
- Codes can be organised into **simple hierarchies** within a codelist.
- Complex, cross-codelist hierarchies are expressed via the **Hierarchy** and **HierarchyAssociation** artefacts (see §3.11).
- SDMX 3.0 introduced **codelist extension** (a codelist can extend another) and **discriminated union of codelists**.

### 3.3 ValueList *(SDMX 3.0 new)*

Similar to a Codelist, but items do not need to conform to SDMX naming rules for identifiable objects. This allows values containing special characters (e.g. currency symbols like `¥`). Unlike codes, values are **not individually identifiable**. Used for microdata enumerations and less structured data.

### 3.4 ConceptScheme

A maintained list of statistical **concepts** (e.g. `FREQ`, `REF_AREA`, `OBS_VALUE`) used in DSDs and MSDs. A concept can specify a **core representation** (e.g. a default codelist), which may be overridden by the DSD or MSD using it.

Concept schemes can be mapped to each other via **ConceptSchemeMap**.

### 3.5 Dataflow (DataflowDefinition)

Represents an ongoing, time-unbounded stream of data of a given type. Points to a DSD and constrains the data reported under it.

- A Dataflow links a DSD to one or more **CategoryScheme** entries via **Categorisation** (for subject-matter classification/discovery).
- **Constraints** (content or availability) can be attached to limit the key space or schedule.

Think of a Dataflow as "the ECB exchange rate series" while the DSD is "the generic structure for exchange rate data".

### 3.6 Metadataflow (MetadataflowDefinition)

Equivalent of a Dataflow but for reference metadata sets. Points to a Metadata Structure Definition (MSD).

### 3.7 Metadata Structure Definition (MSD)

Defines the structure of a reference metadata set: what concepts are reported, their hierarchy, their representation (free text, coded, etc.), and which SDMX objects they are attached to.

### 3.8 MetadataSet

A reference metadata set — a set of information pertaining to any identifiable SDMX object (a dataflow, a DSD, a data provider, etc.). Structured according to an MSD. Contains the actual reference metadata values.

### 3.9 CategoryScheme and Categorisation

- **CategoryScheme**: a hierarchical classification scheme (e.g. SDMX Statistical Subject-Matter Domains).
- **Category**: a node within a CategoryScheme; can have child categories.
- **Categorisation**: a link between a Category and any identifiable SDMX artefact (e.g. linking a Dataflow to a subject-matter domain).

Used primarily for data discovery.

### 3.10 OrganisationScheme

Defines organisations and their structure. Subtypes include:
- **MaintenanceAgencyScheme** — agencies that own/maintain artefacts
- **DataProviderScheme** — organisations that publish data
- **MetadataProviderScheme** — organisations that publish reference metadata
- **DataConsumerScheme**
- **OrganisationUnitScheme**

### 3.11 Hierarchy and HierarchyAssociation *(SDMX 3.0 improved)*

- **Hierarchy**: describes complex code hierarchies spanning potentially multiple codelists. The codes themselves live in their codelists; the Hierarchy references them. Used primarily for data discovery and drill-down.
- **HierarchyAssociation**: links a Hierarchy to the object that needs it (e.g. a Dimension), optionally specifying the context (e.g. a specific Dataflow). This allows the same dimension to use different hierarchies in different dataflows.

### 3.12 ProvisionAgreement and MetadataProvisionAgreement

The formal link between a **DataProvider** (or MetadataProvider) and a **Dataflow** (or Metadataflow). Describes:
- Which data/metadata the provider supplies
- The schedule (release calendar)
- The subset of the key space covered (via Constraint)
- The URL where the actual data/metadata can be retrieved

> **SDMX 3.0 change:** DataProvisionAgreement and MetadataProvisionAgreement are now two distinct artefacts.

### 3.13 Constraint

Describes a **subset** of a data or metadata source. Two types:
- **ContentConstraint**: defines which key combinations (series keys, cube regions) are present or allowed in a data source.
- **AvailabilityConstraint**: describes what data is available (for use with the REST `availability` resource).

Can be attached to: DataProviders, ProvisionAgreements, Dataflows, Metadataflows, DSDs, or MSDs.

### 3.14 StructureMap

A mapping between two DSDs (or Dataflows), describing how to transform a dataset from one structure to another. Contains one or more **ComponentMaps** (mapping source dimensions/attributes to target ones) and optionally **RepresentationMaps** for code-level value translation.

### 3.15 RepresentationMap

A lookup table mapping values from one Codelist or ValueList to values in another. Used within StructureMaps or independently.

### 3.16 ItemSchemeMap

Maps items between any two item schemes (except Codelists/ValueLists, which use RepresentationMap). Types: OrganisationSchemeMap, ConceptSchemeMap, CategorySchemeMap, ReportingTaxonomyMap.

### 3.17 ReportingTaxonomy

Links multiple Dataflows or DSDs hierarchically to describe a complete "report" — a publication (e.g. a statistical yearbook) comprising multiple heterogeneous datasets.

### 3.18 Process

Models a statistical production process as a set of interconnected **ProcessSteps**. Not central to data exchange but supports interoperable exchange of process-related reference metadata.

### 3.19 TransformationScheme (VTL)

A set of **VTL (Validation and Transformation Language) 2.0** transformations designed to be executed together. Can be thought of as a VTL "program". May contain Rulesets, UserDefinedOperators, NamePersonalisations, and VTL Mappings (linking VTL objects to SDMX artefacts).

---

## 4. Artefact Inheritance Hierarchy (simplified)

```
IdentifiableArtefact
└── NameableArtefact
    └── VersionableArtefact
        └── MaintainableArtefact          ← top-level, agency-owned artefacts
            ├── DataStructureDefinition
            ├── MetadataStructureDefinition
            ├── DataflowDefinition
            ├── MetadataflowDefinition
            ├── Codelist
            ├── ValueList
            ├── ConceptScheme
            ├── CategoryScheme
            ├── OrganisationScheme (subtypes)
            ├── Hierarchy
            ├── ProvisionAgreement
            ├── MetadataProvisionAgreement
            ├── Constraint
            ├── StructureMap
            ├── RepresentationMap
            ├── ItemSchemeMap (subtypes)
            ├── ReportingTaxonomy
            ├── Process
            └── TransformationScheme
```

**Item schemes** (Codelist, ConceptScheme, CategoryScheme, OrganisationScheme) are MaintainableArtefacts that contain a collection of **Items** (Code, Concept, Category, Organisation respectively). Items are IdentifiableArtefacts within their parent scheme.

---

## 5. Key Relationships

```
DSD ─── uses ──► ConceptScheme (concepts)
DSD ─── uses ──► Codelist / ValueList (representations)
DSD ─── optionally references ──► MSD

DataflowDefinition ─── references ──► DSD
DataflowDefinition ─── linked via Categorisation ──► CategoryScheme/Category
DataflowDefinition ─── constrained by ──► Constraint

ProvisionAgreement ─── links ──► DataProvider + DataflowDefinition
ProvisionAgreement ─── constrained by ──► Constraint

MetadataSet ─── conforms to ──► MSD
MetadataSet ─── attaches to ──► any IdentifiableArtefact

Hierarchy ─── references codes from ──► Codelist(s)
HierarchyAssociation ─── links ──► Hierarchy + Dimension [in context of Dataflow]
```

---

## 6. Versioning (SDMX 3.0)

SDMX 3.0 adopts **semantic versioning** (semver.org) for all MaintainableArtefacts:

- Format: `MAJOR.MINOR.PATCH` (e.g. `2.1.0`)
- **MAJOR**: breaking change — existing consumers must update
- **MINOR**: backwards-compatible addition
- **PATCH**: backwards-compatible fix

All non-local references between artefacts use **URN** exclusively (agency+id+version tuple references are deprecated in 3.0).

---

## 7. Transmission Formats

The information model is format-neutral. Three active transmission formats in SDMX 3.0:

| Format | Strengths | Message Types |
|--------|-----------|--------------|
| **SDMX-ML** (XML) | Complete coverage, strict DSD validation via XSD, registry services | Structure, Structure-Specific Data, Generic Metadata, Registry |
| **SDMX-JSON** | Web dissemination, carries both codes and labels in one response | Structure, Data, Metadata |
| **SDMX-CSV** | Simplicity, Excel-compatible | Data, Metadata |

Conversion between formats is **lossless** (all are expressions of the same IM).

**Deprecated in 3.0:** SDMX-EDI; Generic/Compact/Utility/Cross-Sectional XML data messages; SOAP web services API; agency+id+version tuple referencing.

---

## 8. REST API (SDMX 3.0)

Five resources (OpenAPI spec at https://github.com/sdmx-twg/sdmx-rest):

| Resource | Purpose |
|----------|---------|
| `structure` | Retrieval and maintenance (PUT/POST/DELETE) of structural metadata |
| `data` | Retrieval of data |
| `schema` | Retrieval of XSD schemas for DSD-specific validation |
| `availability` | Information on data available for a Dataflow |
| `metadata` | Retrieval of reference metadata |

Example data query:
```
GET https://ws-entry-point/data/dataflow/ECB/EXR/1.0.0/M.USD.EUR.SP00.A
```
Pattern: `data/dataflow/{agencyId}/{resourceId}/{version}/{key}`

---

## 9. SDMX 3.0 New Features (vs 2.1)

| Area | Change |
|------|--------|
| Information Model | Multiple measures in DSD; array-valued attributes/measures; MSD reference in DSD |
| Reference metadata | Footnote-style reference metadata reported as dataset attributes |
| Microdata | Supported via multi-measure DSDs and ValueLists |
| Geospatial | GeoFeatureSetCode and GeoGridCode types; geographic codelists |
| Codelists | Codelist extension; discriminated union of codelists |
| Versioning | Semantic versioning adopted for all MaintainableArtefacts |
| REST API | Consolidated to 5 resources; HTTP PUT/POST/DELETE for maintenance; richer data query syntax |
| Referencing | URN-only for non-local references (tuple deprecated) |
| ProvisionAgreement | Data and Metadata provision agreements are now separate artefacts |
| VTL | Updated to align with IM changes and semantic versioning |
| Deprecated | EDI, SOAP API, legacy XML data message variants |

---

## 10. Content-Oriented Guidelines

These are not part of the core technical standard but extend it with shared semantics:

- **Cross-Domain Concepts**: standard concept definitions (e.g. `FREQ`, `REF_AREA`, `TIME_PERIOD`) promoted for interoperability across statistical domains. Implemented as ConceptSchemes.
- **Metadata Common Vocabulary (MCV)**: ISO-compliant definitions for statistical terms; ground truth for concept mapping.
- **Statistical Subject-Matter Domains**: standard CategoryScheme for subject-matter classification.
- **Concept Roles**: standard roles (e.g. `GEO`, `TIME`, `FREQ`) that give concepts semantic meaning for machine processing and visualisation. In SDMX 3.0 this is a normative list.

---

## 11. Glossary

| Term | Definition |
|------|-----------|
| **DSD** | Data Structure Definition — defines the structure (dimensions, measures, attributes) of a dataset |
| **MSD** | Metadata Structure Definition — defines the structure of a reference metadata set |
| **DFD / Dataflow** | DataflowDefinition — an ongoing data series linked to a DSD |
| **MFD / Metadataflow** | MetadataflowDefinition — ongoing metadata series linked to an MSD |
| **Codelist** | Enumeration of codes for representing dimension/attribute values |
| **ValueList** | Like a Codelist but items are not individually identifiable (SDMX 3.0) |
| **ConceptScheme** | Maintained list of statistical concepts |
| **CategoryScheme** | Hierarchical classification scheme for data discovery |
| **Categorisation** | Link between a Category and any identifiable SDMX object |
| **ProvisionAgreement** | Links a DataProvider to a Dataflow; describes what they supply and how |
| **Constraint** | Defines a subset of a data/metadata source |
| **Hierarchy** | Complex code hierarchy, potentially spanning multiple codelists |
| **HierarchyAssociation** | Links a Hierarchy to a Dimension, optionally in a Dataflow context |
| **StructureMap** | Mapping rules for transforming data between two DSDs |
| **RepresentationMap** | Lookup table mapping values between codelists/valuelists |
| **URN** | Uniform Resource Name — unique identifier for any SDMX artefact |
| **Key** | Combination of dimension values that uniquely identifies a series |
| **Series** | A time-ordered sequence of observations sharing the same key |
| **VTL** | Validation and Transformation Language — standard language for SDMX data validation and transformation |
| **Semver** | Semantic versioning (MAJOR.MINOR.PATCH) adopted for MaintainableArtefacts in SDMX 3.0 |
| **Maintenance Agency** | Organisation responsible for creating and maintaining an artefact |
| **MaintainableArtefact** | Top-level, agency-owned artefact type; has ID, version, and agency |

---

*This document is derived from SDMX Technical Standards Section 1 (Framework, v3.0) and Section 2 (Information Model, v3.0), published October 2021 by the SDMX Initiative. For the normative UML specification, refer to Section 2 directly.*
