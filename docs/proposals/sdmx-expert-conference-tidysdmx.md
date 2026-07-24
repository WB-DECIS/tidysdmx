# SDMX Expert Conference — Session Proposal: tidysdmx

Submission answer to the form question: *"Please describe the problem, approach,
innovation, SDMX relevance, expected audience value and key takeaways"*
(500-word limit).

Suggested session title: **tidysdmx: a task-oriented pipeline layer over pysdmx
for producing SDMX data at scale**

---

**Problem.** Statistical organizations increasingly disseminate through SDMX, yet most source data does not arrive as SDMX: it comes from surveys, partner APIs, spreadsheets, and databases, each with its own vocabulary. In practice, every new source spawns a bespoke script for mapping, recoding, and validation — pipelines that are fragile, costly to maintain, and dependent on a small pool of SDMX experts. The people who understand the data — statisticians and analysts — rarely master the SDMX Information Model, so friction accumulates at every step between raw data and a published dataflow.

**Approach.** tidysdmx is an open-source (MIT, PyPI) Python toolbox developed at the World Bank to industrialize this last mile. Built strictly on top of pysdmx, it exposes task-oriented functions named after what pipeline builders actually do: fetch a schema from an FMR in one call; express source-to-target mappings in an Excel template or JSON file; apply them to pandas DataFrames as vectorized operations; validate datasets against codelists and schemas, with results returned as error tables rather than exceptions; and standardize outputs to SDMX-CSV. Excel mapping templates compile into genuine SDMX StructureMaps, so domain experts contribute mappings without writing code or XML. Kedro wrappers let the same functions run unchanged inside orchestrated production pipelines, turning one-off fixes into reusable, institutional building blocks.

**Innovation.** A deliberate two-layer architecture: pysdmx remains the faithful, standard-conformant implementation of the Information Model, while tidysdmx translates at the boundary — DataFrame to Schema, Excel workbook to StructureMap, artefact to publish-ready upload. It also supports iterative development the way modern software does: infer a schema locally from a table when no registry artefact exists yet, refine it, then publish validated artefacts to an FMR for production, guarded by explicit publish-readiness checks.

**SDMX relevance.** Everything tidysdmx emits is standard SDMX: 2.1/3.0 artefacts, StructureMaps and RepresentationMaps conforming to the Information Model, SDMX-CSV outputs, FMR-ready uploads. The package is deliberately complementary to pysdmx, also presented at this conference: it never reimplements upstream functionality, calls pysdmx wherever possible, and contributes improvements back — its artefact publish-readiness validation is being upstreamed. Together they demonstrate a healthy ecosystem pattern: a rigorous core implementation of the standard, plus an ergonomics layer that widens who can use it.

**Expected audience value.** Attendees will leave with a concrete, reusable pattern for harvesting heterogeneous non-SDMX sources into dissemination structures at scale, illustrated with production lessons from World Bank data pipelines. Teams evaluating tooling will get a clear answer to "which layer do I need?": pysdmx for SDMX-aware developers, tidysdmx for pipeline builders. Implementers of similar platforms can adopt the package directly — or replicate its design.

**Key takeaways.** (1) Scaling SDMX adoption requires a task-oriented layer above the Information Model, not only faithful implementations of it. (2) DataFrames and Excel templates are effective boundary objects between domain experts and SDMX. (3) The pysdmx–tidysdmx division of labour — rigorous core, ergonomic wrapper — is a replicable model for the SDMX ecosystem.
