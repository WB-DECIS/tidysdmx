# CLAUDE.md — tidysdmx

## What This Project Is

`tidysdmx` is a Python toolbox for working with SDMX data, built on top of `pysdmx`.
It provides:

- Functions to fetch and validate SDMX schemas via the FMR (Fusion Metadata Registry)
- Tools to build SDMX structure maps and related artefacts
- A data standardisation and mapping pipeline (including Kedro integration)
- Dataset validation against codelists and schemas
- Excel-based mapping template support

The project wraps pysdmx where possible, but also adds higher-level functionality
not present in pysdmx itself.

## Python Environment

- Python 3.11.9+
- Package manager: **Poetry** — install with `poetry install`
- Core runtime dependencies: `pysdmx`, `pandas`, `typeguard`, `openpyxl`

## Key Commands

- Run tests: `pytest`
- Lint + format: `ruff check . && ruff format .`
- Full check: `ruff check . && pytest`
- Pre-commit (all files): `pre-commit run --all-files`

## Repository Layout

```
src/tidysdmx/
├── tidysdmx.py             — Core: fetch schemas from FMR, standardise/map SDMX data
├── structures.py           — Build SDMX artefacts (StructureMap, ValueMap, Codelist, etc.)
├── mapping.py              — Apply StructureMaps to DataFrames
├── structure_map_writer.py — Collect, validate, and prepare StructureMaps for FMR upload
├── validation.py           — Validate datasets against schemas and codelists
├── utils.py                — Utilities: extract components, build mapping rules, Excel helpers
├── qa_utils.py             — QA helpers: coerce numeric, remove duplicates
├── kedro.py                — Kedro pipeline node wrappers
└── tidy_raw.py             — Raw/tidy data filtering

tests/
├── conftest.py             — Loads fixture plugins
└── fixtures/
    ├── fxtr_schemas.py     — Pickled Schema fixtures
    ├── fxtr_dummy_data.py  — Dummy DataFrame fixtures
    ├── fxtr_structures.py  — SDMX structure artefact fixtures
    └── fxtr_mapping.py     — StructureMap fixtures
```

## Key pysdmx Classes Used

- `DataStructureDefinition`, `Component`, `Components` — DSD and its components
- `Codelist`, `Code` — codelist artefacts
- `ConceptScheme`, `Concept` — concept schemes
- `Schema` — schema fetched from FMR
- `StructureMap` and related map types (via `pysdmx.model.map`) — structure map artefacts
- `Role`, `DataType` — component roles and data types
- `ItemReference` — references to artefacts
- `pysdmx.api.fmr` — FMR API client
- `pysdmx.io.format.StructureFormat` — structure serialisation formats

pysdmx source: https://github.com/bis-med-it/pysdmx

## SDMX Domain Knowledge

- This project targets SDMX 2.1 and 3.0 artefacts
- Key artefacts: DSD, Codelist, ConceptScheme, Schema, StructureMap, ProvisionAgreement
- FMR (Fusion Metadata Registry) is used as the metadata store; the `pysdmx.api.fmr`
  client handles API calls
- When unsure about SDMX semantics, consult the official SDMX information model
  before making assumptions

## Design Principles

- **Don't reimplement pysdmx**: call upstream wherever pysdmx provides the functionality
- **Pythonic API**: method names and signatures should be intuitive to Python developers
  unfamiliar with SDMX internals
- **SDMX correctness**: all artefacts must conform to the SDMX information model
- **Type safety**: use `typeguard`'s `@typechecked` decorator and full type annotations;
  prefer pysdmx types over reinventing them
- **Google docstrings**: enforced by ruff (`convention = "google"`)

## Testing

- Tests live in `tests/`, with files mirroring `src/tidysdmx/` module names
- Use fixtures in `tests/fixtures/` to construct pysdmx artefacts and dummy datasets —
  do not create inline artefacts when a fixture already exists
- Fixtures are loaded as pytest plugins via `conftest.py`
- Every new public function should have a corresponding test

## What NOT to Do

- Do not duplicate pysdmx logic — call upstream instead
- Do not invent SDMX concepts not present in the information model
- Do not expose raw pysdmx internal objects in the public API without considering
  whether wrapping is appropriate
