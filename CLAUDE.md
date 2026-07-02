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

## Before Writing Any Code
Always read the following documents first — they define the domain and the
upstream dependency your code wraps:

1. `docs/sdmx-information-model.md` — understand the SDMX artefacts involved
2. `docs/pysdmx-overview.md` — understand how pysdmx models those artefacts
3. `docs/tidysdmx-architecture.md` — understand how tidysdmx maps onto pysdmx

## Python Environment

- Python 3.11.9+
- Package manager: **Poetry** — install with `poetry install`
- Core runtime dependencies: `pysdmx`, `pandas`, `typeguard`, `openpyxl`

## Key Commands

- Run tests: `poetry run pytest`
- Run unit tests only: `poetry run pytest -m "not integration"`
- Run tests with coverage: `poetry run pytest --cov --cov-report=term-missing`
- Lint: `poetry run ruff check .`
- Format check: `poetry run ruff format --check .`
- Auto-fix lint + format: `poetry run ruff check --fix . && poetry run ruff format .`
- Pre-commit (all files): `pre-commit run --all-files`

## Claude Code Commands

- `/test` — run the test suite (accepts pytest flags, e.g. `/test -k test_name`)
- `/lint` — run ruff linting and format checks
- `/review-pr` — review current branch changes against master
- `/add-tests` — generate tests for new/changed functions

## Repository Layout

```
src/tidysdmx/
├── tidysdmx.py             — Core: fetch schemas from FMR, standardise/map SDMX data
├── structures.py           — Build SDMX artefacts (StructureMap, ValueMap, Codelist, etc.)
├── mapping.py              — Apply StructureMaps to DataFrames
├── structure_map_writer.py — Collect, validate, and prepare StructureMaps for FMR upload
├── validation.py           — Validate datasets against schemas and codelists
├── artefact_builder.py     — Build pysdmx artefacts with publish-readiness validation
├── artefact_validation.py  — Validate artefacts before publishing (rules + ValidationError)
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

.claude/
├── settings.json           — Pre-approved permissions for common commands
├── commands/               — Slash commands (/test, /lint, /review-pr, /add-tests)
└── rules/                  — Python and testing convention rules
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

## pysdmx Source Code
When you need to understand how pysdmx implements something, read the installed
source directly rather than guessing. Locate it with
`poetry run python -c "import pysdmx; print(pysdmx.__file__)"`. Key modules:
- `pysdmx/model/` — core data model classes
- `pysdmx/io/` — readers and writers

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
- Every new public function must have a corresponding test
- Mark tests: `@pytest.mark.unit` (default) or `@pytest.mark.integration` (FMR/network)
- See `.claude/rules/testing-conventions.md` for full test writing guidelines

## CI/CD

- **GitHub Actions CI** (`.github/workflows/ci.yml`): runs ruff lint + format check, then
  pytest with coverage on Python 3.11 and 3.12 — triggered on push/PR to master and dev
- **Claude PR Review** (`.github/workflows/pr-review.yml`): automated code review on PRs
  using Claude, checking SDMX correctness, type safety, test coverage, and code quality
- **Release** (`.github/workflows/release.yml`): on push to master,
  `python-semantic-release` computes the version from commit messages, tags, creates a
  GitHub Release, and publishes to PyPI via Trusted Publishing — see `RELEASING.md`.
  Never hand-edit the version in `pyproject.toml`.

## What NOT to Do

- Do not duplicate pysdmx logic — call upstream instead
- Do not invent SDMX concepts not present in the information model
- Do not expose raw pysdmx internal objects in the public API without considering
  whether wrapping is appropriate
- Do not skip writing tests for new public functions
- Do not merge code that fails ruff checks or pytest
