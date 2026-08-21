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

Import name: `tidysdmx`. Everything public is re-exported from the top-level
package and listed in `src/tidysdmx/__init__.py`'s `__all__`.

<!-- These imports pull the conventions into context. Do not remove them: without
     them the rules files are documentation nobody reads. -->
@.claude/rules/python-conventions.md
@.claude/rules/testing-conventions.md
@.claude/rules/commit-conventions.md

## Before Writing Any Code

Always read the following documents first — they define the domain and the
upstream dependency your code wraps:

1. `docs/sdmx-information-model.md` — understand the SDMX artefacts involved
2. `docs/pysdmx-overview.md` — understand how pysdmx models those artefacts
3. `docs/tidysdmx-architecture.md` — understand how tidysdmx maps onto pysdmx

`docs/reviews/` holds architecture reviews. `docs/reviews/2026-06-architecture-review.md`
§7 is the live refactoring backlog; several `TODO` comments in this repository
cite its IDs (ARCH-nn, CONS-nn, TEST-nn, PROD-nn).

## Python Environment

- Python 3.11.9+
- Package manager: **uv** — `uv sync --all-groups` installs everything
- Dependency groups (PEP 735): `dev`, `docs`, `release`, `security`, `notebooks`
- Core runtime dependencies: `pysdmx`, `pandas`, `numpy`, `openpyxl`, `typeguard`

Always run project commands through `uv run` so they use the locked environment,
never a system Python. `poetry` is gone — do not reintroduce it.

## Key Commands

Defined once in the `Makefile` — prefer these over retyping the underlying
commands, and update the `Makefile` rather than inventing new invocations.

- `make check` — lint + typecheck + tests with the coverage gate (what CI runs)
- `make lint` / `make fmt` — ruff check + format check / auto-fix both
- `make typecheck` — mypy
- `make test` — unit tests, no coverage gate (so `-k` works)
- `make cov` — unit tests with coverage, gate enforced
- `make docs` / `make docs-preview` — build / live-preview the docs site
- `make audit` — pip-audit over the locked dependencies
- `make release-dry` — show what the next release would be, changing nothing

Single test: `uv run pytest -k test_name -v`

## Claude Code Commands

- `/test` — run the test suite (accepts pytest flags)
- `/lint` — ruff lint and format checks
- `/typecheck` — mypy, and help fixing what it finds
- `/review-pr` — review the current branch's changes against the PR base branch
- `/add-tests` — generate tests for new or changed functions
- `/commit` — stage and write a Conventional Commit
- `/docs` — build the docs and report what broke
- `/release` — walk the release runbook, dry run first

## Repository Layout

```
src/tidysdmx/
├── __init__.py             — public API re-exports, __all__, __version__
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
├── tidy_raw.py             — Raw/tidy data filtering
└── py.typed                — PEP 561 marker; this package ships its types

tests/
├── conftest.py             — shared fixtures (NOT pytest_plugins — see the rules)
├── fixtures/fxtr_*.py      — reusable test data and cassettes, imported by conftest
└── test_*.py               — one file per source module

docs/                       — SDMX domain references and architecture reviews.
                              Contributor- and agent-facing, NOT published by great-docs.
great-docs.yml              — docs site config; reference.sections lists the public API
index.qmd                   — docs landing page
user_guide/*.qmd            — narrative documentation
SKILL.md                    — API summary published for AI agents consuming this package

.github/workflows/          — ci, release, docs, security, pr-review
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

Import these by name (`from pysdmx.model.map import StructureMap`). Do **not**
write `import pysdmx as px` and then reference `px.model.map.StructureMap`: a
clean `import pysdmx` has no `.model` attribute, so that form resolves only when
some other module happens to have imported the submodule first.

## pysdmx Source Code

When you need to understand how pysdmx implements something, read the installed
source directly rather than guessing. Locate it with
`uv run python -c "import pysdmx; print(pysdmx.__file__)"`. Key modules:

- `pysdmx/model/` — core data model classes
- `pysdmx/io/` — readers and writers

## SDMX Domain Knowledge

- This project targets SDMX 2.1 and 3.0 artefacts
- Key artefacts: DSD, Codelist, ConceptScheme, Schema, StructureMap, ProvisionAgreement
- FMR (Fusion Metadata Registry) is used as the metadata store; the `pysdmx.api.fmr`
  client handles API calls
- When unsure about SDMX semantics, consult the official SDMX information model
  before making assumptions
- Two distinct validation vocabularies share the word "validate": `validation.py`
  checks *datasets* against a schema and returns an error DataFrame;
  `artefact_validation.py` checks *artefacts* for publish-readiness and raises
  `ValidationError`. Do not conflate them.

## Design Principles

- **Don't reimplement pysdmx**: call upstream wherever pysdmx provides the functionality
- **Pythonic API**: names and signatures should read naturally to someone who has
  never seen this package's internals
- **SDMX correctness**: all artefacts must conform to the SDMX information model
- **Type everything.** The package ships `py.typed`, so annotations are part of the
  public contract — a wrong annotation is a bug in downstream users' type checking.
  Static checking is not input validation: values crossing a boundary (an Excel
  workbook, a JSON mapping file) are annotated `object` and narrowed at runtime —
  see `.claude/rules/python-conventions.md`
- **Google docstrings** on every public function, enforced by ruff's `D` rules.
  Accuracy of `Args`/`Returns`/`Raises` is on you, not the linter
- **Specific exceptions.** `ValueError`/`TypeError`, never bare `Exception`
- **Return new objects** instead of mutating arguments

## Branching and Releases

Two long-lived branches:

- **`dev`** — integration. Feature branches target this, and it is where day-to-day
  work lands.
- **`main`** — releases. Only `main` cuts a release, deploys the docs site, and
  publishes to PyPI.

Releasing is a `dev` → `main` pull request. After semantic-release pushes its
version commit and tag to `main`, **merge `main` back into `dev`** or the two
drift. See `RELEASING.md`.

This is a deliberate deviation from pypackage-template, which mandates a single
`main`. The workflows carry a comment saying so; re-apply it after
`copier update`.

## Staying in Sync With the Template

This repository is generated from
[WB-DECIS/pypackage-template](https://github.com/WB-DECIS/pypackage-template) and
records its answers in `.copier-answers.yml`. Pull in template improvements with:

```bash
uvx copier update --trust
```

Review the diff — conflicts are left as `.rej` files, and the documented
deviations above will need re-applying. Keep `.copier-answers.yml` committed.

The recorded baseline is template **v0.3.0**, which added the `branching_model`
question this repository answers `main_dev`. Reconciliation to that version was
done by hand rather than by `copier update`: this repository diverges from the
rendered tree in most files, so a real update produces mostly-noise conflicts and
re-creates the deleted example module.

## CI/CD

- **`ci.yml`** — ruff lint + format, mypy, pytest on Python 3.11–3.14, then a build
  that checks metadata and verifies the wheel is importable. Runs on `main` and `dev`.
- **`release.yml`** — on push to `main`, python-semantic-release computes the version
  from commit messages, tags, and creates the GitHub Release; a separate job builds
  with `uv build` and publishes to PyPI via Trusted Publishing.
  **Never hand-edit the version in `pyproject.toml`** — it is generated. See `RELEASING.md`.
- **`docs.yml`** — builds the great-docs/Quarto site on `main` and `dev`, but deploys
  to GitHub Pages only from `main`, so the published site tracks released API.
- **`security.yml`** — pip-audit over the lockfile, plus zizmor on the workflows.
- **`pr-review.yml`** — automated Claude review on pull requests.

Actions are pinned to full commit SHAs. If you add a workflow step, pin it the
same way and add the `# vX.Y.Z` comment.

## What NOT to Do

- Do not duplicate pysdmx logic — call upstream instead
- Do not invent SDMX concepts not present in the information model
- Do not expose raw pysdmx internal objects in the public API without considering
  whether wrapping is appropriate
- Do not hand-edit the version in `pyproject.toml` or write `CHANGELOG.md` by hand —
  both are generated from commit messages
- Do not commit with a non-Conventional message; the `commit-msg` hook will reject
  it, and the release pipeline depends on it
- Do not add `# type: ignore` or `# noqa` to silence a real problem. Fix it, or
  explain the exemption in a comment
- Do not skip writing tests for new public functions
- Do not add a public name to `__all__` without also adding it to
  `great-docs.yml`'s `reference.sections` — the docs build fails on missing names
- Do not add `--cov` to pytest's `addopts`; it would make focused `-k` runs trip the
  coverage gate. Use `make cov`
- Do not reintroduce Poetry, Sphinx, or Read the Docs — all three were removed
